package v2.connectors.vk;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import v2.connectors.base.SendResult;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static v2.connectors.vk.VkApiClient.*;

/**
 * 📤 Отправка сообщений и файлов в VK.
 *
 * Что умеет:
 *   - sendMessage(peer, text, attachments, replyTo) → message_id   (контракт BaseConnector)
 *   - те же перегрузки, но с SendResult (не бросают исключений) — их зовёт VkApiController
 *   - sendFile / sendFiles — загрузка фото и документов с диска
 *
 * Про вложения: attachments — СТРОКА в формате VK ("photo123_456,doc789_123"),
 * как того требует SendMessageRequest.attachments. Массив склеивается через запятую.
 *
 * Не является Spring-бином: создаётся внутри VkConnector.
 */
public class VkSender {

    private static final Logger log = LoggerFactory.getLogger(VkSender.class);

    /** Реальный лимит VK ~9000 символов. */
    private static final int VK_TEXT_LIMIT = 9000;
    /** Лимит вложений в одном сообщении. */
    private static final int VK_MAX_ATTACHMENTS = 10;
    /** Лимит подписи к документу. */
    private static final int VK_CAPTION_LIMIT = 255;

    private final VkApiClient api;

    public VkSender(VkApiClient api) {
        this.api = api;
    }

    /** Ошибка прав токена (scope), а не технический сбой — чтобы sendFile мог откатиться на документ. */
    public static class VkScopeException extends RuntimeException {
        public VkScopeException(String message) {
            super(message);
        }
    }

    // =========================================================================================
    // 📤 ТЕКСТ
    // =========================================================================================

    /**
     * Контракт BaseConnector — его вызывает UnifiedSendService (POST /api/v2/send)
     * и VkApiController (POST /api/vk/send/message).
     *
     * @param peer        peer_id строкой: uid пользователя, -id сообщества или 2000000000+chat_id
     * @param text        текст (может быть пустым, если есть вложения)
     * @param attachments строка вида "photo123_456,doc789_123" (формат VK) или null
     * @param replyTo     id сообщения VK для ответа, null/0 = без ответа
     * @return message_id отправленного сообщения
     * @throws IllegalArgumentException на невалидные входные данные
     * @throws RuntimeException         если VK вернул ошибку
     */
    public long sendMessage(String peer, String text, String attachments, Long replyTo) {
        if (peer == null || peer.isBlank()) {
            throw new IllegalArgumentException("Не указан peer_id");
        }
        boolean noText = (text == null || text.trim().isEmpty());
        boolean noAtt = (attachments == null || attachments.isBlank());
        if (noText && noAtt) {
            throw new IllegalArgumentException("Пустое сообщение: нет ни текста, ни вложений");
        }
        long peerId;
        try {
            peerId = Long.parseLong(peer.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid peer ID: " + peer, e);
        }

        Map<String, String> params = new LinkedHashMap<>();
        params.put("peer_id", String.valueOf(toPeerId(peerId)));
        if (!noText) {
            params.put("message", truncate(text, VK_TEXT_LIMIT));
        }
        if (!noAtt) {
            params.put("attachment", attachments.trim());
        }
        if (replyTo != null && replyTo > 0) {
            params.put("reply_to", String.valueOf(replyTo));
        }
        params.put("random_id", String.valueOf(ThreadLocalRandom.current().nextLong(1, Integer.MAX_VALUE)));

        try {
            JsonNode resp = api.call("messages.send", params);
            if (hasError(resp)) {
                throw new RuntimeException("VK API error: " + vkError(resp));
            }
            long messageId = resp.path("response").asLong(0);
            if (messageId <= 0) {
                throw new RuntimeException("VK вернул некорректный message_id: " + resp.path("response"));
            }
            log.info("[VK] 📤 отправлено peer_id={}, message_id={}", peerId, messageId);
            return messageId;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("VK send failed: " + describe(e), e);
        }
    }

    /** Вариант с SendResult (не бросает исключений) — его зовёт VkApiController. */
    public SendResult send(long peerId, String text, String attachments, long replyTo) {
        long peer = toPeerId(peerId);
        try {
            long messageId = sendMessage(String.valueOf(peerId), text, attachments,
                    replyTo > 0 ? replyTo : null);
            return SendResult.ok("vk", peer, messageId, text);
        } catch (Exception e) {
            return SendResult.fail("vk", peer, describe(e));
        }
    }

    /** Отправка текста + уже готовых вложений (photo…/doc…). */
    public SendResult send(long peerId, String text, List<String> attachments, long replyTo) {
        return send(peerId, text,
                (attachments == null || attachments.isEmpty()) ? null : String.join(",", attachments),
                replyTo);
    }

    // =========================================================================================
    // 📎 ФАЙЛЫ
    // =========================================================================================

    /**
     * Файл с диска: картинка — как фото (при нехватке scope `photos` — документом),
     * остальное — документом.
     *
     * @param caption подпись к вложению (VK приложит её текстом к сообщению)
     */
    public SendResult sendFile(long peerId, String caption, String filePath, String mimeType, long replyTo) {
        long peer = toPeerId(peerId);
        if (filePath == null || filePath.isBlank()) {
            return SendResult.fail("vk", peer, "Не указан путь к файлу");
        }
        Path file = Path.of(filePath);
        if (!Files.exists(file)) {
            return SendResult.fail("vk", peer, "Файл не найден: " + filePath);
        }
        try {
            List<String> atts = new ArrayList<>();
            if (isImage(file.getFileName().toString(), mimeType)) {
                try {
                    atts.add(uploadPhoto(peer, file));
                } catch (VkScopeException scope) {
                    // нет scope `photos` (error 15/7/20) — шлём документом, чтобы отправка не падала
                    log.warn("[VK] {} → отправляю картинку ДОКУМЕНТОМ. Лечение: верификация аккаунта VK "
                            + "+ токен со scope photos", scope.getMessage());
                    atts.add(uploadDoc(peer, file, caption));
                }
            } else {
                atts.add(uploadDoc(peer, file, caption));
            }
            return send(peer, caption, atts, replyTo);
        } catch (Exception e) {
            return SendResult.fail("vk", peer, describe(e));
        }
    }

    /** Несколько файлов одним сообщением (лимит VK — 10 вложений). */
    public SendResult sendFiles(long peerId, String caption, List<Path> files, long replyTo) {
        long peer = toPeerId(peerId);
        try {
            List<String> atts = new ArrayList<>();
            for (Path f : files) {
                if (atts.size() >= VK_MAX_ATTACHMENTS) {
                    break;
                }
                if (isImage(f.getFileName().toString(), null)) {
                    try {
                        atts.add(uploadPhoto(peer, f));
                    } catch (VkScopeException scope) {
                        atts.add(uploadDoc(peer, f, caption));
                    }
                } else {
                    atts.add(uploadDoc(peer, f, caption));
                }
            }
            if (atts.isEmpty()) {
                return SendResult.fail("vk", peer, "Не удалось загрузить ни одного файла");
            }
            return send(peer, caption, atts, replyTo);
        } catch (Exception e) {
            return SendResult.fail("vk", peer, describe(e));
        }
    }

    // =========================================================================================
    // 🖼 ЗАГРУЗКА ФОТО
    // =========================================================================================

    /** @return строка вложения вида "photo{owner_id}_{id}" */
    public String uploadPhoto(long peer, Path file) throws Exception {
        JsonNode server = api.call("photos.getMessagesUploadServer", params("peer_id", String.valueOf(peer)));
        if (hasError(server)) {
            if (isScopeError(server)) {
                throw new VkScopeException(vkError(server));
            }
            throw new IllegalStateException("photos.getMessagesUploadServer: " + vkError(server));
        }
        String uploadUrl = server.path("response").path("upload_url").asText("");
        if (uploadUrl.isBlank()) {
            throw new IllegalStateException("VK вернул ответ без upload_url: " + server);
        }

        JsonNode uploaded = api.postMultipart(uploadUrl, "photo", file);
        if (hasError(uploaded)) {
            throw new IllegalStateException("Загрузка файла на сервер VK: " + vkError(uploaded));
        }
        if (uploaded.path("photo").isMissingNode() || uploaded.path("hash").asText("").isBlank()) {
            throw new IllegalStateException("Сервер загрузки вернул не photo/server/hash, а: " + uploaded);
        }

        // VK отдаёт "photo" то строкой, то массивом — приводим к строке,
        // иначе saveMessagesPhoto ответит ошибкой 100/129
        Map<String, String> saveParams = new LinkedHashMap<>();
        saveParams.put("photo", asText(uploaded.get("photo")));
        copyIfPresent(uploaded, saveParams, "server", "hash");

        JsonNode saved = api.call("photos.saveMessagesPhoto", saveParams);
        if (hasError(saved)) {
            if (isScopeError(saved)) {
                throw new VkScopeException(vkError(saved));
            }
            throw new IllegalStateException("photos.saveMessagesPhoto: " + vkError(saved));
        }
        JsonNode first = saved.path("response").path(0);
        if (first.isMissingNode() || first.isNull()) {
            throw new IllegalStateException("photos.saveMessagesPhoto вернул пустой ответ: " + saved);
        }
        return "photo" + first.path("owner_id").asLong() + "_" + first.path("id").asLong();
    }

    // =========================================================================================
    // 📎 ЗАГРУЗКА ДОКУМЕНТА
    // =========================================================================================

    /** @return строка вложения вида "doc{owner_id}_{id}" */
    public String uploadDoc(long peer, Path file, String caption) throws Exception {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("peer_id", String.valueOf(peer));
        p.put("type", "doc");
        if (caption != null && !caption.isBlank()) {
            p.put("caption", truncate(caption, VK_CAPTION_LIMIT));
        }

        JsonNode server = api.call("docs.getMessagesUploadServer", p);
        if (hasError(server)) {
            if (isScopeError(server)) {
                throw new VkScopeException(vkError(server));
            }
            throw new IllegalStateException("docs.getMessagesUploadServer: " + vkError(server));
        }
        String uploadUrl = server.path("response").path("upload_url").asText("");
        if (uploadUrl.isBlank()) {
            throw new IllegalStateException("VK вернул ответ без upload_url: " + server);
        }

        JsonNode uploaded = api.postMultipart(uploadUrl, "file", file);
        String rawFile = uploaded.path("file").asText("");
        if (rawFile.isBlank()) {
            throw new IllegalStateException("Сервер загрузки документа вернул не поле file, а: " + uploaded);
        }

        JsonNode saved = api.call("docs.save", Map.of("file", rawFile));
        if (hasError(saved)) {
            if (isScopeError(saved)) {
                throw new VkScopeException(vkError(saved));
            }
            throw new IllegalStateException("docs.save: " + vkError(saved));
        }
        JsonNode doc = saved.path("response").path("doc");
        if (doc.isMissingNode() || doc.path("id").asLong(0) == 0) {
            throw new IllegalStateException("docs.save вернул пустой ответ: " + saved);
        }
        return "doc" + doc.path("owner_id").asLong() + "_" + doc.path("id").asLong();
    }

    // =========================================================================================
    // 🧰
    // =========================================================================================

    /**
     * id из БД → peer_id. Сейчас как есть: положительные id и id бесед (>= 2000000000) идут
     * без изменений, отрицательные — сообщества.
     * Если chatId бесед хранится «коротким» (1, 2, 3…), включите конвертацию:
     *     if (rawId > 0 && rawId < 2_000_000_000L) return 2_000_000_000L + rawId;
     */
    public static long toPeerId(long rawId) {
        return rawId;
    }
}