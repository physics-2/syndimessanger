package ru.VK;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ru.Config;
import ru.send.SendResult;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 📤 БЛОК ОТПРАВКИ ДЛЯ VK-КОННЕКТОРА.
 *
 * ⚠️ Если у вас VkConnector уже умеет дёргать VK API своим методом — замените
 *    приватный {@link #vkApi(String, Map)} на свой и удалите HttpClient/ObjectMapper.
 *    Публичные методы (sendMessage / sendFile) менять не нужно: роутер
 *    ru.send.MessageRouter вызывает именно их.
 *
 * Требования к токену: scope `messages` (+ `photos`, `docs` для вложений).
 * Для отправки ОТ ИМЕНИ СООБЩЕСТВА нужен token сообщества и peer_id = 2000000000 + chat_id.
 */
public class VkSendSupport {

    private static final String API_URL = "https://api.vk.com/method/";
    private static final String API_VERSION = "5.199";   // 👈 поменяйте на свою, если Config отдаёт другую
    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final int VK_TEXT_LIMIT = 9000;         // реальный лимит VK ~9000 символов

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(20))
            .build();
    private final ObjectMapper mapper = new ObjectMapper();



    // =========================================================================================
    // 📤 ПУБЛИЧНОЕ API
    // =========================================================================================

    /**
     * Отправка текста.
     *
     * @param peerId получатель: uid пользователя, -id группы или 2000000000+chat_id беседы.
     *               Если у вас в БД чаты лежат как положительные chat_id бесед — передайте их как есть,
     *               а отрицательные id сообществ конвертируются автоматически ({@link #toPeerId(long)}).
     * @param replyTo id сообщения для ответа, 0 = без ответа
     */
    public SendResult sendMessage(long peerId, String text, long replyTo) {
        long peer = toPeerId(peerId);
        if (text == null || text.isBlank()) return SendResult.fail("vk",peer, "Пустой текст сообщения");

        Map<String, String> params = new LinkedHashMap<>();
        params.put("peer_id", String.valueOf(peer));
        params.put("message", truncate(text, VK_TEXT_LIMIT));
        params.put("random_id", String.valueOf(ThreadLocalRandom.current().nextLong(1, Integer.MAX_VALUE)));
        if (replyTo > 0) params.put("reply_to", String.valueOf(replyTo));

        try {
            JsonNode resp = vkApi("messages.send", params);
            JsonNode err = resp.get("error");
            if (err != null) {
                return SendResult.fail("vk",peer, "VK error " + err.path("error_code").asInt()
                        + ": " + err.path("error_msg").asText());
            }
            long messageId = resp.path("response").asLong(0);
            System.out.println("📤 [VK] Отправлено peer_id=" + peer + ", message_id=" + messageId);
            return SendResult.ok("vk",peer, messageId, text);
        } catch (Exception e) {
            return SendResult.fail("vk",peer, describe(e));
        }
    }

    /**
     * Отправка текста + вложения (ссылки вида photo123_456, doc123_456).
     */
    public SendResult sendMessage(long peerId, String text, List<String> attachments, long replyTo) {
        long peer = toPeerId(peerId);
        boolean emptyText = (text == null || text.isBlank());
        if (emptyText && (attachments == null || attachments.isEmpty())) {
            return SendResult.fail("vk",peer, "Пустое сообщение");
        }

        Map<String, String> params = new LinkedHashMap<>();
        params.put("peer_id", String.valueOf(peer));
        if (!emptyText) params.put("message", truncate(text, VK_TEXT_LIMIT));
        if (attachments != null && !attachments.isEmpty()) params.put("attachment", String.join(",", attachments));
        params.put("random_id", String.valueOf(ThreadLocalRandom.current().nextLong(1, Integer.MAX_VALUE)));
        if (replyTo > 0) params.put("reply_to", String.valueOf(replyTo));

        try {
            JsonNode resp = vkApi("messages.send", params);
            JsonNode err = resp.get("error");
            if (err != null) {
                return SendResult.fail("vk",peer, "VK error " + err.path("error_code").asInt()
                        + ": " + err.path("error_msg").asText());
            }
            return SendResult.ok("vk",peer, resp.path("response").asLong(0), text);
        } catch (Exception e) {
            return SendResult.fail("vk",peer, describe(e));
        }
    }

    /**
     * Отправка файла с диска: картинка уйдёт как фото, остальное — как документ.
     *
     * @param caption подпись к вложению (VK её приложит текстом к сообщению)
     */
    public SendResult sendFile(long peerId, String caption, String filePath, String mimeType, long replyTo) {
        long peer = toPeerId(peerId);
        Path file = Path.of(filePath);
        if (!Files.exists(file)) return SendResult.fail("vk",peer, "Файл не найден: " + filePath);

        try {
            List<String> attachments = new ArrayList<>();
            try {
                attachments.add(uploadPhoto(peer, file));
            } catch (RuntimeException scope) {
                System.out.println("⚠️ [VK] " + scope.getMessage()
                        + "  -> отправляю картинку как ДОКУМЕНТ (в чате будет файлом, не фото). "
                        + "Чтобы слать именно фото: токен со scope `photos` + верификация аккаунта VK.");
                attachments.add(uploadDoc(peer, file, caption));
            }
            return sendMessage(peer, caption, attachments, replyTo);
        } catch (Exception e) {
            return SendResult.fail("vk",peer, describe(e));
        }
    }

    /** Несколько файлов одним сообщением (до 10 вложений у VK). */
    public SendResult sendFiles(long peerId, String caption, List<Path> files, long replyTo) {
        long peer = toPeerId(peerId);
        try {
            List<String> attachments = new ArrayList<>();
            for (Path f : files) {
                if (attachments.size() >= 10) break; // лимит VK
                attachments.add(isImage(f.getFileName().toString(), null)
                        ? uploadPhoto(peer, f)
                        : uploadDoc(peer, f, caption));
            }
            if (attachments.isEmpty()) return SendResult.fail("vk",peer, "Не удалось загрузить ни одного файла");
            return sendMessage(peer, caption, attachments, replyTo);
        } catch (Exception e) {
            return SendResult.fail("vk",peer, describe(e));
        }
    }

    // =========================================================================================
    // 🖼 ЗАГРУЗКА ФОТО
    // =========================================================================================

    private String uploadPhoto(long peer, Path file) throws Exception {
        JsonNode server = vkApi("photos.getMessagesUploadServer", Map.of("peer_id", String.valueOf(peer)));
        String uploadUrl = server.path("response").path("upload_url").asText();
        if (uploadUrl.isBlank()) throw new IllegalStateException("VK не отдал upload_url для фото");

        JsonNode uploaded = postMultipart(uploadUrl, "photo", file);
        if (uploaded.get("error") != null) {
            throw new IllegalStateException("Загрузка фото: " + uploaded.path("error").toString());
        }

        Map<String, String> saveParams = new LinkedHashMap<>();
        copyIfPresent(uploaded, saveParams, "photo", "server", "hash");
        JsonNode saved = vkApi("photos.saveMessagesPhoto", saveParams);
        JsonNode first = saved.path("response").get(0);
        if (first == null) throw new IllegalStateException("photos.saveMessagesPhoto вернул пустой ответ: " + saved);
        return "photo" + first.path("owner_id").asLong() + "_" + first.path("id").asLong();
    }

    // =========================================================================================
    // 📎 ЗАГРУЗКА ДОКУМЕНТА
    // =========================================================================================

    private String uploadDoc(long peer, Path file, String caption) throws Exception {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("peer_id", String.valueOf(peer));
        params.put("type", "doc");
        if (caption != null && !caption.isBlank()) params.put("caption", truncate(caption, 255));

        JsonNode server = vkApi("docs.getMessagesUploadServer", params);
        String uploadUrl = server.path("response").path("upload_url").asText();
        if (uploadUrl.isBlank()) throw new IllegalStateException("VK не отдал upload_url для документа");

        JsonNode uploaded = postMultipart(uploadUrl, "file", file);
        String rawFile = uploaded.path("file").asText("");
        if (rawFile.isBlank()) throw new IllegalStateException("Загрузка документа: " + uploaded);

        JsonNode saved = vkApi("docs.save", Map.of("file", rawFile));
        JsonNode doc = saved.path("response").path("doc");
        if (doc.isMissingNode() || doc.path("id").asLong(0) == 0) {
            throw new IllegalStateException("docs.save вернул пустой ответ: " + saved);
        }
        return "doc" + doc.path("owner_id").asLong() + "_" + doc.path("id").asLong();
    }

    // =========================================================================================
    // 🔧 НИЗКОУРОВНЕВЫЙ HTTP
    // =========================================================================================

    /** GET-вызов метода VK API (параметры уходят в query string). */
    private JsonNode vkApi(String method, Map<String, String> params) throws Exception {
        StringBuilder q = new StringBuilder();
        params.forEach((k, v) -> q.append(q.length() > 0 ? "&" : "")
                .append(enc(k)).append('=').append(enc(v)));
        q.append("&access_token=").append(enc(token()));
        q.append("&v=").append(API_VERSION);

        HttpRequest req = HttpRequest.newBuilder(URI.create(API_URL + method + "?" + q))
                .timeout(TIMEOUT)
                .GET()
                .build();

        String body = http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)).body();
        return mapper.readTree(body);
    }

    /** Классическая multipart-загрузка файла на сервер VK. */
    private JsonNode postMultipart(String url, String fieldName, Path file) throws Exception {
        String boundary = "----SyndiDash" + System.nanoTime();
        byte[] body = buildMultipart(boundary, fieldName, file);

        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofMinutes(5))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        String resp = http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)).body();
        return mapper.readTree(resp);
    }

    private byte[] buildMultipart(String boundary, String fieldName, Path file) throws Exception {
        String fileName = file.getFileName().toString();
        String head = "--" + boundary + "\r\n"
                + "Content-Disposition: form-data; name=\"" + fieldName + "\"; filename=\"" + fileName + "\"\r\n"
                + "Content-Type: application/octet-stream\r\n\r\n";
        String tail = "\r\n--" + boundary + "--\r\n";

        byte[] content = Files.readAllBytes(file);
        byte[] headBytes = head.getBytes(StandardCharsets.UTF_8);
        byte[] tailBytes = tail.getBytes(StandardCharsets.UTF_8);

        byte[] out = new byte[headBytes.length + content.length + tailBytes.length];
        System.arraycopy(headBytes, 0, out, 0, headBytes.length);
        System.arraycopy(content, 0, out, headBytes.length, content.length);
        System.arraycopy(tailBytes, 0, out, headBytes.length + content.length, tailBytes.length);
        return out;
    }

    // =========================================================================================
    // 🧰 УТИЛИТЫ
    // =========================================================================================

    /**
     * Приводит id из вашей БД к peer_id:
     * отрицательные id сообществ (-123456) VK ждёт как 2000000000 - (-123456) только для бесед;
     * для сообщений СООБЩЕСТВУ peer_id = само отрицательное значение.
     * Здесь мы ничего не ломаем: положительные id и id бесед (>= 2000000000) идут как есть,
     * отрицательные — тоже как есть (это паблик/группа).
     */
    public static long toPeerId(long rawId) {
        return rawId;
    }

    private static boolean isImage(String fileName, String mimeType) {
        if (mimeType != null && mimeType.toLowerCase().startsWith("image/")) return true;
        String n = fileName == null ? "" : fileName.toLowerCase();
        return n.endsWith(".jpg") || n.endsWith(".jpeg") || n.endsWith(".png")
                || n.endsWith(".gif") || n.endsWith(".webp") || n.endsWith(".bmp");
    }

    private static void copyIfPresent(JsonNode src, Map<String, String> dst, String... keys) {
        for (String k : keys) {
            JsonNode v = src.get(k);
            if (v != null && !v.isNull()) dst.put(k, v.asText());
        }
    }

    private static String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max - 1) + "…";
    }

    private static String enc(String s) {
        return URLEncoder.encode(s == null ? "" : s, StandardCharsets.UTF_8);
    }

    private static String token() {
        // 👇 подставьте свой геттер токена VK из Config
        return Config.getVkToken();
    }

    private static String describe(Throwable t) {
        Throwable c = (t != null && t.getCause() != null) ? t.getCause() : t;
        if (c == null) return "неизвестная ошибка";
        String m = c.getMessage();
        return (m != null && !m.isBlank()) ? m : c.getClass().getSimpleName();
    }
}
