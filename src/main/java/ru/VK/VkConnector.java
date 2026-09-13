package ru.VK;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
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
 * 📤 БЛОК ОТПРАВКИ СООБЩЕНИЙ VK — целиком внутри коннектора (по образцу TgConnector).
 *
 * ⚠️⚠️ НЕ ЗАМЕНЯЙТЕ этим файлом свой VkConnector!
 *    Здесь НЕТ вашего сканирования, LongPoll и работы с БД.
 *    Скопируйте в СВОЙ класс только секции, перечисленные ниже.
 *
 * ═══════════════════════════════════════════════════════════════════════════════════════
 *  КАК ВСТРОИТЬ В ВАШ VkConnector
 * ═══════════════════════════════════════════════════════════════════════════════════════
 *  Скопируйте в свой класс:
 *    1) секцию «📤 ОТПРАВКА СООБЩЕНИЙ» (публичные методы; SendResult — общий, из ru.send);
 *    2) секцию «🖼 ЗАГРУЗКА ФОТО / 📎 ДОКУМЕНТА»;
 *    3) секцию «🔧 HTTP К VK API» — ЛИБО удалите её и замените вызовы vkApi(...)
 *       на свой существующий метод (у вас в коннекторе он уже есть для сканирования);
 *    4) секцию «🔍 ДИАГНОСТИКА ЗАГРУЗКИ» (необязательно, но очень помогает);
 *    5) импорты сверху файла.
 *
 *  Если у вас свой HTTP-клиент/парсер — замените только приватный vkApi(String, Map),
 *  публичные методы трогать не нужно: их вызывает ru.send.MessageRouter.
 * ═══════════════════════════════════════════════════════════════════════════════════════
 *
 * Требования к токену (vk.token2 в application.properties):
 *   messages — текст;  docs — документы;  photos — встроенные фото.
 *   Если scope `photos` нет (error 15 / subcode 1133), картинка автоматически уходит
 *   ДОКУМЕНТОМ — отправка не падает, просто в чате это файл, а не встроенное фото.
 */
@Component
public class VkConnector {

    private static final String API_URL = "https://api.vk.com/method/";
    private static final String API_VERSION = "5.199";   // 👈 поменяйте на свою, если Config отдаёт другую
    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final int VK_TEXT_LIMIT = 9000;       // реальный лимит VK ~9000 символов
    private static final int VK_MAX_ATTACHMENTS = 10;    // лимит вложений в одном сообщении

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(20))
            .build();
    private final ObjectMapper mapper = new ObjectMapper();

    public VkConnector() {
        // если у вашего коннектора есть конструктор с DatabaseManager/Config — оставьте свой,
        // этот блок отправки от него не зависит
    }

    // =========================================================================================
    // 📤 ОТПРАВКА СООБЩЕНИЙ
    // =========================================================================================

    // SendResult здесь НЕ объявляем: используется единый ru.send.SendResult
    // (иначе в проекте два одноимённых класса и путаница с конвертерами).

    /** Отдельный тип ошибки прав, чтобы отличать «нет scope» от технических сбоев загрузки. */
    public static class VkScopeException extends RuntimeException {
        public VkScopeException(String message) { super(message); }
    }

    /**
     * Отправка текста.
     *
     * @param peerId  получатель: uid пользователя, -id сообщества или 2000000000+chat_id беседы
     * @param replyTo id сообщения для ответа, 0 = без ответа
     */
    public SendResult sendMessage(long peerId, String text, long replyTo) {
        long peer = toPeerId(peerId);
        if (text == null || text.isBlank()) return SendResult.fail("vk", peer, "Пустой текст сообщения");

        Map<String, String> params = new LinkedHashMap<>();
        params.put("peer_id", String.valueOf(peer));
        params.put("message", truncate(text, VK_TEXT_LIMIT));
        params.put("random_id", String.valueOf(ThreadLocalRandom.current().nextLong(1, Integer.MAX_VALUE)));
        if (replyTo > 0) params.put("reply_to", String.valueOf(replyTo));

        try {
            JsonNode resp = vkApi("messages.send", params);
            if (resp.get("error") != null) return SendResult.fail("vk", peer, vkError(resp));
            long messageId = resp.path("response").asLong(0);
            System.out.println("📤 [VK] Отправлено peer_id=" + peer + ", message_id=" + messageId);
            return SendResult.ok("vk", peer, messageId, text);
        } catch (Exception e) {
            return SendResult.fail("vk", peer, describe(e));
        }
    }

    /** Отправка текста + готовых вложений (ссылки вида photo123_456, doc123_456). */
    public SendResult sendMessage(long peerId, String text, List<String> attachments, long replyTo) {
        long peer = toPeerId(peerId);
        boolean emptyText = (text == null || text.isBlank());
        if (emptyText && (attachments == null || attachments.isEmpty())) {
            return SendResult.fail("vk", peer, "Пустое сообщение");
        }

        Map<String, String> params = new LinkedHashMap<>();
        params.put("peer_id", String.valueOf(peer));
        if (!emptyText) params.put("message", truncate(text, VK_TEXT_LIMIT));
        if (attachments != null && !attachments.isEmpty()) params.put("attachment", String.join(",", attachments));
        params.put("random_id", String.valueOf(ThreadLocalRandom.current().nextLong(1, Integer.MAX_VALUE)));
        if (replyTo > 0) params.put("reply_to", String.valueOf(replyTo));

        try {
            JsonNode resp = vkApi("messages.send", params);
            if (resp.get("error") != null) return SendResult.fail("vk", peer, vkError(resp));
            return SendResult.ok("vk", peer, resp.path("response").asLong(0), text);
        } catch (Exception e) {
            return SendResult.fail("vk", peer, describe(e));
        }
    }

    /**
     * Отправка файла с диска: картинка — как фото (при нехватке scope `photos` — документом),
     * всё остальное — как документ.
     *
     * @param caption подпись к вложению (VK приложит её текстом к сообщению)
     */
    public SendResult sendFile(long peerId, String caption, String filePath, String mimeType, long replyTo) {
        long peer = toPeerId(peerId);
        Path file = Path.of(filePath);
        if (!Files.exists(file)) return SendResult.fail("vk", peer, "Файл не найден: " + filePath);

        try {
            List<String> attachments = new ArrayList<>();
            if (isImage(file.getFileName().toString(), mimeType)) {
                try {
                    attachments.add(uploadPhoto(peer, file));
                } catch (VkScopeException scope) {
                    // У токена нет scope `photos` (error 15 / subcode 1133), но `docs` обычно есть —
                    // картинка уходит документом, чтобы отправка не падала.
                    System.out.println("⚠️ [VK] " + scope.getMessage()
                            + "  -> отправляю картинку как ДОКУМЕНТ (в чате будет файлом, не фото). "
                            + "Чтобы слать именно фото: верификация аккаунта VK + токен со scope `photos`.");
                    attachments.add(uploadDoc(peer, file, caption));
                }
            } else {
                attachments.add(uploadDoc(peer, file, caption));
            }
            return sendMessage(peer, caption, attachments, replyTo);
        } catch (Exception e) {
            return SendResult.fail("vk", peer, describe(e));
        }
    }

    /** Несколько файлов одним сообщением (до 10 вложений — лимит VK). */
    public SendResult sendFiles(long peerId, String caption, List<Path> files, long replyTo) {
        long peer = toPeerId(peerId);
        try {
            List<String> attachments = new ArrayList<>();
            for (Path f : files) {
                if (attachments.size() >= VK_MAX_ATTACHMENTS) break;
                if (isImage(f.getFileName().toString(), null)) {
                    try {
                        attachments.add(uploadPhoto(peer, f));
                    } catch (VkScopeException scope) {
                        attachments.add(uploadDoc(peer, f, caption));   // нет scope photos -> документом
                    }
                } else {
                    attachments.add(uploadDoc(peer, f, caption));
                }
            }
            if (attachments.isEmpty()) return SendResult.fail("vk", peer, "Не удалось загрузить ни одного файла");
            return sendMessage(peer, caption, attachments, replyTo);
        } catch (Exception e) {
            return SendResult.fail("vk", peer, describe(e));
        }
    }

    // =========================================================================================
    // 🖼 ЗАГРУЗКА ФОТО
    // =========================================================================================

    private String uploadPhoto(long peer, Path file) throws Exception {
        // 1. просим сервер загрузки
        JsonNode server = vkApi("photos.getMessagesUploadServer", Map.of("peer_id", String.valueOf(peer)));
        if (server.get("error") != null) {
            if (isScopeError(server)) throw new VkScopeException(vkError(server));
            throw new IllegalStateException("photos.getMessagesUploadServer: " + vkError(server));
        }
        String uploadUrl = server.path("response").path("upload_url").asText("");
        if (uploadUrl.isBlank()) {
            throw new IllegalStateException("VK вернул ответ без upload_url: " + server
                    + "  (диагностика: debugUpload(" + peer + "))");
        }

        // 2. грузим файл на сервер VK
        JsonNode uploaded = postMultipart(uploadUrl, "photo", file);
        if (uploaded.get("error") != null) {
            throw new IllegalStateException("Загрузка файла на сервер VK: " + vkError(uploaded));
        }
        if (uploaded.path("photo").isMissingNode() || uploaded.path("hash").asText("").isBlank()) {
            throw new IllegalStateException("Сервер загрузки вернул не photo/server/hash, а: " + uploaded);
        }

        // 3. сохраняем
        // ⚠️ Сервер загрузки VK отдаёт "photo" то строкой, то JSON-массивом —
        //    приводим к строке, иначе saveMessagesPhoto получит мусор и ответит ошибкой 100/129.
        Map<String, String> saveParams = new LinkedHashMap<>();
        saveParams.put("photo", asText(uploaded.get("photo")));
        copyIfPresent(uploaded, saveParams, "server", "hash");

        JsonNode saved = vkApi("photos.saveMessagesPhoto", saveParams);
        if (saved.get("error") != null) {
            if (isScopeError(saved)) throw new VkScopeException(vkError(saved));
            throw new IllegalStateException("photos.saveMessagesPhoto: " + vkError(saved));
        }
        JsonNode first = saved.path("response").get(0);
        if (first == null) {
            throw new IllegalStateException("photos.saveMessagesPhoto вернул пустой ответ: " + saved);
        }
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
        if (server.get("error") != null) {
            if (isScopeError(server)) throw new VkScopeException(vkError(server));
            throw new IllegalStateException("docs.getMessagesUploadServer: " + vkError(server));
        }
        String uploadUrl = server.path("response").path("upload_url").asText("");
        if (uploadUrl.isBlank()) {
            throw new IllegalStateException("VK вернул ответ без upload_url: " + server);
        }

        JsonNode uploaded = postMultipart(uploadUrl, "file", file);
        String rawFile = uploaded.path("file").asText("");
        if (rawFile.isBlank()) {
            throw new IllegalStateException("Сервер загрузки документа вернул не поле file, а: " + uploaded);
        }

        JsonNode saved = vkApi("docs.save", Map.of("file", rawFile));
        if (saved.get("error") != null) {
            if (isScopeError(saved)) throw new VkScopeException(vkError(saved));
            throw new IllegalStateException("docs.save: " + vkError(saved));
        }
        JsonNode doc = saved.path("response").path("doc");
        if (doc.isMissingNode() || doc.path("id").asLong(0) == 0) {
            throw new IllegalStateException("docs.save вернул пустой ответ: " + saved);
        }
        return "doc" + doc.path("owner_id").asLong() + "_" + doc.path("id").asLong();
    }

    // =========================================================================================
    // 🔧 HTTP К VK API
    //    (если у вас в коннекторе уже есть свой вызов VK API — замените vkApi() на него,
    //     остальные методы трогать не нужно)
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

        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (resp.statusCode() != 200) {
            throw new IllegalStateException("VK ответил HTTP " + resp.statusCode() + ": " + resp.body());
        }
        String body = resp.body();
        if (body == null || body.isBlank()) {
            throw new IllegalStateException("VK вернул пустой ответ для " + method);
        }
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
    // 🔍 ДИАГНОСТИКА ЗАГРУЗКИ
    //    GET /api/vk/debug-upload?peerId=... — почему VK не отдаёт upload_url.
    // =========================================================================================

    /**
     * Полный отчёт: чей токен, пользовательский он или ключ сообщества,
     * сырые ответы VK на photos/docs.getMessagesUploadServer и подсказка по коду ошибки.
     */
    public Map<String, Object> debugUpload(long peerId) {
        Map<String, Object> report = new LinkedHashMap<>();

        // 1. токен
        JsonNode me = callQuietly("users.get", Map.of());
        if (me.has("error")) {
            report.put("token", "❌ токен не работает: " + vkError(me));
            report.put("hint", hintFor(me.path("error").path("error_code").asInt()));
            return report;
        }
        JsonNode user0 = me.path("response").path(0);
        report.put("token", "✅ рабочий, владелец: id" + user0.path("id").asLong()
                + " " + user0.path("first_name").asText("") + " " + user0.path("last_name").asText(""));

        // ключ сообщества отвечает на groups.getById своим id, пользовательский — ошибкой/пустотой
        JsonNode asGroup = callQuietly("groups.getById", Map.of());
        boolean looksLikeGroupToken = !asGroup.has("error")
                && asGroup.path("response").path(0).path("id").asLong(0) != 0;
        report.put("token_type", looksLikeGroupToken
                ? "⚠️ КЛЮЧ СООБЩЕСТВА — методы photos.*/docs.* для загрузки в личные сообщения с ним "
                + "НЕ работают, нужен пользовательский токен"
                : "пользовательский (это правильно для фото в ЛС)");

        // 2. получатель
        report.put("peer_id", peerId);
        report.put("peer_hint", peerHint(peerId));

        // 3. сервер загрузки фото
        JsonNode photo = callQuietly("photos.getMessagesUploadServer", Map.of("peer_id", String.valueOf(peerId)));
        report.put("photos.getMessagesUploadServer.raw", photo.toString());
        if (photo.has("error")) {
            int code = photo.path("error").path("error_code").asInt();
            report.put("photos.error", "❌ " + vkError(photo));
            report.put("photos.hint", hintFor(code));
        } else {
            String url = photo.path("response").path("upload_url").asText("");
            report.put("photos.upload_url", url.isBlank() ? "❌ пустой (странный ответ VK)" : "✅ получен");
        }

        // 4. сервер загрузки документа
        JsonNode doc = callQuietly("docs.getMessagesUploadServer",
                Map.of("peer_id", String.valueOf(peerId), "type", "doc"));
        report.put("docs.getMessagesUploadServer.raw", doc.toString());
        if (doc.has("error")) {
            report.put("docs.error", "❌ " + vkError(doc));
            report.put("docs.hint", hintFor(doc.path("error").path("error_code").asInt()));
        } else {
            String url = doc.path("response").path("upload_url").asText("");
            report.put("docs.upload_url", url.isBlank() ? "❌ пустой" : "✅ получен");
        }

        // 5. итог
        boolean photoOk = String.valueOf(report.getOrDefault("photos.upload_url", "")).startsWith("✅");
        boolean docsOk = String.valueOf(report.getOrDefault("docs.upload_url", "")).startsWith("✅");
        report.put("вывод", photoOk && docsOk
                ? "✅ серверы загрузки получаются — проблема в другом шаге (multipart/save)"
                : (photoOk ? "⚠️ фото грузится, документы нет"
                : "❌ сервер загрузки фото не отдаётся — смотрите photos.hint выше"));
        return report;
    }

    /** Печатает отчёт в консоль (можно вызвать из start() при debug.enabled=true). */
    public void printUploadReport(long peerId) {
        System.out.println("🔍 [VK DEBUG] ===== диагностика загрузки для peer_id=" + peerId + " =====");
        debugUpload(peerId).forEach((k, v) -> System.out.println("   " + k + ": " + v));
        System.out.println("🔍 [VK DEBUG] ================================================");
    }

    /** Расшифровка кодов ошибок VK. */
    public static String hintFor(int code) {
        return switch (code) {
            case 5 -> "Токен недействителен/истёк. Перевыпустите его и обновите vk.token2 в application.properties.";
            case 6 -> "Слишком много запросов — VK включил лимит. Добавьте паузы между вызовами.";
            case 7 -> "Нет доступа к методу. Скорее всего у вас КЛЮЧ СООБЩЕСТВА: методы photos.*/docs.* "
                    + "для загрузки фото в личные сообщения работают только с ПОЛЬЗОВАТЕЛЬСКИМ токеном.";
            case 10 -> "Нет доступа к сообществу или к его настройкам.";
            case 11 -> "Выключите «тестовый режим» в настройках приложения.";
            case 14 -> "Нужна CAPTCHA — в ответе есть captcha_sid/captcha_img. Через API её не обойти: "
                    + "зайдите в приложение и решите, либо смените IP.";
            case 15 -> "Access denied: у токена нет нужных прав (для фото в сообщениях нужны scope "
                    + "`photos` И `messages`). Если subcode=1133 и в request_params видно oauth=1 — "
                    + "дело в scope токена и/или в том, что аккаунт VK не прошёл верификацию "
                    + "(с 2024 г. VK требует подтвердить личность, иначе часть методов не отдаётся "
                    + "даже с правильными scope). Лечение: 1) верифицировать аккаунт в настройках VK; "
                    + "2) перевыпустить токен с маской photos+messages+docs+offline. "
                    + "Пока этого нет, sendFile() автоматически шлёт картинки ДОКУМЕНТОМ.";
            case 16 -> "Требуется подтверждение через 2FA. Пройдите его в браузере и перевыпустите токен.";
            case 20 -> "Метод выключен: включите его в настройках приложения (или используйте Standalone-приложение).";
            case 100 -> "Неверный параметр. Чаще всего — не тот peer_id: для беседы нужен 2000000000 + chat_id, "
                    + "для сообщества — отрицательный id, для человека — его uid.";
            case 113 -> "Неверный альбом: для фото в сообщениях альбом не нужен, используйте "
                    + "photos.getMessagesUploadServer + photos.saveMessagesPhoto.";
            case 203 -> "Нет доступа к чужому фото/альбому — загружать можно только от своего имени.";
            case 300 -> "Альбом переполнен.";
            default -> "Код " + code + ": смотрите https://dev.vk.com/reference/errors";
        };
    }

    private static String peerHint(long peerId) {
        if (peerId >= 2_000_000_000L) return "беседа (chat_id = " + (peerId - 2_000_000_000L) + ")";
        if (peerId < 0) return "сообщество (id = " + (-peerId) + ")";
        if (peerId > 0) return "личный чат с пользователем id=" + peerId;
        return "⚠️ peer_id = 0 — так не отправить";
    }

    /** Вызов VK API, который не должен ронять диагностику. */
    private JsonNode callQuietly(String method, Map<String, String> params) {
        try {
            return vkApi(method, params);
        } catch (Exception e) {
            return mapper.createObjectNode().putObject("error")
                    .put("error_code", -1)
                    .put("error_msg", e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    // =========================================================================================
    // 🎛 УПРАВЛЕНИЕ И КОНФИГ
    //
    // Методы, которые нужны VkController:
    //   isLongPollRunning(), isScanning(), getWhitelistGroupIds(), setWhitelistGroupIds(),
    //   isScanPersonalMessages(), setScanPersonalMessages(), setScanGroups(), getConfig()
    //
    // ⚠️ Если они у вас в VkConnector УЖЕ ЕСТЬ — этот блок не копируйте,
    //    скопируйте только секции «📤 ОТПРАВКА», «🖼 ФОТО», «📎 ДОКУМЕНТ» и «🧰 УТИЛИТЫ».
    // =========================================================================================

    /** Прослушка LongPoll включена (у вас флаг может называться иначе — оставьте свой). */
    private volatile boolean isLongPollRunning = false;
    /** Идёт ли сканирование истории прямо сейчас. */
    private volatile boolean isScanning = false;
    /** Сканировать личные сообщения. */
    private boolean scanPersonalMessages = true;
    /** Сканировать группы/беседы. */
    private boolean scanGroups = true;
    /** Пустой список = сканировать ВСЕ группы (логика как в TgConnector). */
    private List<Long> whitelistGroupIds = new ArrayList<>();

    public void startUserLongPoll() {
        isLongPollRunning = true;
        System.out.println("👂 [VK] Прослушка ВКЛ");
        // 👉 здесь ваш запуск LongPoll/Bots LongPoll
    }

    public void stopUserLongPoll() {
        isLongPollRunning = false;
        System.out.println("🔇 [VK] Прослушка ВЫКЛ");
        // 👉 здесь ваша остановка
    }

    public boolean isLongPollRunning() { return isLongPollRunning; }
    public boolean isScanning()        { return isScanning; }

    /** Вызывается из VkController: POST /api/vk/scan. */
    public void initialSync() {
        System.out.println("🔄 [VK] Начальная синхронизация");
        // 👉 здесь ваш первоначальный прогон по диалогам/группам
    }

    /** Вызывается из VkController: POST /api/vk/doSync. */
    public void scanSpecificGroups(List<Long> groupIds) {
        if (isScanning) {
            System.out.println("⏳ [VK] Сканирование уже идёт");
            return;
        }
        isScanning = true;
        // 👉 сканируйте в отдельном потоке и сбросьте isScanning = false в finally
        System.out.println("📡 [VK] Сканирую групп: " + (groupIds == null ? 0 : groupIds.size()));
        isScanning = false;
    }

    public boolean isScanPersonalMessages() { return scanPersonalMessages; }
    public boolean isScanGroups()           { return scanGroups; }

    public void setScanPersonalMessages(boolean v) {
        this.scanPersonalMessages = v;
        System.out.println("⚙️ [VK] scanPersonalMessages = " + v);
    }

    public void setScanGroups(boolean v) {
        this.scanGroups = v;
        System.out.println("⚙️ [VK] scanGroups = " + v);
    }

    public List<Long> getWhitelistGroupIds() { return whitelistGroupIds; }

    public void setWhitelistGroupIds(List<Long> ids) {
        this.whitelistGroupIds = (ids != null) ? new ArrayList<>(ids) : new ArrayList<>();
        System.out.println("⚙️ [VK] Вайтлист групп: " + this.whitelistGroupIds.size() + " ID");
    }

    /**
     * Фильтр чата перед сохранением — зовите из своего цикла сканирования.
     * Пустой вайтлист = разрешены все группы (как в TgConnector).
     *
     * @param isGroup true для бесед/сообществ, false для личных диалогов
     */
    public boolean isChatAllowed(long peerId, boolean isGroup) {
        if (!isGroup) {
            if (!scanPersonalMessages) {
                System.out.println("⏭️ [VK] Личный диалог пропущен (отключено): " + peerId);
                return false;
            }
            return true;
        }
        if (!scanGroups) {
            System.out.println("⏭️ [VK] Группа пропущена (отключено): " + peerId);
            return false;
        }
        long chatId = peerId >= 2_000_000_000L ? peerId - 2_000_000_000L : peerId;
        if (!whitelistGroupIds.isEmpty()
                && !whitelistGroupIds.contains(peerId)
                && !whitelistGroupIds.contains(chatId)) {
            System.out.println("⏭️ [VK] Группа не в вайтлисте: " + peerId);
            return false;
        }
        return true;
    }

    public Map<String, Object> getConfig() {
        return Map.of(
                "scanPersonalMessages", scanPersonalMessages,
                "scanGroups", scanGroups,
                "longPollRunning", isLongPollRunning,
                "scanning", isScanning,
                "whitelistSize", whitelistGroupIds.size(),
                "whitelist", whitelistGroupIds
        );
    }

    // =========================================================================================
    // 🧰 УТИЛИТЫ
    // =========================================================================================

    /**
     * Приводит id из вашей БД к peer_id.
     * Сейчас — как есть: положительные id и id бесед (>= 2000000000) идут без изменений,
     * отрицательные — это сообщества.
     *
     * Если у вас в БД chatId бесед хранится «коротким» (1, 2, 3...), включите конвертацию:
     *     if (rawId > 0 && rawId < 2_000_000_000L) return 2_000_000_000L + rawId;
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

    /** «code=15 msg=Access denied ... -> подсказка» */
    private static String vkError(JsonNode resp) {
        JsonNode e = resp.path("error");
        int code = e.path("error_code").asInt();
        return "code=" + code + " msg=" + e.path("error_msg").asText("") + "  ->  " + hintFor(code);
    }

    /**
     * Ошибка прав (scope), а не технический сбой.
     * error_code 15 с subcode 1133 и текстом "cannot be called with current scopes" —
     * это отсутствие нужного scope у токена (либо непройденная верификация аккаунта VK).
     */
    private static boolean isScopeError(JsonNode resp) {
        JsonNode e = resp.path("error");
        int code = e.path("error_code").asInt();
        String msg = e.path("error_msg").asText("").toLowerCase();
        return code == 15 || code == 7 || code == 20 || msg.contains("current scopes");
    }

    /** JSON-узел -> строка параметра VK (массив [999,1] превращается в "999,1"). */
    private static String asText(JsonNode node) {
        if (node == null || node.isNull()) return "";
        if (node.isArray()) {
            StringBuilder sb = new StringBuilder();
            for (JsonNode item : node) {
                if (sb.length() > 0) sb.append(',');
                sb.append(item.asText());
            }
            return sb.toString();
        }
        return node.asText("");
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
        // 👇 подставьте свой геттер токена VK из Config (у вас это vk.token2)
        return Config.getVkToken();
    }

    private static String describe(Throwable t) {
        Throwable c = (t != null && t.getCause() != null) ? t.getCause() : t;
        if (c == null) return "неизвестная ошибка";
        String m = c.getMessage();
        return (m != null && !m.isBlank()) ? m : c.getClass().getSimpleName();
    }
}
