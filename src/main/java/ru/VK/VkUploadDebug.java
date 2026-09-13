package ru.VK;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import ru.Config;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 🔍 Диагностика загрузки фото/документов в VK.
 *
 * Зачем: «⚠️ VK не отдал upload_url для фото» означает, что VK ответил ошибкой,
 * а не сервером загрузки. Этот класс печатает СЫРОЙ ответ VK и расшифровывает код ошибки.
 *
 * Как использовать — добавьте временный эндпоинт в любой контроллер:
 *
 *     &#64;GetMapping("/api/vk/debug-upload")
 *     public Map&lt;String, Object&gt; debugUpload(@RequestParam long peerId) {
 *         return ru.VK.VkUploadDebug.diagnose(peerId);
 *     }
 *
 * и откройте:  http://localhost:8080/api/vk/debug-upload?peerId=123456
 *
 * Или вызовите из консоли после старта:  VkUploadDebug.printReport(peerId);
 */
public final class VkUploadDebug {

    private VkUploadDebug() { }

    private static final String API_URL = "https://api.vk.com/method/";
    private static final String V = "5.199";   // 👈 та же версия, что в VkSendSupport

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(20))
            .build();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Полный отчёт: токен, получатель, сервер загрузки фото, сервер загрузки документа. */
    public static Map<String, Object> diagnose(long peerId) {
        Map<String, Object> report = new LinkedHashMap<>();

        // ---- 1. токен ----
        JsonNode me = call("users.get", Map.of());
        if (me.has("error")) {
            report.put("token", "❌ токен не работает: " + errText(me));
            report.put("hint", hintFor(me.path("error").path("error_code").asInt()));
            return report;
        }
        JsonNode user0 = me.path("response").path(0);
        report.put("token", "✅ рабочий, владелец: id" + user0.path("id").asLong()
                + " " + user0.path("first_name").asText("") + " " + user0.path("last_name").asText(""));
        // ключ сообщества отвечает на groups.getById "своим" id, пользовательский — ошибкой/пустотой
        JsonNode asGroup = call("groups.getById", Map.of());
        boolean looksLikeGroupToken = !asGroup.has("error")
                && asGroup.path("response").path(0).path("id").asLong(0) != 0;
        report.put("token_type", looksLikeGroupToken
                ? "⚠️ КЛЮЧ СООБЩЕСТВА (group_id=" + asGroup.path("response").path(0).path("id").asLong() + "). "
                + "Методы photos.*/docs.* для загрузки фото в личные сообщения с ним НЕ работают — "
                + "нужен пользовательский токен."
                : "пользовательский (это правильно для фото в ЛС)");

        // ---- 2. получатель ----
        report.put("peer_id", peerId);
        report.put("peer_hint", peerHint(peerId));

        // ---- 3. сервер загрузки ФОТО ----
        JsonNode photo = call("photos.getMessagesUploadServer", Map.of("peer_id", String.valueOf(peerId)));
        report.put("photos.getMessagesUploadServer.raw", photo.toString());
        if (photo.has("error")) {
            int code = photo.path("error").path("error_code").asInt();
            report.put("photos.error", "❌ " + errText(photo));
            report.put("photos.hint", hintFor(code));
        } else {
            String url = photo.path("response").path("upload_url").asText("");
            report.put("photos.upload_url", url.isBlank() ? "❌ пустой (странный ответ VK)" : "✅ получен");
            report.put("photos.response_keys", keysOf(photo.path("response")));
        }

        // ---- 4. сервер загрузки ДОКУМЕНТА ----
        JsonNode doc = call("docs.getMessagesUploadServer",
                Map.of("peer_id", String.valueOf(peerId), "type", "doc"));
        report.put("docs.getMessagesUploadServer.raw", doc.toString());
        if (doc.has("error")) {
            report.put("docs.error", "❌ " + errText(doc));
            report.put("docs.hint", hintFor(doc.path("error").path("error_code").asInt()));
        } else {
            String url = doc.path("response").path("upload_url").asText("");
            report.put("docs.upload_url", url.isBlank() ? "❌ пустой" : "✅ получен");
        }

        // ---- 5. итог ----
        boolean photoOk = String.valueOf(report.getOrDefault("photos.upload_url", "")).startsWith("✅");
        boolean docsOk = String.valueOf(report.getOrDefault("docs.upload_url", "")).startsWith("✅");
        report.put("вывод", photoOk && docsOk
                ? "✅ серверы загрузки получаются — проблема была в другом шаге (multipart/save)"
                : (photoOk
                ? "⚠️ фото грузится, документы нет"
                : "❌ сервер загрузки фото не отдаётся — смотрите photos.hint выше"));

        return report;
    }

    /** Печатает отчёт в консоль. */
    public static void printReport(long peerId) {
        System.out.println("🔍 [VK DEBUG] ===== диагностика загрузки для peer_id=" + peerId + " =====");
        diagnose(peerId).forEach((k, v) -> System.out.println("   " + k + ": " + v));
        System.out.println("🔍 [VK DEBUG] ================================================");
    }

    // =========================================================================================
    // Расшифровка кодов ошибок VK
    // =========================================================================================

    public static String hintFor(int code) {
        return switch (code) {
            case 5 -> "Токен недействителен/истёк. Перевыпустите его и обновите vk.token2 в application.properties.";
            case 6 -> "Слишком много запросов — VK включил лимит. Добавьте паузы между вызовами.";
            case 7 -> "Нет доступа к методу. Скорее всего у вас КЛЮЧ СООБЩЕСТВА: методы photos.*/docs.* "
                    + "для загрузки фото в личные сообщения работают только с ПОЛЬЗОВАТЕЛЬСКИМ токеном.";
            case 10 -> "Нет доступа к сообществу или к его настройкам.";
            case 11 -> "Выключите «тестовый режим» в настройках приложения.";
            case 14 -> "Нужна CAPTCHA — в ответе есть captcha_sid/captcha_img. Через API её не обойти, "
                    + "зайдите в приложение и решите, либо смените IP.";
            case 15 -> "Access denied: у токена нет нужных прав. Для фото в сообщениях обязательны "
                    + "scope `photos` И `messages`. Проверьте, что токен выпущен с ними "
                    + "(например, через VK ID / bnc.vk.com с mask=photos+messages), "
                    + "и что peer_id — это ЛС или беседа, куда у владельца токена есть доступ.";
            case 16 -> "Требуется подтверждение через 2FA. Пройдите его в браузере и перевыпустите токен.";
            case 20 -> "Метод выключен: включите его в настройках приложения (или используйте Standalone-приложение).";
            case 100 -> "Неверный параметр. Чаще всего — не тот peer_id: для беседы нужен 2000000000 + chat_id, "
                    + "для сообщества — отрицательный id, для человека — его uid.";
            case 113 -> "Неверный альбом: для фото в сообщениях альбом не нужен, используйте "
                    + "photos.getMessagesUploadServer + photos.saveMessagesPhoto.";
            case 203 -> "Нет доступа к чужому фото/альбому — загружать можно только от своего имени.";
            case 300 -> "Альбом переполнен (не ваш случай, если грузите в сообщения).";
            default -> "Код " + code + ": смотрите https://dev.vk.com/reference/errors";
        };
    }

    private static String peerHint(long peerId) {
        if (peerId >= 2_000_000_000L) return "беседа (chat_id = " + (peerId - 2_000_000_000L) + ")";
        if (peerId < 0) return "сообщество (id = " + (-peerId) + ")";
        if (peerId > 0) return "личный чат с пользователем id=" + peerId;
        return "⚠️ peer_id = 0 — так не отправить";
    }

    private static String errText(JsonNode resp) {
        JsonNode e = resp.path("error");
        return "code=" + e.path("error_code").asInt()
                + " msg=" + e.path("error_msg").asText("")
                + (e.has("error_text") ? " text=" + e.path("error_text").asText("") : "");
    }

    private static String keysOf(JsonNode node) {
        if (node == null || !node.isObject()) return "—";
        StringBuilder sb = new StringBuilder();
        node.fieldNames().forEachRemaining(n -> sb.append(n).append(' '));
        return sb.toString().trim();
    }

    // =========================================================================================
    // HTTP
    // =========================================================================================

    private static JsonNode call(String method, Map<String, String> params) {
        try {
            StringBuilder q = new StringBuilder();
            params.forEach((k, v) -> q.append(q.length() > 0 ? "&" : "")
                    .append(enc(k)).append('=').append(enc(v)));
            q.append("&access_token=").append(enc(token()));
            q.append("&v=").append(V);

            HttpRequest req = HttpRequest.newBuilder(URI.create(API_URL + method + "?" + q))
                    .timeout(Duration.ofSeconds(30))
                    .GET()
                    .build();

            HttpResponse<String> resp = HTTP.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (resp.statusCode() != 200) {
                return MAPPER.createObjectNode()
                        .putObject("error")
                        .put("error_code", resp.statusCode())
                        .put("error_msg", "HTTP " + resp.statusCode() + ": " + resp.body());
            }
            String body = resp.body();
            if (body == null || body.isBlank()) {
                return MAPPER.createObjectNode()
                        .putObject("error").put("error_code", -1).put("error_msg", "пустой ответ VK");
            }
            return MAPPER.readTree(body);
        } catch (Exception e) {
            return MAPPER.createObjectNode()
                    .putObject("error").put("error_code", -1)
                    .put("error_msg", e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private static String token() {
        // 👇 тот же геттер, что использует VkSendSupport
        return Config.getVkToken();
    }

    private static String enc(String s) {
        return URLEncoder.encode(s == null ? "" : s, StandardCharsets.UTF_8);
    }
}
