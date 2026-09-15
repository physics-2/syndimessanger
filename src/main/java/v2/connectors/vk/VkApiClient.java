package v2.connectors.vk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import v2.Config;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * HTTP-слой VK API + общие утилиты (ошибки, подсказки, нормализация server).
 *
 * Здесь НЕТ ни LongPoll-цикла, ни отправки, ни работы с БД — только «сходить в VK и вернуть JSON».
 * Используется VkLongPoll, VkSender и VkConnector.
 *
 * Не является Spring-бином: создаётся внутри VkConnector в единственном экземпляре,
 * чтобы на всё приложение был один HttpClient и один ObjectMapper.
 */
public class VkApiClient {

    private static final Logger log = LoggerFactory.getLogger(VkApiClient.class);

    private static final String API_URL = "https://api.vk.com/method/";
    private static final String API_VERSION = "5.199";
    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final Duration UPLOAD_TIMEOUT = Duration.ofMinutes(5);

    /** Версия API, которую добавляем к каждому запросу. */
    public static final String VERSION = API_VERSION;

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(20))
            .build();
    private final ObjectMapper mapper = new ObjectMapper();

    // =========================================================================================
    // 🔧 ЗАПРОСЫ
    // =========================================================================================

    /**
     * GET-вызов метода VK API. Токен и v подставляются автоматически.
     *
     * @throws Exception если VK ответил не-200, пустым телом или не-JSON
     */
    public JsonNode call(String method, Map<String, String> params) throws Exception {
        StringBuilder q = new StringBuilder();
        if (params != null) {
            params.forEach((k, v) -> {
                if (v != null) {
                    q.append(q.length() > 0 ? "&" : "").append(enc(k)).append('=').append(enc(v));
                }
            });
        }
        q.append(q.length() > 0 ? "&" : "").append("access_token=").append(enc(Config.getVkToken()));
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

    /**
     * То же, что call(), но не бросает исключение — возвращает JSON-ошибку.
     * Нужно для диагностики, где один упавший запрос не должен ронять весь отчёт.
     */
    public JsonNode callQuietly(String method, Map<String, String> params) {
        try {
            return call(method, params);
        } catch (Exception e) {
            return mapper.createObjectNode().putObject("error")
                    .put("error_code", -1)
                    .put("error_msg", e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    /**
     * Длинный GET к LongPoll-серверу. Отдельный метод, потому что у него свой таймаут
     * (wait + запас) и своя обработка — иначе 25-секундное ожидание упиралось бы
     * в общий TIMEOUT и выглядело как ошибка.
     */
    public HttpResponse<String> longPollGet(String url, Duration timeout) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .timeout(timeout)
                .GET()
                .build();
        return http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    /** Классическая multipart-загрузка файла на сервер VK (photos/docs.getMessagesUploadServer). */
    public JsonNode postMultipart(String url, String fieldName, Path file) throws Exception {
        String boundary = "----VkConnector" + System.nanoTime();
        byte[] body = buildMultipart(boundary, fieldName, file);

        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .timeout(UPLOAD_TIMEOUT)
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

    public ObjectMapper mapper() {
        return mapper;
    }

    // =========================================================================================
    // ⚠️ ОШИБКИ VK
    // =========================================================================================

    /** true, если в ответе есть непустой error. */
    public static boolean hasError(JsonNode resp) {
        return resp != null && resp.has("error") && !resp.path("error").isNull();
    }

    /** "code=15 msg=Access denied … -> подсказка" */
    public static String vkError(JsonNode resp) {
        JsonNode e = resp.path("error");
        int code = e.path("error_code").asInt();
        String msg = e.path("error_msg").asText("");
        return "code=" + code + " msg=" + msg + (msg.isBlank() && code == 0 ? "" : "  ->  " + hintFor(code));
    }

    public static int errorCode(JsonNode resp) {
        return resp == null ? 0 : resp.path("error").path("error_code").asInt(0);
    }

    /**
     * Ошибка ПРАВ токена (scope), а не технический сбой.
     * error_code 15 с subcode 1133 и текстом "cannot be called with current scopes" —
     * это отсутствие нужного scope у токена либо непройденная верификация аккаунта VK.
     */
    public static boolean isScopeError(JsonNode resp) {
        JsonNode e = resp.path("error");
        int code = e.path("error_code").asInt();
        String msg = e.path("error_msg").asText("").toLowerCase(Locale.ROOT);
        return code == 15 || code == 7 || code == 20 || msg.contains("current scopes");
    }

    /** Расшифровка кодов ошибок VK. */
    public static String hintFor(int code) {
        return switch (code) {
            case 5 -> "Токен недействителен/истёк. Перевыпустите его и обновите vk.token2.";
            case 6 -> "Слишком много запросов — VK включил лимит. Добавьте паузы между вызовами.";
            case 7 -> "Нет доступа к методу. Скорее всего у вас КЛЮЧ СООБЩЕСТВА: photos.*/docs.* для "
                    + "загрузки в личные сообщения работают только с ПОЛЬЗОВАТЕЛЬСКИМ токеном.";
            case 9 -> "Слишком много однотипных запросов — увеличьте паузы.";
            case 10 -> "Нет доступа к сообществу или к его настройкам.";
            case 11 -> "Выключите «тестовый режим» в настройках приложения.";
            case 14 -> "Нужна CAPTCHA (в ответе есть captcha_sid/captcha_img). Через API не обходится.";
            case 15 -> "Access denied: у токена нет нужных прав (для фото нужны scope `photos` И `messages`). "
                    + "Если subcode=1133 и в request_params видно oauth=1 — дело в scope и/или в том, что "
                    + "аккаунт VK не прошёл верификацию (с 2024 г. обязательна). sendFile() при этом "
                    + "автоматически шлёт картинки ДОКУМЕНТОМ.";
            case 16 -> "Требуется подтверждение через 2FA. Пройдите его и перевыпустите токен.";
            case 20 -> "Метод выключен: включите его в настройках приложения (или используйте Standalone).";
            case 100 -> "Неверный параметр. Чаще всего не тот peer_id: беседа = 2000000000 + chat_id, "
                    + "сообщество = отрицательный id, человек = его uid.";
            case 113 -> "Неверный альбом: для фото в сообщениях используйте "
                    + "photos.getMessagesUploadServer + photos.saveMessagesPhoto.";
            case 203 -> "Нет доступа к чужому фото/альбому.";
            case 300 -> "Альбом переполнен.";
            default -> "Код " + code + ": https://dev.vk.com/reference/errors";
        };
    }

    // =========================================================================================
    // 🧰 УТИЛИТЫ
    // =========================================================================================

    /**
     * VK отдаёт server в РАЗНЫХ видах — все встречаются в проде:
     *   "https://api.vk.com/gim837611773"   — уже абсолютный URL
     *   "api.vk.com/gim837611773"           — хост + путь БЕЗ схемы
     *   "im.vk.me"                          — голый хост без схемы
     *   "/lp123456"                         — только путь (тогда базовый хост im.vk.me)
     *
     * HttpRequest.newBuilder(URI) требует абсолютный URI со схемой, иначе
     * IllegalArgumentException: URI with undefined scheme.
     *
     * ⚠️ Нельзя считать «хост без схемы» относительным путём и клеить его к https://im.vk.me:
     *    получится https://im.vk.me/api.vk.com/gim… и TLS упадёт с «PKIX path building failed».
     */
    public static String normalizeLongPollServer(String server) {
        String s = server == null ? "" : server.trim();
        if (s.isEmpty()) {
            return "";
        }
        int q = s.indexOf('?');                       // параметры добавляем сами
        if (q >= 0) {
            s = s.substring(0, q).trim();
        }
        String lower = s.toLowerCase(Locale.ROOT);
        if (lower.startsWith("http://") || lower.startsWith("https://")) {
            // уже абсолютный
        } else if (s.startsWith("/")) {
            s = "https://im.vk.me" + s;
        } else {
            s = "https://" + s;
        }
        while (s.endsWith("/")) {
            s = s.substring(0, s.length() - 1);
        }
        return s;
    }

    /** Хост из URL — для понятных сообщений об ошибках. */
    public static String safeHost(String url) {
        try {
            String h = URI.create(url).getHost();
            return h != null ? h : String.valueOf(url);
        } catch (Exception e) {
            return String.valueOf(url);
        }
    }

    /** JSON-узел → строка параметра VK (массив [999,1] превращается в "999,1"). */
    public static String asText(JsonNode node) {
        if (node == null || node.isNull()) {
            return "";
        }
        if (node.isArray()) {
            StringBuilder sb = new StringBuilder();
            for (JsonNode item : node) {
                if (sb.length() > 0) {
                    sb.append(',');
                }
                sb.append(item.asText());
            }
            return sb.toString();
        }
        return node.asText("");
    }

    public static void copyIfPresent(JsonNode src, Map<String, String> dst, String... keys) {
        for (String k : keys) {
            JsonNode v = src.get(k);
            if (v != null && !v.isNull()) {
                dst.put(k, v.asText());
            }
        }
    }

    public static String truncate(String s, int max) {
        if (s == null) {
            return "";
        }
        return s.length() <= max ? s : s.substring(0, max - 1) + "…";
    }

    public static String enc(String s) {
        return URLEncoder.encode(s == null ? "" : s, StandardCharsets.UTF_8);
    }

    public static String describe(Throwable t) {
        Throwable c = (t != null && t.getCause() != null) ? t.getCause() : t;
        if (c == null) {
            return "неизвестная ошибка";
        }
        String m = c.getMessage();
        return (m != null && !m.isBlank()) ? m : c.getClass().getSimpleName();
    }

    public static boolean isImage(String fileName, String mimeType) {
        if (mimeType != null && mimeType.toLowerCase(Locale.ROOT).startsWith("image/")) {
            return true;
        }
        String n = fileName == null ? "" : fileName.toLowerCase(Locale.ROOT);
        return n.endsWith(".jpg") || n.endsWith(".jpeg") || n.endsWith(".png")
                || n.endsWith(".gif") || n.endsWith(".webp") || n.endsWith(".bmp");
    }

    /** Что за peer_id — для человекочитаемых сообщений. */
    public static String peerHint(long peerId) {
        if (peerId >= 2_000_000_000L) return "беседа (chat_id = " + (peerId - 2_000_000_000L) + ")";
        if (peerId < 0) return "сообщество (id = " + (-peerId) + ")";
        if (peerId > 0) return "личный чат с пользователем id=" + peerId;
        return "⚠️ peer_id = 0 — так не отправить";
    }

    /** Пустая карта параметров (чтобы не плодить new LinkedHashMap<>()). */
    public static Map<String, String> params(String... kv) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }
}