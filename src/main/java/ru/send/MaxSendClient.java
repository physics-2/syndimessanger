package ru.send;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.MediaType;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.Map;

/**
 * Клиент к python-сервису MAX (pymax, FastAPI на :8081).
 *
 * Ходим через Java-бэкенд, а не напрямую из браузера, по двум причинам:
 *   1) не нужно настраивать CORS на python-сервисе;
 *   2) фронтенд работает с одним API — /api/send на :8080.
 *
 * Ожидаемые эндпоинты python-сервиса (см. python/max_send_api.py):
 *   POST {base}/api/max/send        JSON   { chat_id, text, reply_to, notify }
 *   POST {base}/api/max/send/media  multipart: chat_id, text, reply_to, notify, file
 */
@Component
public class MaxSendClient {

    private final RestClient http;
    private final ObjectMapper mapper = new ObjectMapper();

    public MaxSendClient(@Value("${max.api.base-url:http://localhost:8081}") String baseUrl) {
        this.http = RestClient.builder().baseUrl(baseUrl).build();
    }

    /** Отправка текста. */
    public SendResult sendText(long chatId, String text, String replyTo, boolean notify) {
        try {
            String body = http.post()
                    .uri("/api/max/send")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of(
                            "chat_id", chatId,
                            "text", text != null ? text : "",
                            "reply_to", replyTo != null ? replyTo : "",
                            "notify", notify))
                    .retrieve()
                    .body(String.class);
            return parse(body, chatId, text);
        } catch (Exception e) {
            return SendResult.fail("max", chatId, "MAX недоступен: " + e.getMessage());
        }
    }

    /** Отправка файла (фото/видео/документ — python-сторона определит сама). */
    public SendResult sendFile(long chatId, String text, String replyTo, boolean notify, SendFile file) {
        try {
            MultipartBodyBuilder mp = new MultipartBodyBuilder();
            mp.part("chat_id", String.valueOf(chatId));
            mp.part("text", text != null ? text : "");
            mp.part("reply_to", replyTo != null ? replyTo : "");
            mp.part("notify", String.valueOf(notify));

            ByteArrayResource resource = new ByteArrayResource(file.bytes() != null
                    ? file.bytes()
                    : java.nio.file.Files.readAllBytes(java.nio.file.Path.of(file.path()))) {
                @Override
                public String getFilename() {
                    return file.name() != null ? file.name() : "file";
                }
            };
            mp.part("file", resource)
                    .contentType(file.mimeType() != null
                            ? MediaType.parseMediaType(file.mimeType())
                            : MediaType.APPLICATION_OCTET_STREAM);

            String body = http.post()
                    .uri("/api/max/send/media")
                    .contentType(MediaType.MULTIPART_FORM_DATA)
                    .body(mp.build())
                    .retrieve()
                    .body(String.class);
            return parse(body, chatId, text);
        } catch (Exception e) {
            return SendResult.fail("max", chatId, "Ошибка отправки файла в MAX: " + e.getMessage());
        }
    }

    /** Ответ python-сервиса: { "status": "ok", "message_id": ..., "chat_id": ... } */
    private SendResult parse(String body, long chatId, String text) throws Exception {
        if (body == null || body.isBlank()) return SendResult.fail("max", chatId, "Пустой ответ MAX-сервиса");
        JsonNode json = mapper.readTree(body);
        boolean ok = "ok".equalsIgnoreCase(json.path("status").asText(""));
        if (!ok) {
            return SendResult.fail("max", chatId, json.path("message").asText(json.path("error").asText("MAX вернул ошибку")));
        }
        return SendResult.ok("max",
                json.path("chat_id").asLong(chatId),
                json.path("message_id").asLong(0),
                text);
    }
}
