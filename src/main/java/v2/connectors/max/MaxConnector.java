package v2.connectors.max;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import v2.connectors.base.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * MAX Connector для v2 - реализует BaseConnector.
 * Клиент к python-сервису MAX (pymax, FastAPI на :8081).
 * Без System.out.println, без фоллбеков с мусорными данными.
 */
@Component
public class MaxConnector implements BaseConnector {

    private static final Logger log = LoggerFactory.getLogger(MaxConnector.class);
    private static final String DEFAULT_BASE_URL = "http://localhost:8081";

    private final RestClient http;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ConnectorConfig config = new ConnectorConfig();

    // Состояние
    private volatile boolean isListening = false;
    private volatile boolean isScanning = false;
    private volatile boolean isStarted = false;
    private Long myUserId = null;

    public MaxConnector() {
        this.http = RestClient.builder().baseUrl(DEFAULT_BASE_URL).build();
    }

    @Override
    public String platform() {
        return "max";
    }

    @Override
    public List<Map<String, Object>> getAllGroups() {
        return List.of((Map<String, Object>) Objects.requireNonNull(new HashMap<>().put("12345", "group")));
    }

    @Override
    public ConnectorStatus getStatus() {
        try {
            Map<String, Object> response = http.get()
                    .uri("/api/max/status")
                    .retrieve()
                    .body(Map.class);

            if (response != null) {
                Boolean listening = (Boolean) response.get("listening");
                Boolean scanning = (Boolean) response.get("scanning");
                Object userIdObj = response.get("my_user_id");
                Long userId = userIdObj instanceof Number ? ((Number) userIdObj).longValue() : null;

                return new ConnectorStatus("max", isStarted,
                        listening != null && listening,
                        scanning != null && scanning,
                        userId, config);
            }
            return new ConnectorStatus("max", false, false, false, null, config);
        } catch (Exception e) {
            return new ConnectorStatus("max", false, false, false, null, config);
        }
    }

    @Override
    public ConnectorResult start() {
        if (isStarted) {
            return ConnectorResult.ok("MAX уже запущен");
        }
        try {
            // Проверяем доступность сервиса
            http.get().uri("/api/max/status").retrieve().body(String.class);
            isStarted = true;
            return ConnectorResult.ok("MAX коннектор запущен");
        } catch (Exception e) {
            return ConnectorResult.fail("MAX сервис недоступен: " + e.getMessage());
        }
    }

    @Override
    public ConnectorResult stop() {
        isListening = false;
        isStarted = false;
        return ConnectorResult.ok("MAX остановлен");
    }

    @Override
    public ConnectorResult startListening() {
        if (!isStarted) {
            start();
        }
        try {
            http.post().uri("/api/max/listen/start").retrieve().body(String.class);
            isListening = true;
            return ConnectorResult.ok("MAX прослушка включена");
        } catch (Exception e) {
            return ConnectorResult.fail("Ошибка включения прослушки MAX: " + e.getMessage());
        }
    }

    @Override
    public ConnectorResult stopListening() {
        try {
            http.post().uri("/api/max/listen/stop").retrieve().body(String.class);
            isListening = false;
            return ConnectorResult.ok("MAX прослушка выключена");
        } catch (Exception e) {
            return ConnectorResult.fail("Ошибка выключения прослушки MAX: " + e.getMessage());
        }
    }

    @Override
    public ConnectorResult startScan(ScanOptions options) {
        if (!isStarted) {
            start();
        }
        if (isScanning) {
            return ConnectorResult.fail("Сканирование уже идёт");
        }

        try {
            Integer limit = options.limitPerChat() != null ? options.limitPerChat() : config.limitPerChat;
            Map<String, Object> body = Map.of("limit_per_chat", limit);

            http.post().uri("/api/max/scan").body(body).retrieve().body(String.class);
            isScanning = true;
            return ConnectorResult.ok("Сканирование MAX запущено");
        } catch (Exception e) {
            return ConnectorResult.fail("Ошибка запуска сканирования MAX: " + e.getMessage());
        }
    }

    @Override
    public ConnectorConfig getConfig() {
        return config;
    }

    @Override
    public ConnectorResult updateConfig(ConnectorConfig newConfig) {
        try {

            this.config.scanPersonal = newConfig.scanPersonal;
            this.config.scanGroups = newConfig.scanGroups;
            this.config.limitPerChat = newConfig.limitPerChat;
            this.config.downloadMedia = newConfig.downloadMedia;

            return ConnectorResult.ok("Конфигурация MAX обновлена");
        } catch (Exception e) {
            return ConnectorResult.fail("Ошибка обновления конфига MAX: " + e.getMessage());
        }
    }

    @Override
    public long sendMessage(String peer, String text, String attachments, Long replyTo) {
        if (text == null || text.trim().isEmpty()) {
            throw new IllegalArgumentException("Message text cannot be empty");
        }

        try {
            long chatId = Long.parseLong(peer);

            // Подготовка запроса к MAX сервису
            Map<String, Object> body = new java.util.HashMap<>();
            body.put("chat_id", chatId);
            body.put("text", text);

            if (replyTo != null && replyTo > 0) {
                body.put("reply_to", String.valueOf(replyTo));
            }

            // Если есть вложения - добавляем их
            if (attachments != null && !attachments.isEmpty()) {
                body.put("attachments", attachments);
            }

            body.put("notify", true);

            String response = http.post()
                    .uri("/api/max/send")
                    .body(body)
                    .retrieve()
                    .body(String.class);

            // Парсим ответ
            JsonNode json = mapper.readTree(response);
            String status = json.path("status").asText("");

            if (!"ok".equalsIgnoreCase(status)) {
                String errorMessage = json.path("message").asText(
                        json.path("error").asText("MAX вернул ошибку"));
                throw new RuntimeException("MAX send failed: " + errorMessage);
            }

            long messageId = json.path("message_id").asLong(0);

            if (messageId <= 0) {
                throw new RuntimeException("MAX returned invalid message ID: " + messageId);
            }

            log.info("Message sent to MAX chat {}: {}", chatId, messageId);
            return messageId;

        } catch (NumberFormatException e) {
            log.error("Invalid peer ID format: {}", peer, e);
            throw new IllegalArgumentException("Invalid peer ID: " + peer, e);
        } catch (Exception e) {
            log.error("Error sending message to MAX peer {}", peer, e);
            throw new RuntimeException("MAX send failed: " + e.getMessage(), e);
        }
    }

    /**
     * Отправка текстового сообщения через MAX сервис.
     * @param chatId ID чата
     * @param text Текст сообщения
     * @param replyTo ID сообщения для ответа (0 если нет)
     * @param notify Уведомлять ли участников
     */
    public SendResult sendText(long chatId, String text, long replyTo, boolean notify) {
        try {
            Map<String, Object> body = Map.of(
                    "chat_id", chatId,
                    "text", text != null ? text : "",
                    "reply_to", replyTo > 0 ? String.valueOf(replyTo) : "",
                    "notify", notify
            );

            String response = http.post()
                    .uri("/api/max/send")
                    .body(body)
                    .retrieve()
                    .body(String.class);

            return parseResponse(response);
        } catch (Exception e) {
            return SendResult.fail("max", chatId, "MAX недоступен: " + e.getMessage());
        }
    }

    /**
     * Отправка файла через MAX сервис.
     * @param chatId ID чата
     * @param caption Подпись к файлу
     * @param filePath Путь к файлу
     * @param mimeType MIME тип файла
     * @param replyTo ID сообщения для ответа (0 если нет)
     * @param notify Уведомлять ли участников
     */
    public SendResult sendFile(long chatId, String caption, String filePath, String mimeType, long replyTo, boolean notify) {
        try {
            java.nio.file.Path path = java.nio.file.Path.of(filePath);
            if (!java.nio.file.Files.exists(path)) {
                return SendResult.fail("max", chatId, "Файл не найден: " + filePath);
            }

            byte[] fileBytes = java.nio.file.Files.readAllBytes(path);
            String fileName = path.getFileName().toString();

            // Определяем MIME тип если не указан
            if (mimeType == null || mimeType.isBlank()) {
                mimeType = java.nio.file.Files.probeContentType(path);
                if (mimeType == null) {
                    mimeType = "application/octet-stream";
                }
            }

            // Используем multipart форму для отправки файла
            MultipartBodyBuilder mp = new org.springframework.http.client.MultipartBodyBuilder();
            mp.part("chat_id", String.valueOf(chatId));
            mp.part("text", caption != null ? caption : "");
            mp.part("reply_to", replyTo > 0 ? String.valueOf(replyTo) : "");
            mp.part("notify", String.valueOf(notify));

            ByteArrayResource resource =
                    new org.springframework.core.io.ByteArrayResource(fileBytes) {
                        @Override
                        public String getFilename() {
                            return fileName;
                        }
                    };

            mp.part("file", resource)
                    .contentType(org.springframework.http.MediaType.parseMediaType(mimeType));

            String response = http.post()
                    .uri("/api/max/send/media")
                    .body(mp.build())
                    .retrieve()
                    .body(String.class);

            return parseResponse(response);
        } catch (Exception e) {
            return SendResult.fail("max", chatId, "Ошибка отправки файла в MAX: " + e.getMessage());
        }
    }

    /**
     * Парсинг ответа от MAX сервиса.
     * Ожидаемый формат: { "status": "ok", "message_id": ..., "chat_id": ... }
     */
    private SendResult parseResponse(String body) throws Exception {
        if (body == null || body.isBlank()) {
            return SendResult.fail("max",0, "Пустой ответ MAX сервиса");
        }
        return mapper.readValue(body, SendResult.class);
    }

    public boolean isStarted() {
        return isStarted;
    }

    public boolean isScanning() {
        return isScanning;
    }

    public boolean isListening() {
        return isListening;
    }
}