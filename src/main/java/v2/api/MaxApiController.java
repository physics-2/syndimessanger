package v2.api;

import org.springframework.web.bind.annotation.*;
import v2.api.base.BaseApiConnector;
import v2.connectors.base.*;
import v2.connectors.max.MaxConnector;

import java.util.*;

/**
 * MAX API Controller для v2.
 * Соответствует структуре VkApiController и реализует BaseApiConnector.
 */
@RestController
@RequestMapping("/api/max")
@CrossOrigin(origins = "*")
public class MaxApiController implements BaseApiConnector {

    private final MaxConnector maxConnector;

    public MaxApiController(MaxConnector maxConnector) {
        this.maxConnector = maxConnector;
    }

    // ==================== СТАТУС ====================

    @Override
    @GetMapping("/status")
    public Map<String, Object> getStatus() {
        ConnectorStatus status = maxConnector.getStatus();
        Map<String, Object> response = new HashMap<>();

        response.put("platform", status.platform());
        response.put("isRunning", status.started());
        response.put("isListening", status.listening());
        response.put("isScanning", status.scanning());
        response.put("myUserId", status.myUserId());
        response.put("success", true);

        return response;
    }

    // ==================== УПРАВЛЕНИЕ КЛИЕНТОМ ====================

    @PostMapping("/start")
    public Map<String, Object> startClient() {
        ConnectorResult result = maxConnector.start();
        return resultToMap(result);
    }

    @PostMapping("/stop")
    public Map<String, Object> stopClient() {
        ConnectorResult result = maxConnector.stop();
        return resultToMap(result);
    }

    // ==================== ПРОСЛУШКА ====================

    @Override
    @PostMapping("/startListening")
    public Map<String, Object> startListening() {
        ConnectorResult result = maxConnector.startListening();
        return resultToMap(result);
    }

    @Override
    @PostMapping("/stopListening")
    public Map<String, Object> stopListening() {
        ConnectorResult result = maxConnector.stopListening();
        return resultToMap(result);
    }

    // ==================== СКАНИРОВАНИЕ ====================

    @Override
    @PostMapping("/startScan")
    public Map<String, Object> startScan() {
        return startScanWithOptions(new ScanOptions(new ArrayList<>(), 200));
    }

    @PostMapping("/startScanWithOptions")
    public Map<String, Object> startScanWithOptions(@RequestBody(required = false) ScanOptions options) {
        try {
            if (options == null) {
                options = new ScanOptions(new ArrayList<>(), 200);
            }
            ConnectorResult result = maxConnector.startScan(options);
            return resultToMap(result);
        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("success", false);
            error.put("message", "Ошибка запуска сканирования: " + e.getMessage());
            return error;
        }
    }

    // ==================== КОНФИГУРАЦИЯ ====================

    @Override
    @PostMapping("/updateConfig")
    public Map<String, Object> updateConfig(@RequestBody Map<String, Object> body) {
        try {
            ConnectorConfig config = maxConnector.getConfig();

            // Парсим настройки из body
            if (body.containsKey("scanGroups")) {
                config.scanGroups = ((Boolean) body.get("scanGroups"));
            }
            if (body.containsKey("scanPersonal")) {
                config.scanPersonal = ((Boolean) body.get("scanPersonal"));
            }
            if (body.containsKey("downloadMedia")) {
                config.downloadMedia = ((Boolean) body.get("downloadMedia"));
            }
            if (body.containsKey("limitPerChat")) {
                config.limitPerChat = ((Number) body.get("limitPerChat")).intValue();
            }

            ConnectorResult result = maxConnector.updateConfig(config);
            return resultToMap(result);

        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("success", false);
            error.put("message", "Ошибка обновления конфига: " + e.getMessage());
            return error;
        }
    }

    @GetMapping("/config")
    public Map<String, Object> getConfig() {
        ConnectorConfig config = maxConnector.getConfig();
        Map<String, Object> response = new HashMap<>();

        response.put("scanGroups", config.scanGroups);
        response.put("scanPersonal", config.scanPersonal);
        response.put("downloadMedia", config.downloadMedia);
        response.put("limitPerChat", config.limitPerChat);
        response.put("success", true);

        return response;
    }

    // ==================== ОТПРАВКА СООБЩЕНИЙ ====================

    /**
     * Отправка текстового сообщения
     * POST /api/max/send/message
     * Body: { "chatId": 123, "text": "...", "replyTo": 0, "notify": true }
     */
    @PostMapping("/send/message")
    public Map<String, Object> sendMessage(@RequestBody Map<String, Object> body) {
        Map<String, Object> response = new HashMap<>();

        try {
            Long chatId = extractLong(body.get("chatId"));
            String text = (String) body.get("text");
            Long replyTo = extractLong(body.getOrDefault("replyTo", 0L));
            Boolean notify = (Boolean) body.getOrDefault("notify", true);

            if (chatId == null || chatId <= 0) {
                response.put("success", false);
                response.put("message", "Неверный chatId");
                return response;
            }

            if (text == null || text.isBlank()) {
                response.put("success", false);
                response.put("message", "Пустой текст сообщения");
                return response;
            }

            SendResult result = maxConnector.sendText(chatId, text, replyTo != null ? replyTo : 0, notify != null && notify);

            if (result.success()) {
                response.put("success", true);
                response.put("messageId", result.messageId());
                response.put("message", "Сообщение отправлено");
            } else {
                response.put("success", false);
                response.put("message", result.error());
            }

        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "Ошибка отправки: " + e.getMessage());
        }

        return response;
    }

    /**
     * Отправка файла
     * POST /api/max/send/file
     * Body: { "chatId": 123, "filePath": "/path/to/file", "caption": "...", "replyTo": 0, "notify": true }
     */
    @PostMapping("/send/file")
    public Map<String, Object> sendFile(@RequestBody Map<String, Object> body) {
        Map<String, Object> response = new HashMap<>();

        try {
            Long chatId = extractLong(body.get("chatId"));
            String filePath = (String) body.get("filePath");
            String caption = (String) body.getOrDefault("caption", "");
            Long replyTo = extractLong(body.getOrDefault("replyTo", 0L));
            Boolean notify = (Boolean) body.getOrDefault("notify", true);

            if (chatId == null || chatId <= 0) {
                response.put("success", false);
                response.put("message", "Неверный chatId");
                return response;
            }

            if (filePath == null || filePath.isBlank()) {
                response.put("success", false);
                response.put("message", "Не указан путь к файлу");
                return response;
            }

            SendResult result = maxConnector.sendFile(chatId, caption, filePath, null, replyTo != null ? replyTo : 0, notify != null && notify);

            if (result.success()) {
                response.put("success", true);
                response.put("messageId", result.messageId());
                response.put("message", "Файл отправлен");
            } else {
                response.put("success", false);
                response.put("message", result.error());
            }

        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "Ошибка отправки файла: " + e.getMessage());
        }

        return response;
    }

    // ==================== УТИЛИТЫ ====================

    private Map<String, Object> resultToMap(ConnectorResult result) {
        Map<String, Object> response = new HashMap<>();
        response.put("success", result.success());
        response.put("message", result.message());
        return response;
    }

    private Long extractLong(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }


}
