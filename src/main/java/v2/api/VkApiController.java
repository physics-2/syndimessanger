package v2.api;

import org.springframework.web.bind.annotation.*;
import v2.api.base.BaseApiConnector;
import v2.connectors.base.ConnectorConfig;
import v2.connectors.base.ConnectorResult;
import v2.connectors.base.ConnectorStatus;
import v2.connectors.base.ScanOptions;
import v2.connectors.vk.VkConnector;

import java.util.*;

/**
 * VK API Controller для v2.
 * Соответствует структуре TgApiController и реализует BaseApiConnector.
 */
@RestController
@RequestMapping("/api/vk")
public class VkApiController implements BaseApiConnector {

    private final VkConnector vkConnector;

    public VkApiController(VkConnector vkConnector) {
        this.vkConnector = vkConnector;
    }

    // ==================== СТАТУС ====================

    @Override
    @GetMapping("/status")
    public Map<String, Object> getStatus() {
        ConnectorStatus status = vkConnector.getStatus();
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
        ConnectorResult result = vkConnector.start();
        return resultToMap(result);
    }

    @PostMapping("/stop")
    public Map<String, Object> stopClient() {
        ConnectorResult result = vkConnector.stop();
        return resultToMap(result);
    }

    // ==================== ПРОСЛУШКА ====================

    @Override
    @PostMapping("/startListening")
    public Map<String, Object> startListening() {
        ConnectorResult result = vkConnector.startListening();
        return resultToMap(result);
    }

    @Override
    @PostMapping("/stopListening")
    public Map<String, Object> stopListening() {
        ConnectorResult result = vkConnector.stopListening();
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
            ConnectorResult result = vkConnector.startScan(options);
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
            ConnectorConfig config = vkConnector.getConfig();

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
            if (body.containsKey("whitelistGroupIds")) {
                @SuppressWarnings("unchecked")
                List<Long> whitelist = ((List<Number>) body.get("whitelistGroupIds"))
                        .stream()
                        .map(Number::longValue)
                        .toList();
                config.whitelist = whitelist;
            }

            ConnectorResult result = vkConnector.updateConfig(config);
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
        ConnectorConfig config = vkConnector.getConfig();
        Map<String, Object> response = new HashMap<>();

        response.put("scanGroups", config.scanGroups);
        response.put("scanPersonal", config.scanPersonal);
        response.put("downloadMedia", config.downloadMedia);
        response.put("whitelistGroupIds", config.whitelist);
        response.put("success", true);

        return response;
    }

    // ==================== ВАЙТЛИСТ ====================

    @GetMapping("/whitelist")
    public Map<String, Object> getWhitelist() {
        Map<String, Object> response = new HashMap<>();
        ConnectorConfig config = vkConnector.getConfig();

        response.put("success", true);
        response.put("whitelistGroupIds", config.whitelist);
        return response;
    }

    @PostMapping("/whitelist")
    public Map<String, Object> updateWhitelist(@RequestBody Map<String, Object> body) {
        Map<String, Object> response = new HashMap<>();

        try {
            @SuppressWarnings("unchecked")
            List<Number> rawIds = (List<Number>) body.get("groupIds");

            if (rawIds == null) {
                response.put("success", false);
                response.put("message", "Поле 'groupIds' обязательно");
                return response;
            }

            List<Long> groupIds = rawIds.stream()
                    .map(Number::longValue)
                    .toList();

            ConnectorConfig config = vkConnector.getConfig();
            config.whitelist = groupIds;
            vkConnector.updateConfig(config);

            response.put("success", true);
            response.put("message", "Вайтлист обновлён: " + groupIds.size() + " групп");
            response.put("whitelistGroupIds", groupIds);

        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "Ошибка обновления вайтлиста: " + e.getMessage());
        }

        return response;
    }

    // ==================== ОТПРАВКА СООБЩЕНИЙ ====================

    /**
     * Отправка текстового сообщения
     * POST /api/vk/send/message
     * Body: { "peerId": 123, "text": "...", "replyTo": 0 }
     */
    @PostMapping("/send/message")
    public Map<String, Object> sendMessage(@RequestBody Map<String, Object> body) {
        Map<String, Object> response = new HashMap<>();

        try {
            Long peerId = extractLong(body.get("peerId"));
            String text = (String) body.get("text");
            Long replyTo = extractLong(body.getOrDefault("replyTo", 0L));

            if (peerId == null || peerId <= 0) {
                response.put("success", false);
                response.put("message", "Неверный peerId");
                return response;
            }

            if (text == null || text.isBlank()) {
                response.put("success", false);
                response.put("message", "Пустой текст сообщения");
                return response;
            }

            VkConnector.SendResult result = vkConnector.sendMessage(peerId, text,"", replyTo != null ? replyTo : 0);

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
     * POST /api/vk/send/file
     * Body: { "peerId": 123, "filePath": "/path/to/file", "caption": "...", "replyTo": 0 }
     */
    @PostMapping("/send/file")
    public Map<String, Object> sendFile(@RequestBody Map<String, Object> body) {
        Map<String, Object> response = new HashMap<>();

        try {
            Long peerId = extractLong(body.get("peerId"));
            String filePath = (String) body.get("filePath");
            String caption = (String) body.getOrDefault("caption", "");
            Long replyTo = extractLong(body.getOrDefault("replyTo", 0L));

            if (peerId == null || peerId <= 0) {
                response.put("success", false);
                response.put("message", "Неверный peerId");
                return response;
            }

            if (filePath == null || filePath.isBlank()) {
                response.put("success", false);
                response.put("message", "Не указан путь к файлу");
                return response;
            }

            VkConnector.SendResult result = vkConnector.sendFile(peerId, caption, filePath, null, replyTo != null ? replyTo : 0);

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
