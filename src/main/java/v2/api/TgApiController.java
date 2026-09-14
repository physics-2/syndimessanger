package v2.api;

import org.springframework.web.bind.annotation.*;
import v2.api.base.BaseApiConnector;
import v2.connectors.base.ConnectorConfig;
import v2.connectors.base.ConnectorResult;
import v2.connectors.base.ConnectorStatus;
import v2.connectors.base.ScanOptions;
import v2.connectors.tg.TgConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/tg")
@CrossOrigin(origins = "*")
public class TgApiController implements BaseApiConnector {

    private final TgConnector tgConnector;

    public TgApiController(TgConnector tgConnector) {
        this.tgConnector = tgConnector;
    }

    // ==================== СТАТУС ====================

    @Override
    @GetMapping("/status")
    public Map<String, Object> getStatus() {
        ConnectorStatus status = tgConnector.getStatus();
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
        ConnectorResult result = tgConnector.start();
        return resultToMap(result);
    }

    @PostMapping("/stop")
    public Map<String, Object> stopClient() {
        ConnectorResult result = tgConnector.stop();
        return resultToMap(result);
    }

    // ==================== ПРОСЛУШКА ====================

    @Override
    @PostMapping("/startListening")
    public Map<String, Object> startListening() {
        ConnectorResult result = tgConnector.startListening();
        return resultToMap(result);
    }

    @Override
    @PostMapping("/stopListening")
    public Map<String, Object> stopListening() {
        ConnectorResult result = tgConnector.stopListening();
        return resultToMap(result);
    }

    // ==================== СКАНИРОВАНИЕ ====================

    @Override
    @PostMapping("/startScan")
    public Map<String, Object> startScan() {
        return startScanWithOptions(new ScanOptions(new ArrayList<>(),200));
    }

    @PostMapping("/startScanWithOptions")
    public Map<String, Object> startScanWithOptions(@RequestBody ScanOptions options) {
        try {
            ConnectorResult result = tgConnector.startScan(options);
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
            ConnectorConfig config = tgConnector.getConfig();

            // Парсим настройки из body
            if (body.containsKey("scanGroups")) {
                config.scanGroups=((Boolean) body.get("scanGroups"));
            }
            if (body.containsKey("scanPersonal")) {
                config.scanPersonal=((Boolean) body.get("scanPersonal"));
            }

            if (body.containsKey("downloadMedia")) {
                config.downloadMedia =((Boolean) body.get("downloadMedia"));
            }
            if (body.containsKey("whitelistGroupIds")) {
                @SuppressWarnings("unchecked")
                List<Long> whitelist = ((List<Number>) body.get("whitelistGroupIds"))
                        .stream()
                        .map(Number::longValue)
                        .toList();
                config.whitelist = whitelist;
            }

            ConnectorResult result = tgConnector.updateConfig(config);
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
        ConnectorConfig config = tgConnector.getConfig();
        Map<String, Object> response = new HashMap<>();

        response.put("scanGroups", config.scanGroups);
        response.put("scanPersonal", config.scanPersonal);
        response.put("downloadMedia", config.downloadMedia);
        response.put("whitelistGroupIds", config.whitelist);
        response.put("success", true);

        return response;
    }

    // ==================== ВАЙТЛИСТ ====================

    /**
     * Получить все группы и каналы для формирования вайтлиста
     */
    @GetMapping("/groups")
    public Map<String, Object> getAllGroups() {
        Map<String, Object> response = new HashMap<>();

        try {
            List<Map<String, Object>> groups = tgConnector.getAllGroups();

            response.put("success", true);
            response.put("count", groups.size());
            response.put("groups", groups);

            return response;
        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "Ошибка получения групп: " + e.getMessage());
            return response;
        }
    }

    /**
     * Получить текущий вайтлист
     */
    @GetMapping("/whitelist")
    public Map<String, Object> getWhitelist() {
        Map<String, Object> response = new HashMap<>();
        ConnectorConfig config = tgConnector.getConfig();

        response.put("success", true);
        response.put("whitelistGroupIds", config.whitelist);
        return response;
    }

    /**
     * Обновить вайтлист (список ID групп)
     */
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

            ConnectorConfig config = tgConnector.getConfig();
            config.whitelist = groupIds;
            tgConnector.updateConfig(config);

            response.put("success", true);
            response.put("message", "Вайтлист обновлён: " + groupIds.size() + " групп");
            response.put("whitelistGroupIds", groupIds);

        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "Ошибка обновления вайтлиста: " + e.getMessage());
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
}