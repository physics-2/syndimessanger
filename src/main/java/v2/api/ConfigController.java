package v2.api;

import org.springframework.web.bind.annotation.*;
import v2.entity.Config;
import v2.services.ConfigService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * API контроллер для управления конфигурацией (синхронизация ID мессенджеров).
 * Соответствует функционалу ru.api.ConfigController, но использует сервисы v2.
 */
@RestController
@RequestMapping("/api/v2/config")
public class ConfigController {

    private final ConfigService configService;

    public ConfigController(ConfigService configService) {
        this.configService = configService;
    }

    /**
     * Получить все ID мессенджеров для синхронизации
     * GET /api/v2/config/ids
     */
    @GetMapping("/ids")
    public Map<String, Object> getAllIds() {
        Map<String, Object> response = new HashMap<>();
        try {
            Config config = configService.getConfig();
            response.put("success", true);
            response.put("vkIds", config.getVk_ids());
            response.put("tgIds", config.getTg_ids());
            response.put("maxIds", config.getMax_ids());
        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "Ошибка получения конфигурации: " + e.getMessage());
        }
        return response;
    }

    /**
     * Обновить VK IDs
     * PUT /api/v2/config/vk
     * Body: ["id1", "id2", ...]
     */
    @PutMapping("/vk")
    public Map<String, Object> updateVkIds(@RequestBody List<String> ids) {
        Map<String, Object> response = new HashMap<>();
        try {
            Config config = configService.getConfig();
            config.setVk_ids(ids != null ? ids : new java.util.ArrayList<>());
            configService.saveOrUpdateConfig(config);
            response.put("success", true);
            response.put("message", "VK IDs обновлены");
            response.put("vkIds", config.getVk_ids());
        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "Ошибка обновления VK IDs: " + e.getMessage());
        }
        return response;
    }

    /**
     * Обновить MAX IDs
     * PUT /api/v2/config/max
     * Body: ["id1", "id2", ...]
     */
    @PutMapping("/max")
    public Map<String, Object> updateMaxIds(@RequestBody List<String> ids) {
        Map<String, Object> response = new HashMap<>();
        try {
            Config config = configService.getConfig();
            config.setMax_ids(ids != null ? ids : new java.util.ArrayList<>());
            configService.saveOrUpdateConfig(config);
            response.put("success", true);
            response.put("message", "MAX IDs обновлены");
            response.put("maxIds", config.getMax_ids());
        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "Ошибка обновления MAX IDs: " + e.getMessage());
        }
        return response;
    }

    /**
     * Обновить Telegram IDs
     * PUT /api/v2/config/telegram
     * Body: ["id1", "id2", ...]
     */
    @PutMapping("/telegram")
    public Map<String, Object> updateTelegramIds(@RequestBody List<String> ids) {
        Map<String, Object> response = new HashMap<>();
        try {
            Config config = configService.getConfig();
            config.setTg_ids(ids != null ? ids : new java.util.ArrayList<>());
            configService.saveOrUpdateConfig(config);
            response.put("success", true);
            response.put("message", "Telegram IDs обновлены");
            response.put("tgIds", config.getTg_ids());
        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "Ошибка обновления Telegram IDs: " + e.getMessage());
        }
        return response;
    }

    /**
     * Получить полную конфигурацию
     * GET /api/v2/config/full
     */
    @GetMapping("/full")
    public Map<String, Object> getFullConfig() {
        Map<String, Object> response = new HashMap<>();
        try {
            Config config = configService.getConfig();
            response.put("success", true);
            response.put("config", config);
        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "Ошибка получения конфигурации: " + e.getMessage());
        }
        return response;
    }
}
