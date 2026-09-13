package ru.api;

import org.springframework.web.bind.annotation.*;
import ru.VK.VkConnector;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/vk")
public class VkController {

    private final VkConnector vk;

    public VkController(VkConnector vk) {
        this.vk = vk;
    }

    // ================= СТАТУС =================
    @GetMapping("/status")
    public Map<String, Object> status() {
        return Map.of(
                "running", vk.isLongPollRunning(),
                "scanning", vk.isScanning(),
                "whitelist", vk.getWhitelistGroupIds(),
                "config", Map.of("scanPersonalMessages", vk.isScanPersonalMessages())
        );
    }

    // ================= ПРОСЛУШКА =================
    @PostMapping("/listen/start")
    public Map<String, Object> listenStart() {
        vk.startUserLongPoll();
        return Map.of("status", "ok", "running", true);
    }

    @PostMapping("/listen/stop")
    public Map<String, Object> listenStop() {
        vk.stopUserLongPoll();
        return Map.of("status", "ok", "running", false);
    }

    // ================= СКАНИРОВАНИЕ =================
    @PostMapping("/doSync")
    public Map<String, Object> scan(@RequestBody(required = false) List<Long> ids) {
        List<Long> target = (ids != null && !ids.isEmpty()) ? ids : vk.getWhitelistGroupIds();
        vk.scanSpecificGroups(target);
        return Map.of("status", "ok", "message", "Сканирование запущено", "groups", target.size());
    }

    // ================= СИНХРОНИЗАЦИЯ =================
    @PostMapping("/scan")
    public Map<String, Object> sync() {
        vk.initialSync();
        return Map.of("status", "ok", "message", "Начальная синхронизация запущена");
    }

    // ================= ВАЙТЛИСТ =================
    @GetMapping("/groups")
    public List<Long> getGroups() {
        return vk.getWhitelistGroupIds();
    }

    @PutMapping("/whitelist")
    public Map<String, Object> setWhitelist(@RequestBody List<Long> ids) {
        vk.setWhitelistGroupIds(ids);
        return Map.of("status", "ok", "whitelist", ids);
    }

    @DeleteMapping("/whitelist")
    public Map<String, Object> clearWhitelist() {
        vk.setWhitelistGroupIds(List.of());
        return Map.of("status", "ok", "message", "Вайтлист очищен");
    }

    // ================= КОНФИГУРАЦИЯ (ТО, ЧЕГО НЕ ХВАТАЛО) =================
    @PutMapping("/config")
    public Map<String, Object> config(@RequestBody Map<String, Object> body) {
        if (body.containsKey("scanPersonalMessages")) {
            vk.setScanPersonalMessages(Boolean.parseBoolean(String.valueOf(body.get("scanPersonalMessages"))));
        }
        if (body.containsKey("scanGroups")) {
            // если во VkConnector есть такое поле
            // vk.setScanGroups(Boolean.parseBoolean(String.valueOf(body.get("scanGroups"))));
        }
        return Map.of(
                "status", "ok",
                "changed", body.keySet(),
                "config", Map.of("scanPersonalMessages", vk.isScanPersonalMessages())
        );
    }

    @PostMapping("/config")
    public Map<String, Object> configPost(@RequestBody Map<String, Object> body) {
        return config(body);  // дублёр для унификации с MAX/TG
    }


}