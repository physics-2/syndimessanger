package ru.api;

import org.springframework.web.bind.annotation.*;
import ru.database.DatabaseManager;
import ru.database.MessengerConfigDto;

import java.util.List;

@RestController
@RequestMapping("/api/config")
@CrossOrigin(origins = "*") // Разрешаем запросы с фронтенда
public class ConfigController {

    private final DatabaseManager db;

    public ConfigController(DatabaseManager db) {
        this.db = db;
    }

    @GetMapping("/ids")
    public MessengerConfigDto getAllIds() {
        MessengerConfigDto config = new MessengerConfigDto();
        config.setVkIds(db.getVkIds());
        config.setMaxIds(db.getMaxIds());
        config.setTelegramIds(db.getTelegramIds());
        return config;
    }

    @PutMapping("/vk")
    public String updateVkIds(@RequestBody List<String> ids) {
        db.updateVkIds(ids);
        return "VK IDs обновлены";
    }

    @PutMapping("/max")
    public String updateMaxIds(@RequestBody List<String> ids) {
        db.updateMaxIds(ids);
        return "MAX IDs обновлены";
    }

    @PutMapping("/telegram")
    public String updateTelegramIds(@RequestBody List<String> ids) {
        db.updateTelegramIds(ids);
        return "Telegram IDs обновлены";
    }

}