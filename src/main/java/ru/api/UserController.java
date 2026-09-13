package ru.api;

import org.springframework.web.bind.annotation.*;
import ru.database.DatabaseManager;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/users")
@CrossOrigin(origins = "*")
public class UserController {

    private final DatabaseManager db;

    public UserController(DatabaseManager db) {
        this.db = db;
    }

    @GetMapping("/{source}/{userId}/tags")
    public List<String> getUserTags(@PathVariable String source, @PathVariable Long userId) {
        return db.getUserTags(source, userId);
    }

    @PostMapping("/{source}/{userId}/tags")
    public String addUserTag(@PathVariable String source, @PathVariable Long userId, @RequestBody Map<String, String> payload) {
        String tag = payload.get("tag");
        if (tag != null && !tag.trim().isEmpty()) {
            db.addUserTag(source, userId, tag.trim());
            return "{\"status\": \"ok\"}";
        }
        return "{\"status\": \"error\", \"message\": \"Пустой тег\"}";
    }

    @DeleteMapping("/{source}/{userId}/tags")
    public String removeUserTag(@PathVariable String source, @PathVariable Long userId, @RequestParam String tag) {
        if (tag != null && !tag.trim().isEmpty()) {
            db.removeUserTag(source, userId, tag.trim()); // См. пункт 2 ниже
            return "{\"status\": \"ok\"}";
        }
        return "{\"status\": \"error\", \"message\": \"Пустой тег\"}";
    }
}