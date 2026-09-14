package v2.api;

import org.springframework.web.bind.annotation.*;
import v2.entity.User;
import v2.services.UserService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * API контроллер для управления пользователями и тегами.
 * Соответствует функционалу ru.api.UserController, но использует сервисы v2.
 */
@RestController
@RequestMapping("/api/v2/users")
@CrossOrigin(origins = "*")
public class UserController {

    private final UserService userService;

    public UserController(UserService userService) {
        this.userService = userService;
    }

    /**
     * Получить все теги пользователя
     * GET /api/v2/users/{source}/{userId}/tags
     */
    @GetMapping("/{source}/{userId}/tags")
    public Map<String, Object> getUserTags(@PathVariable String source, @PathVariable Long userId) {
        Map<String, Object> response = new HashMap<>();
        try {
            List<String> tags = userService.getUser(source, userId)
                    .map(User::getTags)
                    .orElse(List.of());
            response.put("success", true);
            response.put("tags", tags);
        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "Ошибка получения тегов: " + e.getMessage());
        }
        return response;
    }

    /**
     * Добавить тег пользователю
     * POST /api/v2/users/{source}/{userId}/tags
     * Body: { "tag": "tagName" }
     */
    @PostMapping("/{source}/{userId}/tags")
    public Map<String, Object> addUserTag(@PathVariable String source, @PathVariable Long userId,
                                          @RequestBody Map<String, String> payload) {
        Map<String, Object> response = new HashMap<>();
        try {
            String tag = payload.get("tag");
            if (tag == null || tag.trim().isEmpty()) {
                response.put("success", false);
                response.put("message", "Пустой тег");
                return response;
            }

            List<String> tags = userService.addTag(source, userId, tag.trim());
            response.put("success", true);
            response.put("tags", tags);
            response.put("message", "Тег добавлен");
        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "Ошибка добавления тега: " + e.getMessage());
        }
        return response;
    }

    /**
     * Удалить тег у пользователя
     * DELETE /api/v2/users/{source}/{userId}/tags?tag=tagName
     */
    @DeleteMapping("/{source}/{userId}/tags")
    public Map<String, Object> removeUserTag(@PathVariable String source, @PathVariable Long userId,
                                             @RequestParam String tag) {
        Map<String, Object> response = new HashMap<>();
        try {
            if (tag == null || tag.trim().isEmpty()) {
                response.put("success", false);
                response.put("message", "Пустой тег");
                return response;
            }

            List<String> tags = userService.removeTag(source, userId, tag.trim());
            response.put("success", true);
            response.put("tags", tags);
            response.put("message", "Тег удалён");
        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "Ошибка удаления тега: " + e.getMessage());
        }
        return response;
    }

    /**
     * Поиск пользователей по имени/username
     * GET /api/v2/users/search?query=...
     */
    @GetMapping("/search")
    public Map<String, Object> searchUsers(@RequestParam String query) {
        Map<String, Object> response = new HashMap<>();
        try {
            List<User> users = userService.searchUsers(query);
            response.put("success", true);
            response.put("count", users.size());
            response.put("users", users);
        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "Ошибка поиска: " + e.getMessage());
        }
        return response;
    }

    /**
     * Получить всех пользователей с определённым тегом
     * GET /api/v2/users/by-tag?tag=tagName
     */
    @GetMapping("/by-tag")
    public Map<String, Object> getUsersByTag(@RequestParam String tag) {
        Map<String, Object> response = new HashMap<>();
        try {
            // Можно добавить метод в UserService для поиска по тегу
            response.put("success", true);
            response.put("message", "Поиск по тегу: " + tag);
            // Пока возвращаем пустой список - можно расширить
            response.put("users", List.of());
        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "Ошибка поиска по тегу: " + e.getMessage());
        }
        return response;
    }
}
