package ru.api;

import org.springframework.web.bind.annotation.*;
import ru.database.DatabaseManager;
import ru.database.UnifiedMessage;
import ru.database.UserInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*") // Разрешаем запросы с нашего фронтенда
public class ApiController {

    private final DatabaseManager db;

    public ApiController(DatabaseManager db) {
        this.db = db;
    }

    // Получить всех пользователей (для боковой панели)
    @GetMapping("/users")
    public List<UserInfo> getAllUsers() {
        // Добавим этот простой метод в DatabaseManager (см. ниже)
        return db.getAllUsers();
    }


    @GetMapping("/chats")
    public List<Map<String, Object>> getAllChats() {
        return db.getAllChatsWithInfo();
    }

    // Получение истории чата с именами авторов
    @GetMapping("/messages/{chatId}")
    public List<Map<String, Object>> getMessages(@PathVariable long chatId) {
        String source = db.getSourceForChat(chatId);
        System.out.println("📤 API: Запрашиваю сообщения для чата " + chatId + " (source: " + source + ")");
        return db.getChatHistoryWithAuthors(source, chatId, 100);
    }

    /**
     * Запустить заполнение названий личных чатов
     */
    @PostMapping("/chats/fill-titles")
    public Map<String, Object> fillChatTitles(@RequestParam(required = false) Long myUserId) {
        Map<String, Object> response = new HashMap<>();

        if (myUserId == null) {
            // Если ID не передан, пробуем найти его в сообщениях
            response.put("status", "error");
            response.put("message", "Укажите ваш userId параметром ?myUserId=...");
            return response;
        }

        System.out.println("🔄 Запуск заполнения названий чатов для пользователя: " + myUserId);
        db.fillPersonalChatTitles(myUserId);

        response.put("status", "ok");
        response.put("message", "Названия чатов обновлены");
        return response;
    }

    /**
     * Получить статистику чатов без названий
     */
    @GetMapping("/chats/without-titles")
    public List<Map<String, Object>> getChatsWithoutTitles() {
        return db.getChatsWithoutTitles();
    }

    @PostMapping("/max/incoming")
    public String receiveMaxMessage(@RequestBody Map<String, Object> payload) {
        try {
            long authorId = ((Number) payload.get("authorId")).longValue();
            long chatId = ((Number) payload.get("chatId")).longValue();
            long messageId = ((Number) payload.get("messageId")).longValue();
            long timestamp = ((Number) payload.get("timestamp")).longValue();
            String text = (String) payload.get("text");
            String mediaUrl = (String) payload.getOrDefault("mediaUrl", "");

            String firstName = (String) payload.getOrDefault("firstName", "MAX User");
            String lastName = (String) payload.getOrDefault("lastName", "");
            String phoneNumber = (String) payload.getOrDefault("phoneNumber", "");
            String avatarPicId = (String) payload.getOrDefault("avatarPic", "");
            System.out.println("Phone number: "+phoneNumber);
            boolean isGroup = Boolean.parseBoolean(payload.getOrDefault("isGroup", "false").toString());

            String chatTitle = (String) payload.getOrDefault("chatTitle", "");
            UserInfo userInfo = new UserInfo("max", authorId, firstName, lastName, "", avatarPicId, phoneNumber,List.of());
            db.saveOrUpdateUser(userInfo);
            db.saveOrUpdateChat("max", chatId, chatTitle, isGroup);
            UnifiedMessage msg = new UnifiedMessage("max", messageId, chatId, authorId, text, timestamp,mediaUrl);
            db.saveMessage(msg);

            System.out.println("✅ [API] Успешно сохранено: Чат=" + chatId + ", Автор=" + authorId + ", Текст=" + (text != null ? text.substring(0, Math.min(20, text.length())) : "null"));
            return "{\"status\":\"ok\"}";

        } catch (Exception e) {
            System.err.println("❌ Ошибка приема MAX сообщения: " + e.getMessage());
            e.printStackTrace(); // Раскомментируйте, чтобы видеть полный стек ошибки в консоли
            return "{\"status\":\"error\"}";
        }
    }
}