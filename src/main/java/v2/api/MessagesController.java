package v2.api;

import org.springframework.web.bind.annotation.*;
import v2.entity.Message;
import v2.services.MessageService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * История сообщений для фронтенда (messenger.html).
 *
 * Использует ТОЛЬКО публичные методы MessageService:
 *   - getMessagesForChat(Long chatId) → List<MessageDto>
 *   - searchMessages(String query)    → List<Message>
 * Никаких новых методов в репозиториях не требуется.
 *
 * Всегда отвечает HTTP 200 и кладёт результат в {success, data, ...} — как остальные ваши
 * контроллеры. 404 отсюда прийти может только если маппинг не зарегистрирован.
 *
 * ОДИН И ТОТ ЖЕ обработчик повешен на несколько путей — чтобы не зависеть от того,
 * какой шаблон настроен во фронте:
 *   GET /api/v2/chats/{chatId}/messages?limit=200&before=1712345678901
 *   GET /api/v2/messages/chat/{chatId}?limit=200&before=1712345678901
 *   GET /api/v2/messages?chatId=6161645382&limit=200&before=...
 *
 * ДИАГНОСТИКА (если фронт пишет «404»):
 *   GET /api/v2/messages/ping                  → контроллер поднят?
 *   GET /api/v2/messages/ping?chatId=6161645382 → сколько сообщений найдётся по этому chatId
 */
@RestController
@RequestMapping("/api/v2")
@CrossOrigin(origins = "*")
public class MessagesController {

    /** Сколько сообщений отдаём, если limit не указан. */
    private static final int DEFAULT_LIMIT = 200;
    private static final int MAX_LIMIT = 1000;

    private final MessageService messageService;

    public MessagesController(MessageService messageService) {
        this.messageService = messageService;
    }

    // ==================== ДИАГНОСТИКА ====================

    /**
     * Проверка, что контроллер вообще зарегистрирован.
     * GET /api/v2/messages/ping
     * GET /api/v2/messages/ping?chatId=6161645382
     */
    @GetMapping("/messages/ping")
    public Map<String, Object> ping(@RequestParam(required = false) Long chatId) {
        Map<String, Object> r = new HashMap<>();
        r.put("success", true);
        r.put("controller", "MessagesController");
        r.put("message", "Маппинг /api/v2/messages/** зарегистрирован");
        if (chatId != null) {
            try {
                List<MessageService.MessageDto> all = messageService.getMessagesForChat(chatId);
                r.put("chatId", chatId);
                r.put("messagesFound", all.size());
                if (!all.isEmpty()) {
                    MessageService.MessageDto first = all.get(0);
                    r.put("firstMessageText", first.msg().getText());
                    r.put("firstMessageTimestamp", first.msg().getTimestamp());
                    r.put("firstAuthorName", first.authorName());
                }
            } catch (Exception e) {
                r.put("messagesFound", -1);
                r.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        }
        return r;
    }

    // ==================== ИСТОРИЯ ЧАТА ====================

    /**
     * Сообщения чата. chatId — это Chat.chatId (идентификатор чата в соцсети), НЕ PK таблицы chats.
     * Именно его отдаёт ChatDto.chatId и именно с ним работает MessageService.getMessagesForChat.
     *
     * @param limit  сколько последних сообщений вернуть (по умолчанию 200, максимум 1000)
     * @param before курсор «вверх»: вернуть только сообщения старше этого timestamp (мс)
     */
    @GetMapping({"/chats/{chatId}/messages", "/messages/chat/{chatId}"})
    public Map<String, Object> getMessages(@PathVariable Long chatId,
                                           @RequestParam(required = false, defaultValue = "" + DEFAULT_LIMIT) Integer limit,
                                           @RequestParam(required = false) Long before) {
        return messages(chatId, limit, before);
    }

    /**
     * Тот же результат, но chatId передаётся параметром запроса:
     * GET /api/v2/messages?chatId=6161645382&limit=200
     */
    @GetMapping("/messages")
    public Map<String, Object> getMessagesByParam(@RequestParam Long chatId,
                                                  @RequestParam(required = false, defaultValue = "" + DEFAULT_LIMIT) Integer limit,
                                                  @RequestParam(required = false) Long before) {
        return messages(chatId, limit, before);
    }

    private Map<String, Object> messages(Long chatId, Integer limit, Long before) {
        Map<String, Object> response = new HashMap<>();
        try {
            if (chatId == null) {
                response.put("success", false);
                response.put("message", "Не указан chatId");
                response.put("data", List.of());
                return response;
            }

            int lim = (limit == null || limit <= 0) ? DEFAULT_LIMIT : Math.min(limit, MAX_LIMIT);

            List<MessageService.MessageDto> all = messageService.getMessagesForChat(chatId);

            // курсор «вверх»: оставляем только то, что старше before
            List<MessageService.MessageDto> filtered = all;
            if (before != null && before > 0) {
                filtered = new ArrayList<>(all.size());
                for (MessageService.MessageDto dto : all) {
                    if (ts(dto) < before) {
                        filtered.add(dto);
                    }
                }
            }

            // хвост списка = самые свежие (MessageService сортирует по timestamp ASC)
            int from = Math.max(0, filtered.size() - lim);
            List<MessageService.MessageDto> page = new ArrayList<>(filtered.subList(from, filtered.size()));

            response.put("success", true);
            response.put("data", page);
            response.put("count", page.size());
            response.put("total", all.size());
            response.put("hasMore", from > 0);
            response.put("chatId", chatId);
        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "Ошибка получения сообщений: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            response.put("data", List.of());
        }
        return response;
    }

    // ==================== ПОИСК ====================

    /**
     * Глобальный поиск по тексту: GET /api/v2/messages/search?query=привет&limit=50
     * Возвращает «плоские» Message (без имени автора) — MessageService.searchMessages отдаёт именно их.
     */
    @GetMapping("/messages/search")
    public Map<String, Object> searchMessages(@RequestParam(required = false, defaultValue = "") String query,
                                              @RequestParam(required = false, defaultValue = "50") Integer limit) {
        Map<String, Object> response = new HashMap<>();
        try {
            if (query == null || query.isBlank()) {
                response.put("success", true);
                response.put("data", List.of());
                response.put("count", 0);
                return response;
            }
            int lim = Math.min(Math.max(limit == null ? 50 : limit, 1), 500);

            List<Message> found = messageService.searchMessages(query.trim());
            List<Message> page = found.size() > lim ? new ArrayList<>(found.subList(0, lim)) : found;

            response.put("success", true);
            response.put("data", page);
            response.put("count", page.size());
            response.put("total", found.size());
        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "Ошибка поиска: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            response.put("data", List.of());
        }
        return response;
    }

    /**
     * Поиск внутри одного чата: GET /api/v2/chats/{chatId}/messages/search?query=привет
     * Здесь уже есть имена авторов, т.к. идём через getMessagesForChat.
     */
    @GetMapping({"/chats/{chatId}/messages/search", "/messages/chat/{chatId}/search"})
    public Map<String, Object> searchInChat(@PathVariable Long chatId,
                                            @RequestParam(required = false, defaultValue = "") String query) {
        Map<String, Object> response = new HashMap<>();
        try {
            String q = query == null ? "" : query.toLowerCase().trim();
            List<MessageService.MessageDto> hits = new ArrayList<>();
            for (MessageService.MessageDto dto : messageService.getMessagesForChat(chatId)) {
                String text = dto.msg() != null ? dto.msg().getText() : null;
                if (text != null && (q.isEmpty() || text.toLowerCase().contains(q))) {
                    hits.add(dto);
                }
            }
            response.put("success", true);
            response.put("data", hits);
            response.put("count", hits.size());
            response.put("chatId", chatId);
        } catch (Exception e) {
            response.put("success", false);
            response.put("message", "Ошибка поиска в чате: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            response.put("data", List.of());
        }
        return response;
    }

    // ==================== УТИЛИТЫ ====================

    /** Message.timestamp — Long в миллисекундах (System.currentTimeMillis()). Терпим секунды и null. */
    private long ts(MessageService.MessageDto dto) {
        if (dto == null || dto.msg() == null || dto.msg().getTimestamp() == null) {
            return 0L;
        }
        long v = dto.msg().getTimestamp();
        return v < 1_000_000_000_000L ? v * 1000L : v;
    }
}