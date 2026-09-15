package v2.api;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import v2.connectors.base.BaseConnector;
import v2.services.ChatService;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Заменяет текущий v2/api/ChatsController.java целиком.
 *
 * Добавлен GET /api/v2/groups — единая точка, откуда фронт берёт ВСЕ группы и беседы
 * СО ВСЕХ коннекторов одним запросом и, что важно, ДО сканирования:
 *   - TG  — из кэша чатов TDLib (UpdateNewChat приходит при старте клиента;
 *           если кэш ещё пуст, TgConnector сам добирает список через GetChats);
 *   - VK  — из messages.getConversations (беседы peer.type=chat и сообщества peer.type=group);
 *   - MAX — пустой список, пока у коннектора нет такого API (default-метод BaseConnector),
 *           фронт для него откатится на группы из базы.
 *
 * Spring сам соберёт List<BaseConnector> из всех @Component-коннекторов
 * (TgConnector, VkConnector, MaxConnector…), ничего регистрировать не нужно.
 */
@RestController
@RequestMapping("/api/v2")
public class ChatsController {

    private final ChatService chatService;
    private final List<BaseConnector> connectors;

    public ChatsController(ChatService chatService, List<BaseConnector> connectors) {
        this.chatService = chatService;
        this.connectors = connectors;
    }

    @GetMapping("/chats")
    public List<ChatService.ChatDto> getChats() {
        return chatService.getAllChatsForFrontend();
    }

    /**
     * GET /api/v2/groups — все группы/беседы по всем платформам, до сканирования.
     *
     * Ответ:
     * <pre>
     * {
     *   "success": true,
     *   "count": 42,
     *   "groups": {
     *     "tg":  [ { "id": -1001234567890, "title": "…", "type": "Группа", "membersCount": 123 } ],
     *     "vk":  [ { "id": 2000000012,     "title": "…", "type": "беседа", "membersCount": 5 } ],
     *     "max": []
     *   },
     *   "errors": { "vk": "VK не запущен. Вызовите start() сначала." }   // только для упавших платформ
     * }
     * </pre>
     *
     * Ключи приводятся к единой форме (id/title/type/membersCount), поэтому не важно,
     * что именно кладёт в мапу каждый коннектор — chatId, peerId или id: нормализация здесь.
     * Ошибка одной платформы не роняет весь ответ: для неё будет пустой список и запись в errors.
     */
    @GetMapping("/groups")
    public Map<String, Object> getGroups() {
        Map<String, Object> byPlatform = new LinkedHashMap<>();
        Map<String, String> errors = new LinkedHashMap<>();
        int total = 0;

        for (BaseConnector connector : connectors) {
            String platform = connector.platform();
            try {
                List<Map<String, Object>> normalized = new ArrayList<>();
                for (Map<String, Object> g : connector.getAllGroups()) {
                    Object id = firstNonBlank(g, "id", "chatId", "peerId", "peer_id", "groupId");
                    if (id == null) {
                        continue;                                    // запись без id фронту бесполезна
                    }
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("id", id);
                    Object title = firstNonBlank(g, "title", "name");
                    m.put("title", title != null ? title : "Без названия " + id);
                    Object type = firstNonBlank(g, "type");
                    if (type != null) {
                        m.put("type", type);
                    }
                    Object members = firstNonBlank(g, "membersCount", "members_count", "members");
                    if (members != null) {
                        m.put("membersCount", members);
                    }
                    normalized.add(m);
                }
                byPlatform.put(platform, normalized);
                total += normalized.size();
            } catch (Exception e) {
                byPlatform.put(platform, List.of());
                errors.put(platform, describe(e));
            }
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("success", true);
        out.put("count", total);
        out.put("groups", byPlatform);
        if (!errors.isEmpty()) {
            out.put("errors", errors);
        }
        return out;
    }

    /** Первое непустое значение из перечисленных ключей (null и пустые строки пропускаются). */
    private static Object firstNonBlank(Map<String, Object> map, String... keys) {
        for (String k : keys) {
            Object v = map.get(k);
            if (v == null) {
                continue;
            }
            if (v instanceof String s && s.isBlank()) {
                continue;
            }
            return v;
        }
        return null;
    }

    private static String describe(Exception e) {
        String m = e.getMessage();
        return (m == null || m.isBlank()) ? e.getClass().getSimpleName() : m;
    }
}