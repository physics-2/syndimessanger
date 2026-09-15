package v2.api;

import org.springframework.web.bind.annotation.*;
import v2.api.base.IncomingMaxMessage;
import v2.services.MaxIngestService;


import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Новый контроллер — MaxApiController трогать НЕ нужно.
 *
 *   POST /api/max/incoming        <- сюда шлёт python (send_to_java)
 *   POST /api/max/incoming/batch  <- пачками, если захотите
 *   PUT  /api/config/max          <- сюда шлёт python (on_start) json=[MY_USER_ID]
 *   GET  /api/config/max          <- проверить, что зарегистрировалось
 *
 * Почему /api/config/max здесь, а не в MaxApiController:
 * у того класс-уровневый @RequestMapping("/api/max"), а python зовёт путь
 * ВНЕ этого префикса, поэтому маппинг обязан быть абсолютным.
 */
@RestController
@CrossOrigin(origins = "*")
public class MaxIncomingController {

    private final MaxIngestService ingestService;

    public MaxIncomingController(MaxIngestService ingestService) {
        this.ingestService = ingestService;
    }

    // ==================== ВХОДЯЩИЕ ИЗ PYTHON ====================

    @PostMapping("/api/max/incoming")
    public Map<String, Object> incoming(@RequestBody IncomingMaxMessage dto) {
        Map<String, Object> response = new HashMap<>();
        try {
            long chatId    = toLong(dto.getChatId(), 0L);
            long messageId = toLong(dto.getMessageId(), 0L);
            long authorId  = toLong(dto.getAuthorId(), 0L);

            if (chatId <= 0) {
                response.put("success", false);
                response.put("message", "Неверный chatId: " + dto.getChatId());
                return response;
            }

            boolean saved = ingestService.ingestRaw(
                    chatId,
                    dto.getChatTitle(),
                    Boolean.TRUE.equals(dto.getIsGroup()),
                    messageId,
                    authorId,
                    dto.getText(),
                    dto.getMediaUrl(),                       // JSON-строка '["url1",...]'
                    parseTimestamp(dto.getTimestamp()),
                    Boolean.TRUE.equals(dto.getIsOutgoing()),
                    dto.getFirstName(),
                    dto.getLastName(),
                    dto.getPhoneNumber(),
                    dto.getAvatarPic()
            );

            response.put("success", saved);
            response.put("messageId", messageId);
            response.put("chatId", chatId);
            response.put("message", saved ? "Принято" : "Сообщение отброшено (см. лог сервера)");

        } catch (Exception e) {
            // Логируем стек: иначе python увидит только "Ошибка ... : null"
            e.printStackTrace();
            response.put("success", false);
            response.put("message", "Ошибка обработки входящего сообщения: " + e.getMessage());
        }
        return response;
    }

    /** Массовый приём: [{...},{...}] */
    @PostMapping("/api/max/incoming/batch")
    public Map<String, Object> incomingBatch(@RequestBody List<IncomingMaxMessage> batch) {
        int ok = 0;
        List<String> errors = new ArrayList<>();

        if (batch != null) {
            for (IncomingMaxMessage m : batch) {
                Map<String, Object> r = incoming(m);
                if (Boolean.TRUE.equals(r.get("success"))) {
                    ok++;
                } else {
                    errors.add(String.valueOf(r.get("message")));
                }
            }
        }

        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("accepted", ok);
        response.put("total", batch == null ? 0 : batch.size());
        response.put("errors", errors);
        return response;
    }

    // ==================== РЕГИСТРАЦИЯ myUserId ====================

    /** PUT /api/config/max   body: [123456789]  (python: idsList = [MY_USER_ID]) */
    @PutMapping("/api/config/max")
    public Map<String, Object> registerMaxUser(@RequestBody(required = false) List<Object> ids) {
        Map<String, Object> response = new HashMap<>();

        List<Long> parsed = new ArrayList<>();
        if (ids != null) {
            for (Object o : ids) {
                Long v = toLong(o, null);
                if (v != null && v > 0) parsed.add(v);
            }
        }

        if (parsed.isEmpty()) {
            response.put("success", false);
            response.put("message", "Пустой список userId");
            return response;
        }

        long myUserId = parsed.get(0);
        ingestService.setMyUserId(myUserId);

        response.put("success", true);
        response.put("myUserId", myUserId);
        response.put("message", "OK: " + myUserId);
        return response;
    }

    @GetMapping("/api/config/max")
    public Map<String, Object> getRegisteredMaxUser() {
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("myUserId", ingestService.getMyUserId());
        return response;
    }

    // ==================== ПАРСЕРЫ ====================

    /**
     * pymax может отдать epoch millis (int), epoch seconds или строку.
     * Приводим к миллисекундам — Message.timestamp у вас Long.
     */
    private static long parseTimestamp(Object ts) {
        if (ts == null) return System.currentTimeMillis();

        if (ts instanceof Number n) {
            long v = n.longValue();
            return v < 100_000_000_000L ? v * 1000L : v;   // секунды -> миллисекунды
        }

        String s = String.valueOf(ts).trim();
        if (s.isEmpty()) return System.currentTimeMillis();

        try {
            long v = Long.parseLong(s);
            return v < 100_000_000_000L ? v * 1000L : v;
        } catch (NumberFormatException ignored) { }

        try {
            return Instant.parse(s).toEpochMilli();
        } catch (Exception ignored) { }

        try {   // "2026-09-15 12:34:56"
            return java.time.LocalDateTime.parse(s.replace(' ', 'T')).atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
        } catch (Exception ignored) { }

        return System.currentTimeMillis();
    }

    private static Long toLong(Object value, Long def) {
        if (value instanceof Number n) return n.longValue();
        if (value instanceof String s) {
            try { return Long.parseLong(s.trim()); } catch (NumberFormatException e) { return def; }
        }
        return def;
    }
}
