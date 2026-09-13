package ru.api;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import ru.TG.TgConnector;
import ru.send.SendResult;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/tg")
@CrossOrigin("*")
public class TgController {

    private final TgConnector tg;

    public TgController(TgConnector tg) {
        this.tg = tg;
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        return Map.of(
                "started", tg.isStarted(),
                "listening", tg.isListening(),
                "scanning", tg.isScanning(),
                "myUserId", tg.getMyUserId(),
                "config", tg.getConfig()
        );
    }

    @PostMapping("/start")
    public Map<String, Object> start() {
        return Map.of("status", "ok", "message", tg.start());
    }

    @PostMapping("/stop")
    public Map<String, Object> stop() {
        tg.stop();
        return Map.of("status", "ok", "message", "Клиент остановлен");
    }

    @PostMapping("/scan")
    public Map<String, Object> scan() {
        return Map.of("status", "ok", "message", tg.startScan());
    }

    @PostMapping("/listen/start")
    public Map<String, Object> listenStart() {
        tg.startListening();
        return Map.of("status", "ok", "listening", true);
    }

    @PostMapping("/listen/stop")
    public Map<String, Object> listenStop() {
        tg.stopListening();
        return Map.of("status", "ok", "listening", false);
    }

    @PutMapping("/config")
    public Map<String, Object> config(@RequestBody Map<String, Object> body) {
        if (body.containsKey("scanGroups")) tg.setScanGroups(Boolean.parseBoolean(String.valueOf(body.get("scanGroups"))));
        if (body.containsKey("scanPersonal")) tg.setScanPersonal(Boolean.parseBoolean(String.valueOf(body.get("scanPersonal"))));
        if (body.containsKey("scanSavedMessages")) tg.setScanSavedMessages(Boolean.parseBoolean(String.valueOf(body.get("scanSavedMessages"))));
        if (body.containsKey("downloadMedia")) tg.setDownloadMedia(Boolean.parseBoolean(String.valueOf(body.get("downloadMedia"))));
        return Map.of("status", "ok", "changed", body.keySet(), "config", tg.getConfig());
    }

    @PutMapping("/whitelist")
    public Map<String, Object> whitelist(@RequestBody List<Long> ids) {
        tg.setWhitelistGroupIds(ids);
        return Map.of("status", "ok", "whitelist", ids);
    }

    @DeleteMapping("/whitelist")
    public Map<String, Object> clearWhitelist() {
        tg.setWhitelistGroupIds(List.of());
        return Map.of("status", "ok", "message", "Вайтлист очищен");
    }

    // =========================================================================================
    // 📤 ОТПРАВКА СООБЩЕНИЙ  (НОВЫЕ ЭНДПОИНТЫ)
    // =========================================================================================

    /**
     * Тело запроса для отправки текста.
     * chatId — числовой id чата TDLib; username — альтернатива (@name, t.me/name, "saved").
     */
    public record SendRequest(Long chatId, String username, String text,
                              Long replyTo, Boolean markdown) {}

    /** POST /api/tg/send — отправка текста. */
    @PostMapping("/send")
    public Map<String, Object> send(@RequestBody SendRequest req) {
        long chatId = resolveChatId(req);
        if (chatId == 0) {
            return Map.of("status", "error",
                    "message", "Не удалось определить чат (укажите chatId или username)");
        }

        SendResult r = tg.sendMessage(
                chatId,
                req.text(),
                req.replyTo() != null ? req.replyTo() : 0L,
                req.markdown() != null && req.markdown());

        return r.ok()
                ? Map.of("status", "ok", "messageId", r.messageId(), "chatId", r.chatId())
                : Map.of("status", "error", "message", nvl(r.error()));
    }

    /**
     * POST /api/tg/send/media — отправка файла (multipart/form-data).
     * Поля: file (обязательно), chatId | username, caption, replyTo, markdown.
     */
    @PostMapping(value = "/send/media", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Map<String, Object> sendMedia(@RequestPart("file") MultipartFile[] files,
                                         @RequestParam(required = false) Long chatId,
                                         @RequestParam(required = false) String username,
                                         @RequestParam(required = false) String caption,
                                         @RequestParam(required = false) Long replyTo,
                                         @RequestParam(required = false, defaultValue = "false") boolean markdown) {
        long target = resolveChatId(new SendRequest(chatId, username, caption, replyTo, markdown));
        if (target == 0) {
            return Map.of("status", "error",
                    "message", "Не удалось определить чат (укажите chatId или username)");
        }
        if (files == null || files.length == 0 || files[0].isEmpty()) {
            return Map.of("status", "error", "message", "Пустой файл");
        }

        long reply = replyTo != null ? replyTo : 0L;
        int sent = 0;
        long lastMessageId = 0;
        StringBuilder errors = new StringBuilder();

        try {
            for (int i = 0; i < files.length; i++) {
                MultipartFile f = files[i];
                if (f == null || f.isEmpty()) continue;

                SendResult r = tg.sendFileBytes(
                                target,
                                f.getBytes(),
                                f.getOriginalFilename(),
                                f.getContentType(),
                                i == 0 ? nvl(caption) : "",   // подпись только к первому файлу
                                reply,
                                markdown)
                        .get();

                if (r.ok()) {
                    sent++;
                    lastMessageId = r.messageId();
                } else {
                    errors.append(nvl(r.error())).append("; ");
                }
            }
        } catch (Exception e) {
            return Map.of("status", "error", "message", "Ошибка загрузки файла: " + e.getMessage());
        }

        if (sent == 0) return Map.of("status", "error", "message", errors.toString());
        return Map.of("status", "ok", "messageId", lastMessageId, "chatId", target, "sent", sent);
    }

    /** POST /api/tg/send/saved — быстрая заметка себе в "Избранное". */
    @PostMapping("/send/saved")
    public Map<String, Object> sendSaved(@RequestBody SendRequest req) {
        SendResult r = tg.sendToSavedMessages(req.text());
        return r.ok()
                ? Map.of("status", "ok", "messageId", r.messageId(), "chatId", r.chatId())
                : Map.of("status", "error", "message", nvl(r.error()));
    }

    /** GET /api/tg/resolve?username=@durov — узнать chatId по юзернейму. */
    @GetMapping("/resolve")
    public Map<String, Object> resolve(@RequestParam String username) {
        long id = tg.resolveTarget(username);
        return id != 0
                ? Map.of("status", "ok", "username", username, "chatId", id)
                : Map.of("status", "error", "message", "Чат не найден: " + username);
    }

    /** GET /api/tg/chats — чаты из кэша (для отправки туда, куда ещё не писали). */
    @GetMapping("/chats")
    public List<Map<String, Object>> chats() {
        return tg.getCachedChats().stream()
                .map(c -> Map.<String, Object>of(
                        "id", c.id(),
                        "title", c.title(),
                        "isGroup", c.isGroup(),
                        "isChannel", c.isChannel(),
                        "isSavedMessages", c.isSavedMessages()))
                .toList();
    }

    private long resolveChatId(SendRequest req) {
        if (req.chatId() != null && req.chatId() != 0) return req.chatId();
        if (req.username() != null && !req.username().isBlank()) return tg.resolveTarget(req.username());
        return 0;
    }

    private static String nvl(String s) {
        return s != null ? s : "";
    }
}
