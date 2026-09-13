package ru.api;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import ru.send.MessageRouter;
import ru.send.SendFile;
import ru.send.SendRequest;
import ru.send.SendResult;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Единая точка отправки сообщений для фронтенда.
 * Фронт не думает о платформах: один URL, поле platform решает всё.
 *
 *   POST /api/send         JSON      { platform, chatId, text, replyTo, markdown, username }
 *   POST /api/send/media   multipart platform, chatId, text, replyTo, markdown, username, file[]
 *   POST /api/send/bulk    JSON      { messages: [ SendRequest... ] }   — рассылка
 */
@RestController
@RequestMapping("/api/send")
@CrossOrigin("*")
public class SendController {

    private final MessageRouter router;

    public SendController(MessageRouter router) {
        this.router = router;
    }

    @PostMapping
    public Map<String, Object> send(@RequestBody SendRequest req) {
        return toResponse(router.send(req));
    }

    /**
     * Отправка одного или нескольких файлов.
     * Фронтенд кладёт их в поле "file" несколько раз — Spring соберёт в массив.
     */
    @PostMapping(value = "/media", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Map<String, Object> sendMedia(@RequestPart("file") MultipartFile[] files,
                                         @RequestParam(defaultValue = "tg") String platform,
                                         @RequestParam(required = false) Long chatId,
                                         @RequestParam(required = false) String username,
                                         @RequestParam(required = false) String text,
                                         @RequestParam(required = false) Long replyTo,
                                         @RequestParam(required = false, defaultValue = "false") boolean markdown) {

        List<MultipartFile> valid = new ArrayList<>();
        if (files != null) {
            for (MultipartFile f : files) {
                if (f != null && !f.isEmpty()) valid.add(f);
            }
        }
        if (valid.isEmpty()) {
            return Map.of("status", "error", "message", "Пустой файл");
        }

        SendRequest req = new SendRequest(platform, chatId, username, text, replyTo, markdown);

        try {
            List<SendFile> payloads = new ArrayList<>();
            for (MultipartFile f : valid) {
                payloads.add(SendFile.of(f.getOriginalFilename(), f.getContentType(), f.getSize(), f.getBytes()));
            }
            return toResponse(router.sendFiles(req, payloads));
        } catch (Exception e) {
            return Map.of("status", "error", "message", "Ошибка загрузки файла: " + e.getMessage());
        }
    }

    /** Простая рассылка: массив SendRequest, шлём последовательно, чтобы не словить flood-контроль. */
    public record BulkRequest(List<SendRequest> messages) {}

    @PostMapping("/bulk")
    public Map<String, Object> bulk(@RequestBody BulkRequest req) {
        if (req.messages() == null || req.messages().isEmpty()) {
            return Map.of("status", "error", "message", "Пустой список сообщений");
        }

        int ok = 0;
        List<Map<String, Object>> errors = new ArrayList<>();

        for (SendRequest one : req.messages()) {
            SendResult r = router.send(one);
            if (r.ok()) {
                ok++;
            } else {
                Map<String, Object> err = new LinkedHashMap<>();
                err.put("platform", String.valueOf(r.platform()));
                err.put("chatId", r.chatId());
                err.put("error", String.valueOf(r.error()));
                errors.add(err);
            }
            try {
                Thread.sleep(400); // анти-флуд пауза
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("status", "ok");
        resp.put("total", req.messages().size());
        resp.put("sent", ok);
        resp.put("failed", errors.size());
        resp.put("errors", errors);
        return resp;
    }

    private Map<String, Object> toResponse(SendResult r) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("status", r.ok() ? "ok" : "error");
        m.put("platform", r.platform());
        m.put("chatId", r.chatId());
        m.put("messageId", r.messageId());
        if (!r.ok()) m.put("message", r.error());
        return m;
    }
}
