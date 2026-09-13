package ru.api;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import ru.VK.VkConnector;
import ru.VK.VkSendSupport;
import ru.send.SendResult;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * Прямые VK-эндпоинты отправки (по образцу /api/tg/send).
 * Если фронтенд ходит через общий /api/send — этот контроллер можно не подключать.
 */
@RestController
@RequestMapping("/api/vk")
@CrossOrigin("*")
public class VkSendController {

    private final VkConnector vk;
    private static final Path TEMP_DIR = Path.of("uploads", "vk");

    public VkSendController(VkConnector vk) {
        this.vk = vk;
    }

    public record VkSendRequest(Long peerId, String text, Long replyTo) {}

    @PostMapping("/send")
    public Map<String, Object> send(@RequestBody VkSendRequest req) {
        if (req.peerId() == null || req.peerId() == 0) {
            return Map.of("status", "error", "message", "Не указан peer_id");
        }
        SendResult r = vk.sendMessage(req.peerId(), req.text(),
                req.replyTo() != null ? req.replyTo() : 0L);
        return r.ok()
                ? Map.of("status", "ok", "messageId", r.messageId(), "peerId", r.chatId())
                : Map.of("status", "error", "message", String.valueOf(r.error()));
    }

    @PostMapping(value = "/send/media", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Map<String, Object> sendMedia(@RequestPart("file") MultipartFile[] files,
                                         @RequestParam Long peerId,
                                         @RequestParam(required = false) String caption,
                                         @RequestParam(required = false) Long replyTo) {
        if (files == null || files.length == 0 || files[0].isEmpty()) {
            return Map.of("status", "error", "message", "Пустой файл");
        }
        MultipartFile file = files[0]; // VK: несколько файлов -> используйте /api/send/media (общий роутер)

        Path tmp = null;
        try {
            Files.createDirectories(TEMP_DIR);
            String name = (file.getOriginalFilename() == null ? "file" : file.getOriginalFilename())
                    .replaceAll("[\\\\/:*?\"<>|]", "_");
            tmp = TEMP_DIR.resolve(System.currentTimeMillis() + "_" + name);
            file.transferTo(tmp.toAbsolutePath().toFile());

            SendResult r = vk.sendFile(peerId, caption, tmp.toString(),
                    file.getContentType(), replyTo != null ? replyTo : 0L);

            return r.ok()
                    ? Map.of("status", "ok", "messageId", r.messageId(), "peerId", r.chatId())
                    : Map.of("status", "error", "message", String.valueOf(r.error()));
        } catch (Exception e) {
            return Map.of("status", "error", "message", "Ошибка загрузки файла: " + e.getMessage());
        } finally {
            if (tmp != null) {
                try { Files.deleteIfExists(tmp); } catch (Exception ignored) { }
            }
        }
    }

    @GetMapping("/api/vk/debug-upload")
    public Map<String,Object>  debugUpload(@RequestParam long peerId) {
         return ru.VK.VkUploadDebug.diagnose(peerId);
     }
}
