package v2.connectors.tg;

import it.tdlight.jni.TdApi;
import it.tdlight.client.SimpleTelegramClient;
import v2.connectors.base.ConnectorResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Отправка сообщений в Telegram для v2 коннектора.
 * Использует tdlight-java 3.5.4+td.1.8.66 -> tdlight-api 4.0.561 (TDLib 1.8.66)
 */
public class TgSendSupport {

    private static final Logger log = LoggerFactory.getLogger(TgSendSupport.class);

    private static final int TEXT_LIMIT = 4096;
    private static final int CAPTION_LIMIT = 1024;
    private static final long PHOTO_MAX_BYTES = 10L * 1024 * 1024;

    private static final Set<String> PHOTO_EXT = Set.of("jpg", "jpeg", "png", "webp", "bmp", "gif");
    private static final Set<String> VIDEO_EXT = Set.of("mp4", "mov", "m4v", "webm", "mkv");

    private TgSendSupport() { }

    /**
     * Отправка текста.
     */
    public static long sendText(SimpleTelegramClient client, long chatId, String text, long replyTo, boolean markdown) {
        if (client == null) {
            throw new IllegalStateException("TG клиент не запущен");
        }
        if (text == null || text.isBlank()) {
            throw new IllegalArgumentException("Пустой текст сообщения");
        }

        TdApi.InputMessageText content = new TdApi.InputMessageText();
        content.text = formattedText(text, markdown, TEXT_LIMIT, client);
        content.clearDraft = true;

        return send(client, chatId, replyTo, content, text);
    }

    /**
     * Отправка файла с диска.
     */
    public static long sendFile(SimpleTelegramClient client, long chatId, Path file, String caption, long replyTo, boolean markdown) {
        if (client == null) {
            throw new IllegalStateException("TG клиент не запущен");
        }
        if (file == null || !Files.exists(file)) {
            throw new IllegalArgumentException("Файл не найден: " + file);
        }

        try {
            TdApi.InputMessageContent content = fileContent(file, null, file.getFileName().toString(), caption, markdown, client);
            String text = (caption != null && !caption.isBlank()) ? caption : "[Файл: " + file.getFileName() + "]";
            return send(client, chatId, replyTo, content, text);
        } catch (Exception e) {
            throw new RuntimeException("Ошибка отправки файла: " + e.getMessage(), e);
        }
    }

    /**
     * Отправка байтов из браузера.
     */
    public static long sendFileBytes(SimpleTelegramClient client, long chatId, byte[] bytes, String fileName,
                                     String mimeType, String caption, long replyTo, boolean markdown, Path tempDir) {
        if (client == null) {
            throw new IllegalStateException("TG клиент не запущен");
        }
        if (bytes == null || bytes.length == 0) {
            throw new IllegalArgumentException("Пустой файл");
        }

        Path tmp = null;
        try {
            Files.createDirectories(tempDir);
            String name = (fileName != null && !fileName.isBlank()) ? fileName : "file";
            tmp = tempDir.resolve(System.currentTimeMillis() + "_" + sanitize(name));
            Files.write(tmp, bytes);

            TdApi.InputMessageContent content = fileContent(tmp, mimeType, name, caption, markdown, client);
            String text = (caption != null && !caption.isBlank()) ? caption : "[Файл: " + name + "]";
            return send(client, chatId, replyTo, content, text);
        } catch (Exception e) {
            throw new RuntimeException("Ошибка отправки файла: " + e.getMessage(), e);
        } finally {
            if (tmp != null) {
                try { Files.deleteIfExists(tmp); } catch (Exception ignored) { }
            }
        }
    }

    /**
     * FormattedText с поддержкой markdown.
     */
    private static TdApi.FormattedText formattedText(String text, boolean markdown, int limit, SimpleTelegramClient client) {
        String safe = truncate(text, limit);
        if (markdown && client != null && !safe.isBlank()) {
            try {
                TdApi.FormattedText parsed = client
                        .send(new TdApi.ParseTextEntities(safe, new TdApi.TextParseModeMarkdown(2)))
                        .get(15, TimeUnit.SECONDS);
                if (parsed != null && parsed.text != null) return parsed;
            } catch (Exception e) {
                log.warn("Markdown не распарсен, шлём plain text: {}", e.getMessage());
            }
        }
        TdApi.FormattedText ft = new TdApi.FormattedText();
        ft.text = safe;
        return ft;
    }

    /**
     * Создание контента файла (фото/видео/документ).
     */
    private static TdApi.InputMessageContent fileContent(Path file, String mimeType, String displayName,
                                                         String caption, boolean markdown, SimpleTelegramClient client) throws Exception {
        TdApi.InputFileLocal local = new TdApi.InputFileLocal(file.toAbsolutePath().toString());
        TdApi.FormattedText cap = formattedText(caption, markdown, CAPTION_LIMIT, client);

        String ext = extOf(displayName);
        byte[] head = sniffHead(file);
        long size = sizeOf(file);

        String kind = detect(displayName, mimeType, head, ext);

        if ("photo".equals(kind) && size > PHOTO_MAX_BYTES) {
            log.warn("{} - {}, лимит фото в Telegram 10 МБ. Отправляю как файл.", displayName, humanSize(size));
            kind = "doc";
        }
        log.debug("{} mime={} -> {} ({})", displayName, nvl(mimeType), kind, humanSize(size));

        switch (kind) {
            case "photo" -> {
                TdApi.InputPhoto photo = new TdApi.InputPhoto();
                photo.photo = local;
                TdApi.InputMessagePhoto m = new TdApi.InputMessagePhoto();
                m.photo = photo;
                m.caption = cap;
                return m;
            }
            case "video" -> {
                TdApi.InputVideo video = new TdApi.InputVideo();
                video.video = local;
                video.supportsStreaming = isMp4Container(head) || "mov".equals(ext);
                TdApi.InputMessageVideo m = new TdApi.InputMessageVideo();
                m.video = video;
                m.caption = cap;
                return m;
            }
            default -> {
                TdApi.InputDocument doc = new TdApi.InputDocument();
                doc.document = local;
                doc.disableContentTypeDetection = true;
                TdApi.InputMessageDocument m = new TdApi.InputMessageDocument();
                m.document = doc;
                m.caption = cap;
                return m;
            }
        }
    }

    /**
     * Отправка сообщения через TDLib.
     */
    private static long send(SimpleTelegramClient client, long chatId, long replyTo,
                             TdApi.InputMessageContent content, String textForLog) {
        TdApi.SendMessage req = new TdApi.SendMessage();
        req.chatId = chatId;
        req.inputMessageContent = content;

        if (replyTo > 0) {
            TdApi.InputMessageReplyToMessage reply = new TdApi.InputMessageReplyToMessage();
            reply.messageId = replyTo;
            req.replyTo = reply;
        }

        try {
            var msg = client.send(req).get(5, TimeUnit.MINUTES);
            if (msg == null) {
                throw new RuntimeException("Пустой ответ от Telegram API");
            }
            log.info("Отправлено в чат {}, messageId={}", msg.chatId != 0 ? msg.chatId : chatId, msg.id);
            return msg.id;
        } catch (Exception e) {
            throw new RuntimeException("Не удалось отправить сообщение: " + describe(e), e);
        }
    }

    // ==================== Утилиты ====================

    private static String detect(String fileName, String mimeType, byte[] head, String ext) {
        if (head != null && head.length >= 12) {
            if (startsWith(head, 0xFF, 0xD8, 0xFF)) return "photo";
            if (startsWith(head, 0x89, 'P', 'N', 'G')) return "photo";
            if (startsWith(head, 'G', 'I', 'F', '8')) return "photo";
            if (startsWith(head, 'B', 'M')) return "photo";
            if (startsWith(head, 'R', 'I', 'F', 'F') && at(head, 8, 'W', 'E', 'B', 'P')) return "photo";
            if (isMp4Container(head) || startsWith(head, 0x1A, 0x45, 0xDF, 0xA3)) return "video";
        }
        if (mimeType != null) {
            String m = mimeType.toLowerCase(Locale.ROOT);
            if (m.startsWith("image/")) return "photo";
            if (m.startsWith("video/")) return "video";
        }
        if (ext != null) {
            if (PHOTO_EXT.contains(ext)) return "photo";
            if (VIDEO_EXT.contains(ext)) return "video";
        }
        return "doc";
    }

    private static boolean startsWith(byte[] h, int... sig) {
        if (h.length < sig.length) return false;
        for (int i = 0; i < sig.length; i++) if ((h[i] & 0xFF) != sig[i]) return false;
        return true;
    }

    private static boolean startsWith(byte[] h, char... sig) {
        if (h.length < sig.length) return false;
        for (int i = 0; i < sig.length; i++) if ((h[i] & 0xFF) != sig[i]) return false;
        return true;
    }

    private static boolean at(byte[] h, int off, char... sig) {
        if (h.length < off + sig.length) return false;
        for (int i = 0; i < sig.length; i++) if ((h[off + i] & 0xFF) != sig[i]) return false;
        return true;
    }

    private static boolean isMp4Container(byte[] head) {
        if (head.length < 12) return false;
        int ftyp = (head[4] << 24) | (head[5] << 16) | (head[6] << 8) | head[7];
        return ftyp == 0x66747970; // 'ftyp'
    }

    private static byte[] sniffHead(Path p) throws Exception {
        try (InputStream in = Files.newInputStream(p)) {
            byte[] buf = new byte[16];
            int r = in.read(buf);
            return r > 0 ? java.util.Arrays.copyOf(buf, r) : null;
        }
    }

    private static long sizeOf(Path p) throws Exception {
        return Files.size(p);
    }

    private static String extOf(String name) {
        if (name == null) return null;
        int dot = name.lastIndexOf('.');
        return dot > 0 && dot < name.length() - 1 ? name.substring(dot + 1).toLowerCase(Locale.ROOT) : null;
    }

    private static String nvl(String s) {
        return s == null || s.isBlank() ? "-" : s;
    }

    private static String humanSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
        return String.format("%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    }

    private static String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max - 1) + "…";
    }

    private static String sanitize(String name) {
        return name.replaceAll("[\\\\\\\\/:*?\"<>|]", "_");
    }

    private static String describe(Throwable t) {
        Throwable c = (t != null && t.getCause() != null) ? t.getCause() : t;
        if (c == null) return "неизвестная ошибка";
        String m = c.getMessage();
        return (m != null && !m.isBlank()) ? m : c.getClass().getSimpleName();
    }
}
