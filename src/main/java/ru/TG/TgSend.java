package ru.TG;

import it.tdlight.jni.TdApi;
import ru.send.SendResult;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * 📤 Отправка сообщений в Telegram.
 *
 * ═══════════════════════════════════════════════════════════════════════════════════════
 *  Собрано ПО ФАКТИЧЕСКИМ ИСХОДНИКАМ вашей версии:
 *  tdlight-java 3.5.4+td.1.8.66  ->  tdlight-api 4.0.561  (TDLib 1.8.66)
 * ═══════════════════════════════════════════════════════════════════════════════════════
 *  В этой версии схема отличается от «классической» TDLib 1.8.0-1.8.26, поэтому
 *  привычный код не компилируется:
 *
 *    SendMessage            : chatId, topicId, replyTo(InputMessageReplyTo), options,
 *                             replyMarkup, inputMessageContent
 *                             ❗ НЕТ messageThreadId и replyToMessageId
 *    InputMessageText       : text(FormattedText), linkPreviewOptions, clearDraft
 *                             ❗ НЕТ disableWebPagePreview
 *    InputMessagePhoto      : photo(InputPhoto), caption, showCaptionAboveMedia,
 *                             selfDestructType, hasSpoiler
 *    InputPhoto             : photo(InputFile), thumbnail, video, addedStickerFileIds, width, height
 *    InputMessageVideo      : video(InputVideo), caption, ...
 *    InputVideo             : video(InputFile), thumbnail, cover, startTimestamp,
 *                             addedStickerFileIds, duration, width, height, supportsStreaming
 *    InputMessageDocument   : document(InputDocument), caption
 *    InputDocument          : document(InputFile), thumbnail, disableContentTypeDetection
 *                             ❗ disableContentTypeDetection живёт в обёртке, не в InputMessageDocument
 *
 *  Отсюда и ваши ошибки IDE:
 *    «Cannot resolve messageThreadId / replyToMessageId» — таких полей правда нет;
 *    «Required type: InputDocument, Provided: InputFileLocal» — файл надо оборачивать.
 *
 *  Проверено по sources-джару tdlight-api-4.0.561 (репозиторий https://mvn.mchv.eu/repository/mchv/).
 * ═══════════════════════════════════════════════════════════════════════════════════════
 */
public final class TgSend {

    private TgSend() { }

    /** Лимиты Telegram. */
    private static final int TEXT_LIMIT = 4096;
    private static final int CAPTION_LIMIT = 1024;
    private static final long PHOTO_MAX_BYTES = 10L * 1024 * 1024;

    private static final Set<String> PHOTO_EXT = Set.of("jpg", "jpeg", "png", "webp", "bmp", "gif");
    private static final Set<String> VIDEO_EXT = Set.of("mp4", "mov", "m4v", "webm", "mkv");



    /**
     * Колбэк «сообщение реально ушло»: (messageId, chatId, unixTime).
     * Коннектор по нему сразу пишет исходящее в БД, не дожидаясь UpdateNewMessage.
     */
    public interface OnSent {
        void onSent(long messageId, long chatId, int unixTime);
    }

    /**
     * id сообщений, отправленных через этот класс.
     * Telegram пришлёт их ещё раз через UpdateNewMessage — коннектор по этому набору
     * отличает «своё, уже записанное в БД» от нового входящего.
     */
    private static final Set<Long> SENT_IDS =
            java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());

    public static boolean consumeSentMessageId(long messageId) {
        return SENT_IDS.remove(messageId);
    }

    // =====================================================================================
    // 🌐 ПУБЛИЧНОЕ API
    // =====================================================================================

    public static CompletableFuture<SendResult> sendText(it.tdlight.client.SimpleTelegramClient client,
                                                         long chatId, String text, long replyTo, boolean markdown) {
        return sendText(client, chatId, text, replyTo, markdown, null);
    }

    public static CompletableFuture<SendResult> sendText(it.tdlight.client.SimpleTelegramClient client,
                                                         long chatId, String text, long replyTo,
                                                         boolean markdown, OnSent onSent) {
        if (client == null) return done(SendResult.fail("tg",chatId, "TG-клиент не запущен (POST /api/tg/start)"));
        if (text == null || text.isBlank()) return done(SendResult.fail("tg",chatId, "Пустой текст сообщения"));


        TdApi.InputMessageText content = new TdApi.InputMessageText();
        content.text = formattedText(text, markdown, TEXT_LIMIT, client);
        // linkPreviewOptions оставляем null = превью ссылок включено (дефолт Telegram).
        // Присваивать content.disableWebPagePreview в этой версии нельзя — поля нет.
        content.clearDraft = true;

        return send(client, chatId, replyTo, content, text, onSent);
    }

    /** Отправка файла с диска: фото / видео / документ. */
    public static CompletableFuture<SendResult> sendFile(it.tdlight.client.SimpleTelegramClient client,
                                                         long chatId, Path file, String caption,
                                                         long replyTo, boolean markdown) {
        return sendFile(client, chatId, file, caption, replyTo, markdown, null);
    }

    public static CompletableFuture<SendResult> sendFile(it.tdlight.client.SimpleTelegramClient client,
                                                         long chatId, Path file, String caption,
                                                         long replyTo, boolean markdown, OnSent onSent) {
        if (client == null) return done(SendResult.fail("tg",chatId, "TG-клиент не запущен (POST /api/tg/start)"));
        if (file == null || !Files.exists(file)) return done(SendResult.fail("tg",chatId, "Файл не найден: " + file));
        try {
            TdApi.InputMessageContent content =
                    fileContent(file, null, file.getFileName().toString(), caption, markdown, client);
            String text = (caption != null && !caption.isBlank()) ? caption : "[Файл: " + file.getFileName() + "]";
            return send(client, chatId, replyTo, content, text, onSent);
        } catch (Exception e) {
            return done(SendResult.fail("tg",chatId, describe(e)));
        }
    }

    /** Отправка байтов из браузера: кладём во временный файл и отдаём TDLib. */
    public static CompletableFuture<SendResult> sendFileBytes(it.tdlight.client.SimpleTelegramClient client,
                                                              long chatId, byte[] bytes, String fileName,
                                                              String mimeType, String caption, long replyTo,
                                                              boolean markdown, Path tempDir) {
        return sendFileBytes(client, chatId, bytes, fileName, mimeType, caption, replyTo, markdown, tempDir, null);
    }

    public static CompletableFuture<SendResult> sendFileBytes(it.tdlight.client.SimpleTelegramClient client,
                                                              long chatId, byte[] bytes, String fileName,
                                                              String mimeType, String caption, long replyTo,
                                                              boolean markdown, Path tempDir, OnSent onSent) {
        if (client == null) return done(SendResult.fail("tg",chatId, "TG-клиент не запущен (POST /api/tg/start)"));
        if (bytes == null || bytes.length == 0) return done(SendResult.fail("tg",chatId, "Пустой файл"));

        return CompletableFuture.supplyAsync(() -> {
            Path tmp = null;
            try {
                Files.createDirectories(tempDir);
                String name = (fileName != null && !fileName.isBlank()) ? fileName : "file";
                tmp = tempDir.resolve(System.currentTimeMillis() + "_" + sanitize(name));
                Files.write(tmp, bytes);

                TdApi.InputMessageContent content = fileContent(tmp, mimeType, name, caption, markdown, client);
                String text = (caption != null && !caption.isBlank()) ? caption : "[Файл: " + name + "]";
                return send(client, chatId, replyTo, content, text, onSent).get(5, TimeUnit.MINUTES);
            } catch (Exception e) {
                return SendResult.fail("tg",chatId, describe(e));
            } finally {
                if (tmp != null) {
                    try { Files.deleteIfExists(tmp); } catch (Exception ignored) { }
                }
            }
        });
    }

    // =====================================================================================
    // 🧱 СБОРКА КОНТЕНТА
    // =====================================================================================

    /**
     * FormattedText. При markdown=true разметку парсит сам TDLib:
     * ParseTextEntities(text, TextParseModeMarkdown(2)) -> FormattedText.
     */
    private static TdApi.FormattedText formattedText(String text, boolean markdown, int limit,
                                                     it.tdlight.client.SimpleTelegramClient client) {
        String safe = truncate(text, limit);
        if (markdown && client != null && !safe.isBlank()) {
            try {
                TdApi.FormattedText parsed = client
                        .send(new TdApi.ParseTextEntities(safe, new TdApi.TextParseModeMarkdown(2)))
                        .get(15, TimeUnit.SECONDS);
                if (parsed != null && parsed.text != null) return parsed;
            } catch (Exception e) {
                System.out.println("⚠️ [TG] Markdown не распарсен, шлём plain text: " + describe(e));
            }
        }
        TdApi.FormattedText ft = new TdApi.FormattedText();
        ft.text = safe;
        return ft;
    }

    /** Фото / видео / документ — с обёртками InputPhoto / InputVideo / InputDocument. */
    private static TdApi.InputMessageContent fileContent(Path file, String mimeType, String displayName,
                                                         String caption, boolean markdown,
                                                         it.tdlight.client.SimpleTelegramClient client)
            throws Exception {

        TdApi.InputFileLocal local = new TdApi.InputFileLocal(file.toAbsolutePath().toString());
        TdApi.FormattedText cap = formattedText(caption, markdown, CAPTION_LIMIT, client);

        String ext = extOf(displayName);
        byte[] head = sniffHead(file);
        long size = sizeOf(file);

        String kind = detect(displayName, mimeType, head, ext);

        // Лимит Telegram для фото — 10 МБ: крупнее отправляем файлом, иначе TG отклонит.
        if ("photo".equals(kind) && size > PHOTO_MAX_BYTES) {
            System.out.println("⚠️ [TG] \"" + displayName + "\" — " + humanSize(size)
                    + ", лимит фото в Telegram 10 МБ. Отправляю как файл.");
            kind = "doc";
        }
        System.out.println("🎯 [TG] \"" + displayName + "\" mime=" + nvl(mimeType)
                + " -> " + kind + " (" + humanSize(size) + ")");

        switch (kind) {

            case "photo" -> {
                TdApi.InputPhoto photo = new TdApi.InputPhoto();
                photo.photo = local;
                // width/height/thumbnail не заполняем: Telegram определит сам

                TdApi.InputMessagePhoto m = new TdApi.InputMessagePhoto();
                m.photo = photo;
                m.caption = cap;
                return m;
            }

            case "video" -> {
                TdApi.InputVideo video = new TdApi.InputVideo();
                video.video = local;
                // стриминг поддерживают только MP4/MOV
                video.supportsStreaming = isMp4Container(head) || "mov".equals(ext);
                // duration/width/height = 0: Telegram прочитает метаданные сам

                TdApi.InputMessageVideo m = new TdApi.InputMessageVideo();
                m.video = video;
                m.caption = cap;
                return m;
            }

            default -> {
                TdApi.InputDocument doc = new TdApi.InputDocument();
                doc.document = local;
                // 👈 главное: не даём Telegram угадывать тип — файл уйдёт именно файлом
                doc.disableContentTypeDetection = true;

                TdApi.InputMessageDocument m = new TdApi.InputMessageDocument();
                m.document = doc;
                m.caption = cap;
                return m;
            }
        }
    }

    // =====================================================================================
    // 📮 ОТПРАВКА
    // =====================================================================================

    private static CompletableFuture<SendResult> send(it.tdlight.client.SimpleTelegramClient client,
                                                      long chatId, long replyTo,
                                                      TdApi.InputMessageContent content,
                                                      String textForLog, OnSent onSent) {
        TdApi.SendMessage req = new TdApi.SendMessage();
        req.chatId = chatId;
        req.inputMessageContent = content;
        // topicId / options / replyMarkup = null -> дефолты Telegram

        // В этой версии ответ на сообщение — это объект InputMessageReplyTo, а не long-поле.
        if (replyTo > 0) {
            TdApi.InputMessageReplyToMessage reply = new TdApi.InputMessageReplyToMessage();
            reply.messageId = replyTo;
            req.replyTo = reply;
        }

        return client.send(req).handle((msg, err) -> {
            if (err != null || msg == null) {
                String reason = describe(err);
                System.err.println("❌ [TG] Не удалось отправить в чат " + chatId + ": " + reason);
                return SendResult.fail("tg",chatId, reason);
            }

            long cid = msg.chatId != 0 ? msg.chatId : chatId;
            System.out.println("📤 [TG] Отправлено в чат " + cid + ", messageId=" + msg.id
                    + " (" + truncate(textForLog, 60) + ")");

            if (msg.id != 0) {
                SENT_IDS.add(msg.id);
                if (SENT_IDS.size() > 5000) SENT_IDS.clear();   // страховка от роста
            }
            if (onSent != null) {
                try {
                    onSent.onSent(msg.id, cid, msg.date);
                } catch (Exception e) {
                    System.err.println("⚠️ [TG] Отправлено, но не сохранено в БД: " + describe(e));
                }
            }
            return SendResult.ok("tg",cid, msg.id, textForLog);
        });
    }

    private static CompletableFuture<SendResult> done(SendResult r) {
        return CompletableFuture.completedFuture(r);
    }

    // =====================================================================================
    // 🎯 ФОТО / ВИДЕО / ФАЙЛ
    // =====================================================================================

    /**
     * Возвращает "photo" | "video" | "doc".
     * Порядок: магические байты -> MIME -> расширение -> doc.
     */
    private static String detect(String fileName, String mimeType, byte[] head, String ext) {
        // 1. магические байты (надёжнее всего: браузер отдаёт octet-stream почти на всё)
        if (head != null && head.length >= 12) {
            if (startsWith(head, 0xFF, 0xD8, 0xFF)) return "photo";                    // JPEG
            if (startsWith(head, 0x89, 'P', 'N', 'G')) return "photo";                  // PNG
            if (startsWith(head, 'G', 'I', 'F', '8')) return "photo";                   // GIF
            if (startsWith(head, 'B', 'M')) return "photo";                             // BMP
            if (startsWith(head, 'R', 'I', 'F', 'F') && at(head, 8, 'W', 'E', 'B', 'P')) {
                return "photo";                                                          // WebP
            }
            if (isMp4Container(head)) return "video";                                    // mp4 / mov / m4v
            if (startsWith(head, 0x1A, 0x45, 0xDF, 0xA3)) {                              // mkv / webm
                if (containsText(head, "A_OPUS") || containsText(head, "A_VORBIS")) return "doc";
                return "video";
            }
        }

        // 2. MIME (application/octet-stream игнорируем)
        String mime = nvl(mimeType).trim().toLowerCase(Locale.ROOT);
        if (!mime.isEmpty() && !mime.equals("application/octet-stream")) {
            if (mime.startsWith("image/")) {
                // ⚠️ сабтайп сравниваем ЦЕЛИКОМ: "image/jpeg".contains("gif") == true,
                //    из-за чего JPEG улетал как анимация -> "400: Animation must be non-empty"
                String sub = mime.substring("image/".length());
                if (sub.startsWith("heic") || sub.startsWith("heif")) return "doc";  // HEIC TG как фото не принимает
                return "photo";
            }
            if (mime.startsWith("video/")) return "video";
        }

        // 3. расширение
        if (PHOTO_EXT.contains(ext)) return "photo";
        if (VIDEO_EXT.contains(ext)) return "video";

        // 4. не опознали
        return "doc";
    }

    // =====================================================================================
    // 🧰 УТИЛИТЫ
    // =====================================================================================

    private static String extOf(String fileName) {
        if (fileName == null) return "";
        String n = fileName.toLowerCase(Locale.ROOT);
        int dot = n.lastIndexOf('.');
        if (dot < 0 || dot == n.length() - 1) return "";
        String e = n.substring(dot + 1);
        return e.length() <= 8 ? e : "";
    }

    private static byte[] sniffHead(Path file) {
        try (InputStream in = Files.newInputStream(file)) {
            return in.readNBytes(64 * 1024);
        } catch (Exception e) {
            return new byte[0];
        }
    }

    private static boolean isMp4Container(byte[] head) {
        return at(head, 4, 'f', 't', 'y', 'p');
    }

    private static boolean startsWith(byte[] data, int... sig) {
        if (data == null || data.length < sig.length) return false;
        for (int i = 0; i < sig.length; i++) {
            if ((data[i] & 0xFF) != (sig[i] & 0xFF)) return false;
        }
        return true;
    }

    private static boolean at(byte[] data, int offset, char... text) {
        if (data == null || data.length < offset + text.length) return false;
        for (int i = 0; i < text.length; i++) {
            if ((data[offset + i] & 0xFF) != (text[i] & 0xFF)) return false;
        }
        return true;
    }

    private static boolean containsText(byte[] data, String needle) {
        if (data == null || data.length == 0) return false;
        byte[] n = needle.getBytes(StandardCharsets.US_ASCII);
        outer:
        for (int i = 0; i <= data.length - n.length; i++) {
            for (int j = 0; j < n.length; j++) {
                if (data[i + j] != n[j]) continue outer;
            }
            return true;
        }
        return false;
    }

    private static long sizeOf(Path p) {
        try { return Files.size(p); } catch (Exception e) { return 0; }
    }

    private static String humanSize(long bytes) {
        if (bytes < 1024) return bytes + " Б";
        double v = bytes;
        String[] u = {"КБ", "МБ", "ГБ"};
        int i = -1;
        while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
        return String.format(Locale.ROOT, "%.1f %s", v, u[Math.max(i, 0)]);
    }

    private static String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max - 1) + "…";
    }

    private static String sanitize(String name) {
        if (name == null || name.isBlank()) return "file";
        String cleaned = name.replaceAll("[\\\\/:*?\"<>|\\r\\n]", "_").trim();
        if (cleaned.isEmpty()) cleaned = "file";
        return cleaned.length() > 100 ? cleaned.substring(cleaned.length() - 100) : cleaned;
    }

    private static String nvl(String s) {
        return s != null ? s : "";
    }

    private static String describe(Throwable t) {
        if (t == null) return "неизвестная ошибка";
        Throwable c = t.getCause() != null ? t.getCause() : t;
        String m = c.getMessage();
        return (m != null && !m.isBlank()) ? m : c.getClass().getSimpleName();
    }
}
