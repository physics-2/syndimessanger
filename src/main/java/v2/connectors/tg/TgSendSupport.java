package v2.connectors.tg;

import it.tdlight.jni.TdApi;
import it.tdlight.client.SimpleTelegramClient;
import v2.connectors.base.ConnectorResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


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
    public static long sendText(SimpleTelegramClient client, long chatId, String text, long replyTo) {
        if (client == null) {
            throw new IllegalStateException("TG клиент не запущен");
        }
        if (text == null || text.isBlank()) {
            throw new IllegalArgumentException("Пустой текст сообщения");
        }

        TdApi.InputMessageText content = new TdApi.InputMessageText();
        content.text = formattedText(text);
        content.clearDraft = true;

        return send(client, chatId, replyTo, content);
    }

    /**
     * Отправка файла с диска.
     */
    public static long sendFile(SimpleTelegramClient client, long chatId, Path file, String caption, long replyTo) {
        if (client == null) {
            throw new IllegalStateException("TG клиент не запущен");
        }
        if (file == null || !Files.exists(file)) {
            throw new IllegalArgumentException("Файл не найден: " + file);
        }

        try {
            TdApi.InputMessageContent content = fileContent(file,caption);
            return send(client, chatId, replyTo, content);
        } catch (Exception e) {
            throw new RuntimeException("Ошибка отправки файла: " + e.getMessage(), e);
        }
    }

    private static TdApi.FormattedText formattedText(String text) {
        return new TdApi.FormattedText(truncate(text,CAPTION_LIMIT),null);
    }

    /**
     * Создание контента файла (фото/видео/документ).
     */
    private static TdApi.InputMessageContent fileContent(Path file,
                                                         String caption) throws Exception {
        TdApi.FormattedText cap = formattedText(caption);

        DataInput dataInput = new DataInputStream(new FileInputStream(file.toFile()));
        String[] split = file.toFile().getName().split("\\.");
        String extension = split[split.length - 1];

        if(!file.toFile().getName().endsWith(extension)){
            throw new RuntimeException("Something unexpected happened during extension parsing: " + extension + ", split: " + Arrays.toString(split) +", name: " + file.toFile().getName());
        }

        if(PHOTO_EXT.contains(extension)){
            TdApi.InputPhoto photo = new TdApi.InputPhoto(dataInput);
            return new TdApi.InputMessagePhoto(photo,cap,true,null,false);
        }

        if(VIDEO_EXT.contains(extension)){
            TdApi.InputVideo video = new TdApi.InputVideo(dataInput);
            return new TdApi.InputMessageVideo(video,cap,true,null,false);
        }

        throw new RuntimeException("unsupported datatype");
    }

    /**
     * Отправка сообщения через TDLib.
     */
    private static long send(SimpleTelegramClient client, long chatId, long replyTo,TdApi.InputMessageContent content) {

        TdApi.SendMessage req = new TdApi.SendMessage();
        req.chatId = chatId;
        req.inputMessageContent = content;

        if (replyTo != 0) {
            TdApi.InputMessageReplyToMessage reply = new TdApi.InputMessageReplyToMessage();
            reply.messageId = replyTo;
            req.replyTo = reply;
        }

        try {
            var msg = client.send(req).get(15, TimeUnit.SECONDS);
            if (msg == null) {
                throw new RuntimeException("Пустой ответ от Telegram API");
            }
            log.info("Отправлено в чат {}, messageId={}", msg.chatId != 0 ? msg.chatId : chatId, msg.id);
            return msg.id;
        } catch (Exception e) {
            throw new RuntimeException("Не удалось отправить сообщение: " + describe(e), e);
        }
    }

    private static String truncate(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max - 1) + "…";
    }



    private static String describe(Throwable t) {
        Throwable c = (t != null && t.getCause() != null) ? t.getCause() : t;
        if (c == null) return "неизвестная ошибка";
        String m = c.getMessage();
        return (m != null && !m.isBlank()) ? m : c.getClass().getSimpleName();
    }
}
