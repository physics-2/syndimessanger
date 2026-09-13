package ru.send;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;
import ru.TG.TgConnector;
import ru.VK.VkConnector;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Роутер отправки: один вход для фронта -> нужный коннектор.
 *
 * Коннекторы подключаются через ObjectProvider, чтобы приложение стартовало,
 * даже если какой-то из них временно выключен/не поднят (например, MAX на python).
 *
 * ℹ️ SendResult здесь ОДИН на всё приложение — {@link ru.send.SendResult}.
 *    TgConnector, TgSend и VkConnector возвращают именно его,
 *    поэтому никаких конвертеров (fromTg/fromVk) не нужно.
 */
@Service
public class MessageRouter {

    private final ObjectProvider<TgConnector> tg;
    private final ObjectProvider<VkConnector> vk;
    private final MaxSendClient max;

    /** Куда кладем файлы, пришедшие байтами из браузера (для VK/MAX). */
    private static final Path TEMP_DIR = Path.of("uploads", "outgoing");

    public MessageRouter(ObjectProvider<TgConnector> tg,
                         ObjectProvider<VkConnector> vk,
                         MaxSendClient max) {
        this.tg = tg;
        this.vk = vk;
        this.max = max;
    }

    /** Отправка текста. */
    public SendResult send(SendRequest req) {
        return dispatch(req, List.of());
    }

    /** Отправка одного файла (text используется как подпись). */
    public SendResult sendFile(SendRequest req, SendFile file) {
        return dispatch(req, file == null ? List.of() : List.of(file));
    }

    /**
     * Отправка нескольких файлов.
     *  - 1 файл  -> одно сообщение с подписью;
     *  - VK      -> все файлы одним сообщением (sendFiles, до 10 вложений);
     *  - MAX/TG  -> по сообщению на файл, подпись только у первого.
     */
    public SendResult sendFiles(SendRequest req, List<SendFile> files) {
        if (files == null || files.isEmpty()) return dispatch(req, List.of());
        if (files.size() == 1) return dispatch(req, files);

        String platform = req == null ? "tg" : req.platformOrDefault();

        // --- VK: все файлы одним сообщением ---
        if ("vk".equals(platform)) {
            VkConnector c = vk.getIfAvailable();
            if (c == null) return SendResult.fail("vk", 0, "VK-коннектор недоступен");
            long chatId = req.chatId() != null ? req.chatId() : 0L;

            List<Path> paths = new ArrayList<>();
            List<Path> temp = new ArrayList<>();
            try {
                for (SendFile f : files) {
                    Path p = f.materialize(TEMP_DIR);
                    paths.add(p);
                    if (f.path() == null) temp.add(p);   // удаляем только свои временные
                }
                return c.sendFiles(chatId, req.text(), paths, req.replyToOrZero());
            } catch (Exception e) {
                return SendResult.fail("vk", chatId, "Ошибка файлов: " + e.getMessage());
            } finally {
                temp.forEach(SendFile::deleteIfTemp);
            }
        }

        // --- MAX и TG: по сообщению на файл ---
        SendResult last = SendResult.fail(platform, 0, "Нет файлов");
        int sent = 0;
        StringBuilder errors = new StringBuilder();

        for (int i = 0; i < files.size(); i++) {
            long cid = req.chatId() != null ? req.chatId() : 0L;

            if ("max".equals(platform)) {
                SendFile f = files.get(i);
                if (!f.hasBytes()) {
                    errors.append("MAX принимает файлы только байтами (через /api/send/media); ");
                    continue;
                }
                last = max.sendFile(cid, i == 0 ? req.text() : "", null, true, f);
            } else {
                SendRequest one = new SendRequest(platform, req.chatId(), req.username(),
                        i == 0 ? req.text() : "", req.replyToOrZero(), req.markdownOrFalse()
                        );
                last = dispatch(one, List.of(files.get(i)));
            }

            if (last.ok()) sent++;
            else errors.append(last.error()).append("; ");
        }

        return sent == files.size()
                ? last
                : SendResult.fail(platform, last.chatId(),
                "Отправлено " + sent + " из " + files.size() + ": " + errors);
    }

    // =========================================================================================
    // Маршрутизация одного сообщения
    // =========================================================================================

    private SendResult dispatch(SendRequest req, List<SendFile> files) {
        if (req == null) return SendResult.fail("?", 0, "Пустой запрос");
        String platform = req.platformOrDefault();
        long chatId = req.chatId() != null ? req.chatId() : 0L;
        String text = req.text() != null ? req.text() : "";

        SendFile file = (files == null || files.isEmpty()) ? null : files.get(0);
        boolean hasText = !text.isBlank();
        boolean hasFile = file != null;
        if (!hasText && !hasFile) {
            return SendResult.fail(platform, chatId, "Пустое сообщение: нужен текст или файл");
        }

        switch (platform) {

            case "tg", "telegram" -> {
                TgConnector c = tg.getIfAvailable();
                if (c == null) return SendResult.fail("tg", chatId, "TG-коннектор недоступен");

                long target = chatId;
                if (target == 0 && req.username() != null && !req.username().isBlank()) {
                    target = c.resolveTarget(req.username());
                }
                if (target == 0) {
                    return SendResult.fail("tg", 0, "Не удалось определить чат (chatId или username)");
                }

                if (hasFile) {
                    if (file.hasBytes()) {
                        return join(c.sendFileBytes(target, file.bytes(), file.name(), file.mimeType(),
                                text, req.replyToOrZero(), req.markdownOrFalse()));
                    }
                    return c.sendFile(target, file.path(), text, req.replyToOrZero(), req.markdownOrFalse());
                }
                return c.sendMessage(target, text, req.replyToOrZero(), req.markdownOrFalse());
            }

            case "vk" -> {
                VkConnector c = vk.getIfAvailable();
                if (c == null) return SendResult.fail("vk", chatId, "VK-коннектор недоступен");
                if (chatId == 0) return SendResult.fail("vk", 0, "Не указан chatId (peer_id)");

                if (hasFile) {
                    Path tmp = null;
                    try {
                        tmp = file.materialize(TEMP_DIR);
                        return c.sendFile(chatId, text, tmp.toString(), file.mimeType(), req.replyToOrZero());
                    } catch (Exception e) {
                        return SendResult.fail("vk", chatId, "Ошибка файла: " + e.getMessage());
                    } finally {
                        if (file.path() == null) SendFile.deleteIfTemp(tmp); // чужой файл не трогаем
                    }
                }
                return c.sendMessage(chatId, text, req.replyToOrZero());
            }

            case "max" -> {
                if (chatId == 0) return SendResult.fail("max", 0, "Не указан chatId");
                if (hasFile) {
                    if (!file.hasBytes()) {
                        return SendResult.fail("max", chatId,
                                "MAX принимает файлы только байтами (через /api/send/media)");
                    }
                    return max.sendFile(chatId, text, null, true, file);
                }
                return max.sendText(chatId, text, null, true);
            }

            default -> {
                return SendResult.fail(platform, chatId, "Неизвестная платформа: " + platform);
            }
        }
    }

    /** CompletableFuture<SendResult> -> SendResult (синхронно, до 5 минут на duże файлы). */
    private static SendResult join(CompletableFuture<SendResult> f) {
        try {
            return f.get(5, TimeUnit.MINUTES);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return SendResult.fail("tg", 0, "Отправка прервана");
        } catch (Exception e) {
            Throwable c = e.getCause() != null ? e.getCause() : e;
            return SendResult.fail("tg", 0, c.getMessage() != null ? c.getMessage() : c.getClass().getSimpleName());
        }
    }
}