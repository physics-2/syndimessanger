package v2.connectors.tg;

import it.tdlight.jni.TdApi;
import v2.entity.Chat;
import v2.entity.Message;
import v2.entity.User;
import java.util.ArrayList;
import java.util.function.Function;

public class TgMessageMapper {
    private long myUserId;
    private final TgMediaDownloader mediaDownloader;
    private Function<Long, User> userResolver; // 👈 Добавили функцию для поиска юзеров

    public TgMessageMapper(long myUserId, TgMediaDownloader mediaDownloader) {
        this.myUserId = myUserId;
        this.mediaDownloader = mediaDownloader;
    }

    public void setUserResolver(Function<Long, User> userResolver) {
        this.userResolver = userResolver;
    }

    public void updateMyUserId(long myUserId) {
        this.myUserId = myUserId;
    }

    public Message toDomainMessage(TdApi.Message tgMsg, ChatMeta meta) {
        long senderId = extractSenderId(tgMsg);
        String text = contentToText(tgMsg.content);
        String mediaUrl = mediaDownloader.extractMediaLocalPath(tgMsg.content);

        return new Message("tg", tgMsg.id, tgMsg.chatId, senderId, text, mediaUrl);
    }

    public User toDomainUser(TdApi.User u, String avatar) {
        String username = (u.usernames != null && u.usernames.activeUsernames.length > 0)
                ? u.usernames.activeUsernames[0] : "";
        return new User("tg", u.id, nvl(u.firstName), nvl(u.lastName),
                username, avatar, nvl(u.phoneNumber), new ArrayList<>());
    }

    public ChatMeta toChatMeta(TdApi.Chat chat) {
        boolean isGroup = false;
        boolean isChannel = false;
        boolean isSavedMessages = false;
        String title = chat.title;

        if (chat.type instanceof TdApi.ChatTypeBasicGroup) {
            isGroup = true;
        } else if (chat.type instanceof TdApi.ChatTypeSupergroup sg) {
            isGroup = true;
            isChannel = sg.isChannel;
        } else if (chat.type instanceof TdApi.ChatTypePrivate p) {
            isSavedMessages = (p.userId == myUserId);

            // 👇 РЕШЕНИЕ ПРОБЛЕМЫ 2: Если title пустой, берем имя из профиля юзера
            if ((title == null || title.trim().isEmpty()) && userResolver != null && !isSavedMessages) {
                try {
                    User user = userResolver.apply(p.userId);
                    if (user != null) {
                        String fullName = (nvl(user.getFirstName()) + " " + nvl(user.getLastName())).trim();
                        if (!fullName.isEmpty()) {
                            title = fullName;
                        } else if (user.getUsername() != null && !user.getUsername().isEmpty()) {
                            title = "@" + user.getUsername();
                        }
                    }
                } catch (Exception e) {
                    // Если юзер не найден, title останется пустым (честные данные)
                }
            }
        }

        // Если title все еще null (аномалия API), бросаем исключение, а не пишем "Чат 123"
        if (title == null) {
            throw new IllegalArgumentException("TDLib вернул null title для чата " + chat.id);
        }

        return new ChatMeta(chat.id, title.trim(), isGroup, isChannel, isSavedMessages);
    }

    public Chat toDomainChat(ChatMeta meta) {
        return new Chat("tg", meta.id(), meta.title(), meta.isGroup());
    }

    private long extractSenderId(TdApi.Message msg) {
        if (msg.senderId instanceof TdApi.MessageSenderUser u) return u.userId;
        if (msg.senderId instanceof TdApi.MessageSenderChat c) return -c.chatId;
        return 0;
    }

    private String contentToText(TdApi.MessageContent c) {
        // ... (оставляем ваш предыдущий код без изменений)
        if (c instanceof TdApi.MessageText t) return (t.text != null) ? t.text.text : "";
        if (c instanceof TdApi.MessagePhoto) return "[Фото]";
        if (c instanceof TdApi.MessageVideo) return "[Видео]";
        return "[Вложение]";
    }

    private String nvl(String s) { return (s != null) ? s : ""; }
}