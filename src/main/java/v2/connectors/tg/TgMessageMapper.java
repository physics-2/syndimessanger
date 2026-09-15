package v2.connectors.tg;

import it.tdlight.jni.TdApi;
import v2.entity.Chat;
import v2.entity.Message;
import v2.entity.User;
import java.util.ArrayList;
import java.util.Objects;
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

    public Message toDomainMessage(TdApi.Message tgMsg) {
        long senderId = extractSenderId(tgMsg);
        String text = contentToText(tgMsg.content);
        String mediaUrl = mediaDownloader.extractMediaLocalPath(tgMsg.content);
        System.out.println(mediaUrl);
        return new Message("tg", tgMsg.id, tgMsg.chatId, senderId, text, mediaUrl, (long) tgMsg.date);
    }

    public User toDomainUser(TdApi.User user, String avatar) {
        String username = "";
        if(user.usernames != null){
             username = user.usernames.activeUsernames[0];
        }



        return new User("tg", user.id, user.firstName, nvl(user.lastName),
                username, avatar, nvl(user.phoneNumber), new ArrayList<>());
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
        }

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
        if (msg.senderId instanceof TdApi.MessageSenderChat c) return c.chatId;
        return 0;
    }

    private String contentToText(TdApi.MessageContent c) {
        if (c instanceof TdApi.MessageText t) return t.text.text;
        if (c instanceof TdApi.MessagePhoto photo) return Objects.equals(photo.caption.text, "") ? "[Photo]" : photo.caption.text;
        if (c instanceof TdApi.MessageVideo video) return Objects.equals(video.caption.text, "") ? "[Video]" : video.caption.text;
        if(c instanceof TdApi.MessageDocument doc) return Objects.equals(doc.caption.text, "") ? "[Doc]" : doc.caption.text;
        return "[Anything]";
    }

    public void  doMediaDownload(boolean Do){
        mediaDownloader.setDownloadEnabled(Do);
    }

    private String nvl(String s) { return (s != null) ? s : ""; }
}