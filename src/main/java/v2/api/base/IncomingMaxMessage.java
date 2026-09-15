package v2.api.base;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * DTO входящего сообщения из Python-коннектора (pymax).
 *
 * Точное зеркало payload'а, который шлёт python:
 * {
 *   "messageId": 123, "chatId": 456, "authorId": 789,
 *   "text": "...", "timestamp": 1757900000000,
 *   "firstName": "Ivan", "lastName": "Petrov",
 *   "source": "max", "isGroup": false, "chatTitle": "Ivan Petrov",
 *   "isOutgoing": false,
 *   "mediaUrl": "[\"https://...\"]",   // <-- СТРОКА с JSON-массивом, не массив!
 *   "phoneNumber": "+7...", "avatarPic": "https://..."
 * }
 *
 * ВАЖНО: timestamp объявлен как Object, потому что pymax может отдать
 * и int (epoch ms), и строку. Разбор см. в контроллере (parseTimestamp).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IncomingMaxMessage {

    private Object messageId;      // long или string
    private Object chatId;         // long или string
    private Object authorId;       // long или string
    private String text;
    private Object timestamp;      // Number | String
    private String firstName;
    private String lastName;
    private String source;
    private Boolean isGroup;
    private String chatTitle;
    private Boolean isOutgoing;
    private String mediaUrl;       // JSON-массив в виде строки (или пустая строка)
    private String phoneNumber;
    private String avatarPic;

    public IncomingMaxMessage() {
    }

    public Object getMessageId()   { return messageId; }
    public void setMessageId(Object messageId) { this.messageId = messageId; }

    public Object getChatId()      { return chatId; }
    public void setChatId(Object chatId) { this.chatId = chatId; }

    public Object getAuthorId()    { return authorId; }
    public void setAuthorId(Object authorId) { this.authorId = authorId; }

    public String getText()        { return text; }
    public void setText(String text) { this.text = text; }

    public Object getTimestamp()   { return timestamp; }
    public void setTimestamp(Object timestamp) { this.timestamp = timestamp; }

    public String getFirstName()   { return firstName; }
    public void setFirstName(String firstName) { this.firstName = firstName; }

    public String getLastName()    { return lastName; }
    public void setLastName(String lastName) { this.lastName = lastName; }

    public String getSource()      { return source; }
    public void setSource(String source) { this.source = source; }

    public Boolean getIsGroup()    { return isGroup; }
    public void setIsGroup(Boolean isGroup) { this.isGroup = isGroup; }

    public String getChatTitle()   { return chatTitle; }
    public void setChatTitle(String chatTitle) { this.chatTitle = chatTitle; }

    public Boolean getIsOutgoing() { return isOutgoing; }
    public void setIsOutgoing(Boolean isOutgoing) { this.isOutgoing = isOutgoing; }

    public String getMediaUrl()    { return mediaUrl; }
    public void setMediaUrl(String mediaUrl) { this.mediaUrl = mediaUrl; }

    public String getPhoneNumber() { return phoneNumber; }
    public void setPhoneNumber(String phoneNumber) { this.phoneNumber = phoneNumber; }

    public String getAvatarPic()   { return avatarPic; }
    public void setAvatarPic(String avatarPic) { this.avatarPic = avatarPic; }

    @Override
    public String toString() {
        return "IncomingMaxMessage{chatId=" + chatId + ", messageId=" + messageId +
                ", authorId=" + authorId + ", chatTitle='" + chatTitle + '\'' +
                ", isGroup=" + isGroup + ", isOutgoing=" + isOutgoing +
                ", text='" + (text == null ? "" : text) + "'}";
    }
}
