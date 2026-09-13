package v2.entity;

import jakarta.persistence.*;

@Entity
@Table(name = "messages", uniqueConstraints = {
        @UniqueConstraint(columnNames = {"source", "message_id"})
})
public class Message {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String source;

    @Column(name = "message_id", nullable = false)
    private Long messageId;

    @Column(name = "chat_id", nullable = false)
    private Long chatId;

    @Column(name = "author_id", nullable = false)
    private Long authorId;

    @Column(name = "text", columnDefinition = "TEXT")
    private String text;

    @Column(name = "timestamp")
    private Long timestamp;

    @Column(name = "mediaUrl", length = 1000)
    private String mediaUrl;

    public Message() {}

    public Message(String source, Long messageId, Long chatId, Long authorId, String text, String mediaUrl) {
        this.source = source;
        this.messageId = messageId;
        this.chatId = chatId;
        this.authorId = authorId;
        this.text = text;
        this.mediaUrl = mediaUrl;
        this.timestamp = System.currentTimeMillis(); // КРИТИЧНО: задаем время создания
    }


    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }
    public Long getMessageId() { return messageId; }
    public void setMessageId(Long messageId) { this.messageId = messageId; }
    public Long getChatId() { return chatId; }
    public void setChatId(Long chatId) { this.chatId = chatId; }
    public Long getAuthorId() { return authorId; }
    public void setAuthorId(Long authorId) { this.authorId = authorId; }
    public String getText() { return text; }
    public void setText(String text) { this.text = text; }
    public Long getTimestamp() { return timestamp; }
    public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }
    public String getMediaUrl() { return mediaUrl; }
    public void setMediaUrl(String mediaUrl) { this.mediaUrl = mediaUrl; }
}