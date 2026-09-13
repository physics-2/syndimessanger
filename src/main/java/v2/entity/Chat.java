package v2.entity;

import jakarta.persistence.*;

@Entity
@Table(name = "chats", uniqueConstraints = {
        @UniqueConstraint(columnNames = {"source", "chat_id"})
})
public class Chat {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String source;

    @Column(name = "chat_id", nullable = false)
    private Long chatId;

    @Column(name = "chat_title", nullable = false)
    private String title;

    @Column(name = "is_group", nullable = false)
    private boolean isGroup;

    @Column(name = "updated_at")
    private String updated_at;

    public Chat() {}

    public Chat(String source, Long chatId, String title, boolean isGroup) {
        this.source = source;
        this.chatId = chatId;
        this.title = title;
        this.isGroup = isGroup;
    }

    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }
    public Long getChatId() { return chatId; }
    public void setChatId(Long chatId) { this.chatId = chatId; }
    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }
    public boolean isGroup() { return isGroup; }
    public void setGroup(boolean group) { isGroup = group; }
    public String getUpdated_at() { return updated_at; }
    public void setUpdated_at(String updated_at) { this.updated_at = updated_at; }
}