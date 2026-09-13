package v2.entity;

import jakarta.persistence.*;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "users", uniqueConstraints = {
        @UniqueConstraint(columnNames = {"source", "user_id"})
})
public class User {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id; // Первичный ключ БД

    @Column(nullable = false)
    private String source; // "vk", "tg", "max"

    @Column(name = "user_id", nullable = false)
    private Long userId; // ID пользователя в соцсети (бывший platform_id)

    @Column(name = "first_name")
    private String firstName;

    @Column(name = "last_name")
    private String lastName;

    @Column(name = "username")
    private String username;

    @Column(name = "photo_url", length = 1000)
    private String photo_url;

    @Column(name = "phone_number")
    private String phone_number;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "user_tags", joinColumns = @JoinColumn(name = "user_db_id"))
    @Column(name = "tag")
    private List<String> tags = new ArrayList<>();

    @Column(name = "updated_at")
    private String updated_at;

    // Конструкторы
    public User() {}

    public User(String source, Long userId, String firstName, String lastName, String username, String photo_url, String phone_number, List<String> tags) {
        this.source = source;
        this.userId = userId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.username = username;
        this.photo_url = photo_url;
        this.phone_number = phone_number;
        this.tags = tags != null ? tags : new ArrayList<>();
    }

    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }
    public Long getUserId() { return userId; } // ВАЖНО: getUserId, а не getId
    public void setUserId(Long userId) { this.userId = userId; }
    public String getFirstName() { return firstName; }
    public void setFirstName(String firstName) { this.firstName = firstName; }
    public String getLastName() { return lastName; }
    public void setLastName(String lastName) { this.lastName = lastName; }
    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }
    public String getPhoto_url() { return photo_url; }
    public void setPhoto_url(String photo_url) { this.photo_url = photo_url; }
    public String getPhone_number() { return phone_number; }
    public void setPhone_number(String phone_number) { this.phone_number = phone_number; }
    public List<String> getTags() { return tags; }
    public void setTags(List<String> tags) { this.tags = tags; }
    public String getUpdated_at() { return updated_at; }
    public void setUpdated_at(String updated_at) { this.updated_at = updated_at; }
}