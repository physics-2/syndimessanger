package v2.entity;

import jakarta.persistence.*;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "config")
public class Config {

    @Id
    private Long id; // Убрали @GeneratedValue, так как это синглтон (ID всегда = 1)

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "config_vk_ids", joinColumns = @JoinColumn(name = "config_id"))
    @Column(name = "vk_id")
    private List<String> vk_ids = new ArrayList<>();

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "config_tg_ids", joinColumns = @JoinColumn(name = "config_id"))
    @Column(name = "tg_id")
    private List<String> tg_ids = new ArrayList<>();

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "config_max_ids", joinColumns = @JoinColumn(name = "config_id"))
    @Column(name = "max_id")
    private List<String> max_ids = new ArrayList<>();

    public Config() {}

    public Config(List<String> vk_ids, List<String> tg_ids, List<String> max_ids) {
        this.id = 1L; // Жестко задаем ID для синглтона
        this.vk_ids = vk_ids != null ? vk_ids : new ArrayList<>();
        this.tg_ids = tg_ids != null ? tg_ids : new ArrayList<>();
        this.max_ids = max_ids != null ? max_ids : new ArrayList<>();
    }

    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public List<String> getVk_ids() { return vk_ids; }
    public void setVk_ids(List<String> vk_ids) { this.vk_ids = vk_ids; }
    public List<String> getTg_ids() { return tg_ids; }
    public void setTg_ids(List<String> tg_ids) { this.tg_ids = tg_ids; }
    public List<String> getMax_ids() { return max_ids; }
    public void setMax_ids(List<String> max_ids) { this.max_ids = max_ids; }
}