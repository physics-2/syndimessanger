package v2.services;

import org.springframework.stereotype.Service;
import v2.entity.User;
import v2.repository.UserRepository;

import java.util.List;
import java.util.Optional;

@Service
public class UserService {
    private final UserRepository repo;

    public UserService(UserRepository repo) { this.repo = repo; }

    // 1. Сохранить/обновить пользователя (используется коннекторами)
    public User saveOrUpdate(User user) {
        return repo.saveOrUpdate(user);
    }

    // 2. Получить пользователя по ID
    public Optional<User> getUser(String source, Long userId) {
        return repo.findBySourceAndUserId(source, userId);
    }

    // 3. Управление тегами (бизнес-логика!)
    public List<String> addTag(String source, Long userId, String tag) {
        User user = repo.findBySourceAndUserId(source, userId)
                .orElseThrow(() -> new RuntimeException("User not found"));

        if (!user.getTags().contains(tag)) {
            user.getTags().add(tag);
            repo.save(user);
        }
        return user.getTags();
    }

    public List<String> removeTag(String source, Long userId, String tag) {
        User user = repo.findBySourceAndUserId(source, userId)
                .orElseThrow(() -> new RuntimeException("User not found"));

        user.getTags().remove(tag);
        repo.save(user);
        return user.getTags();
    }

    // 4. Поиск пользователей (для фронта)
    public List<User> searchUsers(String query) {
        // Можно добавить @Query в репозиторий:
        // @Query("SELECT u FROM User u WHERE LOWER(u.firstName) LIKE %:q%")
        return repo.searchByName(query);
    }
}