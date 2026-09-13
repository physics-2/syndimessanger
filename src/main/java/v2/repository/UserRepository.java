package v2.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import v2.entity.User;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Repository
public interface UserRepository extends JpaRepository<User, Long> {

    // Исправлено: ищем по userId (ID в соцсети), а не по id БД
    Optional<User> findBySourceAndUserId(String source, Long userId);

    // НОВОЕ: нужно для MessageService, чтобы найти сразу пакет пользователей
    List<User> findBySourceAndUserIdIn(String source, Set<Long> userIds);

    List<User> findBySource(String source);

    @Query("SELECT u FROM User u WHERE " +
            "LOWER(COALESCE(u.firstName, '')) LIKE LOWER(CONCAT('%', :query, '%')) OR " +
            "LOWER(COALESCE(u.lastName, '')) LIKE LOWER(CONCAT('%', :query, '%')) OR " +
            "LOWER(COALESCE(u.username, '')) LIKE LOWER(CONCAT('%', :query, '%'))")
    List<User> searchByName(@Param("query") String query);

    default User saveOrUpdate(User user) {
        Optional<User> existing = findBySourceAndUserId(user.getSource(), user.getUserId());
        if (existing.isPresent()) {
            User found = existing.get();
            found.setFirstName(user.getFirstName());
            found.setLastName(user.getLastName());
            found.setUsername(user.getUsername());
            found.setPhoto_url(user.getPhoto_url());
            found.setPhone_number(user.getPhone_number());
            return save(found);
        }
        return save(user);
    }

    @Query("SELECT DISTINCT u.id FROM User u WHERE :tags MEMBER OF u.tags")
    List<Long> findUserIdsByTags(List<String> tags);
}