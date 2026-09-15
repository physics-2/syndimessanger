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

    // Поиск по userId (ID в соцсети), а не по id БД
    Optional<User> findBySourceAndUserId(String source, Long userId);

    // Нужно MessageService, чтобы найти сразу пакет пользователей
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

    // =====================================================================================
    //  ПОИСК ПО ТЕГАМ — ИСПРАВЛЕНО
    //
    //  Было:
    //      @Query("SELECT DISTINCT u.id FROM User u WHERE :tags MEMBER OF u.tags")
    //      List<Long> findUserIdsByTags(List<String> tags);
    //
    //  Две ошибки:
    //
    //  1) MEMBER OF принимает ОДИН элемент, а не коллекцию.
    //     `:tags MEMBER OF u.tags` с List<String> → Hibernate пытается привязать ArrayList
    //     к параметру типа String →
    //     "Could not convert 'java.util.ArrayList' to 'java.lang.String'" /
    //     "Parameter value [[23]] did not match expected type [basicType(java.lang.String)]".
    //     Для коллекции нужен JOIN по @ElementCollection + IN.
    //
    //  2) u.id — это PK таблицы users, а BroadcastService.resolvePeerId() возвращает его
    //     как есть и подставляет в peer_id при отправке. То есть рассылка ушла бы по
    //     peer_id = 23 вместо реального id пользователя в соцсети.
    //     Нужен u.userId (User.userId — «ID пользователя в соцсети»).
    //
    //  Семантика: пользователь попадает в выборку, если у него есть ХОТЯ БЫ ОДИН из тегов (ANY).
    //  DISTINCT обязателен: без него пользователь с двумя совпавшими тегами вернулся бы дважды
    //  и получил бы два одинаковых сообщения.
    //
    //  ВАЖНО: не вызывайте метод с пустым списком — `IN ()` в JPQL даёт ошибку/всегда false.
    // BroadcastService должен проверять tags.isEmpty() сам (см. ниже).
    // =====================================================================================

    @Query("SELECT DISTINCT u.userId FROM User u JOIN u.tags t WHERE t IN :tags")
    List<Long> findUserIdsByTags(@Param("tags") List<String> tags);
    /**
     * То же, но с фильтром по соцсети — полезно, когда рассылка идёт через один коннектор:
     * у VK и TG userId живут в разных пространствах, и слать «VK-подписчикам» через TG нельзя.
     * BroadcastService может вызывать его вместо findUserIdsByTags, передавая connectorType.
     */
    @Query("SELECT DISTINCT u.userId FROM User u JOIN u.tags t WHERE u.source = :source AND t IN :tags")
    List<Long> findUserIdsBySourceAndTags(@Param("source") String source, @Param("tags") List<String> tags);

    /** Сколько пользователей найдётся по тегам — для предпросмотра перед реальной рассылкой. */
    @Query("SELECT COUNT(DISTINCT u.userId) FROM User u JOIN u.tags t WHERE t IN :tags")
    long countUserIdsByTags(@Param("tags") List<String> tags);

    /** Все теги, которые реально есть в базе — для подсказок во фронте. */
    @Query("SELECT DISTINCT t FROM User u JOIN u.tags t ORDER BY t")
    List<String> findAllTags();
}