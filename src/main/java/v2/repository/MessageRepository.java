package v2.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import v2.entity.Message;

import java.util.List;
import java.util.Optional;

@Repository
public interface MessageRepository extends JpaRepository<Message, Long> {

    List<Message> findByChatIdOrderByTimestampAsc(Long chatId);

    // Нужно для ChatService (сортировка по убыванию для последнего сообщения)
    List<Message> findByChatIdOrderByTimestampDesc(Long chatId);

    List<Message> findBySource(String source);

    // Нужно для MessageService (проверка на дубликаты)
    boolean existsBySourceAndMessageId(String source, Long messageId);

    Optional<Message> findBySourceAndMessageId(String source, Long messageId);

    // Полнотекстовый поиск по сообщениям
    @Query("SELECT m FROM Message m WHERE LOWER(m.text) LIKE LOWER(CONCAT('%', :query, '%'))")
    List<Message> searchByText(@Param("query") String query);

    default Message saveOrUpdate(Message message) {
        return save(message);
    }
}