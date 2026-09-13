package v2.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import v2.entity.Chat;
import java.util.Optional;

@Repository
public interface ChatRepository extends JpaRepository<Chat, Long> {

    Optional<Chat> findByChatId(Long chatId);

    default Chat saveOrUpdate(Chat chat) {
        Optional<Chat> existing = findByChatId(chat.getChatId());
        if (existing.isPresent()) {
            Chat found = existing.get();
            found.setTitle(chat.getTitle());
            found.setGroup(chat.isGroup());
            return save(found);
        }
        return save(chat);
    }
}