package v2.services;

import org.springframework.stereotype.Service;
import v2.entity.Message;
import v2.entity.User;
import v2.repository.MessageRepository;
import v2.repository.UserRepository;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class MessageService {
    private final MessageRepository msgRepo;
    private final UserRepository userRepo;

    public MessageService(MessageRepository msgRepo, UserRepository userRepo) {
        this.msgRepo = msgRepo;
        this.userRepo = userRepo;
    }

    // 1. Получить сообщения для чата (с именами авторов!)
    public List<MessageDto> getMessagesForChat(Long chatId) {
        List<Message> messages = msgRepo.findByChatIdOrderByTimestampAsc(chatId);
        if (messages.isEmpty()) return List.of();

        // Берем source из первого сообщения (все сообщения в чате из одной соцсети)
        String source = messages.get(0).getSource();

        // Собираем ID авторов из соцсети
        Set<Long> authorIds = messages.stream()
                .map(Message::getAuthorId)
                .collect(Collectors.toSet());

        // Ищем пользователей по source и списку их userId в соцсети
        Map<Long, User> authors = userRepo.findBySourceAndUserIdIn(source, authorIds)
                .stream()
                .collect(Collectors.toMap(User::getUserId, u -> u)); // Ключ - userId из соцсети!

        return messages.stream()
                .map(msg -> {
                    User author = authors.get(msg.getAuthorId());
                    return new MessageDto(
                            msg,
                            author != null ? author.getFirstName() : "Unknown",
                            author != null ? author.getPhoto_url() : ""
                    );
                })
                .toList();
    }

    // 2. Сохранить сообщение (используется коннекторами)
    public Message saveMessage(Message msg) {
        // Проверка на дубликат (одно сообщение может прийти 2 раза)
        if (msgRepo.existsBySourceAndMessageId(msg.getSource(), msg.getMessageId())) {
            return msgRepo.findBySourceAndMessageId(msg.getSource(), msg.getMessageId()).get();
        }
        return msgRepo.save(msg);
    }

    // 3. Поиск по тексту сообщений
    public List<Message> searchMessages(String query) {
        return msgRepo.searchByText(query);
    }

    public record MessageDto(Message msg, String authorName, String authorAvatar) {
        // геттеры для JSON
    }
}