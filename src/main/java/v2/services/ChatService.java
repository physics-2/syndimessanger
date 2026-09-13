package v2.services;

import org.springframework.stereotype.Service;
import v2.entity.Chat;
import v2.entity.Message;
import v2.repository.ChatRepository;
import v2.repository.MessageRepository;

import java.util.List;
import java.util.Optional;

@Service
public class ChatService {
    private final ChatRepository chatRepo;
    private final MessageRepository msgRepo;

    public ChatService(ChatRepository chatRepo, MessageRepository msgRepo) {
        this.chatRepo = chatRepo;
        this.msgRepo = msgRepo;
    }

    // 1. Получить список всех чатов для левой панели
    // ВАЖНО: возвращает DTO с последним сообщением!
    public List<ChatDto> getAllChatsForFrontend() {
        List<Chat> chats = chatRepo.findAll();

        return chats.stream()
                .map(chat -> {
                    // Найти последнее сообщение для этого чата
                    List<Message> messages = msgRepo.findByChatIdOrderByTimestampDesc(chat.getChatId());
                    Message lastMsg = messages.isEmpty() ? null : messages.get(0);

                    return new ChatDto(
                            chat,
                            lastMsg != null ? lastMsg.getText() : null,
                            lastMsg != null ? lastMsg.getTimestamp() : null
                    );
                })
                .sorted((a, b) -> Long.compare(
                        b.timestamp != null ? b.timestamp() : 0,
                        a.timestamp() != null ? a.timestamp() : 0
                ))
                .toList();
    }

    // 2. Получить чат по ID
    public Optional<Chat> getChat(Long chatId) {
        return chatRepo.findByChatId(chatId);
    }

    // 3. Создать/обновить чат (используется коннекторами)
    public Chat saveOrUpdate(Chat chat) {
        return chatRepo.saveOrUpdate(chat);
    }

    // DTO для фронта
    public record ChatDto(Chat chat, String lastMessage, Long timestamp) {
        public Long getChatId() { return chat.getChatId(); }
        public String getTitle() { return chat.getTitle(); }
        public boolean isGroup() { return chat.isGroup(); }
        public String getSource() { return chat.getSource(); }
    }
}