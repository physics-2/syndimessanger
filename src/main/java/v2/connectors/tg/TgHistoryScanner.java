package v2.connectors.tg;

import it.tdlight.client.SimpleTelegramClient;
import it.tdlight.jni.TdApi;
import v2.entity.User;
import v2.services.ChatService;
import v2.services.MessageService;
import v2.services.UserService;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class TgHistoryScanner {
    private final SimpleTelegramClient client;
    private final TgMessageMapper mapper;
    private final UserService userService;
    private final ChatService chatService;
    private final MessageService messageService;

    private Function<Long, User> userResolver; // 👈 Функция для получения юзера

    private static final int HISTORY_PAGE = 100;
    private static final int MAX_PAGES_PER_CHAT = 20;

    public TgHistoryScanner(SimpleTelegramClient client, TgMessageMapper mapper,
                            UserService userService, ChatService chatService, MessageService messageService) {
        this.client = client;
        this.mapper = mapper;
        this.userService = userService;
        this.chatService = chatService;
        this.messageService = messageService;
    }

    public void setUserResolver(Function<Long, User> userResolver) {
        this.userResolver = userResolver;
    }

    public void scanChatHistory(ChatMeta meta) {
        long fromMessageId = 0;
        int pages = 0;

        chatService.saveOrUpdate(mapper.toDomainChat(meta));

        while (pages++ < MAX_PAGES_PER_CHAT) {
            try {
                TdApi.Messages msgs = client.send(new TdApi.GetChatHistory(
                                meta.id(), fromMessageId, 0, HISTORY_PAGE, false))
                        .get(60, TimeUnit.SECONDS);

                if (msgs.messages == null || msgs.messages.length == 0) break;

                for (int i = msgs.messages.length - 1; i >= 0; i--) {
                    TdApi.Message tgMsg = msgs.messages[i];

                    // 👇 РЕШЕНИЕ ПРОБЛЕМЫ 1: Сохраняем автора перед сохранением сообщения
                    long senderId = extractSenderId(tgMsg);
                    if (senderId > 0 && userResolver != null) {
                        try {
                            User user = userResolver.apply(senderId);
                            if (user != null) {
                                userService.saveOrUpdate(user);
                            }
                        } catch (Exception e) {
                            System.err.println("⚠️ Не удалось сохранить автора " + senderId + ": " + e.getMessage());
                        }
                    }

                    messageService.saveMessage(mapper.toDomainMessage(tgMsg, meta));
                }

                fromMessageId = msgs.messages[msgs.messages.length - 1].id;
                Thread.sleep(500);
            } catch (Exception e) {
                System.err.println("⚠️ [TG] Ошибка истории чата " + meta.id() + ": " + e.getMessage());
                break;
            }
        }
    }

    private long extractSenderId(TdApi.Message msg) {
        if (msg.senderId instanceof TdApi.MessageSenderUser u) return u.userId;
        if (msg.senderId instanceof TdApi.MessageSenderChat c) return -c.chatId;
        return 0;
    }
}