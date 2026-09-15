package v2.connectors.tg;

import it.tdlight.Init;
import it.tdlight.Log;
import it.tdlight.Slf4JLogMessageHandler;
import it.tdlight.client.*;
import it.tdlight.jni.TdApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import v2.Config;
import v2.connectors.base.*;
import v2.entity.User;
import v2.repository.ConfigRepository;
import v2.services.ChatService;
import v2.services.MessageService;
import v2.services.UserService;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
public class TgConnector implements BaseConnector {

    private final ConnectorConfig config = new ConnectorConfig();
    private static final Logger log = LoggerFactory.getLogger(TgConnector.class);

    private final int apiId;
    private final String apiHash;
    private final String phoneNumber;

    private SimpleTelegramClientFactory clientFactory;
    private SimpleTelegramClient client;
    private volatile long myUserId = 0;

    private volatile boolean isListening = false;
    private volatile boolean isScanning = false;
    private boolean downloadMedia = false;

    // Настройки сканирования
    private boolean scanGroups = true;
    private boolean scanPersonal = true;
    private boolean scanSavedMessages = true;
    private List<Long> whitelistGroupIds = new ArrayList<>();
    private List<Long> failedIds = new ArrayList<>();

    // Компоненты
    private TgMessageMapper mapper;
    private TgHistoryScanner scanner;
    private TgMediaDownloader mediaDownloader;

    // Кэши
    private final ConcurrentHashMap<Long, ChatMeta> chatCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, User> userCache = new ConcurrentHashMap<>();
    private final ExecutorService scanExecutor = Executors.newSingleThreadExecutor();

    // Сервисы
    private final UserService userService;
    private final ChatService chatService;
    private final MessageService messageService;
    private final ConfigRepository configRepository;

    public TgConnector(UserService userService, ChatService chatService, MessageService messageService, ConfigRepository configRepository) {
        this.userService = userService;
        this.chatService = chatService;
        this.messageService = messageService;
        this.configRepository = configRepository;
        this.apiId = Config.getTgApiId();
        this.apiHash = Config.getTgApiHash();
        this.phoneNumber = Config.getTgPhoneNumber();
    }

    public synchronized String startClient() {
        if (client != null) return "Client already started";

        try {
            Init.init();
            Log.setLogMessageHandler(0, new Slf4JLogMessageHandler());
            APIToken apiToken = new APIToken(apiId, apiHash);
            TDLibSettings settings = TDLibSettings.create(apiToken);
            settings.setDatabaseDirectoryPath(Path.of("tdlight-session", "data"));
            settings.setDownloadedFilesDirectoryPath(Path.of("tdlight-session", "downloads"));

            clientFactory = new SimpleTelegramClientFactory();
            SimpleTelegramClientBuilder builder = clientFactory.builder(settings);

            builder.addUpdateHandler(TdApi.UpdateNewChat.class, this::onUpdateNewChat);
            builder.addUpdateHandler(TdApi.UpdateNewMessage.class, this::onUpdateNewMessage);
            builder.addUpdateHandler(TdApi.UpdateAuthorizationState.class, this::onUpdateAuthorizationState);
            builder.addUpdateHandler(TdApi.UpdateUser.class, this::onUpdateUser);

            client = builder.build(AuthenticationSupplier.user(phoneNumber));

            // Инициализация компонентов
            mediaDownloader = new TgMediaDownloader(downloadMedia);
            mapper = new TgMessageMapper(myUserId, mediaDownloader);
            scanner = new TgHistoryScanner(client, mapper, userService, chatService, messageService);

            mapper.setUserResolver(this::getOrFetchUser);
            scanner.setUserResolver(this::getOrFetchUser);

            configRepository.saveOrUpdate(new v2.entity.Config(new ArrayList<>(),List.of(String.valueOf(client.getMeAsync().get().id)),new ArrayList<>()));

            return "Клиент запущен";
        } catch (Exception e) {
            return "Ошибка: " + e.getMessage();
        }
    }

    private void onUpdateUser(TdApi.UpdateUser update) {
        if (update.user == null) return;
        String avatar = mediaDownloader.downloadUserAvatar(update.user);
        User info = mapper.toDomainUser(update.user, avatar);
        userCache.put(update.user.id, info);
    }

    public synchronized void stopClient() {
        isListening = false;
        try { if (client != null) client.close(); } catch (Exception e) { /* ignore */ }
        try { if (clientFactory != null) clientFactory.close(); } catch (Exception e) { /* ignore */ }
        client = null;
        clientFactory = null;
        System.out.println("🛑 [TG] Клиент остановлен.");
    }

    private void onUpdateAuthorizationState(TdApi.UpdateAuthorizationState update) {
        TdApi.AuthorizationState state = update.authorizationState;
        System.out.println("Current auth state: "+state);
        if (state instanceof TdApi.AuthorizationStateReady) {
            System.out.println("✅ [TG] Авторизация успешна!");
            client.getMeAsync().whenComplete((me, err) -> {
                if (err == null && me != null) {
                    myUserId = me.id;
                    if (mapper != null) mapper.updateMyUserId(myUserId); // Обновляем ID в маппере
                    initializeUserConfig();
                }
            });
        } else if (state instanceof TdApi.AuthorizationStateClosed) {
            isListening = false;
        }
    }

    private void onUpdateNewChat(TdApi.UpdateNewChat update) {
        chatCache.put(update.chat.id, mapper.toChatMeta(update.chat));
    }

    private void onUpdateNewMessage(TdApi.UpdateNewMessage update) {
        if (!isListening) return;
        try {

            ChatMeta meta = chatCache.computeIfAbsent(update.message.chatId, this::fetchChatMeta);
            if (!isChatAllowed(meta)) return;

            long senderId = extractSenderId(update.message);
            if (update.message.senderId instanceof TdApi.MessageSenderUser) {
                User user = getOrFetchUser(senderId);
                userService.saveOrUpdate(user);
            }

            chatService.saveOrUpdate(mapper.toDomainChat(meta));
            messageService.saveMessage(mapper.toDomainMessage(update.message));

        } catch (Exception e) {
            // Логируем ошибку, но НЕ пишем кривые данные в БД
            System.err.println("❌ [TG] Пропуск сообщения " + update.message.id + " (ошибка получения данных): " + e.getMessage());
            if (Config.isDebugEnabled()) {
                e.printStackTrace();
            }
        }
    }

    private long extractSenderId(TdApi.Message msg) {
        if (msg.senderId instanceof TdApi.MessageSenderUser u) return u.userId;
        if (msg.senderId instanceof TdApi.MessageSenderChat c) return c.chatId;
        return 0;
    }

    private void initializeUserConfig() {
        List<String> tgIdsToSave = new ArrayList<>();
        if (myUserId != 0) tgIdsToSave.add(String.valueOf(myUserId));
        if (!tgIdsToSave.isEmpty()) {
            v2.entity.Config config = configRepository.get();
            configRepository.saveOrUpdate(new v2.entity.Config(config.getVk_ids(), tgIdsToSave,config.getMax_ids()));
        }
    }

    @Override
    public ConnectorResult startScan(ScanOptions options) {
        if (client == null) return ConnectorResult.fail("Клиент не запущен");
        if (isScanning) return ConnectorResult.fail("Сканирование уже идёт");

        isScanning = true;
        scanExecutor.submit(this::runScan);
        return ConnectorResult.ok("Сканирование запущено");
    }

    private void runScan() {
        try {

            List<ChatMeta> allowed = chatCache.values().stream()
                    .filter(this::isChatAllowed)
                    .toList();

            for (int i = 0; i < allowed.size(); i++) {
                ChatMeta meta = allowed.get(i);
                System.out.println("💬 [TG] Сканирую " + (i + 1) + "/" + allowed.size() + ": " + meta.title());
                try {
                    scanner.scanChatHistory(meta);
                } catch (Exception e) {
                    System.err.println("❌ [TG] Ошибка чата " + meta.id());
                }
                Thread.sleep(1000);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } finally {
            isScanning = false;
        }
    }

    private ChatMeta fetchChatMeta(long chatId) {
        try {
            TdApi.Chat chat = client.send(new TdApi.GetChat(chatId)).get(30, TimeUnit.SECONDS);
            return mapper.toChatMeta(chat);
        } catch (Exception e) {
            throw new RuntimeException("Не удалось получить метаданные чата " + chatId, e);
        }
    }

    private User getOrFetchUser(long userId) {
        User cached = userCache.get(userId);
        if (cached != null) return cached;

        if (failedIds.contains(userId)) {
            throw new RuntimeException("Пользователь " + userId + " уже помечен как недоступный");
        }


        try {
            TdApi.User u = client.send(new TdApi.GetUser(userId)).get(5, TimeUnit.SECONDS);


            if (u == null) {
                failedIds.add(userId);
                throw new RuntimeException("TDLib вернул null для пользователя " + userId);
            }

            String avatar = mediaDownloader.downloadUserAvatar(u);
            User info = mapper.toDomainUser(u, avatar);
            userCache.put(userId, info);
            return info;

        } catch (Exception e) {
            failedIds.add(userId);
            throw new RuntimeException("Не удалось получить пользователя " + userId + ": " + e.getMessage());
        }
    }

    private boolean isChatAllowed(ChatMeta meta) {
        if (meta.isSavedMessages()) return scanSavedMessages;
        if (!meta.isGroup()) return scanPersonal;
        if (!scanGroups) return false;
        if (!whitelistGroupIds.isEmpty() && !whitelistGroupIds.contains(meta.id())) return false;
        return true;
    }


    public List<Map<String, Object>> getAllGroups() {
        List<Map<String, Object>> groups = new ArrayList<>();

        for (ChatMeta meta : chatCache.values()) {
            if (meta.isGroup() || meta.isChannel()) {
                Map<String, Object> group = new HashMap<>();
                group.put("chatId", meta.id());
                group.put("title", meta.title());
                group.put("isChannel", meta.isChannel());
                group.put("type", meta.isChannel() ? "Канал" : "Группа");
                groups.add(group);
            }
        }


        groups.sort((a, b) -> {
            String titleA = (String) a.get("title");
            String titleB = (String) b.get("title");
            return titleA.compareToIgnoreCase(titleB);
        });

        return groups;
    }

    // ====================== BaseConnector Interface =======================

    @Override public String platform() { return "tg"; }

    @Override public ConnectorStatus getStatus() {
        return new ConnectorStatus("tg", client != null, isListening, isScanning, myUserId, config);
    }

    @Override public ConnectorResult start() { return ConnectorResult.ok(startClient()); }

    @Override public ConnectorResult stop() {
        stopClient();
        return ConnectorResult.ok("TG остановлен");
    }

    // ИСПРАВЛЕНО: Убрана бесконечная рекурсия из оригинального кода
    @Override public ConnectorResult startListening() {
        isListening = true;
        return ConnectorResult.ok("TG прослушка ВКЛ");
    }

    @Override public ConnectorResult stopListening() {
        isListening = false;
        return ConnectorResult.ok("TG прослушка ВЫКЛ");
    }

    @Override public ConnectorConfig getConfig() { return config; }


    @Override public ConnectorResult updateConfig(ConnectorConfig c) {
        if (c == null) {
            return ConnectorResult.fail("Конфигурация не может быть null");
        }


        this.scanGroups = c.scanGroups;
        this.scanPersonal = c.scanPersonal;
        this.whitelistGroupIds = c.whitelist;

        v2.entity.Config config = configRepository.get();
        List<String> myself = new ArrayList<>();
        myself.add(String.valueOf(myUserId));
        for (Long id:whitelistGroupIds){
            myself.add(String.valueOf(id));
        }
        config.setTg_ids(myself);
        configRepository.saveOrUpdate(config);


        return ConnectorResult.ok("Конфигурация Telegram обновлена");
    }


    public long sendMessage(String peer, String text, String attachments, Long replyTo) {
        if (text == null || text.trim().isEmpty()) {
            throw new IllegalArgumentException("Message text cannot be empty");
        }

        try {
            if (client == null) {
                throw new IllegalStateException("Telegram client is not started");
            }

            long chatId = Long.parseLong(peer);

            if (attachments != null && !attachments.isEmpty()) {
                //TODO:fix tis
                return TgSendSupport.sendText(client, chatId, text + "\n\n[Вложения: " + attachments + "]", replyTo != null ? replyTo : 0);
            } else {
                return TgSendSupport.sendText(client, chatId, text, replyTo != null ? replyTo : 0);
            }

        } catch (NumberFormatException e) {
            log.error("Invalid peer ID format: {}", peer, e);
            throw new IllegalArgumentException("Invalid peer ID: " + peer, e);
        } catch (Exception e) {
            log.error("Error sending message to TG peer {}", peer, e);
            throw new RuntimeException("TG send failed: " + e.getMessage(), e);
        }
    }
}