package ru.TG;

import it.tdlight.Init;
import it.tdlight.Log;
import it.tdlight.Slf4JLogMessageHandler;
import it.tdlight.client.APIToken;
import it.tdlight.client.AuthenticationSupplier;
import it.tdlight.client.SimpleTelegramClient;
import it.tdlight.client.SimpleTelegramClientBuilder;
import it.tdlight.client.SimpleTelegramClientFactory;
import it.tdlight.client.TDLibSettings;
import it.tdlight.jni.TdApi;
import org.springframework.stereotype.Component;
import ru.Config;
import ru.database.DatabaseManager;
import ru.database.UnifiedMessage;
import ru.database.UserInfo;
import ru.send.SendResult;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
public class TgConnector {

    private final DatabaseManager db;
    private final int apiId;
    private final String apiHash;
    private final String phoneNumber;

    private SimpleTelegramClientFactory clientFactory;
    private SimpleTelegramClient client;
    private volatile long myUserId = 0;

    private volatile boolean isListening = false;
    private volatile boolean isScanning = false;
    private boolean downloadMedia = false; // TG не даёт прямых URL: файлы скачиваются на диск

    /**
     * InputMessageDocument.disableContentTypeDetection.
     * TgSend всегда ставит true (файл уходит именно файлом, Telegram тип не угадывает);
     * флаг оставлен для совместимости с PUT /api/tg/config и отдаётся в getConfig().
     */
    private volatile boolean docContentTypeDetectionDisabled = true;

    // === НАСТРОЙКИ СКАНИРОВАНИЯ (как в VK) ===
    private boolean scanGroups = true;      // сканировать группы/каналы
    private boolean scanPersonal = true;    // сканировать личные чаты
    private boolean scanSavedMessages = true; // сканировать "Избранное" (чат с самим собой)
    private List<Long> whitelistGroupIds = new ArrayList<>(); // пустой = сканировать ВСЕ группы

    // Кэши: чаты приходят апдейтами UpdateNewChat (гарантия порядка TDLib)
    private final ConcurrentHashMap<Long, ChatMeta> chatCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, UserInfo> userCache = new ConcurrentHashMap<>();
    private final ExecutorService scanExecutor = Executors.newSingleThreadExecutor();

    private static final int HISTORY_PAGE = 100;
    private static final int MAX_PAGES_PER_CHAT = 20; // защита: до ~2000 сообщений на чат

    public record ChatMeta(long id, String title, boolean isGroup, boolean isChannel, boolean isSavedMessages) {}

    public TgConnector(DatabaseManager db) {
        this.db = db;
        this.apiId = Config.getTgApiId();
        this.apiHash = Config.getTgApiHash();
        this.phoneNumber = Config.getTgPhoneNumber();
    }

    // =========================================================================================
    // 🚀 СТАРТ / СТОП
    // =========================================================================================

    public synchronized String start() {
        if (client != null) return "Клиент уже запущен";

        try {
            Init.init();
            Log.setLogMessageHandler(0, new Slf4JLogMessageHandler()); // только ERRORS
            APIToken apiToken = new APIToken(apiId, apiHash);
            TDLibSettings settings = TDLibSettings.create(apiToken);
            settings.setDatabaseDirectoryPath(Path.of("tdlight-session", "data"));
            settings.setDownloadedFilesDirectoryPath(Path.of("tdlight-session", "downloads"));

            clientFactory = new SimpleTelegramClientFactory();
            SimpleTelegramClientBuilder builder = clientFactory.builder(settings);

            builder.addUpdateHandler(TdApi.UpdateNewChat.class, this::onUpdateNewChat);
            builder.addUpdateHandler(TdApi.UpdateNewMessage.class, this::onUpdateNewMessage);
            builder.addUpdateHandler(TdApi.UpdateAuthorizationState.class, this::onUpdateAuthorizationState);

            // 👇 ДОБАВИТЬ: Автоматическое кэширование пользователей
            builder.addUpdateHandler(TdApi.UpdateUser.class, this::onUpdateUser);

            client = builder.build(AuthenticationSupplier.user(Config.getTgPhoneNumber()));
            isListening = true;

            return "Клиент запущен";
        } catch (Exception e) {
            return "Ошибка: " + e.getMessage();
        }
    }

    private void onUpdateUser(TdApi.UpdateUser update) {
        TdApi.User u = update.user;
        if (u == null) return;

        String username = (u.usernames != null && u.usernames.activeUsernames.length > 0)
                ? u.usernames.activeUsernames[0] : "";
        String avatar = ""; // аватары скачиваем только при необходимости

        UserInfo info = new UserInfo("tg", u.id, nvl(u.firstName), nvl(u.lastName),
                username, avatar, nvl(u.phoneNumber), new ArrayList<>());

        userCache.put(u.id, info);
        if (Config.isDebugEnabled()) {
            System.out.println("👤 [TG] Пользователь загружен через UpdateUser: " + u.firstName + " " + u.lastName);
        }
    }

    public synchronized void stop() {
        isListening = false;
        try {
            if (client != null) client.close();
        } catch (Exception e) {
            System.err.println("⚠️ [TG] Ошибка закрытия клиента: " + e.getMessage());
        }
        try {
            if (clientFactory != null) clientFactory.close();
        } catch (Exception e) {
            // игнорируем
        }
        client = null;
        clientFactory = null;
        System.out.println("🛑 [TG] Клиент остановлен.");
    }

    private void onUpdateAuthorizationState(TdApi.UpdateAuthorizationState update) {
        TdApi.AuthorizationState state = update.authorizationState;

        if (state instanceof TdApi.AuthorizationStateReady) {
            System.out.println("✅ [TG] Авторизация успешна!");
            client.getMeAsync().whenComplete((me, err) -> {
                if (err == null && me != null) {
                    myUserId = me.id;
                    System.out.println("👤 [TG] Мой ID: " + myUserId);
                    initializeUserConfig();
                }
            });
        } else if (state instanceof TdApi.AuthorizationStateClosed) {
            System.out.println("🛑 [TG] Сессия закрыта.");
            isListening = false;
        } else {
            System.out.println("🔐 [TG] Состояние авторизации: " + state.getClass().getSimpleName());
        }
    }

    // =========================================================================================
    // 📥 АПДЕЙТЫ (LONG POLL от TDLib)
    // =========================================================================================

    private void onUpdateNewChat(TdApi.UpdateNewChat update) {
        TdApi.Chat chat = update.chat;
        chatCache.put(chat.id, toMeta(chat));
    }

    private void onUpdateNewMessage(TdApi.UpdateNewMessage update) {
        if (!isListening) return;
        try {
            processAndSave(update.message);
        } catch (Exception e) {
            System.err.println("❌ [TG] Ошибка обработки сообщения: " + e.getMessage());
        }
    }

    // =========================================================================================
    // 📡 СКАНИРОВАНИЕ ИСТОРИИ
    // =========================================================================================

    /**
     * Сохраняет свой TG ID (и другие нужные идентификаторы) в БД через DatabaseManager
     */
    private void initializeUserConfig() {
        System.out.println("⚙️ [TG] Инициализация конфигурации пользователей...");

        List<String> tgIdsToSave = new ArrayList<>();

        // 1. Добавляем свой ID
        if (myUserId > 0) {
            tgIdsToSave.add(String.valueOf(myUserId));
            System.out.println("   ➕ Добавлен свой ID: " + myUserId);
        } else {
            System.err.println("   ⚠️ Не удалось получить свой ID, пропускаем.");
        }

        if (!tgIdsToSave.isEmpty()) {
            db.updateTelegramIds(tgIdsToSave);
            System.out.println("✅ [TG] Конфигурация сохранена в БД: " + tgIdsToSave.size() + " ID");
        }
    }

    public synchronized String startScan() {
        if (client == null) return "Клиент не запущен (POST /api/tg/start)";
        if (isScanning) return "Сканирование уже идёт";
        isScanning = true;
        scanExecutor.submit(this::runScan);
        return "Сканирование запущено";
    }

    private void runScan() {
        try {
            System.out.println("🔄 [TG] Ожидание заполнения кэша чатов (30 секунд)...");

            for (int i = 0; i < 15; i++) {
                System.out.println("⏳ [TG] Ждём... (в кэше: " + chatCache.size() + " чатов)");
                Thread.sleep(2000);
                if (chatCache.size() > 50 && i > 5) break;
            }

            // Фильтруем чаты по настройкам ДО сканирования
            List<ChatMeta> allowed = chatCache.values().stream()
                    .filter(this::isChatAllowed)
                    .toList();

            System.out.println("📋 [TG] Всего чатов: " + chatCache.size() +
                    ", разрешено к сканированию: " + allowed.size() +
                    " (группы: " + (scanGroups ? "ВКЛ" : "ВЫКЛ") +
                    ", ЛС: " + (scanPersonal ? "ВКЛ" : "ВЫКЛ") +
                    ", вайтлист: " + (whitelistGroupIds.isEmpty() ? "выключен" : "ВКЛ") + ")");

            if (allowed.isEmpty()) {
                System.err.println("⚠️ [TG] Нет чатов для сканирования (всё отфильтровано)");
                return;
            }

            int scanned = 0;
            int failed = 0;

            for (int i = 0; i < allowed.size(); i++) {
                ChatMeta meta = allowed.get(i);
                System.out.println("💬 [TG] Сканирую " + (i + 1) + "/" + allowed.size() + ": " + meta.title() +
                        (meta.isChannel() ? " (канал)" : meta.isGroup() ? " (группа)" : ""));

                try {
                    scanChatHistory(meta.id());
                    scanned++;
                } catch (Exception e) {
                    System.err.println("❌ [TG] Ошибка чата " + meta.id() + ": " + e.getMessage());
                    failed++;
                }

                Thread.sleep(1000);
            }

            System.out.println("✅ [TG] Сканирование завершено: " + scanned + " успешно, " + failed + " с ошибками");

        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.err.println("❌ [TG] Критическая ошибка: " + e.getMessage());
        } finally {
            isScanning = false;
            System.out.println("🏁 [TG] Флаг isScanning сброшен");
        }
    }

    private void scanChatHistory(long chatId) {
        long fromMessageId = 0; // маркер пагинации: id последнего полученного сообщения
        int pages = 0;
        while (pages++ < MAX_PAGES_PER_CHAT) {
            try {
                TdApi.Messages msgs = client
                        .send(new TdApi.GetChatHistory(chatId, fromMessageId, 0, HISTORY_PAGE, false))
                        .get(60, TimeUnit.SECONDS);

                if (msgs.messages == null || msgs.messages.length == 0) break;

                // Приходят от новых к старым -> сохраняем в хронологическом порядке
                for (int i = msgs.messages.length - 1; i >= 0; i--) {
                    processAndSave(msgs.messages[i]);
                }
                fromMessageId = msgs.messages[msgs.messages.length - 1].id;
                Thread.sleep(500);
            } catch (Exception e) {
                System.err.println("⚠️ [TG] Ошибка истории чата " + chatId + ": " + e.getMessage());
                break;
            }
        }
    }

    // =========================================================================================
    // 💾 СОХРАНЕНИЕ В БД
    // =========================================================================================

    private void processAndSave(TdApi.Message msg) {
        long chatId = msg.chatId;
        ChatMeta meta = chatCache.computeIfAbsent(chatId, this::fetchChatMeta);

        // 👇 ДОБАВИТЬ: проверка вайтлиста
        if (!isChatAllowed(meta)) {
            return; // молча пропускаем запрещённые чаты
        }

        long senderId = extractSenderId(msg);

        // 👇 Анти-дубль: свои отправленные сообщения мы уже положили в БД при отправке,
        //    а TG ещё раз пришлёт их через UpdateNewMessage.
        if (myUserId > 0 && senderId == myUserId && TgSend.consumeSentMessageId(msg.id)) {
            if (Config.isDebugEnabled()) {
                System.out.println("♻️ [TG] Своё сообщение " + msg.id + " уже в БД — пропускаем апдейт");
            }
            return;
        }

        UserInfo user = null;
        if (senderId > 0) {
            user = getOrFetchUser(senderId);
            db.saveOrUpdateUser(user);
        }

        String text = contentToText(msg.content);
        String mediaUrl = downloadMedia ? extractMediaLocalPath(msg.content) : "";

        db.saveOrUpdateChat("tg", chatId, meta.title(), meta.isGroup());
        db.saveMessage(new UnifiedMessage("tg", msg.id, chatId, senderId, text, msg.date, mediaUrl));
    }

    private long extractSenderId(TdApi.Message msg) {
        if (msg.senderId instanceof TdApi.MessageSenderUser u) return u.userId;
        if (msg.senderId instanceof TdApi.MessageSenderChat c) return -c.chatId; // как минус-ид групп в VK
        return 0;
    }

    private ChatMeta toMeta(TdApi.Chat chat) {
        boolean isGroup = false;
        boolean isChannel = false;

        if (chat.type instanceof TdApi.ChatTypeBasicGroup) {
            isGroup = true;
        } else if (chat.type instanceof TdApi.ChatTypeSupergroup sg) {
            isGroup = true;
            isChannel = sg.isChannel; // супергруппа может быть каналом
        }

        // "Избранное" = чат с самим собой
        boolean isSavedMessages = (chat.type instanceof TdApi.ChatTypePrivate p) && p.userId == myUserId;

        return new ChatMeta(chat.id, chat.title != null ? chat.title : "Чат " + chat.id,
                isGroup, isChannel, isSavedMessages);
    }

    private ChatMeta fetchChatMeta(long chatId) {
        try {
            TdApi.Chat chat = client.send(new TdApi.GetChat(chatId)).get(30, TimeUnit.SECONDS);
            return toMeta(chat);
        } catch (Exception e) {
            if (Config.isDebugEnabled()) {
                System.err.println("⚠️ [TG] Не удалось получить чат " + chatId + ": " + e.getMessage());
            }
            // Fallback: создаём мету с минимальными данными (все флаги по умолчанию)
            return new ChatMeta(chatId, "Чат " + chatId, false, false, false);
        }
    }

    private UserInfo getOrFetchUser(long userId) {
        // 1. Проверяем кэш (заполняется через UpdateUser)
        UserInfo cached = userCache.get(userId);
        if (cached != null) return cached;

        // 2. Если нет в кэше — пробуем получить через API с retry
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                TdApi.User u = client.send(new TdApi.GetUser(userId)).get(30, TimeUnit.SECONDS);
                String username = (u.usernames != null && u.usernames.activeUsernames.length > 0)
                        ? u.usernames.activeUsernames[0] : "";
                String avatar = (downloadMedia && u.profilePhoto != null)
                        ? downloadFileToLocal(u.profilePhoto.small.id) : "";

                UserInfo info = new UserInfo("tg", u.id, nvl(u.firstName), nvl(u.lastName),
                        username, avatar, nvl(u.phoneNumber), new ArrayList<>());

                userCache.put(userId, info);
                return info;

            } catch (Exception e) {
                if (attempt < 2) {
                    try { Thread.sleep(1000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                }
            }
        }

        // 3. Fallback: если все retry провалились
        UserInfo fallback = new UserInfo("tg", userId, "User_" + userId, "", "", "", "", new ArrayList<>());
        userCache.put(userId, fallback);
        return fallback;
    }

    // =========================================================================================
    // 🧩 КОНТЕНТ И МЕДИА
    // =========================================================================================

    private String contentToText(TdApi.MessageContent c) {
        if (c instanceof TdApi.MessageText t) return (t.text != null) ? t.text.text : "";
        if (c instanceof TdApi.MessagePhoto) return "[Фото]";
        if (c instanceof TdApi.MessageVideo) return "[Видео]";
        if (c instanceof TdApi.MessageVideoNote) return "[Видеосообщение]";
        if (c instanceof TdApi.MessageVoiceNote) return "[Голосовое сообщение]";
        if (c instanceof TdApi.MessageAnimation) return "[GIF]";
        if (c instanceof TdApi.MessageSticker) return "[Стикер]";
        if (c instanceof TdApi.MessageLocation) return "[Геолокация]";
        if (c instanceof TdApi.MessageAudio a) return "[Аудио: " + nvl(a.audio.performer) + " - " + nvl(a.audio.title) + "]";
        if (c instanceof TdApi.MessageDocument d) return "[Документ: " + nvl(d.document.fileName) + "]";
        if (c instanceof TdApi.MessageContact ct) return "[Контакт: " + nvl(ct.contact.firstName) + " " + nvl(ct.contact.lastName) + "]";
        return "[Вложение: " + c.getClass().getSimpleName() + "]";
    }

    /**
     * В TDLib НЕТ прямых HTTP-URL на медиа (в отличие от VK/MAX).
     * Файл скачивается на диск через DownloadFile, возвращаем локальный путь.
     */
    private String extractMediaLocalPath(TdApi.MessageContent c) {
        int fileId = -1;
        if (c instanceof TdApi.MessagePhoto p && p.photo != null && p.photo.sizes.length > 0) {
            fileId = p.photo.sizes[p.photo.sizes.length - 1].photo.id; // максимальный размер
        } else if (c instanceof TdApi.MessageVideo v && v.video != null) {
            fileId = v.video.video.id;
        } else if (c instanceof TdApi.MessageDocument d && d.document != null) {
            fileId = d.document.document.id;
        }
        return (fileId > 0) ? downloadFileToLocal(fileId) : "";
    }

    private String downloadFileToLocal(int fileId) {
        try {
            TdApi.File file = client.send(new TdApi.DownloadFile(fileId, 1, 0, 0, true))
                    .get(60, TimeUnit.SECONDS);
            if (file.local != null && file.local.isDownloadingCompleted) {
                return file.local.path;
            }
        } catch (Exception e) {
            System.err.println("⚠️ [TG] Ошибка скачивания файла " + fileId + ": " + e.getMessage());
        }
        return "";
    }

    private String nvl(String s) {
        return (s != null) ? s : "";
    }

    /**
     * Проверяет, разрешено ли сканировать этот чат согласно настройкам.
     * Логика как в VK: пустой вайтлист = разрешены все группы.
     */
    private boolean isChatAllowed(ChatMeta meta) {
        // 1. "Избранное" (чат с самим собой)
        if (meta.isSavedMessages()) {
            if (!scanSavedMessages) {
                if (Config.isDebugEnabled()) System.out.println("⏭️ [TG] Избранное пропущено (отключено)");
                return false;
            }
            return true;
        }

        // 2. Личные чаты
        if (!meta.isGroup()) {
            if (!scanPersonal) {
                if (Config.isDebugEnabled()) System.out.println("⏭️ [TG] Личный чат пропущен (отключено): " + meta.title());
                return false;
            }
            return true;
        }

        // 3. Группы и каналы
        if (!scanGroups) {
            if (Config.isDebugEnabled()) System.out.println("⏭️ [TG] Группа/канал пропущены (отключено): " + meta.title());
            return false;
        }

        // 4. Вайтлист: пустой = все разрешены, иначе — только из списка
        if (!whitelistGroupIds.isEmpty() && !whitelistGroupIds.contains(meta.id())) {
            if (Config.isDebugEnabled()) System.out.println("⏭️ [TG] Группа пропущена (нет в вайтлисте): " + meta.title());
            return false;
        }

        return true;
    }

    // =========================================================================================
    // 📤 ОТПРАВКА СООБЩЕНИЙ
    //
    // Здесь только публичное API и работа с БД. Сборку TdApi-объектов
    // (InputMessageText / InputPhoto / InputVideo / InputDocument, SendMessage.replyTo)
    // делает TgSend — он написан под tdlight-api 4.0.561 (TDLib 1.8.66).
    // =========================================================================================

    /** Таймаут синхронной отправки, секунд. */
    private static final int SEND_TIMEOUT_SEC = 120;

    /** Папка для временных файлов (загрузка из браузера). TDLib читает файл прямо с диска. */
    private static final Path UPLOAD_DIR = Path.of("tdlight-session", "uploads");

    /** Единый результат отправки для всех платформ. */

    public long getMyUserId() { return myUserId; }

    /** Список известных чатов (кэш UpdateNewChat) — пригодится для отправки по имени. */
    public List<ChatMeta> getCachedChats() { return new ArrayList<>(chatCache.values()); }

    /**
     * Синхронная отправка текста (блокирует поток до ответа TDLib).
     *
     * @param chatId   id чата TDLib (для "Избранного" — свой userId)
     * @param text     текст сообщения
     * @param replyTo  id сообщения для ответа, 0 = без ответа
     * @param markdown true — парсить Telegram-Markdown (bold/italic/ссылки)
     */
    public SendResult sendMessage(long chatId, String text, long replyTo, boolean markdown) {
        try {
            return sendMessageAsync(chatId, text, replyTo, markdown).get(SEND_TIMEOUT_SEC, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return SendResult.fail("tg", chatId, "Отправка прервана");
        } catch (Exception e) {
            return SendResult.fail("tg", chatId,e.getMessage());
        }
    }

    /**
     * Асинхронная отправка текста.
     *
     * Вся работа с TdApi вынесена в {@link TgSend}: он собран под схему
     * tdlight-api 4.0.561 (TDLib 1.8.66), где у SendMessage нет messageThreadId/replyToMessageId,
     * а вложения оборачиваются в InputPhoto / InputVideo / InputDocument.
     */
    public CompletableFuture<SendResult> sendMessageAsync(long chatId, String text, long replyTo, boolean markdown) {
        return TgSend.sendText(client, chatId, text, replyTo, markdown,
                        (id, cid, date) -> persistOutgoing(id, cid, date, text));
    }

    /**
     * Синхронная отправка файла: фото -> InputMessagePhoto, видео -> InputMessageVideo,
     * всё остальное -> InputMessageDocument.
     *
     * @param caption подпись к файлу (может быть пустой)
     */
    public SendResult sendFile(long chatId, String filePath, String caption, long replyTo, boolean markdown) {
        try {
            return sendFileAsync(chatId, filePath, caption, replyTo, markdown)
                    .get(SEND_TIMEOUT_SEC * 2L, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return SendResult.fail("tg", chatId, "Отправка прервана");
        } catch (Exception e) {
            return SendResult.fail("tg", chatId, e.getMessage());
        }
    }

    /** Асинхронная отправка файла с локального диска. */
    /** Асинхронная отправка файла с локального диска. */
    public CompletableFuture<ru.send.SendResult> sendFileAsync(long chatId, String filePath, String caption,
                                                               long replyTo, boolean markdown) {
        if (client == null) {
            return CompletableFuture.completedFuture(
                    SendResult.fail("tg", chatId, "TG-клиент не запущен (POST /api/tg/start)"));
        }
        if (filePath == null || filePath.isBlank()) {
            return CompletableFuture.completedFuture(SendResult.fail("tg", chatId, "Не указан файл"));
        }
        String text = (caption != null && !caption.isBlank())
                ? caption : "[Файл: " + Path.of(filePath).getFileName() + "]";

        // 👇 БЫЛО (неправильно): TgSend.sendFileBytes(client, chatId, bytes, fileName, mimeType, ...)
        return TgSend.sendFile(client, chatId, Path.of(filePath), caption, replyTo, markdown,
                (id, cid, date) -> persistOutgoing(id, cid, date, text));
    }

    /**
     * Отправка загруженных байтов: сами кладём их во временный файл и отдаём TDLib.
     * Используется REST-контроллером при multipart-загрузке из браузера.
     */
    public CompletableFuture<SendResult> sendFileBytes(long chatId, byte[] bytes, String fileName, String mimeType,
                                                       String caption, long replyTo, boolean markdown) {
        String text = (caption != null && !caption.isBlank())
                ? caption : "[Файл: " + nvl(fileName) + "]";
        return TgSend.sendFileBytes(client, chatId, bytes, fileName, mimeType, caption,
                        replyTo, markdown, UPLOAD_DIR, (id, cid, date) -> persistOutgoing(id, cid, date, text));
    }

    /**
     * Пишем ИСХОДЯЩЕЕ сообщение в БД сразу после отправки —
     * UI обновляется мгновенно, не дожидаясь UpdateNewMessage.
     */
    private void persistOutgoing(long messageId, long chatId, int unixTime, String text) {
        ChatMeta meta = chatCache.computeIfAbsent(chatId, this::fetchChatMeta);
        db.saveOrUpdateChat("tg", chatId, meta.title(), meta.isGroup());
        // время: TDLib отдаёт unixtime; если версия его не вернула — берём текущее
        int ts = unixTime > 0 ? unixTime : (int) (System.currentTimeMillis() / 1000);
        db.saveMessage(new UnifiedMessage("tg", messageId, chatId, myUserId, nvl(text), ts, ""));
    }



    /** Отправка в "Избранное" (чат с самим собой) — удобно для заметок/тестов. */
    public SendResult sendToSavedMessages(String text) {
        if (myUserId <= 0) return SendResult.fail("tg", 0, "Не известен свой ID (клиент не авторизован)");
        try {
            TdApi.Chat chat = client.send(new TdApi.CreatePrivateChat(myUserId, true))
                    .get(30, TimeUnit.SECONDS);
            return sendMessage(chat.id, text, 0, false);
        } catch (Exception e) {
            return SendResult.fail("tg", myUserId,e.getMessage());
        }
    }

    /** Резолв @username / t.me-ссылки в chatId (для отправки в ещё не открытый чат). */
    public long resolveChatIdByUsername(String username) {
        if (client == null || username == null || username.isBlank()) return 0;
        String uname = username.trim();
        if (uname.startsWith("@")) uname = uname.substring(1);
        if (uname.startsWith("t.me/")) uname = uname.substring("t.me/".length());
        try {
            TdApi.Chat chat = client.send(new TdApi.SearchPublicChat(uname)).get(30, TimeUnit.SECONDS);
            chatCache.put(chat.id, toMeta(chat));
            return chat.id;
        } catch (Exception e) {
            System.err.println("⚠️ [TG] Не удалось найти @" + uname + ": " + e.getMessage());
            return 0;
        }
    }

    /** Универсальный резолв: username, t.me-ссылка, "saved" или числовой id. */
    public long resolveTarget(String target) {
        if (target == null || target.isBlank()) return 0;
        String t = target.trim();
        if (t.equalsIgnoreCase("saved") || t.equalsIgnoreCase("me")) return myUserId;
        if (t.startsWith("@") || t.startsWith("t.me/") || !t.matches("-?\\d+")) return resolveChatIdByUsername(t);
        return Long.parseLong(t);
    }

    // =========================================================================================
    // 🎛 УПРАВЛЕНИЕ
    // =========================================================================================

    public void startListening() { isListening = true;  System.out.println("👂 [TG] Прослушка ВКЛ"); }
    public void stopListening()  { isListening = false; System.out.println("🔇 [TG] Прослушка ВЫКЛ"); }
    public boolean isListening() { return isListening; }
    public boolean isScanning()  { return isScanning; }
    public boolean isStarted()   { return client != null; }

    // === ГЕТТЕРЫ/СЕТТЕРЫ ДЛЯ НАСТРОЕК ===
    public void setScanGroups(boolean v) { this.scanGroups = v; System.out.println("⚙️ [TG] scanGroups = " + v); }
    public void setScanPersonal(boolean v) { this.scanPersonal = v; System.out.println("⚙️ [TG] scanPersonal = " + v); }
    public void setScanSavedMessages(boolean v) { this.scanSavedMessages = v; System.out.println("⚙️ [TG] scanSavedMessages = " + v); }
    public void setWhitelistGroupIds(List<Long> ids) {
        this.whitelistGroupIds = (ids != null) ? ids : new ArrayList<>();
        System.out.println("⚙️ [TG] Вайтлист групп: " + this.whitelistGroupIds.size() + " ID");
    }

    /** true (по умолчанию) = InputMessageDocument.disableContentTypeDetection. */
    public void setDocContentTypeDetectionDisabled(boolean v) {
        this.docContentTypeDetectionDisabled = v;
        System.out.println("⚙️ [TG] disableContentTypeDetection = " + v);
    }
    public boolean isDocContentTypeDetectionDisabled() { return docContentTypeDetectionDisabled; }

    public Map<String, Object> getConfig() {
        return Map.of(
                "scanGroups", scanGroups,
                "scanPersonal", scanPersonal,
                "scanSavedMessages", scanSavedMessages,
                "downloadMedia", downloadMedia,
                "disableContentTypeDetection", docContentTypeDetectionDisabled,
                "myUserId", myUserId,          // 👈 фронт берёт отсюда свой ID
                "chatsCached", chatCache.size(),
                "whitelistSize", whitelistGroupIds.size(),
                "whitelist", whitelistGroupIds
        );
    }

    public void setDownloadMedia(boolean v) { this.downloadMedia = v; }
}
