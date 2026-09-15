package v2.connectors.vk;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import v2.Config;
import v2.connectors.base.BaseConnector;
import v2.connectors.base.ConnectorConfig;
import v2.connectors.base.ConnectorResult;
import v2.connectors.base.ConnectorStatus;
import v2.connectors.base.ScanOptions;
import v2.connectors.base.SendResult;
import v2.entity.Chat;
import v2.entity.Message;
import v2.entity.User;
import v2.services.ChatService;
import v2.services.MessageService;
import v2.services.UserService;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static v2.connectors.vk.VkApiClient.*;

/**
 * VK-коннектор для v2 — реализует BaseConnector.
 *
 * Класс разбит на четыре файла, чтобы каждый занимался своим:
 * <pre>
 *   VkApiClient   HTTP-слой VK API + утилиты (ошибки, подсказки, нормализация server)
 *   VkLongPoll    User LongPoll: цикл, разбор событий (3 формата), вложения, вайтлист-фильтрация
 *   VkSender      отправка текста/файлов, загрузка фото и документов
 *   VkConnector   ЭТОТ ФАЙЛ: состояние, конфиг, кэши, работа с БД, сканирование истории,
 *                 диагностика. Реализует VkLongPoll.Sink — то есть решает, сохранять ли
 *                 сообщение (вайтлист) и как именно.
 * </pre>
 *
 * Ключевые моменты, из-за которых код выглядит именно так:
 * <ul>
 *   <li><b>myUserId</b> определяется в start() через users.get и отдаётся в
 *       GET /api/vk/status. В Message НЕТ поля outgoing, поэтому фронт определяет исходящие
 *       как authorId == myUserId. Без этого все сообщения рисовались бы входящими.</li>
 *   <li>Сохранение идёт через <b>MessageService.saveMessage()</b> — он проверяет дубликат по
 *       (source, messageId). Прямой messageRepository.save() падал на unique-индексе, когда
 *       LongPoll и сканирование приносили одно и то же сообщение.</li>
 *   <li>Сообщение сохраняется, даже если автора вытащить не удалось (deactivated/сообщество) —
 *       иначе оно терялось молча, а имя в MessageService и так fallback-ится в «Unknown».</li>
 *   <li>timestamp у VK в СЕКУНДАХ, в Message — в миллисекундах. Приводим везде.</li>
 * </ul>
 */
@Component
public class VkConnector implements BaseConnector, VkLongPoll.Sink {

    private static final Logger log = LoggerFactory.getLogger(VkConnector.class);

    /** Сколько секунд не дёргать VK повторно за одним и тем же пользователем после ошибки. */
    private static final long USER_CACHE_NEGATIVE_TTL_MS = 60_000;

    private final ConnectorConfig config = new ConnectorConfig();

    // ==================== СОСТОЯНИЕ ====================
    private volatile boolean isStarted = false;
    private volatile boolean isScanning = false;

    /**
     * ВАЖНО: не final. Определяется в start() через users.get.
     * Отдаётся во фронт через GET /api/vk/status → myUserId.
     */
    private volatile Long myUserId = null;

    /**
     * Применять ли вайтлист/фильтры сканирования к ЖИВЫМ событиям LongPoll.
     * true (по умолчанию) — в БД попадают только разрешённые диалоги;
     * false — пишется всё, фильтры влияют только на выкачку истории.
     *
     * Берётся из ConnectorConfig.listenWhitelist, если вы добавите туда такое поле
     * (публичное, как scanGroups). Пока поля нет — действует значение по умолчанию.
     */
    private volatile boolean listenWhitelist = true;

    /** Сколько живых сообщений отброшено вайтлистом прослушки. */
    private volatile long filteredTotal = 0;

    // ==================== СОТРУДНИКИ ====================
    private final VkApiClient api = new VkApiClient();
    private final VkSender sender = new VkSender(api);
    // 🔁 Множественные лонгполы: один для личных + N для сообществ
    private volatile VkLongPoll personalPoll = new VkLongPoll(api, this, 0);;
    private final ConcurrentHashMap<Long, VkLongPoll> groupPolls = new ConcurrentHashMap<>();

    // ==================== КЭШИ ====================
    private final ConcurrentHashMap<Long, VkChatMeta> chatCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, User> userCache = new ConcurrentHashMap<>();
    /** peer_id → время последней неудачи: не дёргаем VK по кругу для deactivated/сообществ. */
    private final ConcurrentHashMap<Long, Long> userFailCache = new ConcurrentHashMap<>();
    /**
     * Негативный кэш прослушки: peer_id, уже отброшенный вайтлистом.
     * Без него на каждое сообщение из неразрешённой беседы приходился бы запрос
     * messages.getConversationsById к VK. Чистится при изменении конфига.
     */
    private final ConcurrentHashMap<Long, VkChatMeta> blockedPeers = new ConcurrentHashMap<>();

    private final ExecutorService scanExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "vk-scan");
        t.setDaemon(true);
        return t;
    });

    // ==================== СЕРВИСЫ ====================
    private final UserService userService;
    private final ChatService chatService;
    private final MessageService messageService;

    public VkConnector(UserService userService, ChatService chatService, MessageService messageService) {
        this.userService = userService;
        this.chatService = chatService;
        this.messageService = messageService;
    }

    @Override
    public String platform() {
        return "vk";
    }

    @Override
    public ConnectorStatus getStatus() {
        return new ConnectorStatus("vk", isStarted,personalPoll.isRunning(), isScanning, myUserId, config);
    }

    // =========================================================================================
    // 🎛 УПРАВЛЕНИЕ
    // =========================================================================================

    @Override
    public ConnectorResult start() {
        if (isStarted) {
            return ConnectorResult.ok("VK уже запущен (myUserId=" + myUserId + ")");
        }

        try {
            String token = Config.getVkToken();
            if (token == null || token.isBlank()) {
                return ConnectorResult.fail("Токен VK не найден в конфигурации");
            }
            resolveMyUserId();

            isStarted = true;
            log.info("[VK] Коннектор запущен, myUserId={}", myUserId);
            return ConnectorResult.ok("VK коннектор запущен"
                    + (myUserId != null ? " (myUserId=" + myUserId + ")" : " (myUserId не определён)"));
        } catch (Exception e) {
            log.error("[VK] Ошибка запуска", e);
            return ConnectorResult.fail("Ошибка запуска VK: " + describe(e));
        }
    }

    @Override
    public ConnectorResult stop() {
        stopListening();
        isStarted = false;
        log.info("[VK] Коннектор остановлен");
        return ConnectorResult.ok("VK остановлен");
    }

    @Override
    public ConnectorResult startListening() {
        if (!isStarted) {
            return ConnectorResult.fail("VK коннектор не запущен — сначала start()");
        }

        StringBuilder report = new StringBuilder();

        // 1. Личные сообщения пользователя + его беседы (groupId = 0)
        if (config.scanPersonal) {
            personalPoll = new VkLongPoll(api, this, 0);
            report.append("Личные: ").append(personalPoll.start()).append(". ");
        } else if (personalPoll != null) {
            report.append("Личные: ").append(personalPoll.stop()).append(". ");
            personalPoll = null;
        }

        // 2. Сообщества из вайтлиста (groupId > 0) — по экземпляру на каждую
        if (config.scanGroups && !config.whitelist.isEmpty()) {
            Set<Long> wanted = new HashSet<>(config.whitelist);

            // Запускаем новые
            for (Long gid : wanted) {
                VkLongPoll poll = groupPolls.get(gid);
                if (poll == null) {
                    poll = new VkLongPoll(api, this, gid);
                    groupPolls.put(gid, poll);
                    report.append("Группа ").append(gid).append(": ").append(poll.start()).append(". ");
                }
            }

            // Останавливаем те, что убрали из вайтлиста
            for (Iterator<Map.Entry<Long, VkLongPoll>> it = groupPolls.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<Long, VkLongPoll> e = it.next();
                if (!wanted.contains(e.getKey())) {
                    report.append("Группа ").append(e.getKey()).append(": ").append(e.getValue().stop()).append(". ");
                    it.remove();
                }
            }
        } else if (!groupPolls.isEmpty()) {
            for (Iterator<Map.Entry<Long, VkLongPoll>> it = groupPolls.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<Long, VkLongPoll> e = it.next();
                report.append("Группа ").append(e.getKey()).append(": ").append(e.getValue().stop()).append(". ");
                it.remove();
            }
        }

        if (report.length() == 0) {
            return ConnectorResult.ok("Прослушка не запущена (scanPersonal=false и whitelist пуст)");
        }
        return ConnectorResult.ok(report.toString().trim());
    }

    @Override
    public ConnectorResult stopListening() {
        StringBuilder report = new StringBuilder();
        if (personalPoll != null) {
            report.append(personalPoll.stop()).append(". ");
        }
        for (Map.Entry<Long, VkLongPoll> e : groupPolls.entrySet()) {
            report.append("Группа ").append(e.getKey()).append(": ").append(e.getValue().stop()).append(". ");
        }
        groupPolls.clear();
        return ConnectorResult.ok(report.length() > 0 ? report.toString().trim() : "Прослушка остановлена");
    }

    /** users.get без параметров возвращает владельца токена. */
    private void resolveMyUserId() {
        try {
            JsonNode resp = api.call("users.get", new LinkedHashMap<>());
            if (hasError(resp)) {
                log.warn("[VK] users.get для определения myUserId вернул ошибку: {}", vkError(resp));
                return;
            }
            JsonNode u = resp.path("response").path(0);
            long id = u.path("id").asLong(0);
            if (id > 0) {
                myUserId = id;
                log.info("[VK] myUserId={} ({} {})", id,
                        u.path("first_name").asText(""), u.path("last_name").asText(""));
            }
        } catch (Exception e) {
            log.warn("[VK] Не удалось определить myUserId: {}", describe(e));
        }
    }

    private String filterDescription() {
        return listenWhitelist
                ? "scanPersonal=" + config.scanPersonal + ", scanGroups=" + config.scanGroups
                + ", whitelist=" + (config.whitelist.isEmpty() ? "пуст (= все беседы)" : config.whitelist)
                : "ВЫКЛЮЧЕН (пишем все диалоги)";
    }

    @PreDestroy
    public void shutdown() {
        isStarted = false;
        isScanning = false;
        stopListening();
        scanExecutor.shutdownNow();
        try {
            scanExecutor.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("[VK] Коннектор остановлен при завершении приложения");
    }

    // =========================================================================================
    // 🚦 VkLongPoll.Sink — здесь решается, сохранять ли живое сообщение
    //
    //  Серверной фильтрации по peer_id у VK НЕТ:
    //   - messages.getLongPollServer принимает только use_ssl, need_pts, lp_version, group_id;
    //   - filter_id (битмаска) есть ТОЛЬКО в Bots LongPoll для ключа сообщества и фильтрует
    //     ТИПЫ событий (message_new, photo_new…), а не диалоги;
    //   - mode у пользовательского лонгполла — маска ФОРМАТА ответа, а не отбора.
    //  Поэтому вайтлист применяется на клиенте — здесь. См. backend/VK_LONGPOLL.md.
    // =========================================================================================

    @Override
    public Long myUserId() {
        return myUserId;
    }

    @Override
    public void onChatMetaChanged(long peerId) {
        chatCache.remove(peerId);
        blockedPeers.remove(peerId);
    }

    /**
     * Единственная точка сохранения ЖИВОГО сообщения (из LongPoll).
     *
     * @return true, если сообщение сохранено
     */
    @Override
    public boolean onMessage(long peerId, long messageId, long cmid, long fromId,
                             String text, long tsMillis, String mediaUrl, String fmt) {
        if (messageId <= 0) {
            log.warn("[VK] {} peer={}: message_id={} — не сохраняем (иначе сломается антидубликат)",
                    fmt, peerId, messageId);
            return false;
        }
        if (!isPeerAllowed(peerId)) {
            return false;                            // вайтлист прослушки
        }

        VkChatMeta meta = ensureChat(peerId, false);
        if (meta == null) {
            log.warn("[VK] не удалось определить чат peer={} — сообщение id={} не сохранено", peerId, messageId);
            return false;
        }

        boolean mine = myUserId != null && fromId == myUserId;
        if (!mine && fromId > 0) {
            saveAuthorQuietly(fromId);
        }

        try {
            Message msg = new Message("vk", messageId, peerId, fromId, text == null ? "" : text, mediaUrl);
            msg.setTimestamp(tsMillis > 0 ? tsMillis : System.currentTimeMillis());
            messageService.saveMessage(msg);         // дубликат отсеется по (source, messageId)
            log.info("[VK] {} {} id={} cmid={} peer={} «{}»{}",
                    mine ? "← исходящее" : "→ входящее",
                    meta.isGroup() ? "[беседа]" : "[лс]",
                    messageId, cmid, peerId, truncate(text, 60), mediaUrl != null ? " +media" : "");
            return true;
        } catch (Exception e) {
            log.error("[VK] не удалось сохранить сообщение id={}: {}", messageId, describe(e));
            return false;
        }
    }

    /**
     * Пускать ли peer_id в прослушку. Правила те же, что в isChatAllowed():
     *   личный диалог     → config.scanPersonal
     *   беседа/сообщество → config.scanGroups + (whitelist пуст ИЛИ содержит peer_id)
     */
    private boolean isPeerAllowed(long peerId) {
        if (!listenWhitelist) {
            return true;                             // фильтр выключен — пишем всё
        }
        if (blockedPeers.containsKey(peerId)) {
            return false;                            // уже решали, что нельзя — VK не спрашиваем
        }
        VkChatMeta meta = chatCache.get(peerId);
        if (meta == null) {
            meta = fetchChatMeta(peerId);
            if (meta == null) {
                log.debug("[VK] peer={} — мету получить не удалось, событие пропущено", peerId);
                return false;
            }
        }
        if (isChatAllowed(meta)) {
            chatCache.put(peerId, meta);
            return true;
        }
        blockedPeers.put(peerId, meta);
        filteredTotal++;
        log.debug("[VK] peer={} «{}» вне вайтлиста прослушки — пропуск", peerId, meta.title());
        return false;
    }

    /** Сохраняет автора, не роняя вызывающий код. */
    private void saveAuthorQuietly(long fromId) {
        try {
            User author = getOrFetchUser(fromId);
            if (author != null) {
                userService.saveOrUpdate(author);
            } else {
                log.debug("[VK] автор {} не найден (deactivated/сообщество) — имя будет «Unknown»", fromId);
            }
        } catch (Exception e) {
            log.warn("[VK] не удалось сохранить автора {}: {}", fromId, describe(e));
        }
    }

    // =========================================================================================
    // 💬 ЧАТЫ И ПОЛЬЗОВАТЕЛИ (кэш + БД)
    // =========================================================================================

    /** Мета чата с применением фильтров сканирования. */
    VkChatMeta ensureChat(long peerId) {
        return ensureChat(peerId, true);
    }

    /**
     * Достаёт мету чата (кэш → VK) и сохраняет Chat в БД.
     *
     * @param applyScanFilters true — для сканирования истории: чат вне настроек сканирования
     *                         возвращает null и НЕ сохраняется.
     *                         false — для LongPoll и для отправки: живой диалог сохраняется всегда
     *                         (иначе человек вам написал, а чата в UI нет). Фильтрацию прослушки
     *                         делает отдельный isPeerAllowed().
     */
    VkChatMeta ensureChat(long peerId, boolean applyScanFilters) {
        VkChatMeta meta = chatCache.get(peerId);
        if (meta != null) {
            // в кэше могут лежать и отфильтрованные чаты — для скана их всё равно не пускаем
            return (applyScanFilters && !isChatAllowed(meta)) ? null : meta;
        }
        meta = fetchChatMeta(peerId);
        if (meta == null) {
            return null;
        }
        if (applyScanFilters && !isChatAllowed(meta)) {
            return null;
        }
        chatCache.put(peerId, meta);
        try {
            chatService.saveOrUpdate(new Chat("vk", meta.id(), meta.title(), meta.isGroup()));
        } catch (Exception e) {
            log.error("[VK] не удалось сохранить чат peer={}: {}", peerId, describe(e));
        }
        return meta;
    }

    private boolean isChatAllowed(VkChatMeta meta) {
        if (!meta.isGroup()) {
            return config.scanPersonal;
        }
        if (!config.scanGroups) {
            return false;
        }
        return config.whitelist.isEmpty() || config.whitelist.contains(meta.id());
    }

    /**
     * Мета чата по peer_id.
     * isGroup больше не всегда true (был баг); название сообщества берётся из groups.getById,
     * т.к. в ответе messages.getConversationsById поля groups НЕТ.
     */
    private VkChatMeta fetchChatMeta(long peerId) {
        try {
            JsonNode response = api.call("messages.getConversationsById",
                    params("peer_ids", String.valueOf(peerId), "extended", "1"));
            if (hasError(response)) {
                log.warn("[VK] messages.getConversationsById peer={}: {}", peerId, vkError(response));
                return null;
            }
            JsonNode item = response.path("response").path("items").path(0);
            if (item.isMissingNode() || item.isNull()) {
                log.warn("[VK] messages.getConversationsById вернул пустой items для peer={}", peerId);
                return null;
            }

            String peerType = item.path("peer").path("type").asText("");
            JsonNode chatSettings = item.path("chat_settings");

            String title;
            boolean isGroup;
            switch (peerType) {
                case "user" -> {
                    isGroup = false;
                    User u = getOrFetchUser(peerId);
                    title = u != null
                            ? (u.getFirstName() + " " + (u.getLastName() == null ? "" : u.getLastName())).strip()
                            : "";
                    if (title.isBlank()) {
                        title = "Пользователь " + peerId;
                    }
                }
                case "chat" -> {
                    isGroup = true;                             // беседа
                    title = chatSettings.path("title").asText("");
                    if (title.isBlank()) {
                        title = "Беседа " + (peerId - 2_000_000_000L);
                    }
                }
                case "group" -> {
                    isGroup = true;                             // сообщество (peer_id < 0)
                    title = fetchGroupName(peerId);
                }
                default -> {
                    isGroup = peerId >= 2_000_000_000L || peerId < 0;
                    title = chatSettings.path("title").asText("");
                    if (title.isBlank()) {
                        title = "Чат " + peerId;
                    }
                }
            }
            return new VkChatMeta(peerId, title, isGroup);
        } catch (Exception e) {
            log.warn("[VK] Ошибка получения меты чата peer={}: {}", peerId, describe(e));
            return null;
        }
    }

    /** Название сообщества: groups.getById (в ответах messages.* его нет). */
    private String fetchGroupName(long peerId) {
        long groupId = Math.abs(peerId);
        try {
            JsonNode resp = api.call("groups.getById", Map.of("group_ids", String.valueOf(groupId)));
            if (hasError(resp)) {
                log.debug("[VK] groups.getById {}: {}", groupId, vkError(resp));
                return "Сообщество " + groupId;
            }
            String name = resp.path("response").path(0).path("name").asText("");
            return name.isBlank() ? "Сообщество " + groupId : name;
        } catch (Exception e) {
            log.debug("[VK] groups.getById {} не удался: {}", groupId, describe(e));
            return "Сообщество " + groupId;
        }
    }

    /** Пользователь по id с кэшем (в т.ч. негативным — чтобы не долбить VK по deactivated). */
    private User getOrFetchUser(long userId) {
        User cached = userCache.get(userId);
        if (cached != null) {
            return cached;
        }
        Long failedAt = userFailCache.get(userId);
        if (failedAt != null && System.currentTimeMillis() - failedAt < USER_CACHE_NEGATIVE_TTL_MS) {
            return null;
        }
        try {
            JsonNode response = api.call("users.get",
                    params("user_ids", String.valueOf(userId), "fields", "photo_100,screen_name"));
            if (hasError(response)) {
                log.debug("[VK] users.get {}: {}", userId, vkError(response));
                userFailCache.put(userId, System.currentTimeMillis());
                return null;
            }
            JsonNode userNode = response.path("response").path(0);
            if (userNode.isMissingNode() || userNode.isNull()) {
                userFailCache.put(userId, System.currentTimeMillis());
                return null;
            }
            String firstName = userNode.path("first_name").asText("");
            String lastName = userNode.path("last_name").asText("");
            if (firstName.isBlank() && lastName.isBlank()) {
                userFailCache.put(userId, System.currentTimeMillis());   // deactivated
                return null;
            }
            User user = new User("vk", userId, firstName, lastName,
                    userNode.path("screen_name").asText(null),
                    userNode.path("photo_100").asText(""), null, new ArrayList<>());
            userCache.put(userId, user);
            return user;
        } catch (Exception e) {
            log.debug("[VK] Ошибка получения пользователя {}: {}", userId, describe(e));
            userFailCache.put(userId, System.currentTimeMillis());
            return null;
        }
    }

    // =========================================================================================
    // 🔍 СКАНИРОВАНИЕ ИСТОРИИ
    // =========================================================================================

    @Override
    public ConnectorResult startScan(ScanOptions options) {
        if (!isStarted) {
            return ConnectorResult.fail("VK не запущен");
        }
        if (isScanning) {
            return ConnectorResult.fail("Сканирование уже идёт");
        }
        if (options == null) {
            return ConnectorResult.fail("Не переданы ScanOptions");
        }
        isScanning = true;
        scanExecutor.submit(() -> runScan(options));
        return ConnectorResult.ok("Сканирование VK запущено");
    }

    private void runScan(ScanOptions options) {
        try {
            List<Long> chatIds = options.chatIds();
            Integer limit = options.limitPerChat() != null ? options.limitPerChat() : config.limitPerChat;
            int lim = limit != null && limit > 0 ? limit : 200;

            if (chatIds == null || chatIds.isEmpty()) {
                chatIds = getDialogsFromVK(lim);
            }
            log.info("[VK] Сканирование: чатов {}, лимит на чат {}", chatIds.size(), lim);

            int saved = 0, skipped = 0;
            for (Long peerId : chatIds) {
                if (!isScanning) {
                    break;
                }
                try {
                    VkChatMeta meta = ensureChat(peerId);      // применяет фильтры и сохраняет Chat
                    if (meta == null) {
                        skipped++;
                        log.debug("[VK] пропущен peer={} (настройки сканирования)", peerId);
                        continue;
                    }

                    for (Message msg : fetchMessages(peerId, lim)) {
                        try {
                            long fromId = msg.getAuthorId() != null ? msg.getAuthorId() : 0;
                            boolean mine = myUserId != null && fromId == myUserId;
                            if (!mine && fromId != 0) {
                                saveAuthorQuietly(fromId);
                            }
                            messageService.saveMessage(msg);   // сам проверяет дубликат
                            saved++;
                        } catch (Exception e) {
                            log.warn("[VK] не сохранено сообщение id={}: {}", msg.getMessageId(), describe(e));
                        }
                    }
                    sleep(500);                                // бережём лимиты VK
                } catch (Exception e) {
                    log.error("[VK] ошибка сканирования peer={}: {}", peerId, describe(e));
                }
            }
            log.info("[VK] Сканирование завершено: сохранено {}, пропущено чатов {}", saved, skipped);
        } finally {
            isScanning = false;
        }
    }

    private List<Long> getDialogsFromVK(int limit) {
        List<Long> dialogs = new ArrayList<>();
        try {
            JsonNode response = api.call("messages.getConversations",
                    Map.of("count", String.valueOf(Math.min(Math.max(limit, 1), 200))));
            if (hasError(response)) {
                log.error("[VK] messages.getConversations: {}", vkError(response));
                return dialogs;
            }
            for (JsonNode item : response.path("response").path("items")) {
                long peerId = item.path("conversation").path("peer").path("id").asLong(0);
                if (peerId != 0) {
                    dialogs.add(peerId);
                }
            }
        } catch (Exception e) {
            log.error("[VK] Ошибка получения списка диалогов: {}", describe(e));
        }
        return dialogs;
    }

    /**
     * История переписки.
     * ВАЖНО: VK отдаёт date в СЕКУНДАХ, а Message.timestamp в миллисекундах — приводим,
     * иначе фронт рисует 1970 год.
     */
    private List<Message> fetchMessages(long peerId, int limit) {
        List<Message> messages = new ArrayList<>();
        try {
            JsonNode response = api.call("messages.getHistory",
                    params("peer_id", String.valueOf(peerId),
                            "count", String.valueOf(Math.min(Math.max(limit, 1), 200)),
                            "extended", "1"));
            if (hasError(response)) {
                log.warn("[VK] messages.getHistory peer={}: {}", peerId, vkError(response));
                return messages;
            }
            JsonNode items = response.path("response").path("items");
            if (!items.isArray()) {
                return messages;
            }
            for (JsonNode item : items) {
                long messageId = item.path("id").asLong(0);
                if (messageId == 0) {
                    continue;
                }
                long fromId = item.path("from_id").asLong(0);
                long dateSec = item.path("date").asLong(0);
                Message msg = new Message("vk", messageId, peerId, fromId == 0 ? peerId : fromId,
                        item.path("text").asText(""),
                        VkLongPoll.extractMediaUrl(item.path("attachments")));
                msg.setTimestamp(dateSec > 0 ? dateSec * 1000L : System.currentTimeMillis());
                messages.add(msg);
            }
            Collections.reverse(messages);     // VK отдаёт от новых к старым
        } catch (Exception e) {
            log.warn("[VK] Ошибка получения истории peer={}: {}", peerId, describe(e));
        }
        return messages;
    }

    // =========================================================================================
    // ⚙️ КОНФИГ
    // =========================================================================================

    @Override
    public ConnectorConfig getConfig() {
        return config;
    }

    @Override
    public ConnectorResult updateConfig(ConnectorConfig newConfig) {
        if (newConfig == null) {
            return ConnectorResult.fail("Пустой конфиг");
        }
        this.config.scanPersonal = newConfig.scanPersonal;
        this.config.scanGroups = newConfig.scanGroups;
        this.config.whitelist = newConfig.whitelist != null ? newConfig.whitelist : new ArrayList<>();
        this.config.limitPerChat = newConfig.limitPerChat;
        this.config.downloadMedia = newConfig.downloadMedia;

        // Вайтлист изменился — прежние решения о блокировке peer_id больше недействительны.
        // Без очистки диалог, который только что добавили в вайтлист, молчал бы до перезапуска.
        int dropped = blockedPeers.size();
        blockedPeers.clear();
        filteredTotal = 0;

        // Если добавите в ConnectorConfig публичное поле `listenWhitelist` (как scanGroups) —
        // подхватится автоматически. Пока поля нет, работает значение по умолчанию (true).
        try {
            java.lang.reflect.Field f = newConfig.getClass().getField("listenWhitelist");
            if (f.get(newConfig) instanceof Boolean b) {
                this.listenWhitelist = b;
            }
        } catch (NoSuchFieldException ignored) {
            // поля ещё нет — не ошибка
        } catch (Exception e) {
            log.debug("[VK] не удалось прочитать listenWhitelist из конфига: {}", describe(e));
        }

        log.info("[VK] Конфиг обновлён: scanPersonal={}, scanGroups={}, whitelist={}, limitPerChat={}, "
                        + "downloadMedia={}, listenWhitelist={} (сброшено заблокированных peer: {})",
                config.scanPersonal, config.scanGroups, config.whitelist.size(), config.limitPerChat,
                config.downloadMedia, listenWhitelist, dropped);
        return ConnectorResult.ok("Конфигурация VK обновлена"
                + (dropped > 0 ? " (переоценено заблокированных диалогов: " + dropped + ")" : ""));
    }

    // =========================================================================================
    // 📤 ОТПРАВКА — делегируем VkSender, здесь только сохранение в БД
    // =========================================================================================

    /**
     * Контракт BaseConnector: его вызывают UnifiedSendService (POST /api/v2/send)
     * и VkApiController (POST /api/vk/send/message).
     *
     * @param attachments строка вида "photo123_456,doc789_123" (формат VK) или null
     * @return message_id отправленного сообщения
     */
    @Override
    public long sendMessage(String peer, String text, String attachments, Long replyTo) {
        long messageId = sender.sendMessage(peer, text, attachments, replyTo);
        // Сохраняем СВОЁ сообщение в БД. Без этого фронт покажет его оптимистично,
        // но после обновления истории оно исчезнет (в messages его не будет).
        saveOutgoing(VkSender.toPeerId(Long.parseLong(peer.trim())), messageId, text, attachments);
        return messageId;
    }

    /**
     * Автор — myUserId (если определён), иначе peer_id: в Message.authorId стоит NOT NULL.
     * mediaUrl заполняем, только если вложения — http-ссылка; строки вида photo123_456
     * ссылкой не являются, их подберёт следующее сканирование истории.
     */
    private void saveOutgoing(long peerId, long messageId, String text, String attachments) {
        try {
            long author = myUserId != null ? myUserId : peerId;
            String media = (attachments != null && attachments.trim().startsWith("http"))
                    ? attachments.trim() : null;
            Message msg = new Message("vk", messageId, peerId, author, text == null ? "" : text, media);
            msg.setTimestamp(System.currentTimeMillis());

            ensureChat(peerId, false);             // чтобы чат точно был в таблице chats
            messageService.saveMessage(msg);       // дубликат отсеется по (source, messageId)
        } catch (Exception e) {
            // сообщение УЖЕ отправлено — не роняем вызов из-за проблем с БД
            log.warn("[VK] отправлено (message_id={}), но сохранить в БД не удалось: {}", messageId, describe(e));
        }
    }

    /** Перегрузки с SendResult — их зовёт VkApiController. */
    public SendResult sendMessage(long peerId, String text, String attachments, long replyTo) {
        return sender.send(peerId, text, attachments, replyTo);
    }

    public SendResult sendMessage(long peerId, String text, List<String> attachments, long replyTo) {
        return sender.send(peerId, text, attachments, replyTo);
    }

    public SendResult sendFile(long peerId, String caption, String filePath, String mimeType, long replyTo) {
        return sender.sendFile(peerId, caption, filePath, mimeType, replyTo);
    }

    public SendResult sendFiles(long peerId, String caption, List<Path> files, long replyTo) {
        return sender.sendFiles(peerId, caption, files, replyTo);
    }

    // =========================================================================================
    // 🔍 ДИАГНОСТИКА  (GET /api/vk/debug-upload?peerId=…)
    // =========================================================================================

    /**
     * Полный отчёт: чей токен, пользовательский он или ключ сообщества, сырые ответы VK на
     * photos/docs.getMessagesUploadServer, состояние LongPoll и подсказка по коду ошибки.
     */
    public Map<String, Object> debugUpload(long peerId) {
        Map<String, Object> report = new LinkedHashMap<>();

        JsonNode me = api.callQuietly("users.get", Map.of());
        if (hasError(me)) {
            report.put("token", "❌ токен не работает: " + vkError(me));
            report.put("hint", hintFor(errorCode(me)));
            return report;
        }
        JsonNode user0 = me.path("response").path(0);
        report.put("token", "✅ рабочий, владелец: id" + user0.path("id").asLong()
                + " " + user0.path("first_name").asText("") + " " + user0.path("last_name").asText(""));

        // ключ сообщества отвечает на groups.getById своим id, пользовательский — ошибкой/пустотой
        JsonNode asGroup = api.callQuietly("groups.getById", Map.of());
        boolean looksLikeGroupToken = !hasError(asGroup)
                && asGroup.path("response").path(0).path("id").asLong(0) != 0;
        report.put("token_type", looksLikeGroupToken
                ? "⚠️ КЛЮЧ СООБЩЕСТВА — photos.*/docs.* для загрузки в личные сообщения с ним НЕ работают"
                : "пользовательский (это правильно для фото в ЛС)");

        report.put("peer_id", peerId);
        report.put("peer_hint", peerHint(peerId));

        JsonNode photo = api.callQuietly("photos.getMessagesUploadServer",
                Map.of("peer_id", String.valueOf(peerId)));
        report.put("photos.getMessagesUploadServer.raw", photo.toString());
        if (hasError(photo)) {
            report.put("photos.error", "❌ " + vkError(photo));
            report.put("photos.hint", hintFor(errorCode(photo)));
        } else {
            String url = photo.path("response").path("upload_url").asText("");
            report.put("photos.upload_url", url.isBlank() ? "❌ пустой (странный ответ VK)" : "✅ получен");
        }

        JsonNode doc = api.callQuietly("docs.getMessagesUploadServer",
                Map.of("peer_id", String.valueOf(peerId), "type", "doc"));
        report.put("docs.getMessagesUploadServer.raw", doc.toString());
        if (hasError(doc)) {
            report.put("docs.error", "❌ " + vkError(doc));
            report.put("docs.hint", hintFor(errorCode(doc)));
        } else {
            String url = doc.path("response").path("upload_url").asText("");
            report.put("docs.upload_url", url.isBlank() ? "❌ пустой" : "✅ получен");
        }

        // LongPoll: отдельно проверяем, что сервер вообще выдаётся
        JsonNode lp = api.callQuietly("messages.getLongPollServer", params("need_pts", "1", "lp_version", "3"));
        if (hasError(lp)) {
            report.put("longpoll", "❌ " + vkError(lp));
            report.put("longpoll.hint", hintFor(errorCode(lp)));
        } else {
            JsonNode lpr = lp.path("response");
            report.put("longpoll", "✅ сервер получен, ts=" + lpr.path("ts").asLong(0)
                    + ", server от VK = «" + lpr.path("server").asText("") + "» → «"
                    + normalizeLongPollServer(lpr.path("server").asText("")) + "»");
        }
        Map<String, Object> lpState = new LinkedHashMap<>(personalPoll.stats());
        lpState.put("filteredByWhitelist", filteredTotal);
        lpState.put("blockedPeersCached", blockedPeers.size());
        lpState.put("filter", filterDescription());
        report.put("longpoll.state", lpState);

        boolean photoOk = String.valueOf(report.getOrDefault("photos.upload_url", "")).startsWith("✅");
        boolean docsOk = String.valueOf(report.getOrDefault("docs.upload_url", "")).startsWith("✅");
        report.put("вывод", photoOk && docsOk
                ? "✅ серверы загрузки получаются — проблема в другом шаге (multipart/save)"
                : (photoOk ? "⚠️ фото грузится, документы нет"
                : "❌ сервер загрузки фото не отдаётся — смотрите photos.hint выше"));
        return report;
    }

    public void printUploadReport(long peerId) {
        log.info("🔍 [VK DEBUG] ===== диагностика для peer_id={} =====", peerId);
        debugUpload(peerId).forEach((k, v) -> log.info("   {}: {}", k, v));
    }

    /** id из БД → peer_id (см. VkSender.toPeerId). */
    public static long toPeerId(long rawId) {
        return VkSender.toPeerId(rawId);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


    public List<Map<String, Object>> getAllGroups() {
        if (!isStarted) {
            throw new IllegalStateException("VK не запущен. Вызовите start() сначала.");
        }
        List<Map<String, Object>> groups = new ArrayList<>();
        final int page = 200;
        final int maxTotal = 2000;
        int offset = 0;
        try {
            while (offset < maxTotal) {
                JsonNode resp = api.call("messages.getConversations", params(
                        "count", String.valueOf(page),
                        "offset", String.valueOf(offset)));
                if (hasError(resp)) {
                    log.warn("[VK] getAllGroups: messages.getConversations offset={}: {}", offset, vkError(resp));
                    break;
                }
                JsonNode items = resp.path("response").path("items");
                if (!items.isArray() || items.isEmpty()) {
                    break;
                }
                for (JsonNode item : items) {
                    JsonNode conv = item.path("conversation");
                    JsonNode peer = conv.path("peer");
                    String peerType = peer.path("type").asText("");
                    long peerId = peer.path("id").asLong(0);
                    if (peerId == 0 || (!peerType.equals("chat") && !peerType.equals("group"))) {
                        continue;                                  // только беседы и сообщества
                    }
                    String title;
                    String type;
                    Integer members = null;
                    if (peerType.equals("chat")) {
                        JsonNode cs = conv.path("chat_settings");
                        title = cs.path("title").asText("");
                        if (title.isBlank()) {
                            title = "Беседа " + (peerId - 2_000_000_000L);
                        }
                        if (cs.path("members_count").isNumber()) {
                            members = cs.path("members_count").asInt();
                        }
                        type = "беседа";
                    } else {
                        title = fetchGroupName(peerId);            // groups.getById, уже реализован
                        type = "сообщество";
                    }
                    Map<String, Object> g = new LinkedHashMap<>();
                    g.put("chatId", peerId);                       // ChatsController нормализует chatId → id
                    g.put("title", title);
                    g.put("type", type);
                    if (members != null) {
                        g.put("membersCount", members);
                    }
                    groups.add(g);
                }
                int totalCount = resp.path("response").path("count").asInt(0);
                offset += page;
                if (totalCount > 0 && offset >= totalCount) {
                    break;
                }
                sleep(350);                                        // бережём лимиты VK
            }
        } catch (Exception e) {
            log.warn("[VK] getAllGroups прерван: {}", describe(e));
        }
        groups.sort((a, b) -> String.valueOf(a.get("title")).compareToIgnoreCase(String.valueOf(b.get("title"))));
        log.info("[VK] getAllGroups: {} бесед и сообществ", groups.size());
        return groups;
    }

    /** Метаданные чата. */
    record VkChatMeta(long id, String title, boolean isGroup) {}
}