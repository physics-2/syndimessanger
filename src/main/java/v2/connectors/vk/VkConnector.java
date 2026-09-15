package v2.connectors.vk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import v2.Config;
import v2.connectors.base.*;
import v2.entity.Chat;
import v2.entity.Message;
import v2.entity.User;
import v2.services.ChatService;
import v2.services.MessageService;
import v2.services.UserService;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * VK Connector для v2 — реализует BaseConnector.
 *
 * Добавлено в этой версии:
 *   1. ПОЛНОЦЕННЫЙ LONGPOLL (Bots LongPoll на пользовательском токене):
 *      messages.getLongPollServer → длинный опрос → events → message_new/message_reply/
 *      message_edit/message_del/failed. Всё уходит в БД через сервисы.
 *   2. myUserId больше не null — определяется в start() через users.get.
 *      Без него фронт не может отличить исходящие сообщения (в Message нет поля outgoing).
 *   3. Исправлен fetchChatMeta: isGroup раньше ВСЕГДА был true (даже для личных диалогов),
 *      а название сообщества читалось из response.groups, которого в
 *      messages.getConversationsById не существует → теперь groups.getById.
 *   4. Сохранение сообщений идёт через MessageService.saveMessage() — он проверяет дубликаты
 *      по (source, messageId). Прямой messageRepository.save() падал на unique-индексе,
 *      когда LongPoll и скан приносили одно и то же сообщение.
 *   5. Сообщение сохраняется даже если автора вытащить не удалось (раньше оно терялось).
 */
@Component
public class VkConnector implements BaseConnector {

    private static final Logger log = LoggerFactory.getLogger(VkConnector.class);

    private static final String API_URL = "https://api.vk.com/method/";
    private static final String API_VERSION = "5.199";
    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final int VK_TEXT_LIMIT = 9000;
    private static final int VK_MAX_ATTACHMENTS = 10;

    /** Сколько секунд сервер VK держит соединение открытым (их лимит — 90). */
    private static final int LONGPOLL_WAIT = 25;
    /** Таймаут HTTP именно для длинного опроса: wait + запас. */
    private static final Duration LONGPOLL_TIMEOUT = Duration.ofSeconds(LONGPOLL_WAIT + 15);
    /** Пауза после сетевой ошибки, чтобы не долбить VK. */
    private static final long LONGPOLL_BACKOFF_MS = 3000;
    /** Сколько секунд не дёргать VK повторно за одним и тем же пользователем после ошибки. */
    private static final long USER_CACHE_NEGATIVE_TTL_MS = 60_000;

    private final ConnectorConfig config = new ConnectorConfig();

    // ==================== СОСТОЯНИЕ ====================
    private volatile boolean isStarted = false;
    private volatile boolean isListening = false;
    private volatile boolean isScanning = false;

    /**
     * ВАЖНО: не final. Определяется в start() через users.get.
     * Фронт использует его (GET /api/vk/status → myUserId), чтобы понять,
     * какие сообщения исходящие: authorId == myUserId.
     */
    private volatile Long myUserId = null;

    // ==================== LONGPOLL ====================
    private volatile String lpServer = null;
    private volatile String lpKey = null;
    private volatile long lpTs = 0;
    /** Ключи сервера/`ts` протухли — нужно заново вызвать messages.getLongPollServer. */
    private volatile boolean lpNeedServer = true;
    private volatile long lpEventsTotal = 0;
    private volatile long lpSavedTotal = 0;
    private volatile long lpLastEventAt = 0;
    /** Защита от второго параллельного цикла после быстрого stop → start. */
    private final java.util.concurrent.atomic.AtomicBoolean lpRunning =
            new java.util.concurrent.atomic.AtomicBoolean(false);
    private final ExecutorService longPollExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "vk-longpoll");
        t.setDaemon(true);
        return t;
    });

    // ==================== КЭШИ ====================
    private final ConcurrentHashMap<Long, VkChatMeta> chatCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, User> userCache = new ConcurrentHashMap<>();
    /** peer_id → время последней неудачи: не дёргаем VK по кругу для deactivated/сообществ. */
    private final ConcurrentHashMap<Long, Long> userFailCache = new ConcurrentHashMap<>();
    private final ExecutorService scanExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "vk-scan");
        t.setDaemon(true);
        return t;
    });

    // ==================== СЕРВИСЫ ====================
    private final UserService userService;
    private final ChatService chatService;
    private final MessageService messageService;

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(20))
            .build();
    private final ObjectMapper mapper = new ObjectMapper();

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
        return new ConnectorStatus("vk", isStarted, isListening, isScanning, myUserId, config);
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
            // Определяем владельца токена — без этого фронт не различает входящие/исходящие
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

    /** users.get без параметров возвращает владельца токена. */
    private void resolveMyUserId() {
        try {
            JsonNode resp = vkApi("users.get", new LinkedHashMap<>());
            if (resp.has("error")) {
                log.warn("[VK] users.get для определения myUserId вернул ошибку: {}", vkError(resp));
                return;
            }
            JsonNode u = resp.path("response").path(0);
            long id = u.path("id").asLong(0);
            if (id > 0) {
                myUserId = id;
                log.info("[VK] myUserId={} ({} {})", id, u.path("first_name").asText(""), u.path("last_name").asText(""));
            }
        } catch (Exception e) {
            log.warn("[VK] Не удалось определить myUserId: {}", describe(e));
        }
    }

    // =========================================================================================
    // 👂 LONGPOLL — прослушка входящих в реальном времени
    //
    // Схема (Bots LongPoll на пользовательском токене):
    //   1) messages.getLongPollServer → { server, key, ts }
    //   2) GET https://{server}?act=a_check&key={key}&ts={ts}&wait=25&v=5.199&mode=2&version=3
    //      → соединение висит до 25 с, пока не придут события
    //   3) { ts, updates:[...] } — ts из ответа подставляем в следующий запрос
    //   4) { failed: 1|2|3, ts } — история событий утеряна/протухла: берём новый ts (или сервер)
    //
    // mode=2 — прикладывать media_url к вложениям (нужно для Message.mediaUrl)
    // version=3 — формат событий message_new с полем object (а не object.message, как у ботов)
    // =========================================================================================

    @Override
    public ConnectorResult startListening() {
        if (!isStarted) {
            return ConnectorResult.fail("VK не запущен. Вызовите start() сначала.");
        }
        if (isListening) {
            return ConnectorResult.ok("VK прослушка уже включена");
        }
        isListening = true;
        lpNeedServer = true;               // заставим цикл запросить свежие server/key/ts
        if (lpRunning.compareAndSet(false, true)) {
            longPollExecutor.submit(() -> {
                try {
                    longPollLoop();
                } finally {
                    lpRunning.set(false);
                }
            });
            log.info("[VK] Прослушка включена");
        } else {
            log.info("[VK] Прослушка включена (цикл уже работает)");
        }
        return ConnectorResult.ok(lpRunning.get()
                ? "VK прослушка включена (LongPoll запущен)"
                : "VK прослушка включена");
    }

    @Override
    public ConnectorResult stopListening() {
        boolean was = isListening;
        isListening = false;
        if (was) {
            log.info("[VK] Прослушка выключена (обработано событий: {}, сохранено сообщений: {})",
                    lpEventsTotal, lpSavedTotal);
        }
        return ConnectorResult.ok("VK прослушка выключена");
    }

    /** Получаем server/key/ts. При ошибке возвращает false — цикл сделает паузу и попробует снова. */
    private boolean refreshLongPollServer() {
        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("need_pts", "1");
            params.put("lp_version", "3");

            JsonNode resp = vkApi("messages.getLongPollServer", params);
            if (resp.has("error")) {
                log.error("[VK] messages.getLongPollServer: {}", vkError(resp));
                return false;
            }
            JsonNode r = resp.path("response");
            String server = r.path("server").asText("");
            String key = r.path("key").asText("");
            long ts = r.path("ts").asLong(0);

            if (server.isBlank() || key.isBlank() || ts == 0) {
                log.error("[VK] messages.getLongPollServer вернул неполные данные: {}", r);
                return false;
            }
            lpServer = normalizeLongPollServer(server);
            lpKey = key;
            lpTs = ts;
            lpNeedServer = false;
            log.info("[VK] LongPoll сервер получен: {}, ts={}", lpServer, ts);
            return true;
        } catch (Exception e) {
            log.error("[VK] Не удалось получить LongPoll сервер: {}", describe(e));
            return false;
        }
    }

    /**
     * VK отдаёт server в разных видах: "im.vk.me", "https://im.vk.me", "/lp123456".
     * HttpRequest.newBuilder(URI) требует абсолютный URI со схемой, иначе
     * IllegalArgumentException: URI with undefined scheme.
     */
    static String normalizeLongPollServer(String server) {
        String s = server == null ? "" : server.trim();
        if (s.isEmpty()) {
            return "";
        }
        String lower = s.toLowerCase(Locale.ROOT);
        if (lower.startsWith("http://") || lower.startsWith("https://")) {
            // уже абсолютный — не трогаем
        } else if (s.startsWith("/")) {
            s = "https://im.vk.me" + s;      // только путь → базовый хост im.vk.me
        } else {
            s = "https://" + s;              // "api.vk.com/gim…" или "im.vk.me" → просто добавляем схему
        }
        return s;
    }

    /** Проверка, что из server вообще можно построить URI — иначе перевыпустим сервер. */
    private boolean serverUrlValid() {
        if (lpServer == null || lpServer.isBlank() || lpKey == null || lpKey.isBlank()) {
            return false;
        }
        try {
            URI uri = URI.create(lpServer);
            return uri.isAbsolute() && uri.getScheme() != null && uri.getHost() != null;
        } catch (Exception e) {
            return false;
        }
    }

    private void longPollLoop() {
        log.info("[VK] LongPoll: цикл запущен");
        while (isListening && isStarted) {
            try {
                if ((lpNeedServer || !serverUrlValid()) && !refreshLongPollServer()) {
                    lpNeedServer = true;
                    sleep(LONGPOLL_BACKOFF_MS);
                    continue;
                }

                String url = lpServer
                        + "?act=a_check"
                        + "&key=" + enc(lpKey)
                        + "&ts=" + lpTs
                        + "&wait=" + LONGPOLL_WAIT
                        + "&v=" + API_VERSION
                        + "&mode=2"
                        + "&version=3";

                HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                        .timeout(LONGPOLL_TIMEOUT)
                        .GET()
                        .build();

                HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                if (!isListening) {
                    break;
                }
                if (resp.statusCode() != 200) {
                    log.warn("[VK] LongPoll HTTP {} — перевыпускаю сервер", resp.statusCode());
                    lpNeedServer = true;
                    sleep(LONGPOLL_BACKOFF_MS);
                    continue;
                }

                String body = resp.body();
                if (body == null || body.isBlank()) {
                    continue;                       // пустой ответ = просто таймаут ожидания
                }

                JsonNode json = mapper.readTree(body);

                // --- failed: 1/2/3 ---
                if (json.has("failed")) {
                    int code = json.path("failed").asInt(0);
                    long newTs = json.path("ts").asLong(0);
                    log.warn("[VK] LongPoll failed={}, ts={}", code, newTs);
                    switch (code) {
                        case 1 -> {                 // история утеряна — работаем с новым ts
                            if (newTs > 0) lpTs = newTs;
                        }
                        case 2, 3 -> {              // ключ/сервер протухли
                            lpNeedServer = true;
                        }
                        default -> lpNeedServer = true;
                    }
                    continue;
                }

                long ts = json.path("ts").asLong(0);
                if (ts > 0) {
                    lpTs = ts;
                }

                JsonNode updates = json.path("updates");
                if (!updates.isArray() || updates.isEmpty()) {
                    continue;                       // тишина в эфире
                }

                for (JsonNode upd : updates) {
                    lpEventsTotal++;
                    lpLastEventAt = System.currentTimeMillis();
                    try {
                        handleUpdate(upd);
                    } catch (Exception e) {
                        log.error("[VK] Ошибка обработки события {}: {}", upd.path("type").asText("?"), describe(e), e);
                    }
                }
            } catch (java.net.http.HttpTimeoutException e) {
                // нормально: wait истёк, событий не было
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (java.io.IOException e) {
                log.warn("[VK] LongPoll: сетевая ошибка ({}), пауза {} мс", describe(e), LONGPOLL_BACKOFF_MS);
                sleep(LONGPOLL_BACKOFF_MS);
            } catch (IllegalArgumentException e) {
                // почти всегда "URI with undefined scheme" — server пришёл битый
                log.error("[VK] LongPoll: некорректный server («{}») — перевыпускаю: {}", lpServer, describe(e));
                lpNeedServer = true;
                sleep(LONGPOLL_BACKOFF_MS);
            } catch (Exception e) {
                log.error("[VK] LongPoll: неожиданная ошибка — {}", describe(e), e);
                lpNeedServer = true;
                sleep(LONGPOLL_BACKOFF_MS);
            }
        }
        log.info("[VK] LongPoll: цикл завершён (всего событий: {}, сохранено: {})", lpEventsTotal, lpSavedTotal);
    }

    /** Разбор одного события LongPoll. */
    private void handleUpdate(JsonNode upd) {
        String type = upd.path("type").asText("");
        JsonNode obj = upd.path("object");
        switch (type) {
            case "message_new" -> onIncomingMessage(obj);
            // своё же сообщение, отправленное из официального клиента/веба — тоже сохраняем
            case "message_reply" -> onIncomingMessage(obj);
            case "message_edit" -> {
                // правка сообщения: в Message нет флага edited, поэтому только логируем
                log.debug("[VK] message_edit id={}", obj.path("id").asLong(0));
            }
            case "message_del", "message_restore" ->
                    log.info("[VK] {}: message_id={}", type, obj.path("id").asLong(0));
            case "messages.delete", "messages.restore" ->
                    log.info("[VK] {}: {}", type, obj);
            case "read_history_incoming", "read_history_outgoing",
                 "message_typing_state", "user_typing", "user_online", "user_offline",
                 "messages.edit", "messages.read" -> {
                // служебные — игнорируем, но считаем
            }
            default -> log.debug("[VK] необработанное событие: {}", type);
        }
    }

    /**
     * message_new / message_reply → Message в БД.
     *
     * @param m для message_new это объект сообщения; для message_reply — тоже сообщение
     *          (но у него нет client_info/бывает меньше полей, поэтому всё читаем через path())
     */
    private void onIncomingMessage(JsonNode m) {
        if (m == null || m.isMissingNode()) {
            return;
        }
        long messageId = m.path("id").asLong(0);
        long peerId = m.path("peer_id").asLong(0);
        long fromId = m.path("from_id").asLong(0);
        if (messageId == 0 || peerId == 0) {
            log.warn("[VK] message_new без id/peer_id: {}", m);
            return;
        }

        // Свои сообщения, отправленные через ЭТОТ коннектор, уже сохранены sendMessage().
        // Но отправленное с телефона/веба тем же аккаунтом надо забрать — поэтому проверяем
        // дубликат через MessageService, а не отбрасываем всё от себя.
        String text = m.path("text").asText("");
        long date = m.path("date").asLong(0);
        String mediaUrl = extractMediaUrl(m.path("attachments"));

        Message msg = new Message("vk", messageId, peerId, fromId == 0 ? peerId : fromId, text, mediaUrl);
        msg.setTimestamp(date > 0 ? date * 1000L : System.currentTimeMillis());   // VK отдаёт СЕКУНДЫ

        // --- 1) сохраняем/обновляем чат (фильтры сканирования тут НЕ применяются) ---
        VkChatMeta meta = ensureChat(peerId, false);
        if (meta == null) {
            log.warn("[VK] не удалось определить чат peer_id={} — сообщение id={} не сохранено", peerId, messageId);
            return;
        }

        // --- 2) автор (если это не мы) ---
        boolean mine = myUserId != null && fromId == myUserId;
        if (!mine && fromId != 0) {
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

        // --- 3) сообщение (с проверкой дубликата) ---
        try {
            messageService.saveMessage(msg);
            lpSavedTotal++;
            log.info("[VK] {} {} id={} peer={} «{}»{}",
                    mine ? "← исходящее" : "→ входящее",
                    meta.isGroup() ? "[группа]" : "[лс]",
                    messageId, peerId, truncate(text, 60), mediaUrl != null ? " +media" : "");
        } catch (Exception e) {
            log.error("[VK] не удалось сохранить сообщение id={}: {}", messageId, describe(e));
        }
    }

    /**
     * Первое вложение с URL → Message.mediaUrl.
     * mode=2 в LongPoll прикладывает media_url, но надёжнее дочитать photo/doc явно.
     */
    private String extractMediaUrl(JsonNode attachments) {
        if (attachments == null || !attachments.isArray() || attachments.isEmpty()) {
            return null;
        }
        for (JsonNode a : attachments) {
            String t = a.path("type").asText("");
            JsonNode payload = a.path(t.isEmpty() ? "photo" : t);
            if (payload.isMissingNode()) {
                continue;
            }
            // фото: максимальный размер из sizes[]
            if ("photo".equals(t)) {
                String best = null;
                long bestW = -1;
                for (JsonNode s : payload.path("sizes")) {
                    long w = s.path("width").asLong(0);
                    if (w >= bestW) {
                        bestW = w;
                        best = s.path("url").asText(null);
                    }
                }
                if (best != null) {
                    return best;
                }
            }
            // документ/аудио/видео: готовые url-поля
            for (String f : new String[]{"url", "preview_url", "uri"}) {
                String u = payload.path(f).asText("");
                if (!u.isBlank()) {
                    return u;
                }
            }
            // media_url от mode=2
            String mu = a.path("media_url").asText("");
            if (!mu.isBlank()) {
                return mu;
            }
        }
        return null;
    }

    /** Проверяет фильтры сканирования (scanPersonal / scanGroups / whitelist). */
    private VkChatMeta ensureChat(long peerId) {
        return ensureChat(peerId, true);
    }

    /**
     * Достаёт мету чата (кэш → VK) и сохраняет Chat в БД.
     *
     * @param applyScanFilters true — для сканирования истории: чат вне настроек сканирования
     *                         возвращает null и НЕ сохраняется.
     *                         false — для LongPoll и для отправки: живой диалог сохраняется
     *                         всегда, иначе человек вам написал, а в UI этого чата нет.
     */
    private VkChatMeta ensureChat(long peerId, boolean applyScanFilters) {
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

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @PreDestroy
    public void shutdown() {
        isListening = false;
        isStarted = false;
        isScanning = false;
        longPollExecutor.shutdownNow();
        scanExecutor.shutdownNow();
        try {
            longPollExecutor.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("[VK] Коннектор остановлен при завершении приложения");
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
                    VkChatMeta meta = fetchChatMeta(peerId);
                    if (meta == null) {
                        skipped++;
                        continue;
                    }
                    // ensureChat(peerId) уже применил фильтры и сохранил Chat
                    if (!isChatAllowed(meta)) {
                        skipped++;
                        log.debug("[VK] пропущен peer={} (настройки сканирования)", peerId);
                        continue;
                    }
                    chatCache.put(peerId, meta);
                    chatService.saveOrUpdate(new Chat("vk", meta.id(), meta.title(), meta.isGroup()));

                    List<Message> messages = fetchMessages(peerId, lim);
                    for (Message msg : messages) {
                        try {
                            long fromId = msg.getAuthorId() != null ? msg.getAuthorId() : 0;
                            boolean mine = myUserId != null && fromId == myUserId;
                            if (!mine && fromId != 0) {
                                User author = getOrFetchUser(fromId);
                                if (author != null) {
                                    userService.saveOrUpdate(author);
                                }
                            }
                            // saveMessage сам проверяет дубликат по (source, messageId)
                            messageService.saveMessage(msg);
                            saved++;
                        } catch (Exception e) {
                            log.warn("[VK] не сохранено сообщение id={}: {}", msg.getMessageId(), describe(e));
                        }
                    }
                    sleep(500);                       // бережём лимиты VK
                } catch (Exception e) {
                    log.error("[VK] ошибка сканирования peer={}: {}", peerId, describe(e));
                }
            }
            log.info("[VK] Сканирование завершено: сохранено {}, пропущено чатов {}", saved, skipped);
        } finally {
            isScanning = false;
        }
    }

    private boolean isChatAllowed(VkChatMeta meta) {
        if (!meta.isGroup()) {
            return config.scanPersonal;
        }
        if (!config.scanGroups) {
            return false;
        }
        if (!config.whitelist.isEmpty() && !config.whitelist.contains(meta.id())) {
            return false;
        }
        return true;
    }

    private List<Long> getDialogsFromVK(int limit) {
        List<Long> dialogs = new ArrayList<>();
        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("count", String.valueOf(Math.min(Math.max(limit, 1), 200)));
            JsonNode response = vkApi("messages.getConversations", params);

            if (response.has("error")) {
                log.error("[VK] messages.getConversations: {}", vkError(response));
                return dialogs;
            }
            JsonNode items = response.path("response").path("items");
            if (items.isArray()) {
                for (JsonNode item : items) {
                    long peerId = item.path("conversation").path("peer").path("id").asLong(0);
                    if (peerId != 0) {
                        dialogs.add(peerId);
                    }
                }
            }
        } catch (Exception e) {
            log.error("[VK] Ошибка получения списка диалогов: {}", describe(e));
        }
        return dialogs;
    }

    /**
     * Мета чата по peer_id.
     * ИСПРАВЛЕНО: isGroup больше не всегда true; название сообщества берётся из groups.getById,
     * т.к. в ответе messages.getConversationsById поля groups НЕТ.
     */
    private VkChatMeta fetchChatMeta(long peerId) {
        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("peer_ids", String.valueOf(peerId));
            params.put("extended", "1");
            JsonNode response = vkApi("messages.getConversationsById", params);

            if (response.has("error")) {
                log.warn("[VK] messages.getConversationsById peer={}: {}", peerId, vkError(response));
                return null;
            }
            JsonNode item = response.path("response").path("items").path(0);
            if (item.isMissingNode() || item.isNull()) {
                log.warn("[VK] messages.getConversationsById вернул пустой items для peer={}", peerId);
                return null;
            }

            JsonNode peer = item.path("peer");
            String peerType = peer.path("type").asText("");
            JsonNode chatSettings = item.path("chat_settings");

            String title;
            boolean isGroup;
            switch (peerType) {
                case "user" -> {
                    isGroup = false;
                    User u = getOrFetchUser(peerId);
                    title = u != null
                            ? (u.getFirstName() + " " + (u.getLastName() == null ? "" : u.getLastName())).strip()
                            : "Пользователь " + peerId;
                    if (title.isBlank()) {
                        title = "Пользователь " + peerId;
                    }
                }
                case "chat" -> {
                    isGroup = true;                       // беседа
                    title = chatSettings.path("title").asText("");
                    if (title.isBlank()) {
                        title = "Беседа " + (peerId - 2_000_000_000L);
                    }
                }
                case "group" -> {
                    isGroup = true;                       // сообщество (peer_id < 0)
                    title = fetchGroupName(peerId);
                }
                default -> {
                    // не распознали — считаем беседой, если peer_id в диапазоне чатов
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

    /** Название сообщества: groups.getById (у сообщений его в ответе нет). */
    private String fetchGroupName(long peerId) {
        long groupId = Math.abs(peerId);
        try {
            JsonNode resp = vkApi("groups.getById", Map.of("group_ids", String.valueOf(groupId)));
            if (resp.has("error")) {
                log.debug("[VK] groups.getById {}: {}", groupId, vkError(resp));
                return "Сообщество " + groupId;
            }
            JsonNode g = resp.path("response").path(0);
            String name = g.path("name").asText("");
            return name.isBlank() ? "Сообщество " + groupId : name;
        } catch (Exception e) {
            log.debug("[VK] groups.getById {} не удался: {}", groupId, describe(e));
            return "Сообщество " + groupId;
        }
    }

    /**
     * История переписки.
     * ВАЖНО: VK отдаёт date в СЕКУНДАХ, а Message.timestamp у вас в миллисекундах
     * (System.currentTimeMillis()) — приводим, иначе фронт рисует 1970 год.
     */
    private List<Message> fetchMessages(long peerId, int limit) {
        List<Message> messages = new ArrayList<>();
        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("peer_id", String.valueOf(peerId));
            params.put("count", String.valueOf(Math.min(Math.max(limit, 1), 200)));
            params.put("extended", "1");

            JsonNode response = vkApi("messages.getHistory", params);
            if (response.has("error")) {
                log.warn("[VK] messages.getHistory peer={}: {}", peerId, vkError(response));
                return messages;
            }
            JsonNode items = response.path("response").path("items");
            if (!items.isArray()) {
                return messages;
            }
            for (JsonNode item : items) {
                long fromId = item.path("from_id").asLong(0);
                long messageId = item.path("id").asLong(0);
                String text = item.path("text").asText("");
                long dateSec = item.path("date").asLong(0);
                if (messageId == 0) {
                    continue;
                }
                Message msg = new Message("vk", messageId, peerId, fromId == 0 ? peerId : fromId,
                        text, extractMediaUrl(item.path("attachments")));
                msg.setTimestamp(dateSec > 0 ? dateSec * 1000L : System.currentTimeMillis());
                messages.add(msg);
            }
            // VK отдаёт от новых к старым — переворачиваем в хронологический порядок
            Collections.reverse(messages);
        } catch (Exception e) {
            log.warn("[VK] Ошибка получения истории peer={}: {}", peerId, describe(e));
        }
        return messages;
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
            Map<String, String> params = new LinkedHashMap<>();
            params.put("user_ids", String.valueOf(userId));
            params.put("fields", "photo_100,screen_name");

            JsonNode response = vkApi("users.get", params);
            if (response.has("error")) {
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
            String photoUrl = userNode.path("photo_100").asText("");
            String screenName = userNode.path("screen_name").asText(null);

            if (firstName.isBlank() && lastName.isBlank()) {
                // deactivated/удалённый аккаунт
                userFailCache.put(userId, System.currentTimeMillis());
                return null;
            }
            User user = new User("vk", userId, firstName, lastName, screenName, photoUrl, null, new ArrayList<>());
            userCache.put(userId, user);
            return user;
        } catch (Exception e) {
            log.debug("[VK] Ошибка получения пользователя {}: {}", userId, describe(e));
            userFailCache.put(userId, System.currentTimeMillis());
            return null;
        }
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
        log.info("[VK] Конфиг обновлён: scanPersonal={}, scanGroups={}, whitelist={}, limitPerChat={}, downloadMedia={}",
                config.scanPersonal, config.scanGroups, config.whitelist.size(), config.limitPerChat, config.downloadMedia);
        return ConnectorResult.ok("Конфигурация VK обновлена");
    }

    // =========================================================================================
    // 📤 ОТПРАВКА СООБЩЕНИЙ
    // =========================================================================================

    /**
     * Контракт BaseConnector — его вызывает UnifiedSendService (POST /api/v2/send)
     * и VkApiController (POST /api/vk/send/message).
     *
     * @param attachments строка вида "photo123_456,doc789_123" (формат VK) или null
     * @param replyTo     id сообщения VK для ответа, null/0 = без ответа
     * @return message_id отправленного сообщения
     */
    @Override
    public long sendMessage(String peer, String text, String attachments, Long replyTo) {
        if (peer == null || peer.isBlank()) {
            throw new IllegalArgumentException("Не указан peer_id");
        }
        boolean noText = (text == null || text.trim().isEmpty());
        boolean noAtt = (attachments == null || attachments.isBlank());
        if (noText && noAtt) {
            throw new IllegalArgumentException("Пустое сообщение: нет ни текста, ни вложений");
        }
        long peerId;
        try {
            peerId = Long.parseLong(peer.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid peer ID: " + peer, e);
        }

        Map<String, String> params = new LinkedHashMap<>();
        params.put("peer_id", String.valueOf(toPeerId(peerId)));
        if (!noText) {
            params.put("message", truncate(text, VK_TEXT_LIMIT));
        }
        if (!noAtt) {
            params.put("attachment", attachments.trim());
        }
        if (replyTo != null && replyTo > 0) {
            params.put("reply_to", String.valueOf(replyTo));
        }
        params.put("random_id", String.valueOf(ThreadLocalRandom.current().nextLong(1, Integer.MAX_VALUE)));

        try {
            JsonNode resp = vkApi("messages.send", params);
            if (resp.has("error") && !resp.path("error").isNull()) {
                throw new RuntimeException("VK API error: " + vkError(resp));
            }
            long messageId = resp.path("response").asLong(0);
            if (messageId <= 0) {
                throw new RuntimeException("VK вернул некорректный message_id: " + resp.path("response"));
            }
            log.info("[VK] 📤 отправлено peer_id={}, message_id={}", peerId, messageId);

            // Сохраняем СВОЁ сообщение в БД. Без этого фронт покажет его оптимистично,
            // но после обновления истории оно исчезнет (в messages его не будет).
            saveOutgoing(toPeerId(peerId), messageId, text, attachments, replyTo);

            return messageId;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("VK send failed: " + describe(e), e);
        }
    }

    /**
     * Кладет отправленное нами сообщение в таблицу messages.
     * Автор — myUserId (если он определён), иначе peer_id: в Message.authorId стоит NOT NULL.
     * mediaUrl заполняем, только если вложения — это http-ссылка; строки вида photo123_456
     * ссылкой не являются, их подберёт следующее сканирование истории.
     */
    private void saveOutgoing(long peerId, long messageId, String text, String attachments, Long replyTo) {
        try {
            long author = myUserId != null ? myUserId : peerId;
            String media = (attachments != null && attachments.trim().startsWith("http"))
                    ? attachments.trim() : null;
            Message msg = new Message("vk", messageId, peerId, author,
                    text == null ? "" : text, media);
            msg.setTimestamp(System.currentTimeMillis());

            ensureChat(peerId, false);          // чтобы чат точно был в таблице chats
            messageService.saveMessage(msg);    // дубликат отсеется по (source, messageId)
        } catch (Exception e) {
            // сообщение УЖЕ отправлено — не роняем вызов из-за проблем с БД
            log.warn("[VK] отправлено (message_id={}), но сохранить в БД не удалось: {}", messageId, describe(e));
        }
    }

    /** Вариант с SendResult (не бросает исключений) — его зовёт VkApiController. */
    public SendResult sendMessage(long peerId, String text, String attachments, long replyTo) {
        long peer = toPeerId(peerId);
        try {
            long messageId = sendMessage(String.valueOf(peerId), text, attachments,
                    replyTo > 0 ? replyTo : null);
            return SendResult.ok("vk", peer, messageId, text);
        } catch (Exception e) {
            return SendResult.fail("vk", peer, describe(e));
        }
    }

    /** Отправка текста + уже готовых вложений (photo…/doc…). */
    public SendResult sendMessage(long peerId, String text, List<String> attachments, long replyTo) {
        return sendMessage(peerId, text,
                (attachments == null || attachments.isEmpty()) ? null : String.join(",", attachments),
                replyTo);
    }

    /**
     * Файл с диска: картинка — как фото (при нехватке scope `photos` — документом),
     * остальное — документом.
     */
    public SendResult sendFile(long peerId, String caption, String filePath, String mimeType, long replyTo) {
        long peer = toPeerId(peerId);
        if (filePath == null || filePath.isBlank()) {
            return SendResult.fail("vk", peer, "Не указан путь к файлу");
        }
        Path file = Path.of(filePath);
        if (!Files.exists(file)) {
            return SendResult.fail("vk", peer, "Файл не найден: " + filePath);
        }
        try {
            List<String> atts = new ArrayList<>();
            if (isImage(file.getFileName().toString(), mimeType)) {
                try {
                    atts.add(uploadPhoto(peer, file));
                } catch (VkScopeException scope) {
                    // нет scope `photos` (error 15/7/20) — шлём документом, чтобы отправка не падала
                    log.warn("[VK] {} → отправляю картинку ДОКУМЕНТОМ. Лечение: верификация аккаунта VK "
                            + "+ токен со scope photos", scope.getMessage());
                    atts.add(uploadDoc(peer, file, caption));
                }
            } else {
                atts.add(uploadDoc(peer, file, caption));
            }
            return sendMessage(peer, caption, atts, replyTo);
        } catch (Exception e) {
            return SendResult.fail("vk", peer, describe(e));
        }
    }

    /** Несколько файлов одним сообщением (лимит VK — 10 вложений). */
    public SendResult sendFiles(long peerId, String caption, List<Path> files, long replyTo) {
        long peer = toPeerId(peerId);
        try {
            List<String> atts = new ArrayList<>();
            for (Path f : files) {
                if (atts.size() >= VK_MAX_ATTACHMENTS) {
                    break;
                }
                if (isImage(f.getFileName().toString(), null)) {
                    try {
                        atts.add(uploadPhoto(peer, f));
                    } catch (VkScopeException scope) {
                        atts.add(uploadDoc(peer, f, caption));
                    }
                } else {
                    atts.add(uploadDoc(peer, f, caption));
                }
            }
            if (atts.isEmpty()) {
                return SendResult.fail("vk", peer, "Не удалось загрузить ни одного файла");
            }
            return sendMessage(peer, caption, atts, replyTo);
        } catch (Exception e) {
            return SendResult.fail("vk", peer, describe(e));
        }
    }

    /** Ошибка прав токена (scope), а не технический сбой. */
    public static class VkScopeException extends RuntimeException {
        public VkScopeException(String message) {
            super(message);
        }
    }

    // =========================================================================================
    // 🖼 ЗАГРУЗКА ФОТО / 📎 ДОКУМЕНТА
    // =========================================================================================

    private String uploadPhoto(long peer, Path file) throws Exception {
        JsonNode server = vkApi("photos.getMessagesUploadServer", Map.of("peer_id", String.valueOf(peer)));
        if (server.has("error") && !server.path("error").isNull()) {
            if (isScopeError(server)) {
                throw new VkScopeException(vkError(server));
            }
            throw new IllegalStateException("photos.getMessagesUploadServer: " + vkError(server));
        }
        String uploadUrl = server.path("response").path("upload_url").asText("");
        if (uploadUrl.isBlank()) {
            throw new IllegalStateException("VK вернул ответ без upload_url: " + server);
        }

        JsonNode uploaded = postMultipart(uploadUrl, "photo", file);
        if (uploaded.has("error") && !uploaded.path("error").isNull()) {
            throw new IllegalStateException("Загрузка файла на сервер VK: " + vkError(uploaded));
        }
        if (uploaded.path("photo").isMissingNode() || uploaded.path("hash").asText("").isBlank()) {
            throw new IllegalStateException("Сервер загрузки вернул не photo/server/hash, а: " + uploaded);
        }

        // VK отдаёт "photo" то строкой, то массивом — приводим к строке,
        // иначе saveMessagesPhoto ответит ошибкой 100/129
        Map<String, String> saveParams = new LinkedHashMap<>();
        saveParams.put("photo", asText(uploaded.get("photo")));
        copyIfPresent(uploaded, saveParams, "server", "hash");

        JsonNode saved = vkApi("photos.saveMessagesPhoto", saveParams);
        if (saved.has("error") && !saved.path("error").isNull()) {
            if (isScopeError(saved)) {
                throw new VkScopeException(vkError(saved));
            }
            throw new IllegalStateException("photos.saveMessagesPhoto: " + vkError(saved));
        }
        JsonNode first = saved.path("response").path(0);
        if (first.isMissingNode() || first.isNull()) {
            throw new IllegalStateException("photos.saveMessagesPhoto вернул пустой ответ: " + saved);
        }
        return "photo" + first.path("owner_id").asLong() + "_" + first.path("id").asLong();
    }

    private String uploadDoc(long peer, Path file, String caption) throws Exception {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("peer_id", String.valueOf(peer));
        params.put("type", "doc");
        if (caption != null && !caption.isBlank()) {
            params.put("caption", truncate(caption, 255));
        }

        JsonNode server = vkApi("docs.getMessagesUploadServer", params);
        if (server.has("error") && !server.path("error").isNull()) {
            if (isScopeError(server)) {
                throw new VkScopeException(vkError(server));
            }
            throw new IllegalStateException("docs.getMessagesUploadServer: " + vkError(server));
        }
        String uploadUrl = server.path("response").path("upload_url").asText("");
        if (uploadUrl.isBlank()) {
            throw new IllegalStateException("VK вернул ответ без upload_url: " + server);
        }

        JsonNode uploaded = postMultipart(uploadUrl, "file", file);
        String rawFile = uploaded.path("file").asText("");
        if (rawFile.isBlank()) {
            throw new IllegalStateException("Сервер загрузки документа вернул не поле file, а: " + uploaded);
        }

        JsonNode saved = vkApi("docs.save", Map.of("file", rawFile));
        if (saved.has("error") && !saved.path("error").isNull()) {
            if (isScopeError(saved)) {
                throw new VkScopeException(vkError(saved));
            }
            throw new IllegalStateException("docs.save: " + vkError(saved));
        }
        JsonNode doc = saved.path("response").path("doc");
        if (doc.isMissingNode() || doc.path("id").asLong(0) == 0) {
            throw new IllegalStateException("docs.save вернул пустой ответ: " + saved);
        }
        return "doc" + doc.path("owner_id").asLong() + "_" + doc.path("id").asLong();
    }

    // =========================================================================================
    // 🔧 HTTP К VK API
    // =========================================================================================

    private JsonNode vkApi(String method, Map<String, String> params) throws Exception {
        StringBuilder q = new StringBuilder();
        if (params != null) {
            params.forEach((k, v) -> {
                if (v != null) {
                    q.append(q.length() > 0 ? "&" : "").append(enc(k)).append('=').append(enc(v));
                }
            });
        }
        q.append(q.length() > 0 ? "&" : "").append("access_token=").append(enc(Config.getVkToken()));
        q.append("&v=").append(API_VERSION);

        HttpRequest req = HttpRequest.newBuilder(URI.create(API_URL + method + "?" + q))
                .timeout(TIMEOUT)
                .GET()
                .build();

        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (resp.statusCode() != 200) {
            throw new IllegalStateException("VK ответил HTTP " + resp.statusCode() + ": " + resp.body());
        }
        String body = resp.body();
        if (body == null || body.isBlank()) {
            throw new IllegalStateException("VK вернул пустой ответ для " + method);
        }
        return mapper.readTree(body);
    }

    private JsonNode postMultipart(String url, String fieldName, Path file) throws Exception {
        String boundary = "----VkConnector" + System.nanoTime();
        byte[] body = buildMultipart(boundary, fieldName, file);

        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofMinutes(5))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        String resp = http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8)).body();
        return mapper.readTree(resp);
    }

    private byte[] buildMultipart(String boundary, String fieldName, Path file) throws Exception {
        String fileName = file.getFileName().toString();
        String head = "--" + boundary + "\r\n"
                + "Content-Disposition: form-data; name=\"" + fieldName + "\"; filename=\"" + fileName + "\"\r\n"
                + "Content-Type: application/octet-stream\r\n\r\n";
        String tail = "\r\n--" + boundary + "--\r\n";

        byte[] content = Files.readAllBytes(file);
        byte[] headBytes = head.getBytes(StandardCharsets.UTF_8);
        byte[] tailBytes = tail.getBytes(StandardCharsets.UTF_8);

        byte[] out = new byte[headBytes.length + content.length + tailBytes.length];
        System.arraycopy(headBytes, 0, out, 0, headBytes.length);
        System.arraycopy(content, 0, out, headBytes.length, content.length);
        System.arraycopy(tailBytes, 0, out, headBytes.length + content.length, tailBytes.length);
        return out;
    }

    // =========================================================================================
    // 🔍 ДИАГНОСТИКА ЗАГРУЗКИ  (GET /api/vk/debug-upload?peerId=…)
    // =========================================================================================

    public Map<String, Object> debugUpload(long peerId) {
        Map<String, Object> report = new LinkedHashMap<>();

        JsonNode me = callQuietly("users.get", Map.of());
        if (me.has("error")) {
            report.put("token", "❌ токен не работает: " + vkError(me));
            report.put("hint", hintFor(me.path("error").path("error_code").asInt()));
            return report;
        }
        JsonNode user0 = me.path("response").path(0);
        report.put("token", "✅ рабочий, владелец: id" + user0.path("id").asLong()
                + " " + user0.path("first_name").asText("") + " " + user0.path("last_name").asText(""));

        JsonNode asGroup = callQuietly("groups.getById", Map.of());
        boolean looksLikeGroupToken = !asGroup.has("error")
                && asGroup.path("response").path(0).path("id").asLong(0) != 0;
        report.put("token_type", looksLikeGroupToken
                ? "⚠️ КЛЮЧ СООБЩЕСТВА — photos.*/docs.* для загрузки в личные сообщения с ним НЕ работают"
                : "пользовательский (это правильно для фото в ЛС)");

        report.put("peer_id", peerId);
        report.put("peer_hint", peerHint(peerId));

        JsonNode photo = callQuietly("photos.getMessagesUploadServer", Map.of("peer_id", String.valueOf(peerId)));
        report.put("photos.getMessagesUploadServer.raw", photo.toString());
        if (photo.has("error")) {
            report.put("photos.error", "❌ " + vkError(photo));
            report.put("photos.hint", hintFor(photo.path("error").path("error_code").asInt()));
        } else {
            String url = photo.path("response").path("upload_url").asText("");
            report.put("photos.upload_url", url.isBlank() ? "❌ пустой (странный ответ VK)" : "✅ получен");
        }

        JsonNode doc = callQuietly("docs.getMessagesUploadServer",
                Map.of("peer_id", String.valueOf(peerId), "type", "doc"));
        report.put("docs.getMessagesUploadServer.raw", doc.toString());
        if (doc.has("error")) {
            report.put("docs.error", "❌ " + vkError(doc));
            report.put("docs.hint", hintFor(doc.path("error").path("error_code").asInt()));
        } else {
            String url = doc.path("response").path("upload_url").asText("");
            report.put("docs.upload_url", url.isBlank() ? "❌ пустой" : "✅ получен");
        }

        // LongPoll
        JsonNode lp = callQuietly("messages.getLongPollServer", Map.of("need_pts", "1", "lp_version", "3"));
        if (lp.has("error")) {
            report.put("longpoll", "❌ " + vkError(lp));
            report.put("longpoll.hint", hintFor(lp.path("error").path("error_code").asInt()));
        } else {
            report.put("longpoll", "✅ сервер получен, ts=" + lp.path("response").path("ts").asLong(0));
        }
        report.put("longpoll.state", Map.of(
                "isListening", isListening,
                "eventsTotal", lpEventsTotal,
                "savedTotal", lpSavedTotal,
                "lastEventAt", lpLastEventAt == 0 ? "ещё не было" : new Date(lpLastEventAt).toString(),
                "myUserId", String.valueOf(myUserId)));

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

    private JsonNode callQuietly(String method, Map<String, String> params) {
        try {
            return vkApi(method, params);
        } catch (Exception e) {
            return mapper.createObjectNode().putObject("error")
                    .put("error_code", -1)
                    .put("error_msg", e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    public static String hintFor(int code) {
        return switch (code) {
            case 5 -> "Токен недействителен/истёк. Перевыпустите его и обновите vk.token2.";
            case 6 -> "Слишком много запросов — VK включил лимит. Добавьте паузы между вызовами.";
            case 7 -> "Нет доступа к методу. Скорее всего у вас КЛЮЧ СООБЩЕСТВА: photos.*/docs.* для "
                    + "загрузки в личные сообщения работают только с ПОЛЬЗОВАТЕЛЬСКИМ токеном.";
            case 9 -> "Слишком много однотипных запросов — увеличьте паузы.";
            case 10 -> "Нет доступа к сообществу или к его настройкам.";
            case 11 -> "Выключите «тестовый режим» в настройках приложения.";
            case 14 -> "Нужна CAPTCHA (в ответе есть captcha_sid/captcha_img). Через API не обходится.";
            case 15 -> "Access denied: у токена нет нужных прав (для фото нужны scope `photos` И `messages`). "
                    + "Если subcode=1133 и в request_params видно oauth=1 — дело в scope и/или в том, что "
                    + "аккаунт VK не прошёл верификацию (с 2024 г. обязательна). sendFile() при этом "
                    + "автоматически шлёт картинки ДОКУМЕНТОМ.";
            case 16 -> "Требуется подтверждение через 2FA. Пройдите его и перевыпустите токен.";
            case 20 -> "Метод выключен: включите его в настройках приложения (или используйте Standalone).";
            case 100 -> "Неверный параметр. Чаще всего не тот peer_id: беседа = 2000000000 + chat_id, "
                    + "сообщество = отрицательный id, человек = его uid.";
            case 113 -> "Неверный альбом: для фото в сообщениях используйте "
                    + "photos.getMessagesUploadServer + photos.saveMessagesPhoto.";
            case 203 -> "Нет доступа к чужому фото/альбому.";
            case 300 -> "Альбом переполнен.";
            default -> "Код " + code + ": https://dev.vk.com/reference/errors";
        };
    }

    private static String peerHint(long peerId) {
        if (peerId >= 2_000_000_000L) return "беседа (chat_id = " + (peerId - 2_000_000_000L) + ")";
        if (peerId < 0) return "сообщество (id = " + (-peerId) + ")";
        if (peerId > 0) return "личный чат с пользователем id=" + peerId;
        return "⚠️ peer_id = 0 — так не отправить";
    }

    // =========================================================================================
    // 🧰 УТИЛИТЫ
    // =========================================================================================

    /**
     * id из БД → peer_id. Сейчас как есть: положительные id и id бесед (>= 2000000000) идут
     * без изменений, отрицательные — сообщества.
     * Если у вас chatId бесед хранится «коротким» (1, 2, 3…), включите конвертацию:
     *     if (rawId > 0 && rawId < 2_000_000_000L) return 2_000_000_000L + rawId;
     */
    public static long toPeerId(long rawId) {
        return rawId;
    }

    private static boolean isImage(String fileName, String mimeType) {
        if (mimeType != null && mimeType.toLowerCase().startsWith("image/")) {
            return true;
        }
        String n = fileName == null ? "" : fileName.toLowerCase();
        return n.endsWith(".jpg") || n.endsWith(".jpeg") || n.endsWith(".png")
                || n.endsWith(".gif") || n.endsWith(".webp") || n.endsWith(".bmp");
    }

    /** "code=15 msg=Access denied … -> подсказка" */
    private static String vkError(JsonNode resp) {
        JsonNode e = resp.path("error");
        int code = e.path("error_code").asInt();
        String msg = e.path("error_msg").asText("");
        String hint = hintFor(code);
        return "code=" + code + " msg=" + msg + (msg.isBlank() && code == 0 ? "" : "  ->  " + hint);
    }

    /** Ошибка прав (scope), а не технический сбой. */
    private static boolean isScopeError(JsonNode resp) {
        JsonNode e = resp.path("error");
        int code = e.path("error_code").asInt();
        String msg = e.path("error_msg").asText("").toLowerCase();
        return code == 15 || code == 7 || code == 20 || msg.contains("current scopes");
    }

    /** JSON-узел → строка параметра VK (массив [999,1] превращается в "999,1"). */
    private static String asText(JsonNode node) {
        if (node == null || node.isNull()) {
            return "";
        }
        if (node.isArray()) {
            StringBuilder sb = new StringBuilder();
            for (JsonNode item : node) {
                if (sb.length() > 0) {
                    sb.append(',');
                }
                sb.append(item.asText());
            }
            return sb.toString();
        }
        return node.asText("");
    }

    private static void copyIfPresent(JsonNode src, Map<String, String> dst, String... keys) {
        for (String k : keys) {
            JsonNode v = src.get(k);
            if (v != null && !v.isNull()) {
                dst.put(k, v.asText());
            }
        }
    }

    private static String truncate(String s, int max) {
        if (s == null) {
            return "";
        }
        return s.length() <= max ? s : s.substring(0, max - 1) + "…";
    }

    private static String enc(String s) {
        return URLEncoder.encode(s == null ? "" : s, StandardCharsets.UTF_8);
    }

    private static String describe(Throwable t) {
        Throwable c = (t != null && t.getCause() != null) ? t.getCause() : t;
        if (c == null) {
            return "неизвестная ошибка";
        }
        String m = c.getMessage();
        return (m != null && !m.isBlank()) ? m : c.getClass().getSimpleName();
    }

    /** Метаданные чата. */
    private record VkChatMeta(long id, String title, boolean isGroup) {}
}
