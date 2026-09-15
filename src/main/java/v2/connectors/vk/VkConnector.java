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
    /** Сколько событий отброшено вайтлистом прослушки. */
    private volatile long lpFilteredTotal = 0;
    /** Сколько сырых кортежей уже вывели в лог (для диагностики раскладки). */
    private volatile int lpRawDumped = 0;
    /**
     * Версия User LongPoll. По официальной документации VK актуальная — 3:
     * «version — Версия. Актуальная версия: 3».
     *
     * С version=3 события приходят кодами 4/5/6/7… и КЛАССИЧЕСКОЙ раскладкой кортежа:
     *   [4, message_id, flags, peer_id, timestamp, text, additional{title,from}, attachments{attach1_*}, random_id?]
     * Именно её показывает пример ответа в доках (8 элементов, 9-й — random_id при mode & 128).
     *
     * Если VK когда-нибудь ответит {failed:4, min_version, max_version} — возьмём его
     * max_version и повторим (см. longPollLoop).
     */
    private volatile int lpVersion = 3;
    private volatile long lpPts = 0;
    /**
     * mode — битовая маска ФОРМАТА ответа (не фильтр!):
     * 2 = вложения, 8 = расширенные события, 32 = pts, 128 = random_id.
     * Бит 128 важен: с ним в кортеже v21 гарантированно присутствует randomId, а следом messageId —
     * без него индексы «плывут» и в БД уехал бы conversationMessageId вместо message_id.
     */
    private static final int LONGPOLL_MODE = 2 | 8 | 32 | 128;   // = 138

    /**
     * Негативный кэш для прослушки: peer_id, уже отброшенный вайтлистом.
     * Без него на каждое сообщение из неразрешённой беседы приходился бы запрос
     * messages.getConversationsById к VK. Чистится при изменении конфига.
     */
    private final ConcurrentHashMap<Long, VkChatMeta> blockedPeers = new ConcurrentHashMap<>();

    /**
     * Применять ли вайтлист/фильтры сканирования к ЖИВЫМ событиям LongPoll.
     * true (по умолчанию) — в БД попадают только разрешённые диалоги;
     * false — пишется всё, фильтры влияют только на выкачку истории.
     *
     * Берётся из ConnectorConfig.listenWhitelist, если вы добавите туда такое поле
     * (публичное, как scanGroups). Пока поля нет — действует значение по умолчанию.
     */
    private volatile boolean listenWhitelist = true;
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
    // 👂 USER LONGPOLL — прослушка входящих в реальном времени
    //
    // По официальной документации VK «User Long Poll API» (актуальная версия — 3):
    //   1) messages.getLongPollServer?lp_version=3 → { server, key, ts }
    //      (server «начинается с https://», но по факту VK отдаёт и хост без схемы —
    //       см. normalizeLongPollServer)
    //   2) {server}?act=a_check&key={key}&ts={ts}&wait=25&mode={mode}&version={version}
    //      → соединение висит до wait секунд, пока не придут события
    //   3) { "ts": 1820350874, "updates": [ [4, 1619489, 561, 123456, 1464958914, "hello",
    //                                        {"title":"…"}, {"attach1_type":"photo", …}] ] }
    //      ts из ответа подставляем в следующий запрос
    //   4) { failed: 1, ts }  — история утеряна, работаем с новым ts
    //      { failed: 2 }      — key инвалидировался → новый messages.getLongPollServer
    //      { failed: 4, min_version, max_version } — неверная версия → берём max_version
    //
    // wait=25 — рекомендация VK: «некоторые прокси-серверы обрывают соединение после 30 секунд»
    //
    // mode — сумма кодов (это ФОРМАТ ответа, не фильтр):
    //    2 = вложения, 8 = расширенный набор событий, 32 = pts (нужен для getLongPollHistory),
    //   64 = extra в событии 8, 128 = поле random_id
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
            log.info("[VK] Прослушка включена: version={}, mode={}, фильтр={}",
                    lpVersion, LONGPOLL_MODE, listenWhitelist
                            ? ("scanPersonal=" + config.scanPersonal + ", scanGroups=" + config.scanGroups
                            + ", whitelist=" + (config.whitelist.isEmpty() ? "пуст (= все беседы)" : config.whitelist))
                            : "ВЫКЛЮЧЕН (пишем все диалоги)");
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
            log.info("[VK] Прослушка выключена (событий: {}, сохранено: {}, отфильтровано вайтлистом: {})",
                    lpEventsTotal, lpSavedTotal, lpFilteredTotal);
        }
        return ConnectorResult.ok("VK прослушка выключена");
    }

    /** Получаем server/key/ts. При ошибке возвращает false — цикл сделает паузу и попробует снова. */
    private boolean refreshLongPollServer() {
        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("need_pts", "1");           // pts нужен для messages.getLongPollHistory
            params.put("lp_version", String.valueOf(lpVersion));

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
            String normalized = normalizeLongPollServer(server);
            if (!normalized.equals(server)) {
                log.info("[VK] LongPoll: server от VK «{}» → нормализован в «{}»", server, normalized);
            }
            lpServer = normalized;
            lpKey = key;
            lpTs = ts;
            lpNeedServer = false;
            log.info("[VK] LongPoll сервер получен: {}, ts={}, lp_version={}", lpServer, ts, lpVersion);
            return true;
        } catch (Exception e) {
            log.error("[VK] Не удалось получить LongPoll сервер: {}", describe(e));
            return false;
        }
    }

    /**
     * VK отдаёт server в РАЗНЫХ видах — все три встречаются в проде:
     *   "https://api.vk.com/gim837611773"   — уже абсолютный URL (новые версии API)
     *   "api.vk.com/gim837611773"           — хост + путь БЕЗ схемы   ← ваш случай
     *   "im.vk.me"                          — голый хост без схемы
     *   "/lp123456"                         — только путь (тогда базовый хост im.vk.me)
     *
     * HttpRequest.newBuilder(URI) требует абсолютный URI со схемой, иначе
     * IllegalArgumentException: URI with undefined scheme.
     *
     * ⚠️ Прежняя версия метода считала «хост без схемы» относительным путём и клеила его
     *    к https://im.vk.me — получалось https://im.vk.me/api.vk.com/gim… и TLS падал с
     *    «PKIX path building failed» (сертификата на такой хост не существует).
     */
    static String normalizeLongPollServer(String server) {
        String s = server == null ? "" : server.trim();
        if (s.isEmpty()) {
            return "";
        }
        // параметры добавляем сами — хвостовой query от VK не нужен
        int q = s.indexOf('?');
        if (q >= 0) {
            s = s.substring(0, q).trim();
        }
        String lower = s.toLowerCase(Locale.ROOT);

        if (lower.startsWith("http://") || lower.startsWith("https://")) {
            // уже абсолютный — ничего не трогаем
        } else if (s.startsWith("/")) {
            // только путь: по документации VK базовый сервер — im.vk.me
            s = "https://im.vk.me" + s;
        } else {
            // "api.vk.com/gim…" или "im.vk.me" — хост (возможно с путём), схемы не хватает
            s = "https://" + s;
        }

        while (s.endsWith("/")) {
            s = s.substring(0, s.length() - 1);
        }
        return s;
    }

    /** Хост из URL — для понятных сообщений об ошибках. */
    private static String safeHost(String url) {
        try {
            String h = URI.create(url).getHost();
            return h != null ? h : String.valueOf(url);
        } catch (Exception e) {
            return String.valueOf(url);
        }
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
                        + "&mode=" + LONGPOLL_MODE
                        + "&version=" + lpVersion;

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
                        case 2, 3 -> {              // ключ/сервер протухли (key живёт ~час и привязан к IP)
                            lpNeedServer = true;
                        }
                        case 4 -> {                 // неверная версия лонгполла — берём разрешённую
                            int maxV = json.path("max_version").asInt(0);
                            int minV = json.path("min_version").asInt(0);
                            lpVersion = maxV > 0 ? maxV : Math.max(minV, 3);
                            log.warn("[VK] LongPoll: версия не поддержана, переключаюсь на {}", lpVersion);
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
                long pts = json.path("pts").asLong(0);
                if (pts > 0) {
                    lpPts = pts;      // для messages.getLongPollHistory, если понадобится догнать пропуски
                }

                JsonNode updates = json.path("updates");
                if (!updates.isArray() || updates.isEmpty()) {
                    continue;                       // тишина в эфире
                }

                for (JsonNode upd : updates) {
                    lpEventsTotal++;
                    lpLastEventAt = System.currentTimeMillis();
                    if (lpRawDumped < 3 && upd.isArray()) {
                        lpRawDumped++;
                        log.info("[VK] LongPoll: пример сырого кортежа #{} (длина {}): {}",
                                lpRawDumped, upd.size(), truncate(upd.toString(), 400));
                    }
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
            } catch (javax.net.ssl.SSLException e) {
                // сертификат/рукопожатие: почти всегда прокси, антивирус с MITM или кривой server
                log.error("[VK] LongPoll TLS-ошибка на хосте {}: {}. "
                                + "Если server нормализован верно — проверьте корпоративный прокси/антивирус "
                                + "(нужен их root-сертификат в cacerts) и -Djavax.net.ssl.trustStore",
                        safeHost(lpServer), describe(e));
                lpNeedServer = true;
                sleep(LONGPOLL_BACKOFF_MS * 2);
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
        log.info("[VK] LongPoll: цикл завершён (событий: {}, сохранено: {}, отфильтровано вайтлистом: {})",
                lpEventsTotal, lpSavedTotal, lpFilteredTotal);
    }

    // =========================================================================================
    // 📨 РАЗБОР СОБЫТИЙ
    //
    //  VK отдаёт updates в ОДНОМ ИЗ ТРЁХ форматов — зависит от версии лонгполла и от того,
    //  какой сервер выдали (в вашем логе был https://api.vk.com/gim<uid>, то есть «gim»).
    //  Поэтому формат определяем по факту, а не угадываем:
    //
    //   A) ОБЪЕКТ {type:"message_new", object:{...}}   — bots-подобный формат
    //   B) КОРТЕЖ [10004, cmid, flags, peerId, ts, text, additional, attachments, randomId, messageId, updTs]
    //                                                   — User LongPoll v21 (актуальный)
    //   C) КОРТЕЖ [4, message_id, flags, peer_id, ts, text, attachments{...}, ...]
    //                                                   — классический старый формат
    //
    //  ⚠️ В B) индекс 1 — это conversationMessageId (нумерация ВНУТРИ беседы), а глобальный
    //     message_id лежит в конце кортежа. Перепутаешь — и антидубликат по уникальному
    //     индексу (source, message_id) перестанет работать.
    // =========================================================================================

    /** Разбор одного события LongPoll (любого из трёх форматов). */
    private void handleUpdate(JsonNode upd) {
        if (upd == null || upd.isNull() || upd.isMissingNode()) {
            return;
        }
        if (upd.isObject()) {
            handleBotsFormat(upd);
        } else if (upd.isArray()) {
            handleTupleFormat(upd);
        } else {
            log.debug("[VK] непонятный формат события: {}", upd);
        }
    }

    // ---------- A) объектовый (bots-подобный) формат ----------

    private void handleBotsFormat(JsonNode upd) {
        String type = upd.path("type").asText("");
        JsonNode obj = upd.path("object");
        switch (type) {
            case "message_new", "message_reply" ->
                    saveFromMessageObject(obj.path("message").isMissingNode() ? obj : obj.path("message"));
            case "message_edit" -> log.debug("[VK] message_edit id={}", obj.path("id").asLong(0));
            case "message_del", "message_restore" ->
                    log.info("[VK] {}: message_id={}", type, obj.path("id").asLong(0));
            case "read_history_incoming", "read_history_outgoing", "message_typing_state",
                 "user_typing", "user_online", "user_offline", "messages.edit", "messages.read" -> {
                // служебные — считаем, но не обрабатываем
            }
            default -> log.debug("[VK] необработанное событие (объект): {}", type);
        }
    }

    /** object из message_new: {id, peer_id, from_id, text, date(сек), attachments[]} */
    private void saveFromMessageObject(JsonNode m) {
        if (m == null || m.isMissingNode()) {
            return;
        }
        long messageId = m.path("id").asLong(0);
        long peerId = m.path("peer_id").asLong(0);
        long fromId = m.path("from_id").asLong(0);
        if (peerId == 0) {
            log.warn("[VK] message_new без peer_id: {}", m);
            return;
        }
        long dateSec = m.path("date").asLong(0);
        saveLiveMessage(peerId, messageId, messageId,
                fromId == 0 ? peerId : fromId,
                m.path("text").asText(""),
                dateSec > 0 ? dateSec * 1000L : System.currentTimeMillis(),
                extractMediaUrl(m.path("attachments")),
                "объект");
    }

    // ---------- B/C) кортежный формат ----------

    private void handleTupleFormat(JsonNode arr) {
        if (arr.size() < 2) {
            return;
        }
        int code = arr.path(0).asInt(0);
        switch (code) {
            case 4, 10004 -> parseNewMessageTuple(arr, code);
            case 5, 10005, 10018 -> log.debug("[VK] событие {} (правка/обновление сообщения)", code);
            case 10002, 10003, 10019 -> log.debug("[VK] служебное событие {} (флаги сообщения)", code);
            case 10006, 10007 -> log.debug("[VK] прочтение: peer={} cmid={} count={}",
                    arr.path(1).asLong(0), arr.path(2).asLong(0), arr.path(3).asLong(0));
            case 51, 52 -> {
                // изменились данные беседы (название/аватарка) — сбрасываем кэш меты,
                // чтобы следующее событие подтянуло свежий title
                long peerId = code == 51 ? arr.path(1).asLong(0) : arr.path(2).asLong(0);
                if (peerId != 0) {
                    chatCache.remove(peerId);
                    blockedPeers.remove(peerId);
                }
                log.info("[VK] событие {}: обновлены данные беседы peer={}", code, peerId);
            }
            case 61, 62, 63, 64, 65, 66, 67, 68 -> {
                // «печатает…», запись голосового, загрузка медиа — можно вывести в UI позже
            }
            case 80 -> log.debug("[VK] непрочитанных диалогов: {}", arr.path(1).asInt(0));
            case 8, 9, 10, 12, 20, 21, 50, 81, 90, 91, 114, 115, 119,
                 501, 502, 503, 504, 505, 506, 507, 601, 602, 10013 -> {
                // известные служебные события — игнорируем
            }
            default -> log.debug("[VK] необработанное событие (кортеж), code={}", code);
        }
    }

    // =========================================================================================
    //  ⚠️ ДВЕ РАСКЛАДКИ КОРТЕЖА — и код события НЕ является надёжным признаком
    //
    //  Документация v21 описывает:
    //    [10004, cmid, flags, peerId, timestamp, text, additional{from,…}, attachments{…},
    //            randomId, messageId, updateTimestamp]
    //
    //  Фактически VK присылает и такой вариант (подтверждено на живом аккаунте):
    //    [10004, message_id, flags, peer_id, timestamp, text, attachments{…}]
    //  то есть НОВЫЙ код события со СТАРОЙ раскладкой, где индекс 1 — это уже глобальный
    //  message_id, а не conversationMessageId.
    //
    //  Если их перепутать, в БД уезжает message_id=25 вместо 1619489, а в text — число
    //  1789464697 (это timestamp). Дедупликация по (source, message_id) после этого ломается:
    //  разные сообщения с маленькими id начинают collide между беседами.
    //
    //  Поэтому раскладку ОПРЕДЕЛЯЕМ ПО СОДЕРЖИМОМУ, а не по коду.
    // =========================================================================================

    /** Единая точка разбора «нового сообщения» для кодов 4 и 10004. */
    private void parseNewMessageTuple(JsonNode arr, int code) {
        if (arr.size() < 6) {
            log.warn("[VK] событие {} слишком короткое ({} элементов): {}", code, arr.size(), arr);
            return;
        }

        int flags = arr.path(2).asInt(0);
        long peerId = arr.path(3).asLong(0);
        long tsSec = arr.path(4).asLong(0);
        if (peerId == 0) {
            log.warn("[VK] событие {} без peer_id: {}", code, arr);
            return;
        }

        long messageId;
        long cmid;
        String text;
        JsonNode atts;
        long fromId;

        if (isV21Layout(arr)) {
            // [10004, cmid, flags, peerId, ts, text, additional, attachments, randomId, messageId, updTs?]
            //     0     1     2      3      4    5        6            7            8         9        10
            cmid = arr.path(1).asLong(0);
            text = decodeVkText(arr.path(5).asText(""));
            JsonNode additional = arr.path(6);
            atts = arr.path(7);
            messageId = arr.path(10).asLong(0);         // если хвост длиннее (с updateTimestamp)
            if (messageId <= 0) {
                messageId = arr.path(9).asLong(0);      // штатная позиция messageId
            }
            if (messageId <= 0) {
                messageId = arr.path(8).asLong(0);      // вариант без randomId
            }
            if (messageId == 0) {
                messageId = resolveMessageId(peerId, cmid);
            }
            fromId = additional.path("from").asLong(0);
            if (fromId == 0) {
                fromId = classicFromId(peerId, flags, additional, atts);
            }
        } else {
            // КЛАССИЧЕСКАЯ раскладка из официальной документации (version=3):
            // [4, message_id, flags, peer_id, timestamp, text, additional{title,from}, attachments{attach1_*}, random_id?]
            messageId = arr.path(1).asLong(0);
            cmid = messageId;                          // в классике отдельного cmid нет
            text = decodeVkText(arr.path(5).asText(""));
            atts = arr.path(7);                        // вложения — индекс 7, не 6!
            JsonNode additional = arr.path(6);         // {title, from, …}
            fromId = classicFromId(peerId, flags, additional, atts);
        }

        if (messageId <= 0) {
            log.warn("[VK] событие {} peer={}: не удалось определить message_id — сообщение НЕ сохранено. "
                    + "Сырой кортеж: {}", code, peerId, arr);
            return;
        }

        // страховка от перепутанных индексов: текст не должен выглядеть как unix-timestamp
        if (text.matches("1\\d{9}")) {
            log.error("[VK] ПОДОЗРЕНИЕ НА СМЕЩЕНИЕ ИНДЕКСОВ: text=\"{}\" похож на unix-timestamp, peer={}. "
                            + "Сырой кортеж: {} — если это воспроизводится, пришлите этот лог, поправим раскладку.",
                    text, peerId, arr);
        }
        if (tsSec <= 0 || tsSec < 1_000_000_000L || tsSec > 4_000_000_000L) {
            log.warn("[VK] событие {} peer={}: странный timestamp={} (ожиданы секунды ~1.7e9), берём текущее время",
                    code, peerId, tsSec);
            tsSec = System.currentTimeMillis() / 1000L;
        }

        String media = isV21Layout(arr) ? extractMediaUrlFromV21(atts) : extractMediaUrlFromClassic(atts);
        boolean v21 = isV21Layout(arr);
        saveLiveMessage(peerId, messageId, cmid, fromId, text, tsSec * 1000L, media,
                "кортеж-" + code + (v21 ? "/v21" : "/classic,len=" + arr.size()));
    }

    /**
     * Похож ли кортеж на раскладку «cmid в начале, messageId в хвосте» (неофициальные версии
     * лонгполла с кодами событий 10002…10019).
     *
     * ⚠️ ПОРОГ — РОВНО 11 ЭЛЕМЕНТОВ, и вот почему.
     * Классический кортеж из официальной документации (version=3) выглядит так:
     *   [4, message_id, flags, peer_id, timestamp, text, additional, attachments, random_id]
     *    0      1          2        3         4         5       6          7            8
     * — это 8 элементов, а с random_id (бит 128 в mode) — 9, и ПОСЛЕДНИЙ ЭЛЕМЕНТ ЧИСЛО.
     * Прежняя версия метода считала v21 всё, что имеет size >= 9 и число в хвосте,
     * то есть ложно срабатывала на обычном классическом кортеже: message_id читался
     * с позиции 10 (пусто), text — с позиции 5 (а там timestamp), и в лог уходило
     * «id=11 cmid=11 peer=26 «1789466530»».
     *
     * Раскладка с messageId в хвосте (неофициальные версии лонгполла) выглядит так:
     *   [10004, cmid, flags, peerId, ts, text, additional, attachments, randomId, messageId, updateTs?]
     *      0      1     2      3      4    5       6            7           8         9        10
     * → messageId на позиции 9, длина 10–11.
     *
     * Классический кортеж из документации (version=3) имеет длину 8–9, и его ПОСЛЕДНИЙ элемент —
     * число (random_id). Поэтому порога «size >= 9» недостаточно: нужно требовать
     * size >= 10 И валидный messageId на позиции 9 ИЛИ 10.
     */
    private boolean isV21Layout(JsonNode arr) {
        if (arr.size() < 10) {
            return false;                      // классика (8–9 элементов) — точно не она
        }
        JsonNode mid = arr.path(6);
        boolean additionalLooksRight = mid.isObject() && !mid.has("attach1_type");
        long tailMessageId = Math.max(arr.path(9).asLong(0), arr.path(10).asLong(0));
        return additionalLooksRight && tailMessageId > 0;
    }

    /**
     * Автор сообщения в классической раскладке.
     *
     * По документации VK поле `from` (id автора) приходит в объекте additional — «Вложения
     * и дополнительные данные (title, from) приходят в отдельных объектах». На всякий случай
     * смотрим и additional, и attachments: в старых версиях `from` мог лежать во вложениях.
     *
     * Личный диалог (0 < peer_id < 2e9): собеседник = peer_id.
     * Исходящее (флаг OUTBOX = бит 2): автор — мы.
     */
    private long classicFromId(long peerId, int flags, JsonNode additional, JsonNode atts) {
        boolean outbox = (flags & 2) != 0;
        for (JsonNode src : new JsonNode[]{additional, atts}) {
            if (src == null || src.isMissingNode()) {
                continue;
            }
            String from = src.path("from").asText("");
            if (!from.isBlank()) {
                try {
                    long id = Long.parseLong(from.trim());
                    if (id != 0) {
                        return id;
                    }
                } catch (NumberFormatException ignored) {
                    // fallthrough
                }
            }
        }
        boolean isDialog = peerId > 0 && peerId < 2_000_000_000L;
        if (isDialog && !outbox) {
            return peerId;
        }
        return (outbox && myUserId != null) ? myUserId : peerId;
    }

    /** conversationMessageId → глобальный message_id (нужен для unique-индекса в БД). */
    private long resolveMessageId(long peerId, long cmid) {
        if (cmid <= 0) {
            return 0;
        }
        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("peer_id", String.valueOf(peerId));
            params.put("conversation_message_ids", String.valueOf(cmid));
            JsonNode resp = vkApi("messages.getByConversationMessageId", params);
            if (resp.has("error")) {
                log.debug("[VK] getByConversationMessageId peer={} cmid={}: {}", peerId, cmid, vkError(resp));
                return 0;
            }
            return resp.path("response").path("items").path(0).path("id").asLong(0);
        } catch (Exception e) {
            log.debug("[VK] getByConversationMessageId не удался: {}", describe(e));
            return 0;
        }
    }

    /** VK в лонгполле отдаёт текст с <br> и экранированием & " < > — раскрываем обратно. */
    private static String decodeVkText(String text) {
        if (text == null || text.isEmpty()) {
            return "";
        }
        return text.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
                .replace("&lt;", "<").replace("&gt;", ">")
                .replace("&quot;", "\"").replace("&amp;", "&");
    }

    /**
     * Первая подходящая ссылка из массива вложений VK API → Message.mediaUrl.
     * Для фото берём максимальный размер из sizes[].
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
            if ("photo".equals(t)) {
                String best = null;
                long bestW = -1;
                for (JsonNode sz : payload.path("sizes")) {
                    long w = sz.path("width").asLong(0);
                    if (w >= bestW) {
                        bestW = w;
                        best = sz.path("url").asText(null);
                    }
                }
                if (best != null && !best.isBlank()) {
                    return best;
                }
            }
            for (String f : new String[]{"url", "preview_url", "uri"}) {
                String u = payload.path(f).asText("");
                if (!u.isBlank()) {
                    return u;
                }
            }
            String mu = a.path("media_url").asText("");
            if (!mu.isBlank()) {
                return mu;
            }
        }
        return null;
    }

    /** Вложения v21: объект { attachments_count, attachments:"JSON-строка", attach1_type, … } */
    private String extractMediaUrlFromV21(JsonNode atts) {
        if (atts == null || atts.isMissingNode() || atts.isNull()) {
            return null;
        }
        String raw = atts.path("attachments").asText("");
        if (!raw.isBlank()) {
            try {
                String url = extractMediaUrl(mapper.readTree(raw));
                if (url != null) {
                    return url;
                }
            } catch (Exception e) {
                log.debug("[VK] не распарсен attachments-JSON: {}", describe(e));
            }
        }
        return extractFromFlatAttachFields(atts);
    }

    /** Вложения классического формата: { attach1_type:"photo", attach1_photo:"123_456", attach1_url?:… } */
    private String extractMediaUrlFromClassic(JsonNode atts) {
        if (atts == null || !atts.isObject()) {
            return null;
        }
        return extractFromFlatAttachFields(atts);
    }

    /** Плоские поля attach{N}_url — единственное, что даёт реальную ссылку для Message.mediaUrl. */
    private String extractFromFlatAttachFields(JsonNode atts) {
        for (int i = 1; i <= 10; i++) {
            String type = atts.path("attach" + i + "_type").asText("");
            if (type.isBlank()) {
                continue;
            }
            String url = atts.path("attach" + i + "_url").asText("");
            if (!url.isBlank()) {
                return url;
            }
        }
        return null;
    }

    // =========================================================================================
    // 🚦 ВАЙТЛИСТ ДЛЯ ПРОСЛУШКИ
    //
    //  Серверной фильтрации по peer_id у VK НЕТ:
    //   - messages.getLongPollServer принимает только use_ssl, need_pts, lp_version, group_id;
    //   - filter_id (битовая маска) есть ТОЛЬКО в Bots LongPoll для ключа сообщества
    //     и фильтрует ТИПЫ событий (message_new, photo_new…), а не диалоги;
    //   - mode у пользовательского лонгполла — маска ФОРМАТА ответа, а не отбора.
    //  Поэтому вайтлист применяется на клиенте — здесь.
    // =========================================================================================

    /**
     * Пускать ли peer_id в прослушку. Правила те же, что в isChatAllowed():
     *   личный диалог     → config.scanPersonal
     *   беседа/сообщество → config.scanGroups + (whitelist пуст ИЛИ содержит peer_id/chat_id)
     *
     * Отрицательный результат кешируется в blockedPeers: иначе на каждое сообщение из
     * неразрешённой беседы приходился бы запрос messages.getConversationsById к VK.
     */
    private boolean isPeerAllowed(long peerId) {
        if (!listenWhitelist) {
            return true;                       // фильтр выключен — пишем всё
        }
        if (blockedPeers.containsKey(peerId)) {
            return false;                      // уже решали, что нельзя — VK не спрашиваем
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
        lpFilteredTotal++;
        log.debug("[VK] peer={} «{}» вне вайтлиста прослушки — пропуск", peerId, meta.title());
        return false;
    }

    // =========================================================================================
    // 💾 ЕДИНАЯ ТОЧКА СОХРАНЕНИЯ ЖИВОГО СООБЩЕНИЯ
    // =========================================================================================

    /**
     * @param messageId глобальный message_id — по нему работает антидубликат (source, message_id)
     * @param cmid      conversationMessageId, только для логов
     * @param fmt       откуда пришло событие (для логов)
     */
    private void saveLiveMessage(long peerId, long messageId, long cmid, long fromId,
                                 String text, long tsMillis, String mediaUrl, String fmt) {
        if (messageId <= 0) {
            log.warn("[VK] {} peer={}: message_id={} — не сохраняем (иначе сломается антидубликат)",
                    fmt, peerId, messageId);
            return;
        }
        if (!isPeerAllowed(peerId)) {
            return;                            // вайтлист прослушки
        }

        VkChatMeta meta = ensureChat(peerId, false);
        if (meta == null) {
            log.warn("[VK] не удалось определить чат peer={} — сообщение id={} не сохранено", peerId, messageId);
            return;
        }

        boolean mine = myUserId != null && fromId == myUserId;
        if (!mine && fromId > 0) {
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

        try {
            Message msg = new Message("vk", messageId, peerId, fromId, text == null ? "" : text, mediaUrl);
            msg.setTimestamp(tsMillis > 0 ? tsMillis : System.currentTimeMillis());
            messageService.saveMessage(msg);   // дубликат отсеется по (source, messageId)
            lpSavedTotal++;
            log.info("[VK] {} {} id={} cmid={} peer={} «{}»{}",
                    mine ? "← исходящее" : "→ входящее",
                    meta.isGroup() ? "[беседа]" : "[лс]",
                    messageId, cmid, peerId, truncate(text, 60), mediaUrl != null ? " +media" : "");
        } catch (Exception e) {
            log.error("[VK] не удалось сохранить сообщение id={}: {}", messageId, describe(e));
        }
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
     *                         false — для LongPoll и для отправки: живой диалог сохраняется всегда
     *                         (иначе человек вам написал, а чата в UI нет). Фильтрацию прослушки
     *                         делает отдельный isPeerAllowed().
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

        // Вайтлист/фильтры изменились — прежние решения о блокировке peer_id больше недействительны.
        // Без очистки диалог, который только что добавили в вайтлист, молчал бы до перезапуска.
        int dropped = blockedPeers.size();
        blockedPeers.clear();
        lpFilteredTotal = 0;

        // Если добавите в ConnectorConfig публичное поле `listenWhitelist` (как scanGroups) —
        // подхватится автоматически. Пока поля нет, работает значение по умолчанию (true).
        try {
            java.lang.reflect.Field f = newConfig.getClass().getField("listenWhitelist");
            Object v = f.get(newConfig);
            if (v instanceof Boolean b) {
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
        JsonNode lp = callQuietly("messages.getLongPollServer",
                Map.of("need_pts", "1", "lp_version", String.valueOf(lpVersion)));
        if (lp.has("error")) {
            report.put("longpoll", "❌ " + vkError(lp));
            report.put("longpoll.hint", hintFor(lp.path("error").path("error_code").asInt()));
        } else {
            report.put("longpoll", "✅ сервер получен, ts=" + lp.path("response").path("ts").asLong(0));
        }
        Map<String, Object> lpState = new LinkedHashMap<>();
        lpState.put("isListening", isListening);
        lpState.put("lpVersion", lpVersion);
        lpState.put("mode", LONGPOLL_MODE);
        lpState.put("server", String.valueOf(lpServer));
        lpState.put("eventsTotal", lpEventsTotal);
        lpState.put("savedTotal", lpSavedTotal);
        lpState.put("filteredByWhitelist", lpFilteredTotal);
        lpState.put("blockedPeersCached", blockedPeers.size());
        lpState.put("lastEventAt", lpLastEventAt == 0 ? "ещё не было" : new Date(lpLastEventAt).toString());
        lpState.put("myUserId", String.valueOf(myUserId));
        lpState.put("filter", listenWhitelist
                ? "scanPersonal=" + config.scanPersonal + ", scanGroups=" + config.scanGroups
                + ", whitelist=" + config.whitelist
                : "ВЫКЛЮЧЕН — сохраняются все диалоги");
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
