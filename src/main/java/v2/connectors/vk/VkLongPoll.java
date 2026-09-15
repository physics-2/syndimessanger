package v2.connectors.vk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static v2.connectors.vk.VkApiClient.*;

/**
 * 👂 USER LONGPOLL — прослушка входящих сообщений VK в реальном времени.
 *
 * По официальной документации VK «User Long Poll API» (актуальная версия — 21, обратная совместимость с 3):
 * <pre>
 *   1) messages.getLongPollServer?lp_version=3&need_pts=1  →  { server, key, ts }
 *   2) {server}?act=a_check&key={key}&ts={ts}&wait=25&mode={mode}&version={version}
 *   3) Ответ ВСЕГДА содержит массив кортежей (arrays):
 *      { "ts": ..., "updates": [ [10004, cmid, flags, ...], [4, mid, ...] ] }
 *      ⚠️ Объектный формат {"type": "message_new", "object": {...}} используется ТОЛЬКО
 *         в Bots Long Poll (для сообществ) и в User Long Poll не встречается.
 *   4) { failed: 1, ts }  — история утеряна, работаем с новым ts
 *      { failed: 2 }      — key инвалидировался → новый messages.getLongPollServer
 *      { failed: 4, min_version, max_version } — неверная версия → берём max_version
 * </pre>
 *
 * mode — сумма кодов (рекомендуемая):
 *   2 (вложения) | 8 (расширенные события 114/115/119) | 32 (pts) | 128 (random_id) = 170.
 */
public class VkLongPoll {

    private static final Logger log = LoggerFactory.getLogger(VkLongPoll.class);

    private static final int WAIT_SECONDS = 25;
    private static final Duration LP_TIMEOUT = Duration.ofSeconds(WAIT_SECONDS + 15);
    private static final long BACKOFF_MS = 3000;

    private volatile int version = 3;

    /** 2 | 8 | 32 | 128 = 170 */
    private static final int MODE = 2 | 8 | 32 | 128;

    public interface Sink {
        boolean onMessage(long peerId, long messageId, long cmid, long fromId,
                          String text, long tsMillis, String mediaUrl, String fmt);
        void onChatMetaChanged(long peerId);
        Long myUserId();
    }

    // ==================== СОСТОЯНИЕ СЕССИИ ====================
    private final VkApiClient api;
    private final Sink sink;

    private volatile String server = null;
    private volatile String key = null;
    private volatile long ts = 0;
    private volatile long pts = 0;
    private volatile boolean needServer = true;

    private volatile boolean running = false;
    private final AtomicBoolean loopActive = new AtomicBoolean(false);
    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "vk-longpoll");
        t.setDaemon(true);
        return t;
    });

    // ==================== СТАТИСТИКА ====================
    private volatile long eventsTotal = 0;
    private volatile long savedTotal = 0;
    private volatile long lastEventAt = 0;
    private volatile int rawDumped = 0;

    private final long groupId; // 0 = личные сообщения, >0 = ID сообщества

    public VkLongPoll(VkApiClient api, Sink sink, long groupId) {
        this.api = api;
        this.sink = sink;
        this.groupId = groupId;
    }

    // =========================================================================================
    // 🎛 УПРАВЛЕНИЕ
    // =========================================================================================

    public String start() {
        if (running) return "VK прослушка уже включена";
        running = true;
        needServer = true;
        if (loopActive.compareAndSet(false, true)) {
            executor.submit(() -> {
                try {
                    loop();
                } finally {
                    loopActive.set(false);
                }
            });
            log.info("[VK] Прослушка включена: version={}, mode={}", version, MODE);
            return "VK прослушка включена (LongPoll запущен, version=" + version + ", mode=" + MODE + ")";
        }
        return "VK прослушка включена";
    }

    public String stop() {
        boolean was = running;
        running = false;
        if (was) {
            log.info("[VK] Прослушка выключена (событий: {}, сохранено: {})", eventsTotal, savedTotal);
        }
        return "VK прослушка выключена";
    }

    public void shutdown() {
        running = false;
        executor.shutdownNow();
        try {
            executor.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public boolean isRunning() {
        return running;
    }

    public void resetStats() {}

    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("isListening", running);
        m.put("lpVersion", version);
        m.put("mode", MODE);
        m.put("wait", WAIT_SECONDS);
        m.put("server", String.valueOf(server));
        m.put("ts", ts);
        m.put("pts", pts);
        m.put("eventsTotal", eventsTotal);
        m.put("savedTotal", savedTotal);
        m.put("lastEventAt", lastEventAt == 0 ? "ещё не было" : new Date(lastEventAt).toString());
        m.put("myUserId", String.valueOf(sink.myUserId()));
        return m;
    }

    // =========================================================================================
    // 🔁 ЦИКЛ
    // =========================================================================================

    private void loop() {
        log.info("[VK] LongPoll: цикл запущен");
        while (running) {
            try {
                if ((needServer || !serverValid()) && !refreshServer()) {
                    needServer = true;
                    sleep(BACKOFF_MS);
                    continue;
                }

                String url = server
                        + "?act=a_check"
                        + "&key=" + enc(key)
                        + "&ts=" + ts
                        + "&wait=" + WAIT_SECONDS
                        + "&v=" + VERSION
                        + "&mode=" + MODE
                        + "&version=" + version;

                var resp = api.longPollGet(url, LP_TIMEOUT);
                if (!running) break;

                if (resp.statusCode() != 200) {
                    log.warn("[VK] LongPoll HTTP {} — перевыпускаю сервер", resp.statusCode());
                    needServer = true;
                    sleep(BACKOFF_MS);
                    continue;
                }

                String body = resp.body();
                if (body == null || body.isBlank()) continue;

                JsonNode json = api.mapper().readTree(body);

                if (json.has("failed")) {
                    handleFailed(json);
                    continue;
                }

                long newTs = json.path("ts").asLong(0);
                if (newTs > 0) ts = newTs;

                long newPts = json.path("pts").asLong(0);
                if (newPts > 0) pts = newPts;

                JsonNode updates = json.path("updates");
                if (!updates.isArray() || updates.isEmpty()) continue;

                for (JsonNode upd : updates) {
                    eventsTotal++;
                    lastEventAt = System.currentTimeMillis();
                    if (rawDumped < 3 && upd.isArray()) {
                        rawDumped++;
                        log.info("[VK] LongPoll: пример сырого кортежа #{} (длина {}): {}",
                                rawDumped, upd.size(), truncate(upd.toString(), 400));
                    }
                    try {
                        handleUpdate(upd);
                    } catch (Exception e) {
                        log.error("[VK] Ошибка обработки события {}: {}", describeEvent(upd), describe(e), e);
                    }
                }
            } catch (java.net.http.HttpTimeoutException e) {
                // нормально: wait истёк, событий не было
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (javax.net.ssl.SSLException e) {
                log.error("[VK] LongPoll TLS-ошибка на хосте {}: {}. Проверьте корпоративный прокси/антивирус.", safeHost(server), describe(e));
                needServer = true;
                sleep(BACKOFF_MS * 2);
            } catch (java.io.IOException e) {
                log.warn("[VK] LongPoll: сетевая ошибка ({}), пауза {} мс", describe(e), BACKOFF_MS);
                sleep(BACKOFF_MS);
            } catch (IllegalArgumentException e) {
                log.error("[VK] LongPoll: некорректный server («{}») — перевыпускаю: {}", server, describe(e));
                needServer = true;
                sleep(BACKOFF_MS);
            } catch (Exception e) {
                log.error("[VK] LongPoll: неожиданная ошибка — {}", describe(e), e);
                needServer = true;
                sleep(BACKOFF_MS);
            }
        }
        log.info("[VK] LongPoll: цикл завершён (событий: {}, сохранено: {})", eventsTotal, savedTotal);
    }

    private void handleFailed(JsonNode json) {
        int code = json.path("failed").asInt(0);
        long newTs = json.path("ts").asLong(0);
        log.warn("[VK] LongPoll failed={}, ts={}", code, newTs);
        switch (code) {
            case 1 -> { if (newTs > 0) ts = newTs; }
            case 2, 3 -> needServer = true;
            case 4 -> {
                int maxV = json.path("max_version").asInt(0);
                int minV = json.path("min_version").asInt(0);
                version = maxV > 0 ? maxV : Math.max(minV, 3);
                log.warn("[VK] LongPoll: версия не поддержана, переключаюсь на {}", version);
                needServer = true;
            }
            default -> needServer = true;
        }
    }

    private boolean refreshServer() {
        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("need_pts", "1");
            params.put("lp_version", String.valueOf(version));
            if (groupId > 0) {
                params.put("group_id", String.valueOf(groupId));
            }

            JsonNode resp = api.call("messages.getLongPollServer", params);
            if (hasError(resp)) {
                log.error("[VK {}] messages.getLongPollServer: {}", logPrefix(), vkError(resp));
                return false;
            }
            JsonNode r = resp.path("response");
            String rawServer = r.path("server").asText("");
            String newKey = r.path("key").asText("");
            long newTs = r.path("ts").asLong(0);

            if (rawServer.isBlank() || newKey.isBlank() || newTs == 0) {
                log.error("[VK {}] messages.getLongPollServer вернул неполные данные: {}", logPrefix(), r);
                return false;
            }

            // 🔑 ИСПРАВЛЕНИЕ: VK присылает server БЕЗ схемы (api.vk.com/gim837611773)
            // Добавляем https:// если нет
            String normalized = normalizeLongPollServer(rawServer);
            if (normalized.isBlank() || !normalized.contains("://")) {
                normalized = "https://" + rawServer;
            }

            if (!normalized.equals(rawServer)) {
                log.info("[VK {}] LongPoll: server от VK «{}» → нормализован в «{}»", logPrefix(), rawServer, normalized);
            }

            server = normalized;
            key = newKey;
            ts = newTs;
            needServer = false;
            log.info("[VK {}] LongPoll сервер получен: {}, ts={}, lp_version={}", logPrefix(), server, ts, version);
            return true;
        } catch (Exception e) {
            log.error("[VK {}] Не удалось получить LongPoll сервер: {}", logPrefix(), describe(e));
            return false;
        }
    }
    private boolean serverValid() {
        if (server == null || server.isBlank() || key == null || key.isBlank()) {
            return false;
        }
        try {
            // Если server не содержит схемы, добавляем её временно для проверки
            String checkServer = server.contains("://") ? server : "https://" + server;
            var uri = java.net.URI.create(checkServer);
            return uri.isAbsolute() && uri.getScheme() != null && uri.getHost() != null;
        } catch (Exception e) {
            return false;
        }
    }

    private static String describeEvent(JsonNode upd) {
        if (upd == null) return "?";
        return upd.isObject() ? upd.path("type").asText("?") : "code=" + upd.path(0).asInt(-1);
    }

    private String logPrefix() {
        return groupId > 0 ? "G" + groupId : "U";
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    // =========================================================================================
    // 📨 РАЗБОР СОБЫТИЙ (ТОЛЬКО КОРТЕЖИ)
    // =========================================================================================

    private void handleUpdate(JsonNode upd) {
        if (upd == null || upd.isNull() || upd.isMissingNode() || !upd.isArray()) {
            // User Long Poll API возвращает ТОЛЬКО кортежи (массивы).
            // Объектный формат относится к Bots Long Poll и здесь игнорируется.
            if (upd != null && upd.isObject()) {
                log.debug("[VK] проигнорирован объектный формат (артефакт или Bots Long Poll): {}", upd.path("type").asText(""));
            }
            return;
        }
        handleTupleFormat(upd);
    }

    private void handleTupleFormat(JsonNode arr) {
        if (arr.size() < 2) return;
        int code = arr.path(0).asInt(0);

        switch (code) {
            case 4, 10004, 10005, 10018 -> parseNewMessageTuple(arr, code);
            case 10002, 10003 -> log.debug("[VK] служебное событие {} (флаги сообщения)", code);
            case 10006, 10007 -> log.debug("[VK] прочтение: peer={} cmid={} count={}",
                    arr.path(1).asLong(0), arr.path(2).asLong(0), arr.path(3).asLong(0));
            case 51, 52 -> {
                long peerId = code == 51 ? arr.path(1).asLong(0) : arr.path(2).asLong(0);
                if (peerId != 0) sink.onChatMetaChanged(peerId);
                log.info("[VK] событие {}: обновлены данные беседы peer={}", code, peerId);
            }
            case 61, 62, 63, 64, 65, 66, 67, 68 -> {
                // «печатает…», запись голосового, загрузка медиа
            }
            case 80 -> log.debug("[VK] непрочитанных диалогов: {}", arr.path(1).asInt(0));
            case 8, 9, 10, 12, 20, 21, 50, 81, 90, 91, 114, 115, 119,
                 501, 502, 503, 504, 505, 506, 507, 601, 602, 10013, 10019 -> {
                // известные служебные события — игнорируем без спама в лог
            }
            default -> log.debug("[VK] необработанное событие (кортеж), code={}, size={}", code, arr.size());
        }
    }

    /**
     * Единая точка разбора «нового сообщения» для кодов 4, 10004, 10005, 10018.
     * Структура строго определена документацией VK, эвристики больше не нужны.
     */
    private void parseNewMessageTuple(JsonNode arr, int code) {
        boolean isModern = (code == 10004 || code == 10005 || code == 10018);

        if (isModern) {
            // Современный формат (v21):
            // 10004: [type, cmid, flags, minorId, peerId, ts, text, additional, attachments, randomId, messageId, updateTs] (12 эл.)
            // 10005/10018: [type, cmid, flags, peerId, ts, text, additional, attachments, randomId, messageId, updateTs] (11 эл.)
            int offset = (code == 10004 && arr.size() >= 12) ? 1 : 0;

            long cmid = arr.path(1).asLong(0);
            int flags = arr.path(2).asInt(0);
            long peerId = arr.path(3 + offset).asLong(0);
            long tsSec = arr.path(4 + offset).asLong(0);
            String text = decodeVkText(arr.path(5 + offset).asText(""));
            JsonNode additional = arr.path(6 + offset);
            JsonNode atts = arr.path(7 + offset);

            long messageId = arr.path(9 + offset).asLong(0);
            if (messageId <= 0) {
                messageId = resolveMessageId(peerId, cmid);
            }

            if (peerId == 0) {
                log.warn("[VK] событие {} без peer_id: {}", code, arr);
                return;
            }
            if (messageId <= 0) {
                log.warn("[VK] событие {} peer={}: не удалось определить message_id. Сырой кортеж: {}", code, peerId, arr);
                return;
            }

            long fromId = additional.path("from").asLong(0);
            if (fromId == 0) {
                fromId = resolveAuthor(peerId, flags, additional, atts);
            }

            // Страховка от смещения индексов
            if (text.matches("1\\d{9}")) {
                log.error("[VK] ПОДОЗРЕНИЕ НА СМЕЩЕНИЕ ИНДЕКСОВ: text=\"{}\" похож на unix-timestamp, peer={}. Сырой кортеж: {}", text, peerId, arr);
            }
            if (tsSec < 1_000_000_000L || tsSec > 4_000_000_000L) {
                log.warn("[VK] событие {} peer={}: странный timestamp={}, берём текущее время", code, peerId, tsSec);
                tsSec = System.currentTimeMillis() / 1000L;
            }

            String media = extractMediaUrlFromTail(atts);
            deliver(peerId, messageId, cmid, fromId, text, tsSec * 1000L, media, "кортеж-" + code + "/modern");

        } else {
            // Классический формат (код 4)
            if (arr.size() < 6) {
                log.warn("[VK] событие 4 слишком короткое ({} элементов): {}", arr.size(), arr);
                return;
            }
            long messageId = arr.path(1).asLong(0);
            long cmid = messageId;
            int flags = arr.path(2).asInt(0);
            long peerId = arr.path(3).asLong(0);
            long tsSec = arr.path(4).asLong(0);
            String text = decodeVkText(arr.path(5).asText(""));
            JsonNode additional = arr.path(6);
            JsonNode atts = arr.path(7);

            if (peerId == 0) {
                log.warn("[VK] событие 4 без peer_id: {}", arr);
                return;
            }
            if (messageId <= 0) {
                log.warn("[VK] событие 4 peer={}: не удалось определить message_id. Сырой кортеж: {}", peerId, arr);
                return;
            }

            long fromId = resolveAuthor(peerId, flags, additional, atts);

            if (text.matches("1\\d{9}")) {
                log.error("[VK] ПОДОЗРЕНИЕ НА СМЕЩЕНИЕ ИНДЕКСОВ: text=\"{}\" похож на unix-timestamp, peer={}. Сырой кортеж: {}", text, peerId, arr);
            }
            if (tsSec < 1_000_000_000L || tsSec > 4_000_000_000L) {
                log.warn("[VK] событие 4 peer={}: странный timestamp={}, берём текущее время", peerId, tsSec);
                tsSec = System.currentTimeMillis() / 1000L;
            }

            String media = extractMediaUrlFromClassic(atts);
            deliver(peerId, messageId, cmid, fromId, text, tsSec * 1000L, media, "кортеж-4/classic");
        }
    }

    private long resolveAuthor(long peerId, int flags, JsonNode additional, JsonNode atts) {
        boolean outbox = (flags & 2) != 0;

        // 1. from_admin — ID админа, отправившего от имени сообщества
        if (additional != null && !additional.isMissingNode()) {
            long fromAdmin = additional.path("from_admin").asLong(0);
            if (fromAdmin != 0) return fromAdmin;
        }

        // 2. from — ID пользователя в беседе
        for (JsonNode src : new JsonNode[]{additional, atts}) {
            if (src == null || src.isMissingNode()) continue;
            String from = src.path("from").asText("");
            if (!from.isBlank()) {
                try {
                    long id = Long.parseLong(from.trim());
                    if (id != 0) return id;
                } catch (NumberFormatException ignored) {}
            }
        }

        // 3. Личные сообщения: собеседник = peer_id (если входящее)
        Long me = sink.myUserId();
        boolean isDialog = peerId > 0 && peerId < 2_000_000_000L;
        if (isDialog && !outbox) return peerId;

        // 4. Исходящие — мы
        if (outbox && me != null) return me;

        return peerId;
    }

    private long resolveMessageId(long peerId, long cmid) {
        if (cmid <= 0) return 0;
        try {
            JsonNode resp = api.call("messages.getByConversationMessageId",
                    params("peer_id", String.valueOf(peerId), "conversation_message_ids", String.valueOf(cmid)));
            if (hasError(resp)) {
                log.debug("[VK] getByConversationMessageId peer={} cmid={}: {}", peerId, cmid, vkError(resp));
                return 0;
            }
            return resp.path("response").path("items").path(0).path("id").asLong(0);
        } catch (Exception e) {
            log.debug("[VK] getByConversationMessageId не удался: {}", describe(e));
            return 0;
        }
    }

    private void deliver(long peerId, long messageId, long cmid, long fromId,
                         String text, long tsMillis, String mediaUrl, String fmt) {
        if (sink.onMessage(peerId, messageId, cmid, fromId, text, tsMillis, mediaUrl, fmt)) {
            savedTotal++;
        }
    }

    // =========================================================================================
    // 📎 ВЛОЖЕНИЯ
    // =========================================================================================

    static String decodeVkText(String text) {
        if (text == null || text.isEmpty()) return "";
        return text.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
                .replace("&lt;", "<").replace("&gt;", ">")
                .replace("&quot;", "\"").replace("&amp;", "&");
    }

    static String extractMediaUrl(JsonNode attachments) {
        if (attachments == null || !attachments.isArray() || attachments.isEmpty()) return null;
        for (JsonNode a : attachments) {
            String t = a.path("type").asText("");
            JsonNode payload = a.path(t.isEmpty() ? "photo" : t);
            if (payload.isMissingNode()) continue;

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
                if (best != null && !best.isBlank()) return best;
            }
            for (String f : new String[]{"url", "preview_url", "uri"}) {
                String u = payload.path(f).asText("");
                if (!u.isBlank()) return u;
            }
            String mu = a.path("media_url").asText("");
            if (!mu.isBlank()) return mu;
        }
        return null;
    }

    /** Вложения современного формата: содержат JSON-строку `attachments` или плоские поля `attach1_type` и т.д. */
    static String extractMediaUrlFromTail(JsonNode atts) {
        if (atts == null || atts.isMissingNode() || atts.isNull()) return null;
        String raw = atts.path("attachments").asText("");
        if (!raw.isBlank()) {
            try {
                String url = extractMediaUrl(new ObjectMapper().readTree(raw));
                if (url != null) return url;
            } catch (Exception e) {
                log.debug("[VK] не распарсен attachments-JSON: {}", describe(e));
            }
        }
        return extractFromFlatAttachFields(atts);
    }

    /** Вложения классического формата: плоские поля `attach1_type`, `attach1_url` и т.д. */
    static String extractMediaUrlFromClassic(JsonNode atts) {
        if (atts == null || !atts.isObject()) return null;
        return extractFromFlatAttachFields(atts);
    }

    private static String extractFromFlatAttachFields(JsonNode atts) {
        for (int i = 1; i <= 10; i++) {
            String type = atts.path("attach" + i + "_type").asText("");
            if (type.isBlank()) continue;
            String url = atts.path("attach" + i + "_url").asText("");
            if (!url.isBlank()) return url;
        }
        return null;
    }
}