package v2.services;



import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import v2.entity.Chat;
import v2.entity.Message;
import v2.entity.User;
import v2.repository.ChatRepository;
import v2.repository.MessageRepository;
import v2.repository.UserRepository;


import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Приём входящих сообщений из python-коннектора (pymax) и раскладка их
 * по JPA-сущностям v2.entity.{Chat,User,Message}.
 *
 * Полностью самостоятельный: MaxConnector НЕ требуется, поэтому ваш
 * MaxApiController можно не трогать вообще.
 *
 * Контракт python -> java (POST /api/max/incoming):
 *   messageId, chatId, authorId, text, timestamp, firstName, lastName,
 *   source="max", isGroup, chatTitle, isOutgoing,
 *   mediaUrl = СТРОКА с JSON-массивом ('["url1","url2"]' или ""),
 *   phoneNumber, avatarPic
 */
@Service
public class MaxIngestService {

    /** "max" — значение для колонки source. */
    public static final String SOURCE = "max";

    /** Ограничение колонки messages.mediaUrl (length = 1000). */
    private static final int MEDIA_URL_MAX_LEN = 1000;

    /**
     * Как хранить mediaUrl, если вложений несколько:
     *   true  -> JSON-массив '["url1","url2"]' (фронт делает JSON.parse)
     *   false -> только первая ссылка (как одиночный путь в tg-коннекторе)
     */
    private static final boolean STORE_MEDIA_AS_JSON_ARRAY = true;

    private static final DateTimeFormatter TS_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final ChatRepository chatRepository;
    private final UserRepository userRepository;
    private final MessageRepository messageRepository;

    /** Свой ID в MAX: приходит из python через PUT /api/config/max. */
    private volatile long myUserId = 0L;

    public MaxIngestService(ChatRepository chatRepository,
                            UserRepository userRepository,
                            MessageRepository messageRepository) {
        this.chatRepository = chatRepository;
        this.userRepository = userRepository;
        this.messageRepository = messageRepository;
    }

    public void setMyUserId(long myUserId) {
        this.myUserId = myUserId;
        System.out.println("👤 [MAX] myUserId установлен: " + myUserId);
    }

    public long getMyUserId() {
        return myUserId;
    }

    // =====================================================================
    //  ГЛАВНЫЙ МЕТОД: сохранение одного входящего сообщения
    // =====================================================================

    /**
     * @return true — сообщение сохранено (или уже было в БД), false — отброшено.
     */
    @Transactional
    public boolean ingest(long chatId, String chatTitle, boolean isGroup,
                          long messageId, long authorId, String text,
                          List<String> mediaUrls, long timestamp, boolean isOutgoing,
                          String firstName, String lastName, String phone, String avatar) {

        if (chatId <= 0) {
            System.err.println("⚠️ [MAX] Пропуск: chatId <= 0");
            return false;
        }
        if (messageId <= 0) {
            System.err.println("⚠️ [MAX] Пропуск: messageId <= 0 (chatId=" + chatId + ")");
            return false;
        }

        // --- 1. Дедупликация: при повторном скане истории python шлёт те же сообщения.
        // Без этой проверки unique-констрейнт (source, message_id) даст 500 на каждом дубле.
        if (messageRepository.existsBySourceAndMessageId(SOURCE, messageId)) {
            return true;
        }

        // --- 2. Автор. Для исходящих python присылает ваш же ID.
        if (authorId <= 0) {
            authorId = isOutgoing && myUserId > 0 ? myUserId : 0L;
        }
        if (authorId > 0) {
            upsertUser(authorId, firstName, lastName, phone, avatar);
        } else {
            System.err.println("⚠️ [MAX] authorId неизвестен для message " + messageId
                    + " — message.author_id NOT NULL будет нарушен");
            return false;
        }

        // --- 3. Чат.
        String safeTitle = (chatTitle == null || chatTitle.isBlank())
                ? (isGroup ? "Чат " + chatId : "Пользователь " + authorId)
                : chatTitle.trim();
        upsertChat(chatId, safeTitle, isGroup);

        // --- 4. Сообщение.
        Message msg = new Message(SOURCE, messageId, chatId, authorId, nvl(text), buildMediaUrl(mediaUrls));

        // ВАЖНО: конструктор Message(...) ставит timestamp = System.currentTimeMillis(),
        // а нам нужно время ИЗ MAX (иначе вся история при скане получит "сейчас").
        msg.setTimestamp(timestamp > 0 ? timestamp : System.currentTimeMillis());

        messageRepository.save(msg);

        // --- 5. Точка расширения: здесь можно дёрнуть ваш пайплайн
        //     (WebSocket-пуш на фронт, автоответ, триггеры и т.п.):
        //     eventPublisher.publishEvent(new IncomingMessageEvent(msg));

        return true;
    }

    /**
     * Перегрузка, если удобнее передавать mediaUrl сырой строкой из DTO.
     */
    @Transactional
    public boolean ingestRaw(long chatId, String chatTitle, boolean isGroup,
                             long messageId, long authorId, String text,
                             String mediaUrlRaw, long timestamp, boolean isOutgoing,
                             String firstName, String lastName, String phone, String avatar) {
        return ingest(chatId, chatTitle, isGroup, messageId, authorId, text,
                parseMediaUrls(mediaUrlRaw), timestamp, isOutgoing,
                firstName, lastName, phone, avatar);
    }

    // =====================================================================
    //  UPSERT-хелперы
    // =====================================================================

    /**
     * Пользователь: не создаём дубликат (unique source+user_id), а обновляем поля.
     * username у MAX в payload нет — оставляем пустой строкой.
     *
     * Вызывается из ingest(...) -> транзакция уже открыта.
     * (Отдельный @Transactional здесь бесполезен: self-invocation идёт мимо прокси.)
     */
    public User upsertUser(long userId, String firstName, String lastName, String phone, String avatar) {
        Optional<User> found = userRepository.findBySourceAndUserId(SOURCE, userId);
        String now = LocalDateTime.now().format(TS_FMT);

        User u = found.orElseGet(() -> new User(
                SOURCE, userId, nvl(firstName), nvl(lastName), "",
                nvl(avatar), nvl(phone), new ArrayList<>()));

        // обновляем только непустыми значениями, чтобы не затирать данные fallback-ом
        if (notBlank(firstName)) u.setFirstName(firstName.trim());
        if (notBlank(lastName))  u.setLastName(lastName.trim());
        if (notBlank(phone))     u.setPhone_number(phone.trim());
        if (notBlank(avatar))    u.setPhoto_url(trimTo(avatar.trim(), 1000));
        if (u.getTags() == null) u.setTags(new ArrayList<>());
        u.setUpdated_at(now);

        return userRepository.save(u);
    }

    /**
     * Чат: unique source+chat_id. title NOT NULL — пустой не пройдёт.
     * Вызывается из ingest(...) -> транзакция уже открыта.
     */
    public Chat upsertChat(long chatId, String title, boolean isGroup) {
        String safeTitle = trimTo((title == null || title.isBlank()) ? ("Чат " + chatId) : title.trim(), 255);
        String now = LocalDateTime.now().format(TS_FMT);

        Chat c = chatRepository.findByChatId(chatId)
                .orElseGet(() -> new Chat(SOURCE, chatId, safeTitle, isGroup));

        c.setTitle(safeTitle);
        c.setGroup(isGroup);           // сеттер в Chat называется setGroup(...)
        c.setUpdated_at(now);
        return chatRepository.save(c);
    }

    // =====================================================================
    //  mediaUrl: из python приходит СТРОКА с JSON-массивом
    // =====================================================================

    /** Разбор '["url1","url2"]' / "url1" / "" / null в List<String>. */
    public static List<String> parseMediaUrls(String raw) {
        List<String> out = new ArrayList<>();
        if (raw == null) return out;

        String s = raw.trim();
        if (s.isEmpty()) return out;

        if (s.startsWith("[") && s.endsWith("]")) {
            String inner = s.substring(1, s.length() - 1).trim();
            if (inner.isEmpty()) return out;
            for (String part : inner.split(",")) {
                String u = stripQuotes(part.trim());
                if (!u.isEmpty()) out.add(u);
            }
            return out;
        }
        out.add(stripQuotes(s));
        return out;
    }

    /**
     * Собираем значение под колонку String(1000) без риска DataException.
     */
    private String buildMediaUrl(List<String> urls) {
        if (urls == null || urls.isEmpty()) return "";

        if (urls.size() == 1 || !STORE_MEDIA_AS_JSON_ARRAY) {
            return trimTo(urls.get(0), MEDIA_URL_MAX_LEN);
        }

        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < urls.size(); i++) {
            if (sb.length() + urls.get(i).length() + 4 > MEDIA_URL_MAX_LEN) break; // не влезаем
            if (i > 0) sb.append(',');
            sb.append('"').append(urls.get(i)).append('"');
        }
        sb.append(']');
        return trimTo(sb.toString(), MEDIA_URL_MAX_LEN);
    }

    // =====================================================================
    //  Утилиты
    // =====================================================================

    private static String stripQuotes(String s) {
        if (s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"")) {
            return s.substring(1, s.length() - 1).trim();
        }
        return s;
    }

    private static String trimTo(String s, int max) {
        if (s == null) return "";
        return s.length() <= max ? s : s.substring(0, max);
    }

    private static String nvl(String s) { return s != null ? s : ""; }

    private static boolean notBlank(String s) { return s != null && !s.isBlank(); }
}
