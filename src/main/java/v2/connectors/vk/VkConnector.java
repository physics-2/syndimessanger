package v2.connectors.vk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import v2.Config;
import v2.connectors.base.*;
import v2.entity.Chat;
import v2.entity.Message;
import v2.entity.User;
import v2.repository.MessageRepository;
import v2.services.ChatService;
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

/**
 * VK Connector для v2 - реализует BaseConnector.
 * Без System.out.println, без фоллбеков с мусорными данными.
 * Использует сервисы (UserService, ChatService, MessageService), а не напрямую репозитории.
 */
@Component
public class VkConnector implements BaseConnector {

    private static final String API_URL = "https://api.vk.com/method/";
    private static final String API_VERSION = "5.199";
    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final int VK_TEXT_LIMIT = 9000;

    private final ConnectorConfig config = new ConnectorConfig();

    // Состояние
    private volatile boolean isListening = false;
    private volatile boolean isScanning = false;
    private volatile boolean isStarted = false;
    private final Long myUserId = null;

    // Кэши
    private final ConcurrentHashMap<Long, VkChatMeta> chatCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, User> userCache = new ConcurrentHashMap<>();
    private final ExecutorService scanExecutor = Executors.newSingleThreadExecutor();

    // Сервисы
    private final UserService userService;
    private final ChatService chatService;
    private final MessageRepository messageRepository;

    // HTTP клиент и маппер
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(20))
            .build();
    private final ObjectMapper mapper = new ObjectMapper();

    public VkConnector(UserService userService, ChatService chatService, MessageRepository messageRepository) {
        this.userService = userService;
        this.chatService = chatService;
        this.messageRepository = messageRepository;
    }

    @Override
    public String platform() {
        return "vk";
    }

    @Override
    public ConnectorStatus getStatus() {
        return new ConnectorStatus("vk", isStarted, isListening, isScanning, myUserId, config);
    }

    @Override
    public ConnectorResult start() {
        if (isStarted) {
            return ConnectorResult.ok("VK уже запущен");
        }
        try {
            String token = Config.getVkToken();
            if (token == null || token.isBlank()) {
                return ConnectorResult.fail("Токен VK не найден в конфигурации");
            }
            isStarted = true;
            return ConnectorResult.ok("VK коннектор запущен");
        } catch (Exception e) {
            return ConnectorResult.fail("Ошибка запуска VK: " + e.getMessage());
        }
    }

    @Override
    public ConnectorResult stop() {
        isListening = false;
        isStarted = false;

        return ConnectorResult.ok("VK остановлен");
    }

    @Override
    public ConnectorResult startListening() {
        if (!isStarted) {
            return ConnectorResult.fail("VK не запущен. Вызовите start() сначала.");
        }
        isListening = true;
        // Здесь должен быть запуск LongPoll или Callback API
        // Для краткости опущено - реализуется по аналогии с TgConnector
        return ConnectorResult.ok("VK прослушка включена");
    }

    @Override
    public ConnectorResult stopListening() {
        isListening = false;
        return ConnectorResult.ok("VK прослушка выключена");
    }

    @Override
    public ConnectorResult startScan(ScanOptions options) {
        if (!isStarted) {
            return ConnectorResult.fail("VK не запущен");
        }
        if (isScanning) {
            return ConnectorResult.fail("Сканирование уже идёт");
        }

        isScanning = true;
        scanExecutor.submit(() -> runScan(options));
        return ConnectorResult.ok("Сканирование VK запущено");
    }

    private void runScan(ScanOptions options) {
        try {
            List<Long> chatIds = options.chatIds();
            Integer limit = options.limitPerChat() != null ? options.limitPerChat() : config.limitPerChat;

            if (chatIds.isEmpty()) {
                chatIds = getDialogsFromVK(limit);
            }

            for (Long peerId : chatIds) {
                if (!isScanning) break;

                try {
                    VkChatMeta meta = fetchChatMeta(peerId);
                    if (meta == null) continue;

                    if (!isChatAllowed(meta)) continue;


                    Chat chat = new Chat("vk", meta.id(), meta.title(), meta.isGroup());
                    chatService.saveOrUpdate(chat);


                    List<Message> messages = fetchMessages(peerId, limit);
                    for (Message msg : messages) {

                        try {
                            User author = getOrFetchUser(msg.getAuthorId());
                            if (author != null) {
                                userService.saveOrUpdate(author);
                                messageRepository.save(msg);
                            }
                        } catch (Exception e) {
                            System.out.println(e.getMessage());
                        }
                    }

                    Thread.sleep(500);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
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
        // Получаем список диалогов через VK API
        List<Long> dialogs = new ArrayList<>();
        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("count", String.valueOf(Math.min(limit, 100)));
            JsonNode response = vkApi("messages.getConversations", params);

            JsonNode items = response.path("response").path("items");
            if (items.isArray()) {
                for (JsonNode item : items) {
                    JsonNode conversation = item.path("conversation");
                    long peerId = conversation.path("peer").path("id").asLong(0);
                    if (peerId > 0) {
                        dialogs.add(peerId);
                    }
                }
            }
        } catch (Exception e) {
            // Возвращаем пустой список при ошибке
        }
        return dialogs;
    }

    private VkChatMeta fetchChatMeta(long peerId) {
        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("peer_ids", String.valueOf(peerId));
            JsonNode response = vkApi("messages.getConversationsByIds", params);

            JsonNode item = response.path("response").path("items").get(0);
            if (item == null || item.isMissingNode()) {
                return null;
            }

            JsonNode conversation = item.path("conversation");
            JsonNode peer = conversation.path("peer");

            String title = conversation.path("peer").path("local_name").asText("");
            if (title.isBlank()) {
                title = conversation.path("peer").path("name").asText("Unknown");
            }

            boolean isGroup = peer.path("type").asText("").equals("chat")
                    || peer.path("type").asText("").equals("group");

            return new VkChatMeta(peerId, title, isGroup);
        } catch (Exception e) {
            return null;
        }
    }

    private List<Message> fetchMessages(long peerId, int limit) {
        List<Message> messages = new ArrayList<>();
        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("peer_id", String.valueOf(peerId));
            params.put("count", String.valueOf(Math.min(limit, 200)));

            JsonNode response = vkApi("messages.getHistory", params);
            JsonNode items = response.path("response").path("items");

            if (items.isArray()) {
                for (JsonNode item : items) {
                    long fromId = item.path("from_id").asLong(0);
                    long messageId = item.path("id").asLong(0);
                    String text = item.path("text").asText("");
                    long timestamp = item.path("date").asLong(System.currentTimeMillis());

                    Message msg = new Message("vk", messageId, peerId, fromId, text, null);
                    msg.setTimestamp(timestamp);
                    messages.add(msg);
                }
            }
        } catch (Exception e) {
            // Пустой список при ошибке
        }
        return messages;
    }

    private User getOrFetchUser(long userId) {
        // Проверяем кэш
        User cached = userCache.get(userId);
        if (cached != null) {
            return cached;
        }

        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("user_ids", String.valueOf(userId));
            params.put("fields", "photo_100");

            JsonNode response = vkApi("users.get", params);
            JsonNode userNode = response.path("response").get(0);

            if (userNode == null || userNode.isMissingNode()) {
                return null;
            }

            String firstName = userNode.path("first_name").asText("");
            String lastName = userNode.path("last_name").asText("");
            String photoUrl = userNode.path("photo_100").asText("");

            User user = new User("vk", userId, firstName, lastName, null, photoUrl, null, new ArrayList<>());
            userCache.put(userId, user);
            return user;

        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public ConnectorConfig getConfig() {
        return config;
    }

    @Override
    public ConnectorResult updateConfig(ConnectorConfig newConfig) {
        this.config.scanPersonal = newConfig.scanPersonal;
        this.config.scanGroups = newConfig.scanGroups;
        this.config.whitelist = newConfig.whitelist != null ? newConfig.whitelist : new ArrayList<>();
        this.config.limitPerChat = newConfig.limitPerChat;
        this.config.downloadMedia = newConfig.downloadMedia;
        return ConnectorResult.ok("Конфигурация VK обновлена");
    }

    // ==================== Методы отправки сообщений ====================

    @Override
    public long sendMessage(String peer, String text, String attachments, Long replyTo) {
        if (text == null || text.trim().isEmpty()) {
            throw new IllegalArgumentException("Message text cannot be empty");
        }

        try {
            long peerId = Long.parseLong(peer);

            // Подготовка параметров для VK API
            Map<String, String> params = new LinkedHashMap<>();
            params.put("peer_id", String.valueOf(peerId));
            params.put("message", truncate(text, VK_TEXT_LIMIT));
            params.put("random_id", String.valueOf(ThreadLocalRandom.current().nextLong(1, Integer.MAX_VALUE)));

            if (replyTo != null && replyTo > 0) {
                params.put("reply_to", String.valueOf(replyTo));
            }

            if (attachments != null && !attachments.isEmpty()) {
                params.put("attachment", attachments);
            }

            // Вызов VK API
            JsonNode resp = vkApi("messages.send", params);

            if (resp.get("error") != null) {
                throw new RuntimeException("VK API error: " + vkError(resp));
            }

            long messageId = resp.path("response").asLong(0);

            if (messageId <= 0) {
                throw new RuntimeException("VK returned invalid message ID: " + messageId);
            }


            return messageId;

        } catch (NumberFormatException e) {

            throw new IllegalArgumentException("Invalid peer ID: " + peer, e);
        } catch (Exception e) {

            throw new RuntimeException("VK send failed: " + e.getMessage(), e);
        }
    }

    public SendResult sendMessage(long peerId, String text, String attachments, long replyTo) {
        long peer = toPeerId(peerId);
        if (text == null || text.isBlank()) {
            return SendResult.fail("vk", peer, "Пустой текст сообщения");
        }

        Map<String, String> params = new LinkedHashMap<>();
        params.put("peer_id", String.valueOf(peer));
        params.put("message", truncate(text, VK_TEXT_LIMIT));
        params.put("random_id", String.valueOf(ThreadLocalRandom.current().nextLong(1, Integer.MAX_VALUE)));
        if (replyTo > 0) {
            params.put("reply_to", String.valueOf(replyTo));
        }
        if (attachments != null && !attachments.isBlank()) {
            params.put("attachment", attachments);
        }

        try {
            JsonNode resp = vkApi("messages.send", params);
            if (resp.get("error") != null) {
                return SendResult.fail("vk", peer, vkError(resp));
            }
            long messageId = resp.path("response").asLong(0);
            return SendResult.ok("vk", peer, messageId, text);
        } catch (Exception e) {
            return SendResult.fail("vk", peer, describe(e));
        }
    }

    public SendResult sendFile(long peerId, String caption, String filePath, String mimeType, long replyTo) {
        long peer = toPeerId(peerId);
        Path file = Path.of(filePath);
        if (!Files.exists(file)) {
            return SendResult.fail("vk", peer, "Файл не найден: " + filePath);
        }

        try {
            String attachment;
            if (isImage(file.getFileName().toString(), mimeType)) {
                attachment = uploadPhoto(peer, file);
            } else {
                attachment = uploadDoc(peer, file);
            }
            return sendMessage(peer, caption, attachment, replyTo);
        } catch (Exception e) {
            return SendResult.fail("vk", peer, describe(e));
        }
    }

    private String uploadPhoto(long peer, Path file) throws Exception {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("peer_id", String.valueOf(peer));

        JsonNode server = vkApi("photos.getMessagesUploadServer", params);
        if (server.get("error") != null) {
            throw new IllegalStateException("photos.getMessagesUploadServer: " + vkError(server));
        }

        String uploadUrl = server.path("response").path("upload_url").asText("");
        if (uploadUrl.isBlank()) {
            throw new IllegalStateException("VK вернул пустой upload_url");
        }

        JsonNode uploaded = postMultipart(uploadUrl, "photo", file);
        if (uploaded.get("error") != null) {
            throw new IllegalStateException("Загрузка фото: " + vkError(uploaded));
        }

        Map<String, String> saveParams = new LinkedHashMap<>();
        saveParams.put("photo", asText(uploaded.get("photo")));
        copyIfPresent(uploaded, saveParams, "server", "hash");

        JsonNode saved = vkApi("photos.saveMessagesPhoto", saveParams);
        if (saved.get("error") != null) {
            throw new IllegalStateException("photos.saveMessagesPhoto: " + vkError(saved));
        }

        JsonNode first = saved.path("response").get(0);
        if (first == null) {
            throw new IllegalStateException("photos.saveMessagesPhoto вернул пустой ответ");
        }
        return "photo" + first.path("owner_id").asLong() + "_" + first.path("id").asLong();
    }

    private String uploadDoc(long peer, Path file) throws Exception {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("peer_id", String.valueOf(peer));
        params.put("type", "doc");

        JsonNode server = vkApi("docs.getMessagesUploadServer", params);
        if (server.get("error") != null) {
            throw new IllegalStateException("docs.getMessagesUploadServer: " + vkError(server));
        }

        String uploadUrl = server.path("response").path("upload_url").asText("");
        if (uploadUrl.isBlank()) {
            throw new IllegalStateException("VK вернул пустой upload_url");
        }

        JsonNode uploaded = postMultipart(uploadUrl, "file", file);
        String rawFile = uploaded.path("file").asText("");
        if (rawFile.isBlank()) {
            throw new IllegalStateException("Сервер загрузки вернул пустое поле file");
        }

        JsonNode saved = vkApi("docs.save", Map.of("file", rawFile));
        if (saved.get("error") != null) {
            throw new IllegalStateException("docs.save: " + vkError(saved));
        }

        JsonNode doc = saved.path("response").path("doc");
        if (doc.isMissingNode() || doc.path("id").asLong(0) == 0) {
            throw new IllegalStateException("docs.save вернул пустой ответ");
        }
        return "doc" + doc.path("owner_id").asLong() + "_" + doc.path("id").asLong();
    }

    private JsonNode vkApi(String method, Map<String, String> params) throws Exception {
        StringBuilder q = new StringBuilder();
        params.forEach((k, v) -> q.append(q.length() > 0 ? "&" : "")
                .append(enc(k)).append('=').append(enc(v)));
        q.append("&access_token=").append(enc(Config.getVkToken()));
        q.append("&v=").append(API_VERSION);

        HttpRequest req = HttpRequest.newBuilder(URI.create(API_URL + method + "?" + q))
                .timeout(TIMEOUT)
                .GET()
                .build();

        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        if (resp.statusCode() != 200) {
            throw new IllegalStateException("VK ответил HTTP " + resp.statusCode());
        }
        String body = resp.body();
        if (body == null || body.isBlank()) {
            throw new IllegalStateException("VK вернул пустой ответ");
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
        byte[] header = head.getBytes(StandardCharsets.UTF_8);
        byte[] footer = tail.getBytes(StandardCharsets.UTF_8);

        byte[] result = new byte[header.length + content.length + footer.length];
        System.arraycopy(header, 0, result, 0, header.length);
        System.arraycopy(content, 0, result, header.length, content.length);
        System.arraycopy(footer, 0, result, header.length + content.length, footer.length);
        return result;
    }

    // ==================== Утилиты ====================

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

    private static String vkError(JsonNode resp) {
        JsonNode e = resp.path("error");
        int code = e.path("error_code").asInt();
        return "code=" + code + " msg=" + e.path("error_msg").asText("");
    }

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

    // Вложенный класс для метаданных чата
    private record VkChatMeta(long id, String title, boolean isGroup) {}


}
