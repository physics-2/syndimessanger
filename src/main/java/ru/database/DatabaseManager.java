package ru.database;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.stereotype.Component;
import ru.Config;

import java.sql.*;
import java.util.*;

import static java.sql.DriverManager.getConnection;

@Component
public class DatabaseManager {

    private final HikariDataSource dataSource;


    public DatabaseManager() {
        // Настройка пула соединений
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(Config.getDbUrl());
        config.setUsername(Config.getDbUsername());
        config.setPassword(Config.getDbPassword());  // Ваш пароль
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(10000);

        this.dataSource = new HikariDataSource(config);

        // Создаем таблицу при старте (если нет)
        initializeSchema();
        System.out.println("✅ [DB] PostgreSQL подключен, пул соединений активен.");
    }

    private void initializeSchema() {
        String createMessageTable = DatabaseMessages.CREATE_MESSAGE_TABLE;


        try (Connection conn = dataSource.getConnection();
            PreparedStatement preparedStatement = conn.prepareStatement(createMessageTable)) {
            preparedStatement.execute();
        } catch (SQLException e) {
            System.err.println("❌ [DB] Ошибка создания схемы: " + e.getMessage());
        }

        String createChats = DatabaseMessages.CREATE_CHATS_TABLE;


        try (Connection conn = dataSource.getConnection();
             PreparedStatement preparedStatement = conn.prepareStatement(createChats)) {
            preparedStatement.execute();
        } catch (SQLException e) {
            System.err.println("❌ [DB] Ошибка создания схемы: " + e.getMessage());
        }

        String createUsersTable = DatabaseMessages.CREATE_USERS_TABLE;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement preparedStatement = conn.prepareStatement(createUsersTable)) {
            preparedStatement.execute();
            System.out.println("✅ [DB] Схема БД проверена/создана.");
        } catch (SQLException e) {
            System.err.println("❌ [DB] Ошибка создания схемы: " + e.getMessage());
        }

        String createUserConfigTable = DatabaseMessages.CREATE_USER_CONFIG_TABLE;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement preparedStatement = conn.prepareStatement(createUserConfigTable)) {
            preparedStatement.execute();
            System.out.println("✅ [DB] Схема БД проверена/создана.");
        } catch (SQLException e) {
            System.err.println("❌ [DB] Ошибка создания схемы: " + e.getMessage());
        }
    }

    public void saveMessage(UnifiedMessage msg) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(DatabaseMessages.INSERT_INTO_MESSAGE)) {

            pstmt.setString(1, msg.source());
            pstmt.setLong(2, msg.messageId());
            pstmt.setLong(3, msg.chatId());
            pstmt.setLong(4, msg.authorId());
            pstmt.setString(5, msg.text());
            pstmt.setLong(6, msg.timestamp());
            pstmt.setString(7, msg.mediaUrl() != null ? msg.mediaUrl() : ""); // <-- ДОБАВЛЕНО

            int rowsAffected = pstmt.executeUpdate();
            if (rowsAffected > 0) {
                System.out.println("💾 [DB] Сообщение сохранено: " + (msg.text().length() > 30 ? msg.text().substring(0, 30) + "..." : msg.text()));
            }
        } catch (SQLException e) {
            System.err.println("❌ [DB] Ошибка сохранения: " + e.getMessage());
        }
    }

    // Метод для получения истории чата (понадобится для UI)
    public java.util.List<UnifiedMessage> getChatHistory(String source, long chatId, int limit) {
        String sql = DatabaseMessages.GET_MESSAGE_HISTORY;

        java.util.List<UnifiedMessage> result = new java.util.ArrayList<>();


        try (Connection conn = dataSource.getConnection();
             PreparedStatement preparedStatement = conn.prepareStatement(sql)) {

            preparedStatement.setString(1, source);
            preparedStatement.setLong(2, chatId);
            preparedStatement.setInt(3, limit);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    result.add(new UnifiedMessage(
                            resultSet.getString("source"),
                            resultSet.getLong("message_id"), // 👈 ВОТ ЗДЕСЬ БЫЛА ОШИБКА! (раньше тут могло быть "id")
                            resultSet.getLong("chat_id"),
                            resultSet.getLong("author_id"),
                            resultSet.getString("text"),
                            resultSet.getLong("timestamp"),
                            resultSet.getString("media_url")
                    ));
                }
            }
        } catch (SQLException e) {
            System.err.println("❌ [DB] Ошибка чтения истории: " + e.getMessage());
        }

        return result;

    }

    public String getSourceForChat(long chatId) {
        // Ищем source по chatId в таблице messages
        String sql = "SELECT DISTINCT source FROM messages WHERE chat_id = ? LIMIT 1";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setLong(1, chatId);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("source");
                }
            }
        } catch (SQLException e) {
            System.err.println("❌ [DB] Ошибка определения source для чата " + chatId + ": " + e.getMessage());
        }

        // Если не нашли, возвращаем "vk" как fallback
        return "vk";
    }

    public void saveOrUpdateUser(UserInfo user) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(DatabaseMessages.UPSERT_USER)) {

            preparedStatement.setString(1, user.source());
            preparedStatement.setLong(2, user.platformId());
            preparedStatement.setString(3, user.firstName());
            preparedStatement.setString(4, user.lastName());
            preparedStatement.setString(5, user.username());
            preparedStatement.setString(6, user.photoUrl());

            // Преобразуем List<String> в SQL Array
            String[] tagsArray = user.tags() != null ? user.tags().toArray(new String[0]) : new String[0];
            preparedStatement.setArray(7, connection.createArrayOf("text", tagsArray));
            preparedStatement.setString(8,user.phoneNumber());

            preparedStatement.executeUpdate();
            System.out.println("👤 [DB] Пользователь сохранен: " + user.firstName() + " | Теги: " + String.join(", ", tagsArray));

        } catch (SQLException e) {
            System.err.println("❌ [DB] Ошибка сохранения пользователя: " + e.getMessage());
        }
    }

    public List<Map<String, Object>> getAllChats() {
        String sql = DatabaseMessages.GET_ALL_CHATS;

        List<Map<String, Object>> result = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                Map<String, Object> chat = new HashMap<>();
                chat.put("chatId", rs.getLong("chat_id"));
                chat.put("source", rs.getString("source"));
                chat.put("authorId", rs.getLong("author_id"));
                chat.put("lastMessage", rs.getString("last_message"));
                chat.put("timestamp", rs.getLong("timestamp"));
                chat.put("firstName", rs.getString("first_name"));
                chat.put("lastName", rs.getString("last_name"));
                chat.put("photoUrl", rs.getString("photo_url"));
                result.add(chat);
            }
        } catch (SQLException e) {
            System.err.println("❌ [DB] Ошибка получения чатов: " + e.getMessage());
        }

        return result;
    }

    public List<UserInfo> getAllUsers() {
        String sql = DatabaseMessages.GET_ALL_USERS;
        List<UserInfo> result = new java.util.ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            while (rs.next()) {
                java.sql.Array sqlTags = rs.getArray("tags");
                String[] tags = sqlTags != null ? (String[]) sqlTags.getArray() : new String[0];

                result.add(new UserInfo(
                        rs.getString("source"),
                        rs.getLong("platform_id"),       // 👈 platform_id из БД
                        rs.getString("first_name"),      // 👈 first_name из БД
                        rs.getString("last_name"),       // 👈 last_name из БД
                        rs.getString("username"),
                        rs.getString("photo_url"),
                        rs.getString("phone_number"),// 👈 photo_url из БД
                        java.util.Arrays.asList(tags)

                ));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    // Сохранение информации о чате
    public void saveOrUpdateChat(String source, long chatId, String chatTitle, boolean isGroup) {
        String sql = DatabaseMessages.SAVE_OR_UPDATE_CHAT;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, source);
            pstmt.setLong(2, chatId);
            pstmt.setString(3, chatTitle);
            pstmt.setBoolean(4, isGroup);

            pstmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("❌ [DB] Ошибка сохранения чата: " + e.getMessage());
        }
    }

    // Получение всех чатов с информацией о последнем сообщении и участниках
    public List<Map<String, Object>> getAllChatsWithInfo() {
        String sql = DatabaseMessages.GET_ALL_CHATS_WITH_INFO;

        List<Map<String, Object>> result = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                Map<String, Object> chat = new HashMap<>();
                chat.put("chatId", rs.getLong("chat_id"));
                chat.put("source", rs.getString("source"));
                chat.put("chatTitle", rs.getString("chat_title"));
                chat.put("isGroup", rs.getBoolean("is_group"));
                chat.put("lastMessage", rs.getString("last_message"));
                chat.put("timestamp", rs.getLong("timestamp"));
                chat.put("authorId", rs.getLong("author_id"));
                chat.put("firstName", rs.getString("first_name"));
                chat.put("lastName", rs.getString("last_name"));
                chat.put("photoUrl", rs.getString("photo_url"));
                try {
                    java.sql.Array sqlTags = rs.getArray("tags");
                    if (sqlTags != null) {
                        chat.put("tags", Arrays.asList((String[]) sqlTags.getArray()));
                    } else {
                        chat.put("tags", new ArrayList<>());
                    }
                } catch (SQLException e) {
                    chat.put("tags", new ArrayList<>());
                }
                result.add(chat);
            }
        } catch (SQLException e) {
            System.err.println(" [DB] Ошибка получения чатов: " + e.getMessage());
        }

        return result;
    }

    // Получение истории чата с именами всех авторов
    // Получение истории чата с именами всех авторов и медиа
    public List<Map<String, Object>> getChatHistoryWithAuthors(String source, long chatId, int limit) {
        String sql = DatabaseMessages.GET_CHAT_HISTORY_WITH_AUTHORS;

        List<Map<String, Object>> result = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setLong(1, chatId);
            pstmt.setString(2, source);
            pstmt.setInt(3, limit);

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> msg = new HashMap<>();
                    msg.put("messageId", rs.getLong("message_id"));
                    msg.put("chatId", rs.getLong("chat_id"));
                    msg.put("authorId", rs.getLong("author_id"));
                    msg.put("text", rs.getString("text"));
                    msg.put("timestamp", rs.getLong("timestamp"));
                    msg.put("authorName", rs.getString("author_name"));

                    // 👇 ДОБАВЛЕНО: Чтение media_url из базы
                    msg.put("mediaUrl", rs.getString("media_url"));
                    msg.put("source", rs.getString("source"));

                    result.add(msg);
                }
            }
        } catch (SQLException e) {
            System.err.println("❌ [DB] Ошибка получения истории: " + e.getMessage());
        }

        return result;
    }

    /**
     * Заполняет названия личных чатов именами собеседников
     * (для чатов где is_group=false и chat_title IS NULL)
     */
    public void fillPersonalChatTitles(long myUserId) {
        String sql = """
        WITH chat_participants AS (
            SELECT DISTINCT 
                c.chat_id,
                m.author_id
            FROM chats c
            JOIN messages m ON c.chat_id = m.chat_id AND c.source = m.source
            WHERE c.source = 'max'
              AND c.is_group = false
              AND (c.chat_title IS NULL OR c.chat_title = '')
        ),
        other_users AS (
            SELECT 
                cp.chat_id,
                u.first_name,
                u.last_name
            FROM chat_participants cp
            JOIN users u ON cp.author_id = u.platform_id AND u.source = 'max'
            WHERE cp.author_id != ?
            GROUP BY cp.chat_id, u.first_name, u.last_name
        )
        UPDATE chats c
        SET chat_title = CASE 
            WHEN ou.last_name IS NOT NULL AND ou.last_name != '' 
                THEN ou.first_name || ' ' || ou.last_name
            ELSE ou.first_name
        END
        FROM other_users ou
        WHERE c.chat_id = ou.chat_id 
          AND c.source = 'max'
          AND (c.chat_title IS NULL OR c.chat_title = '')
        """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setLong(1, myUserId);
            int updated = pstmt.executeUpdate();
            System.out.println("✅ [DB] Обновлено названий личных чатов: " + updated);

        } catch (SQLException e) {
            System.err.println("❌ [DB] Ошибка заполнения названий чатов: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Получить список чатов без названий (для отладки)
     */
    public List<Map<String, Object>> getChatsWithoutTitles() {
        String sql = """
        SELECT c.chat_id, c.is_group, COUNT(m.message_id) as message_count
        FROM chats c
        LEFT JOIN messages m ON c.chat_id = m.chat_id AND c.source = m.source
        WHERE c.source = 'max'
          AND (c.chat_title IS NULL OR c.chat_title = '')
        GROUP BY c.chat_id, c.is_group
        ORDER BY c.chat_id
        """;

        List<Map<String, Object>> result = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                Map<String, Object> chat = new HashMap<>();
                chat.put("chatId", rs.getLong("chat_id"));
                chat.put("isGroup", rs.getBoolean("is_group"));
                chat.put("messageCount", rs.getInt("message_count"));
                result.add(chat);
            }
        } catch (SQLException e) {
            System.err.println("❌ [DB] Ошибка получения чатов без названий: " + e.getMessage());
        }

        return result;
    }

    public List<String> getVkIds() {
        return getIdArrayFromDb("vk_ids");
    }

    public List<String> getMaxIds() {
        return getIdArrayFromDb("max_ids");
    }

    public List<String> getTelegramIds() {
        return getIdArrayFromDb("telegram_ids");
    }

    private List<String> getIdArrayFromDb(String columnName) {
        List<String> result = new ArrayList<>();
        // Безопасная подстановка имени колонки
        String sql = "SELECT %s FROM user_config WHERE id = 1".formatted(columnName);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            if (rs.next()) {
                Array sqlArray = rs.getArray(columnName);
                if (sqlArray != null) {
                    String[] javaArray = (String[]) sqlArray.getArray();
                    result = Arrays.asList(javaArray);
                }
            }
        } catch (SQLException e) {
            System.err.println("❌ [DB] Ошибка чтения " + columnName + ": " + e.getMessage());
        }
        return result;
    }

    // ==========================================
    // 2. ПОЛНАЯ ЗАМЕНА СПИСКА (для обновлений с фронта)
    // ==========================================

    public void updateVkIds(List<String> ids) {
        updateIdArrayInDb("vk_ids", ids);
    }

    public void updateMaxIds(List<String> ids) {
        updateIdArrayInDb("max_ids", ids);
    }

    public void updateTelegramIds(List<String> ids) {
        updateIdArrayInDb("telegram_ids", ids);
    }

    private void updateIdArrayInDb(String columnName, List<String> ids) {
        // Используем .formatted() для безопасной вставки имени колонки
        String sql = """
            INSERT INTO user_config (id, %s, first_name, last_name)
            VALUES (1, ?::text[], 'System', 'Config')
            ON CONFLICT (id) 
            DO UPDATE SET %s = EXCLUDED.%s,
            updated_at = CURRENT_TIMESTAMP
            """.formatted(columnName, columnName, columnName);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // Безопасное создание SQL-массива через JDBC (избегает проблем с экранированием)
            pstmt.setArray(1, conn.createArrayOf("text", ids.toArray(new String[0])));

            pstmt.executeUpdate();
            System.out.println("✅ [DB] Поле " + columnName + " полностью обновлено. Элементов: " + ids.size());

        } catch (SQLException e) {
            System.err.println("❌ [DB] Ошибка обновления " + columnName + ": " + e.getMessage());
        }
    }

    // ==========================================
    // 3. ДОБАВЛЕНИЕ ОДНОГО ID (Оптимально через SQL)
    // ==========================================
    // Используем array_append и COALESCE, чтобы не читать весь массив в Java
    // и избежать дубликатов на уровне базы данных.

    public void addMaxId(String newId) {
        addSingleIdToDb("max_ids", newId);
    }

    public void addVkId(String newId) {
        addSingleIdToDb("vk_ids", newId);
    }

    public void addTelegramId(String newId) {
        addSingleIdToDb("telegram_ids", newId);
    }

    private void addSingleIdToDb(String columnName, String newId) {
        String sql = """
            INSERT INTO user_config (id, %s, first_name, last_name)
            VALUES (1, ARRAY[?]::text[], 'System', 'Config')
            ON CONFLICT (id) 
            DO UPDATE SET %s = 
                CASE 
                    WHEN ?::text = ANY(COALESCE(user_config.%s, ARRAY[]::text[])) 
                    THEN user_config.%s
                    ELSE array_append(COALESCE(user_config.%s, ARRAY[]::text[]), ?::text)
                END,
                updated_at = CURRENT_TIMESTAMP
            """.formatted(columnName, columnName, columnName, columnName, columnName);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // Подставляем newId три раза для разных мест в запросе
            pstmt.setString(1, newId); // для ARRAY[?]
            pstmt.setString(2, newId); // для проверки ANY
            pstmt.setString(3, newId); // для array_append

            pstmt.executeUpdate();
            System.out.println("✅ [DB] ID " + newId + " добавлен в " + columnName);

        } catch (SQLException e) {
            System.err.println("❌ [DB] Ошибка добавления ID в " + columnName + ": " + e.getMessage());
        }
    }

    // ==========================================
    // 4. РАБОТА С ТЕГАМИ ПОЛЬЗОВАТЕЛЯ
    // ==========================================
    public List<String> getUserTags(String source, Long userId) {
        List<String> result = new ArrayList<>();
        // 👇 ЯВНО используем platform_id
        String sql = "SELECT tags FROM users WHERE source = ? AND platform_id = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, source);
            pstmt.setLong(2, userId);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    Array sqlArray = rs.getArray("tags");
                    if (sqlArray != null) {
                        String[] javaArray = (String[]) sqlArray.getArray();
                        result = Arrays.asList(javaArray);
                    }
                }
            }
        } catch (SQLException e) {
            System.err.println("❌ [DB] Ошибка чтения тегов: " + e.getMessage());
        }
        return result;
    }

    public void addUserTag(String source, Long userId, String tag) {
        if (tag == null || tag.trim().isEmpty() || userId == null) {
            System.err.println("❌ [DB] Попытка добавить пустой тег или userId = null");
            return;
        }
        String cleanTag = tag.trim();

        // 👇 ЯВНО указываем platform_id в INSERT и ON CONFLICT
        String sql = """
            INSERT INTO users (source, platform_id, first_name, last_name, tags)
            VALUES (?, ?, 'Unknown', '', ARRAY[?]::text[])
            ON CONFLICT (source, platform_id) 
            DO UPDATE SET tags = 
                CASE 
                    WHEN ?::text = ANY(COALESCE(users.tags, ARRAY[]::text[])) 
                    THEN users.tags
                    ELSE array_append(COALESCE(users.tags, ARRAY[]::text[]), ?::text)
                END
            """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, source);
            pstmt.setLong(2, userId); // Теперь это гарантированно попадет в platform_id
            pstmt.setString(3, cleanTag);
            pstmt.setString(4, cleanTag);
            pstmt.setString(5, cleanTag);

            pstmt.executeUpdate();
            System.out.println("✅ [DB] Тег '" + cleanTag + "' добавлен пользователю " + source + ":" + userId);

        } catch (SQLException e) {
            System.err.println("❌ [DB] Ошибка добавления тега: " + e.getMessage());
        }
    }

    public void removeUserTag(String source, Long userId, String tag) {
        if (tag == null || tag.trim().isEmpty() || userId == null) return;
        String cleanTag = tag.trim();

        // 👇 ЯВНО используем platform_id для удаления
        String sql = """
            UPDATE users 
            SET tags = array_remove(COALESCE(tags, ARRAY[]::text[]), ?::text)
            WHERE source = ? AND platform_id = ?
            """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, cleanTag);
            pstmt.setString(2, source);
            pstmt.setLong(3, userId);

            pstmt.executeUpdate();
            System.out.println("✅ [DB] Тег '" + cleanTag + "' удален у пользователя " + source + ":" + userId);
        } catch (SQLException e) {
            System.err.println("❌ [DB] Ошибка удаления тега: " + e.getMessage());
        }
    }

    // Закрытие пула при завершении приложения
    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            System.out.println("🔒 [DB] Пул соединений закрыт.");
        }
    }
}