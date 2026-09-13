package ru.database;

public class DatabaseMessages {
    public static final String CREATE_MESSAGE_TABLE = """
    CREATE TABLE IF NOT EXISTS messages (
        source VARCHAR(20) NOT NULL,
        message_id BIGINT NOT NULL,
        chat_id BIGINT NOT NULL,
        author_id BIGINT NOT NULL,
        text TEXT,
        timestamp BIGINT NOT NULL,
        media_url TEXT,
        PRIMARY KEY (source, message_id)
    )
    """;

    public static final String CREATE_CHATS_TABLE = """
                        CREATE TABLE IF NOT EXISTS chats (
                        id SERIAL PRIMARY KEY,
                        source VARCHAR(20) NOT NULL,
                        chat_id BIGINT NOT NULL,
                        chat_title VARCHAR(255),
                        is_group BOOLEAN DEFAULT false,
                        last_message_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(source, chat_id)
                        );
                        
                   
                        CREATE INDEX IF NOT EXISTS idx_chats_source_chatid ON chats(source, chat_id)""";

    public static final String INSERT_INTO_MESSAGE = """
    INSERT INTO messages (source, message_id, chat_id, author_id, text, timestamp, media_url)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (source, message_id) DO NOTHING
    """;
    public static final String GET_MESSAGE_HISTORY = """
    SELECT source, message_id, chat_id, author_id, text, timestamp, media_url
    FROM messages
    WHERE source = ? AND chat_id = ?
    ORDER BY timestamp DESC
    LIMIT ?
    """;

    public static final String CREATE_USERS_TABLE = """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                source VARCHAR(20) NOT NULL,
                platform_id BIGINT NOT NULL,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                username VARCHAR(100),
                photo_url TEXT,
                tags TEXT[] DEFAULT '{}', 
                phone_number VARCHAR(25),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source, platform_id)
            );
            """;

    public static final String CREATE_USER_CONFIG_TABLE = """
            CREATE TABLE IF NOT EXISTS user_config (
                id SERIAL PRIMARY KEY,
                vk_ids TEXT[] DEFAULT '{}', 
                max_ids TEXT[] DEFAULT '{}', 
                telegram_ids TEXT[] DEFAULT '{}', 
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """;

    public static final String UPSERT_USER = """
            INSERT INTO users(source, platform_id, first_name, last_name, username, photo_url, tags,phone_number)
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT (source, platform_id) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                username = EXCLUDED.username,
                photo_url = EXCLUDED.photo_url,
                tags = EXCLUDED.tags,
                phone_number = EXCLUDED.phone_number,
                updated_at = CURRENT_TIMESTAMP
            """;

    // Добавим запрос для поиска пользователей по тегу (на будущее)
    public static final String GET_ALL_CHATS = """
        SELECT DISTINCT ON (m.chat_id) 
            m.chat_id,
            m.source,
            m.author_id,
            m.text as last_message,
            m.timestamp,
            u.first_name,
            u.last_name,
            u.photo_url
        FROM messages m
        LEFT JOIN users u ON m.source = u.source AND m.author_id = u.platform_id
        ORDER BY m.chat_id, m.timestamp DESC
        """;

    public static final String GET_ALL_USERS =  "SELECT source, platform_id, first_name, last_name, username, photo_url, tags FROM users ORDER BY updated_at DESC";

    public static final String SAVE_OR_UPDATE_CHAT = """
        INSERT INTO chats(source, chat_id, chat_title, is_group, last_message_time)
        VALUES(?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (source, chat_id) DO UPDATE SET
            chat_title = EXCLUDED.chat_title,
            is_group = EXCLUDED.is_group,
            last_message_time = CURRENT_TIMESTAMP
        """;


    public static final String GET_ALL_CHATS_WITH_INFO = """
        SELECT 
            c.chat_id,
            c.source,
            c.chat_title,
            c.is_group,
            c.last_message_time,
            m.text as last_message,
            m.timestamp,
            m.author_id,
            u.first_name,
            u.last_name,
            u.photo_url,
            u.tags
        FROM chats c
        LEFT JOIN LATERAL (
            SELECT text, timestamp, author_id
            FROM messages
            WHERE messages.chat_id = c.chat_id AND messages.source = c.source
            ORDER BY timestamp DESC
            LIMIT 1
        ) m ON true
        LEFT JOIN users u ON m.author_id = u.platform_id AND c.source = u.source
        ORDER BY c.last_message_time DESC
        """;

    public static final String GET_CHAT_HISTORY_WITH_AUTHORS = """
    SELECT 
        m.message_id,
        m.chat_id,
        m.author_id,
        m.text,
        m.timestamp,
        m.media_url,
        u.first_name,
        m.source,
        u.last_name,
        CASE 
            WHEN m.author_id = u.platform_id THEN concat(u.first_name, ' ', u.last_name)
            ELSE 'User ' || m.author_id
        END as author_name
    FROM messages m
    LEFT JOIN users u ON m.author_id = u.platform_id AND m.source = u.source
    WHERE m.chat_id = ? AND m.source = ?
    ORDER BY m.timestamp DESC
    LIMIT ?
    """;

}
