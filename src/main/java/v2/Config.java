package v2;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {

    private static final Properties props = new Properties();
    private static boolean loaded = false;

    private static void load() {
        if (loaded) return;

        try (InputStream input = Config.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new RuntimeException("❌ Файл application.properties не найден в resources!");
            }
            props.load(input);
            loaded = true;
            System.out.println("✅ [CONFIG] Конфигурация загружена из application.properties");
        } catch (IOException e) {
            throw new RuntimeException("❌ Ошибка чтения конфигурации: " + e.getMessage(), e);
        }
    }

    public static String get(String key) {
        load();
        String value = props.getProperty(key);
        if (value == null || value.isEmpty()) {
            throw new RuntimeException("❌ Ключ '" + key + "' не найден в application.properties!");
        }
        return value;
    }

    public static String get(String key, String defaultValue) {
        load();
        return props.getProperty(key, defaultValue);
    }

    public static boolean getBoolean(String key, boolean defaultValue) {
        load();
        return Boolean.parseBoolean(props.getProperty(key, String.valueOf(defaultValue)));
    }

    public static int getInt(String key){
        load();
        return Integer.parseInt(props.getProperty(key,null));
    }


    // Специальные геттеры для удобства
    public static String getVkToken() {
        return get("vk.token");
    }

    public static String getDbUrl() {
        return get("db.url");
    }

    public static String getDbUsername() {
        return get("db.username");
    }

    public static String getDbPassword() {
        return get("db.password");
    }

    public static int getTgApiId() {
        return getInt("tg.app_id");
    }

    public static String getTgApiHash() {
        return get("tg.hash");
    }

    public static String getTgPhoneNumber() {
        return get("tg.phone");
    }

    public static boolean isDebugEnabled() {
        return getBoolean("debug.enabled", false);
    }
}