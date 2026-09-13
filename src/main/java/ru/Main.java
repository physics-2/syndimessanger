package ru;

import ru.database.DatabaseManager;
import ru.database.UnifiedMessage;
import ru.database.UserInfo;

public final class Main {

    public static void main(String[] args) throws Exception {
        System.out.println("🚀 Запуск агрегатора (VK + Users + PostgreSQL)...");

        DatabaseManager db = new DatabaseManager();

        // 1. Обработчик новых ПОЛЬЗОВАТЕЛЕЙ
        java.util.function.Consumer<UserInfo> userHandler = user -> {
            db.saveOrUpdateUser(user);
        };

        // 2. Обработчик СООБЩЕНИЙ
        java.util.function.Consumer<UnifiedMessage> messageHandler = msg -> {
            System.out.printf("📨 [%s] MsgID: %d | Чат: %d | От: %d | Текст: %s%n",
                    msg.source().toUpperCase(), msg.messageId(), msg.chatId(), msg.authorId(), msg.text());
            db.saveMessage(msg);
        };

        System.out.println("🔵 Инициализация VK...");
        String vkToken = Config.getVkToken();

        // Передаем ОБА обработчика в коннектор
       // new VkConnector(vkToken, messageHandler, userHandler);

        System.out.println("✅ Система запущена. Ожидание сообщений... (Нажмите Enter для выхода)");
        System.in.read();

        db.close();
    }
}