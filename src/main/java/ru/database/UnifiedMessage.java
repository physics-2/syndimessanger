package ru.database;

public record UnifiedMessage(
        String source,
        long messageId,
        long chatId,
        long authorId,
        String text,
        long timestamp,
        String mediaUrl // <-- ДОБАВЛЕНО
) {}