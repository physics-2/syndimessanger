package ru.send;

/**
 * Единый запрос на отправку для /api/send.
 *
 * @param platform "tg" | "vk" | "max"
 * @param chatId   id чата во внутренней нумерации платформы
 *                 (TG: chatId TDLib; VK: peer_id/uid; MAX: chat_id)
 * @param username альтернативный адресат (TG: @name / t.me/name / "saved")
 * @param text     текст сообщения (для файла — подпись)
 * @param replyTo  id сообщения, на которое отвечаем (0 = без ответа)
 * @param markdown парсить разметку (TG-Markdown / VK-Markdown)
 */
public record SendRequest(
        String platform,
        Long chatId,
        String username,
        String text,
        Long replyTo,
        Boolean markdown
) {
    public String platformOrDefault() {
        return (platform == null || platform.isBlank()) ? "tg" : platform.trim().toLowerCase();
    }

    public long replyToOrZero() {
        return replyTo != null ? replyTo : 0L;
    }

    public boolean markdownOrFalse() {
        return markdown != null && markdown;
    }
}
