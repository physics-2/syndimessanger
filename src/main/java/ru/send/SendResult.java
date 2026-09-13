package ru.send;

/**
 * Единый ответ об отправке для всех платформ (TG / VK / MAX),
 * чтобы фронтенд не зависел от внутреннего типа каждого коннектора.
 */
public record SendResult(
        boolean ok,
        String platform,   // "tg" | "vk" | "max"
        long chatId,
        long messageId,    // 0, если платформа не вернула id
        String text,
        String error       // null, если ok
) {
    public static SendResult ok(String platform, long chatId, long messageId, String text) {
        return new SendResult(true, platform, chatId, messageId, text, null);
    }

    public static SendResult fail(String platform, long chatId, String error) {
        return new SendResult(false, platform, chatId, 0, null, error);
    }

    public static SendResult fromTg(SendResult r) {
        return r.ok()
                ? ok("tg", r.chatId(), r.messageId(), r.text())
                : fail("tg", r.chatId(), r.error());
    }

    /** VK: основной вариант — record из VkSendSupport. */
    public static SendResult fromVk(SendResult r) {
        return r.ok()
                ? ok("vk", r.chatId(), r.messageId(), r.text())
                : fail("vk", r.chatId(), r.error());
    }

    /**
     * VK: запасной вариант — если вы скопировали record SendResult внутрь своего VkConnector.
     * Не используется по умолчанию, чтобы не было неоднозначности перегрузок.
     */
    public static SendResult fromVkConnector(SendResult r) {
        return r.ok()
                ? ok("vk", r.chatId(), r.messageId(), r.text())
                : fail("vk", r.chatId(), r.error());
    }
}
