package v2.connectors.base;

public record SendResult(boolean success, String platform, long chatId, long messageId, String error) {
    public static SendResult ok(String platform, long chatId, long messageId, String text) {
        return new SendResult(true, platform, chatId, messageId, null);
    }

    public static SendResult fail(String platform, long chatId, String error) {
        return new SendResult(false, platform, chatId, 0, error);
    }
}