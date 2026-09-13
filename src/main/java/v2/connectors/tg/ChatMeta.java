package v2.connectors.tg;

public record ChatMeta(long id, String title, boolean isGroup, boolean isChannel, boolean isSavedMessages) {}