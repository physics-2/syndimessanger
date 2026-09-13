package v2.connectors.base;

public record ConnectorStatus(
        String platform,
        boolean started,      // клиент создан/подключен
        boolean listening,    // real-time работает
        boolean scanning,     // идёт сканирование
        Long myUserId,        // ID владельца аккаунта
        ConnectorConfig config


) {
    @Override
    public String platform() {
        return platform;
    }

    @Override
    public boolean started() {
        return started;
    }

    @Override
    public boolean listening() {
        return listening;
    }

    @Override
    public boolean scanning() {
        return scanning;
    }

    @Override
    public Long myUserId() {
        return myUserId;
    }

    @Override
    public ConnectorConfig config() {
        return config;
    }
}