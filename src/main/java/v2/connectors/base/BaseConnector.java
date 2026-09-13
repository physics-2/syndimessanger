package v2.connectors.base;

public interface BaseConnector {

    /** Идентификатор платформы: "vk" | "tg" | "max" */
    String platform();

    /** Единый формат статуса */
    ConnectorStatus getStatus();

    /** Жизненный цикл клиента */
    ConnectorResult start();
    ConnectorResult stop();

    /** Real-time прослушка */
    ConnectorResult startListening();
    ConnectorResult stopListening();

    /** Сканирование истории */
    ConnectorResult startScan(ScanOptions options);

    /** Конфигурация в едином формате */
    ConnectorConfig getConfig();
    ConnectorResult updateConfig(ConnectorConfig config);
}