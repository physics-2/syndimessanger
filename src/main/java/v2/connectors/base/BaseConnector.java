package v2.connectors.base;

import java.util.List;
import java.util.Map;

public interface BaseConnector {

    /** Идентификатор платформы: "vk" | "tg" | "max" */
    String platform();

    List<Map<String, Object>> getAllGroups();
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

    long sendMessage(String peer, String message, String attachments, Long replyTo);
}