package v2.api.base;

import java.util.Map;

public interface BaseApiConnector {

    // Статус
    Map<String, Object> getStatus();

    // Управление прослушкой
    Map<String, Object> startListening();
    Map<String, Object> stopListening();

    // Сканирование
    Map<String, Object> startScan();

    // Конфигурация
    Map<String, Object> updateConfig(Map<String, Object> body);


}