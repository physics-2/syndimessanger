package v2.connectors.max;

import org.springframework.web.client.RestTemplate;
import v2.connectors.base.*;

import java.util.Map;

public class MaxConnector implements BaseConnector {

    private final RestTemplate http = new RestTemplate();
    private static final String BASE = "http://localhost:8081/api/max";
    private final ConnectorConfig config = new ConnectorConfig();



    @Override public String platform() { return "max"; }

    @Override
    public ConnectorStatus getStatus() {
        try {
            Map<String, Object> r = http.getForObject(BASE + "/status", Map.class);
            return new ConnectorStatus(
                    "max",
                    true,
                    Boolean.TRUE.equals(r.get("listening")),
                    Boolean.TRUE.equals(r.get("scanning")),
                    r.get("my_user_id") != null ? ((Number) r.get("my_user_id")).longValue() : null,
                    config
            );
        } catch (Exception e) {
            return new ConnectorStatus("max", false, false, false, null, config);
        }
    }

    @Override public ConnectorResult start()          { return startListening(); }
    @Override public ConnectorResult stop()           { return stopListening(); }

    @Override
    public ConnectorResult startListening() {
        http.postForObject(BASE + "/listen/start", null, String.class);
        return ConnectorResult.ok("MAX прослушка ВКЛ");
    }

    @Override
    public ConnectorResult stopListening() {
        http.postForObject(BASE + "/listen/stop", null, String.class);
        return ConnectorResult.ok("MAX прослушка ВЫКЛ");
    }

    @Override
    public ConnectorResult startScan(ScanOptions options) {
        Map<String, Object> body = Map.of("limit_per_chat",
                options.limitPerChat() != null ? options.limitPerChat() : config.limitPerChat);
        http.postForObject(BASE + "/scan", body, String.class);
        return ConnectorResult.ok("Сканирование MAX запущено");
    }

    @Override public ConnectorConfig getConfig() { return config; }

    @Override
    public ConnectorResult updateConfig(ConnectorConfig c) {
        http.postForObject(BASE + "/config", Map.of(
                "scan_personal", c.scanPersonal,
                "scan_groups",   c.scanGroups,
                "limit_per_chat", c.limitPerChat
        ), String.class);
        return ConnectorResult.ok("Конфиг MAX обновлён");
    }
}
