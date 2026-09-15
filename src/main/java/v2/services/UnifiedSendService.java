package v2.services;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import v2.connectors.base.BaseConnector;
import v2.connectors.max.MaxConnector;
import v2.connectors.tg.TgConnector;
import v2.connectors.vk.VkConnector;
import v2.dto.ApiResponse;

import java.util.Map;

@Service
public class UnifiedSendService {

    private  Map<String, BaseConnector> connectors;


    public UnifiedSendService(VkConnector vkConnector, TgConnector tgConnector, MaxConnector maxConnector) {
        this.connectors = Map.of(
                "vk", vkConnector,
                "tg", tgConnector,
                "max", maxConnector
        );
    }

    public ApiResponse<Long> sendMessage(String connectorType, String peer, String message, String attachments, Long replyTo) {
        BaseConnector connector = connectors.get(connectorType.toLowerCase());

        if (connector == null) {
            return ApiResponse.error("Неизвестный коннектор: " + connectorType);
        }

        try {
            long messageId = connector.sendMessage(peer, message, attachments, replyTo);
            return ApiResponse.success(messageId);
        } catch (Exception e) {
            return ApiResponse.error("Ошибка отправки: " + e.getMessage());
        }
    }
}
