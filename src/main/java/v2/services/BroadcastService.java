package v2.services;

import org.springframework.stereotype.Service;
import v2.connectors.base.BaseConnector;
import v2.dto.BroadcastRequest;
import v2.dto.BroadcastResponse;
import v2.repository.UserRepository;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class BroadcastService {

    private final Map<String, BaseConnector> connectors;
    private final UserRepository userTagRepository;

    public BroadcastService(List<BaseConnector> connectorList, UserRepository userTagRepository) {
        this.connectors = connectorList.stream()
                .collect(Collectors.toMap(
                        c -> c.platform().toLowerCase(),
                        c -> c
                ));
        this.userTagRepository = userTagRepository;
    }

    public BroadcastResponse sendBroadcast(BroadcastRequest request) {
        int totalSuccess = 0;
        int totalFailed = 0;
        Map<String, String> allErrors = new HashMap<>();

        // Определяем, какие коннекторы использовать
        List<String> targetConnectors;
        if (request.isSendToAllConnectors()) {
            // Рассылка по всем доступным коннекторам
            targetConnectors = connectors.keySet().stream().toList();
        } else {
            // Только указанный коннектор
            String type = request.getConnectorType().toLowerCase();
            if (!connectors.containsKey(type)) {
                throw new IllegalArgumentException("Неизвестный тип коннектора: " + request.getConnectorType());
            }
            targetConnectors = List.of(type);
        }

        // Получаем список пользователей по тегам
        List<Long> recipientIds = userTagRepository.findUserIdsByTags(request.getTags());
        int totalRecipients = recipientIds.size();

        for (String connectorType : targetConnectors) {
            BaseConnector connector = connectors.get(connectorType);

            int success = 0;
            int failed = 0;

            for (Long userId : recipientIds) {
                try {
                    Long peerId = resolvePeerId(userId, connectorType);
                    connector.sendMessage(String.valueOf(peerId), request.getMessage(), request.getAttachments(), null);
                    success++;
                } catch (Exception e) {
                    failed++;
                    allErrors.put(connectorType + "_user_" + userId, e.getMessage());
                }
            }

            totalSuccess += success;
            totalFailed += failed;
        }

        return new BroadcastResponse(totalRecipients * targetConnectors.size(), totalSuccess, totalFailed, allErrors);
    }

    private Long resolvePeerId(Long userId, String type) {
        // Логика преобразования userId в peerId зависит от типа коннектора
        // Для VK: peerId = userId (для личных сообщений)
        // Для TG: chatId может храниться отдельно
        // Здесь упрощённая реализация, в реальности нужно брать из БД
        return userId;
    }
}