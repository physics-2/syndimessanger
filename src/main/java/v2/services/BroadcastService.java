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
        String type = request.getConnectorType().toLowerCase();
        BaseConnector connector = connectors.get(type);

        if (connector == null) {
            throw new IllegalArgumentException("Неизвестный тип коннектора: " + request.getConnectorType());
        }

        List<Long> recipientIds = userTagRepository.findUserIdsByTags(request.getTags());

        int total = recipientIds.size();
        int success = 0;
        int failed = 0;
        Map<String, String> errors = new HashMap<>();

        for (Long userId : recipientIds) {
            try {
                Long peerId = resolvePeerId(userId, type);
                connector.sendMessage(String.valueOf(peerId), request.getMessage(), request.getAttachments(), null);
                success++;
            } catch (Exception e) {
                failed++;
                errors.put(String.valueOf(userId), e.getMessage());
            }
        }

        return new BroadcastResponse(total, success, failed, errors);
    }

    private Long resolvePeerId(Long userId, String type) {
        // Логика преобразования userId в peerId зависит от типа коннектора
        // Для VK: peerId = userId (для личных сообщений)
        // Для TG: chatId может храниться отдельно
        // Здесь упрощённая реализация, в реальности нужно брать из БД
        return userId;
    }
}