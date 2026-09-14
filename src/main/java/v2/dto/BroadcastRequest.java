package v2.dto;

import java.util.List;

public class BroadcastRequest {
    private String connectorType; // "vk", "tg", "max" - для отправки через конкретный коннектор
    private List<String> tags; // теги для выбора получателей
    private String message;
    private String attachments; // опционально, строка в формате VK API или URL для TG

    // НОВОЕ: рассылка по всем коннекторам
    private boolean sendToAllConnectors = false; // если true - игнорируем connectorType и шлём везде

    public BroadcastRequest() {}

    public BroadcastRequest(String connectorType, List<String> tags, String message, String attachments) {
        this.connectorType = connectorType;
        this.tags = tags;
        this.message = message;
        this.attachments = attachments;
    }

    public String getConnectorType() { return connectorType; }
    public void setConnectorType(String connectorType) { this.connectorType = connectorType; }

    public List<String> getTags() { return tags; }
    public void setTags(List<String> tags) { this.tags = tags; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public String getAttachments() { return attachments; }
    public void setAttachments(String attachments) { this.attachments = attachments; }

    public boolean isSendToAllConnectors() { return sendToAllConnectors; }
    public void setSendToAllConnectors(boolean sendToAllConnectors) { this.sendToAllConnectors = sendToAllConnectors; }
}