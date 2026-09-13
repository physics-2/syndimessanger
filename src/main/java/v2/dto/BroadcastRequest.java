package v2.dto;

import java.util.List;

public class BroadcastRequest {
    private String connectorType; // "vk", "tg", "max"
    private List<String> tags;
    private String message;
    private String attachments; // опционально, строка в формате VK API или URL для TG

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
}
