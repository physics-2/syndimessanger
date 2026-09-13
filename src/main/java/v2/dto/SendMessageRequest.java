package v2.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SendMessageRequest {
    @JsonProperty("connector")
    private String connector; // "vk", "tg", "max"

    @JsonProperty("peer")
    private String peer; // ID получателя (чат, пользователь)

    @JsonProperty("message")
    private String message; // Текст сообщения

    @JsonProperty("attachments")
    private String attachments; // Строка с вложениями (опционально)

    @JsonProperty("replyTo")
    private Long replyTo; // ID сообщения для ответа (опционально)

    public String getConnector() {
        return connector;
    }

    public void setConnector(String connector) {
        this.connector = connector;
    }

    public String getPeer() {
        return peer;
    }

    public void setPeer(String peer) {
        this.peer = peer;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getAttachments() {
        return attachments;
    }

    public void setAttachments(String attachments) {
        this.attachments = attachments;
    }

    public Long getReplyTo() {
        return replyTo;
    }

    public void setReplyTo(Long replyTo) {
        this.replyTo = replyTo;
    }
}