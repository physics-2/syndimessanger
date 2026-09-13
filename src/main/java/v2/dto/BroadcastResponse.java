package v2.dto;

import java.util.Map;

public class BroadcastResponse {
    private int totalRecipients;
    private int successCount;
    private int failedCount;
    private Map<String, String> errors; // key: userId или peerId, value: ошибка

    public BroadcastResponse() {}

    public BroadcastResponse(int totalRecipients, int successCount, int failedCount, Map<String, String> errors) {
        this.totalRecipients = totalRecipients;
        this.successCount = successCount;
        this.failedCount = failedCount;
        this.errors = errors;
    }

    public int getTotalRecipients() { return totalRecipients; }
    public void setTotalRecipients(int totalRecipients) { this.totalRecipients = totalRecipients; }

    public int getSuccessCount() { return successCount; }
    public void setSuccessCount(int successCount) { this.successCount = successCount; }

    public int getFailedCount() { return failedCount; }
    public void setFailedCount(int failedCount) { this.failedCount = failedCount; }

    public Map<String, String> getErrors() { return errors; }
    public void setErrors(Map<String, String> errors) { this.errors = errors; }
}
