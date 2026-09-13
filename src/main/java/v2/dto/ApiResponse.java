package v2.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ApiResponse<T> {
    @JsonProperty("success")
    private boolean success;

    @JsonProperty("data")
    private T data;

    @JsonProperty("error")
    private String error;

    public static <T> ApiResponse<T> success(T data) {
        return new ApiResponse<>(true,data,"");
    }

    public static <T> ApiResponse<T> error(String message) {
        return new ApiResponse<>(false,null,message);
    }

    public ApiResponse(boolean success, T data, String error) {
        this.success = success;
        this.data = data;
        this.error = error;
    }
}