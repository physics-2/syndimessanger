package v2.api;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import v2.dto.ApiResponse;
import v2.dto.BroadcastRequest;
import v2.dto.BroadcastResponse;
import v2.dto.SendMessageRequest;
import v2.entity.Chat;
import v2.services.BroadcastService;
import v2.services.ChatService;
import v2.services.UnifiedSendService;

import java.util.List;


@RestController
@RequestMapping("/api/v2")
@CrossOrigin(origins = "*")
public class UnifiedSendController {

    private  UnifiedSendService sendService;
    private  BroadcastService broadcastService;


    public UnifiedSendController(UnifiedSendService sendService, BroadcastService broadcastService) {
        this.sendService = sendService;
        this.broadcastService = broadcastService;
    }

    @PostMapping("/send")
    public ApiResponse<Long> sendMessage(@RequestBody SendMessageRequest request) {
        return sendService.sendMessage(
                request.getConnector(),
                request.getPeer(),
                request.getMessage(),
                request.getAttachments(),
                request.getReplyTo()
        );
    }



    @PostMapping("/broadcast")
    public ApiResponse<BroadcastResponse> sendBroadcast(@RequestBody BroadcastRequest request) {
        BroadcastResponse response = broadcastService.sendBroadcast(request);
        return new ApiResponse<BroadcastResponse>(true, response, "200");
    }
}
