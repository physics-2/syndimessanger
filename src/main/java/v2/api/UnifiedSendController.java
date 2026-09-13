package v2.api;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import v2.dto.ApiResponse;
import v2.dto.BroadcastRequest;
import v2.dto.BroadcastResponse;
import v2.dto.SendMessageRequest;
import v2.services.BroadcastService;
import v2.services.UnifiedSendService;


@RestController
@RequestMapping("/api/v2")
@RequiredArgsConstructor
@CrossOrigin(origins = "*")
public class UnifiedSendController {

    private  UnifiedSendService sendService;
    private  BroadcastService broadcastService;



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
