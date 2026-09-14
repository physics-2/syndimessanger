package v2.api;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import v2.services.ChatService;

import java.util.List;

@RestController
@RequestMapping("/api/v2")
public class ChatsController {
    ChatService chatService;
    public ChatsController(ChatService chatService){
        this.chatService = chatService;
    }

    @GetMapping("/chats")
    public List<ChatService.ChatDto> getChats(){
        return chatService.getAllChatsForFrontend();
    }
}
