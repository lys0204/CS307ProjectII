package io.sustc.controller;

import io.sustc.dto.ChatRequest;
import io.sustc.dto.ChatResponse;
import io.sustc.service.AIService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
@Slf4j
public class ChatController {

    @Autowired
    private AIService aiService;

    @PostMapping("/chat")
    public ResponseEntity<?> chat(@RequestBody ChatRequest request) {
        if (request.getMessage() == null || request.getMessage().trim().isEmpty()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(ChatResponse.builder()
                            .reply("Please tell me what you'd like to eat, e.g. 'I want a light chicken dish'")
                            .build());
        }

        try {
            ChatResponse response = aiService.chat(request.getMessage().trim());
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Chat error", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ChatResponse.builder()
                            .reply("Sorry, the AI service is temporarily unavailable: " + e.getMessage())
                            .build());
        }
    }
}
