package io.sustc.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.sustc.dto.ChatResponse;
import io.sustc.dto.PageResult;
import io.sustc.dto.RecipeRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;

@Service
@Slf4j
public class AIService {

    @Value("${deepseek.api-key}")
    private String apiKey;

    @Value("${deepseek.base-url}")
    private String baseUrl;

    @Value("${deepseek.model}")
    private String model;

    @Autowired
    private RecipeService recipeService;

    @Autowired
    private ObjectMapper objectMapper;

    private final RestTemplate restTemplate = new RestTemplate();

    public ChatResponse chat(String userMessage) {
        String keyword = extractKeyword(userMessage);

        PageResult<RecipeRecord> searchResult = recipeService.searchRecipes(
                keyword, null, null, 1, 5, "rating_desc");

        List<RecipeRecord> recipes = searchResult != null ? searchResult.getItems() : Collections.emptyList();

        String recipeContext = buildRecipeContext(recipes);
        String systemPrompt = buildSystemPrompt(recipeContext);
        String llmReply = callDeepSeek(systemPrompt, userMessage);

        return ChatResponse.builder()
                .reply(llmReply)
                .recipes(recipes)
                .build();
    }

    private String extractKeyword(String userMessage) {
        String prompt = "Extract search keywords from the following user message. Return only the keywords separated by spaces, nothing else.\nUser message: " + userMessage;

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("model", model);
        requestBody.put("messages", List.of(
                Map.of("role", "system", "content", "You are a keyword extraction tool. Return only keywords, no explanation or other content."),
                Map.of("role", "user", "content", prompt)
        ));
        requestBody.put("max_tokens", 50);
        requestBody.put("temperature", 0.1);

        try {
            String response = callDeepSeekApi(requestBody);
            return parseKeywordResponse(response).trim();
        } catch (Exception e) {
            log.warn("Keyword extraction failed, using original message: {}", e.getMessage());
            return userMessage;
        }
    }

    private String buildRecipeContext(List<RecipeRecord> recipes) {
        if (recipes.isEmpty()) {
            return "(No matching recipes found in the database)";
        }

        try {
            List<Map<String, Object>> simpleRecipes = new ArrayList<>();
            for (RecipeRecord r : recipes) {
                Map<String, Object> simple = new LinkedHashMap<>();
                simple.put("id", r.getRecipeId());
                simple.put("name", r.getName());
                simple.put("category", r.getRecipeCategory());
                simple.put("description", r.getDescription());
                simple.put("ingredients", r.getRecipeIngredientParts());
                simple.put("rating", r.getAggregatedRating());
                simple.put("calories", r.getCalories());
                simple.put("protein", r.getProteinContent());
                simple.put("fat", r.getFatContent());
                simpleRecipes.add(simple);
            }
            return objectMapper.writeValueAsString(simpleRecipes);
        } catch (Exception e) {
            log.error("Failed to serialize recipes", e);
            return "(Recipe data serialization failed)";
        }
    }

    private String buildSystemPrompt(String recipeContext) {
        return "You are a professional chef and nutrition advisor. Users will ask you for food recommendations.\n" +
                "Answer based on the local recipe data provided below.\n" +
                "If no matching recipes are found, be honest and offer general advice.\n" +
                "Reply in English with a friendly and natural tone.\n" +
                "For each recommended recipe, explain why you recommend it (taste, nutrition, rating, etc.).\n\n" +
                "[Local Recipe Data]\n" + recipeContext;
    }

    private String callDeepSeek(String systemPrompt, String userMessage) {
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("model", model);
        requestBody.put("messages", List.of(
                Map.of("role", "system", "content", systemPrompt),
                Map.of("role", "user", "content", userMessage)
        ));
        requestBody.put("max_tokens", 800);
        requestBody.put("temperature", 0.7);

        return callDeepSeekApi(requestBody);
    }

    private String callDeepSeekApi(Map<String, Object> requestBody) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(apiKey);

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(requestBody, headers);

        try {
            ResponseEntity<Map> response = restTemplate.postForEntity(
                    baseUrl + "/v1/chat/completions",
                    request,
                    Map.class
            );

            if (response.getBody() != null) {
                List<Map<String, Object>> choices = (List<Map<String, Object>>) response.getBody().get("choices");
                if (choices != null && !choices.isEmpty()) {
                    Map<String, Object> message = (Map<String, Object>) choices.get(0).get("message");
                    return (String) message.get("content");
                }
            }
            return "Sorry, the AI service cannot respond right now. Please try again later.";
        } catch (Exception e) {
            log.error("DeepSeek API call failed", e);
            return "Sorry, the AI service is temporarily unavailable: " + e.getMessage();
        }
    }

    private String parseKeywordResponse(String response) {
        return response.replaceAll("(?i)keywords?[：:]\\s*", "").replaceAll("[\\n\\r]", " ").trim();
    }
}
