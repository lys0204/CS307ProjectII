package io.sustc.service;

import io.sustc.agent.RecipeTools;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.openai.OpenAiChatModel;
import dev.langchain4j.service.AiServices;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.EmbeddingStore;
import io.sustc.dto.ChatResponse;
import io.sustc.dto.RecipeRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@Slf4j
public class AIService {

    @Autowired
    private OpenAiChatModel chatLanguageModel;

    @Autowired
    private EmbeddingModel embeddingModel;

    @Autowired
    private EmbeddingStore<TextSegment> embeddingStore;

    @Autowired
    private RecipeTools recipeTools;

    interface Assistant {
        String chat(String userMessage);
    }

    /**
     * Convert a RecipeRecord to a text segment for embedding.
     */
    public static TextSegment recipeToTextSegment(RecipeRecord r) {
        StringBuilder sb = new StringBuilder();
        sb.append("Recipe: ").append(r.getName()).append("\n");
        sb.append("Category: ").append(r.getRecipeCategory()).append("\n");
        if (r.getDescription() != null && !r.getDescription().isEmpty()) {
            sb.append("Description: ").append(r.getDescription()).append("\n");
        }
        if (r.getRecipeIngredientParts() != null && r.getRecipeIngredientParts().length > 0) {
            sb.append("Ingredients: ").append(String.join(", ", r.getRecipeIngredientParts())).append("\n");
        }
        if (r.getCalories() > 0) {
            sb.append("Calories: ").append(r.getCalories()).append("\n");
        }
        if (r.getAggregatedRating() > 0) {
            sb.append("Rating: ").append(String.format("%.1f", r.getAggregatedRating())).append("/5\n");
        }
        sb.append("Protein: ").append(r.getProteinContent()).append("g, ");
        sb.append("Fat: ").append(r.getFatContent()).append("g\n");
        return TextSegment.from(sb.toString());
    }

    public ChatResponse chat(String userMessage) {
        // Step 1: embed user query
        Embedding queryEmbedding = embeddingModel.embed(userMessage).content();

        // Step 2: search vector store
        EmbeddingSearchRequest searchRequest = EmbeddingSearchRequest.builder()
                .queryEmbedding(queryEmbedding)
                .maxResults(5)
                .build();
        List<EmbeddingMatch<TextSegment>> matches = embeddingStore.search(searchRequest).matches();
        List<RecipeRecord> recipes = new ArrayList<>();
        StringBuilder contextBuilder = new StringBuilder();
        for (EmbeddingMatch<TextSegment> match : matches) {
            String recipeId = match.embedded().metadata().getString("recipeId");
            if (recipeId != null) {
                contextBuilder.append("[Recipe ID: ").append(recipeId).append("]\n");
            }
            contextBuilder.append(match.embedded().text()).append("\n---\n");
        }

        String context = contextBuilder.length() > 0 ? contextBuilder.toString()
                : "(No recipes found in the database matching this query)";

        // Step 3: build system prompt with RAG context
        String systemPrompt = "You are a professional chef and nutrition advisor. " +
                "Answer user questions based on the recipe data provided below. " +
                "If the data doesn't contain relevant recipes, say so honestly. " +
                "Be friendly and helpful. For each recipe you recommend, briefly explain why.\n\n" +
                "[Recipe Database]\n" + context;

        // Step 4: create Agent with tools and chat
        Assistant assistant = AiServices.builder(Assistant.class)
                .chatLanguageModel(chatLanguageModel)
                .tools(recipeTools)
                .build();

        List<ChatMessage> messages = new ArrayList<>();
        messages.add(new SystemMessage(systemPrompt));
        messages.add(new UserMessage(userMessage));

        String reply;
        try {
            // Step 5: collect recipe details from embedding matches for the response
            for (EmbeddingMatch<TextSegment> match : matches) {
                String recipeId = match.embedded().metadata().getString("recipeId");
                if (recipeId != null) {
                    try {
                        RecipeRecord r = recipeTools.getRecipeDetail(Long.parseLong(recipeId));
                        if (r != null) {
                            recipes.add(r);
                        }
                    } catch (Exception e) {
                        log.debug("Failed to load recipe {}: {}", recipeId, e.getMessage());
                    }
                }
            }

            reply = assistant.chat(userMessage);
        } catch (Exception e) {
            log.error("Agent chat failed", e);
            reply = "Sorry, the AI service is temporarily unavailable: " + e.getMessage();
        }

        return ChatResponse.builder()
                .reply(reply)
                .recipes(recipes)
                .build();
    }
}
