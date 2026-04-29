package io.sustc.init;

import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.store.embedding.EmbeddingStore;
import io.sustc.dto.RecipeRecord;
import io.sustc.dto.ReviewRecord;
import io.sustc.dto.UserRecord;
import io.sustc.service.AIService;
import io.sustc.service.DatabaseService;
import io.sustc.util.CsvParser;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.util.List;

@Component
@Slf4j
@Order(1)
public class DataInitializer implements CommandLineRunner {

    @Autowired
    private DatabaseService databaseService;

    @Autowired
    private EmbeddingModel embeddingModel;

    @Autowired
    private EmbeddingStore<TextSegment> embeddingStore;

    @Override
    public void run(String... args) {
        log.info("Starting data import (tables will be rebuilt if schema changed)...");

        try {
            List<UserRecord> users;
            try (InputStream is = getClass().getResourceAsStream("/data/users.csv")) {
                if (is == null) {
                    log.error("users.csv not found on classpath");
                    return;
                }
                users = CsvParser.loadUsers(is);
            }

            List<RecipeRecord> recipes;
            try (InputStream is = getClass().getResourceAsStream("/data/recipes.csv")) {
                if (is == null) {
                    log.error("recipes.csv not found on classpath");
                    return;
                }
                recipes = CsvParser.loadRecipes(is);
            }

            List<ReviewRecord> reviews;
            try (InputStream is = getClass().getResourceAsStream("/data/reviews.csv")) {
                if (is == null) {
                    log.error("reviews.csv not found on classpath");
                    return;
                }
                reviews = CsvParser.loadReviews(is);
            }

            databaseService.importData(reviews, users, recipes);
            log.info("Import complete: {} users, {} recipes, {} reviews",
                    users.size(), recipes.size(), reviews.size());

            // Build vector index
            log.info("Embedding {} recipes into vector store...", recipes.size());
            int embedded = 0;
            for (RecipeRecord r : recipes) {
                try {
                    TextSegment segment = AIService.recipeToTextSegment(r);
                    segment.metadata().put("recipeId", String.valueOf(r.getRecipeId()));
                    Embedding embedding = embeddingModel.embed(segment).content();
                    embeddingStore.add(embedding, segment);
                    embedded++;
                } catch (Exception e) {
                    log.debug("Failed to embed recipe {}: {}", r.getRecipeId(), e.getMessage());
                }
            }
            log.info("Embedded {}/{} recipes into memory store", embedded, recipes.size());

        } catch (Exception e) {
            log.error("Failed to import CSV data on startup", e);
        }
    }
}
