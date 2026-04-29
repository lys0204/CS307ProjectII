package io.sustc.agent;

import dev.langchain4j.agent.tool.Tool;
import io.sustc.dto.PageResult;
import io.sustc.dto.RecipeRecord;
import io.sustc.dto.ReviewRecord;
import io.sustc.service.RecipeService;
import io.sustc.service.ReviewService;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

@Component
public class RecipeTools {

    private final RecipeService recipeService;
    private final ReviewService reviewService;

    public RecipeTools(RecipeService recipeService, ReviewService reviewService) {
        this.recipeService = recipeService;
        this.reviewService = reviewService;
    }

    @Tool("Search recipes by keyword. Returns matching recipes with name, description, ingredients, rating and calories.")
    public List<RecipeRecord> searchRecipesByKeyword(String keyword) {
        PageResult<RecipeRecord> result = recipeService.searchRecipes(keyword, null, null, 1, 5, "rating_desc");
        return result != null ? result.getItems() : Collections.emptyList();
    }

    @Tool("Get full details of a recipe by its ID, including all nutrition facts and ingredients")
    public RecipeRecord getRecipeDetail(long recipeId) {
        return recipeService.getRecipeById(recipeId);
    }

    @Tool("Get reviews for a recipe by its ID. Returns page of reviews with ratings and comments.")
    public List<ReviewRecord> getRecipeReviews(long recipeId) {
        PageResult<ReviewRecord> result = reviewService.listByRecipe(recipeId, 1, 5, "date_desc");
        return result != null ? result.getItems() : Collections.emptyList();
    }
}
