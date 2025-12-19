package io.sustc.controller;

import io.sustc.dto.*;
import io.sustc.service.RecipeService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/recipes")
@CrossOrigin(origins = "*")
@Slf4j
public class RecipeController {

    @Autowired
    private RecipeService recipeService;

    @GetMapping("/name/{id}")
    public ResponseEntity<?> getNameFromID(@PathVariable long id) {
        try {
            String name = recipeService.getNameFromID(id);
            return ResponseEntity.ok(Map.of("name", name != null ? name : ""));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/{recipeId}")
    public ResponseEntity<?> getRecipeById(@PathVariable long recipeId) {
        try {
            RecipeRecord recipe = recipeService.getRecipeById(recipeId);
            if (recipe == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "Recipe not found"));
            }
            return ResponseEntity.ok(recipe);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/search")
    public ResponseEntity<?> searchRecipes(
            @RequestParam(required = false) String keyword,
            @RequestParam(required = false) String category,
            @RequestParam(required = false) Double minRating,
            @RequestParam(defaultValue = "1") Integer page,
            @RequestParam(defaultValue = "10") Integer size,
            @RequestParam(required = false) String sort) {
        try {
            PageResult<RecipeRecord> result = recipeService.searchRecipes(keyword, category, minRating, page, size, sort);
            return ResponseEntity.ok(result);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping
    public ResponseEntity<?> createRecipe(@RequestBody Map<String, Object> request) {
        try {
            RecipeRecord dto = parseRecipeRecord(request);
            AuthInfo auth = parseAuthInfo(request);
            
            long recipeId = recipeService.createRecipe(dto, auth);
            return ResponseEntity.ok(Map.of("recipeId", recipeId));
        } catch (SecurityException e) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of("error", e.getMessage()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @DeleteMapping("/{recipeId}")
    public ResponseEntity<?> deleteRecipe(@PathVariable long recipeId, @RequestBody AuthInfo auth) {
        try {
            recipeService.deleteRecipe(recipeId, auth);
            return ResponseEntity.ok(Map.of("success", true));
        } catch (SecurityException e) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @PutMapping("/{recipeId}/times")
    public ResponseEntity<?> updateTimes(
            @PathVariable long recipeId,
            @RequestBody Map<String, Object> request) {
        try {
            AuthInfo auth = parseAuthInfo(request);
            String cookTimeIso = (String) request.get("cookTimeIso");
            String prepTimeIso = (String) request.get("prepTimeIso");
            
            recipeService.updateTimes(auth, recipeId, cookTimeIso, prepTimeIso);
            return ResponseEntity.ok(Map.of("success", true));
        } catch (SecurityException e) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of("error", e.getMessage()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/closest-calorie-pair")
    public ResponseEntity<?> getClosestCaloriePair() {
        try {
            Map<String, Object> result = recipeService.getClosestCaloriePair();
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/top3-complex")
    public ResponseEntity<?> getTop3MostComplexRecipesByIngredients() {
        try {
            List<Map<String, Object>> result = recipeService.getTop3MostComplexRecipesByIngredients();
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    private AuthInfo parseAuthInfo(Map<String, Object> request) {
        AuthInfo auth = new AuthInfo();
        if (request.get("authorId") != null) {
            auth.setAuthorId(((Number) request.get("authorId")).longValue());
        }
        if (request.get("password") != null) {
            auth.setPassword((String) request.get("password"));
        }
        return auth;
    }

    private RecipeRecord parseRecipeRecord(Map<String, Object> request) {
        RecipeRecord dto = new RecipeRecord();
        if (request.get("name") != null) dto.setName((String) request.get("name"));
        if (request.get("description") != null) dto.setDescription((String) request.get("description"));
        if (request.get("recipeCategory") != null) dto.setRecipeCategory((String) request.get("recipeCategory"));
        if (request.get("cookTime") != null) dto.setCookTime((String) request.get("cookTime"));
        if (request.get("prepTime") != null) dto.setPrepTime((String) request.get("prepTime"));
        if (request.get("totalTime") != null) dto.setTotalTime((String) request.get("totalTime"));
        if (request.get("recipeIngredientParts") != null) {
            @SuppressWarnings("unchecked")
            List<String> parts = (List<String>) request.get("recipeIngredientParts");
            dto.setRecipeIngredientParts(parts.toArray(new String[0]));
        }
        // 可以添加更多字段的解析
        return dto;
    }
}

