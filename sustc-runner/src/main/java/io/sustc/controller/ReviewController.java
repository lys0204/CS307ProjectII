package io.sustc.controller;

import io.sustc.dto.*;
import io.sustc.service.ReviewService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/reviews")
@CrossOrigin(origins = "*")
@Slf4j
public class ReviewController {

    @Autowired
    private ReviewService reviewService;

    @PostMapping
    public ResponseEntity<?> addReview(@RequestBody Map<String, Object> request) {
        try {
            AuthInfo auth = parseAuthInfo(request);
            long recipeId = ((Number) request.get("recipeId")).longValue();
            int rating = ((Number) request.get("rating")).intValue();
            String review = (String) request.get("review");
            
            long reviewId = reviewService.addReview(auth, recipeId, rating, review);
            return ResponseEntity.ok(Map.of("reviewId", reviewId));
        } catch (SecurityException e) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of("error", e.getMessage()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @PutMapping("/{reviewId}")
    public ResponseEntity<?> editReview(
            @PathVariable long reviewId,
            @RequestBody Map<String, Object> request) {
        try {
            AuthInfo auth = parseAuthInfo(request);
            long recipeId = ((Number) request.get("recipeId")).longValue();
            int rating = ((Number) request.get("rating")).intValue();
            String review = (String) request.get("review");
            
            reviewService.editReview(auth, recipeId, reviewId, rating, review);
            return ResponseEntity.ok(Map.of("success", true));
        } catch (SecurityException e) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of("error", e.getMessage()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @DeleteMapping("/{reviewId}")
    public ResponseEntity<?> deleteReview(
            @PathVariable long reviewId,
            @RequestBody Map<String, Object> request) {
        try {
            AuthInfo auth = parseAuthInfo(request);
            long recipeId = ((Number) request.get("recipeId")).longValue();
            
            reviewService.deleteReview(auth, recipeId, reviewId);
            return ResponseEntity.ok(Map.of("success", true));
        } catch (SecurityException e) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of("error", e.getMessage()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/{reviewId}/like")
    public ResponseEntity<?> likeReview(@PathVariable long reviewId, @RequestBody AuthInfo auth) {
        try {
            long likes = reviewService.likeReview(auth, reviewId);
            return ResponseEntity.ok(Map.of("likes", likes));
        } catch (SecurityException e) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of("error", e.getMessage()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @DeleteMapping("/{reviewId}/like")
    public ResponseEntity<?> unlikeReview(@PathVariable long reviewId, @RequestBody AuthInfo auth) {
        try {
            long likes = reviewService.unlikeReview(auth, reviewId);
            return ResponseEntity.ok(Map.of("likes", likes));
        } catch (SecurityException e) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of("error", e.getMessage()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/recipe/{recipeId}")
    public ResponseEntity<?> listByRecipe(
            @PathVariable long recipeId,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "10") int size,
            @RequestParam(required = false) String sort) {
        try {
            PageResult<ReviewRecord> result = reviewService.listByRecipe(recipeId, page, size, sort);
            return ResponseEntity.ok(result);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/recipe/{recipeId}/refresh-rating")
    public ResponseEntity<?> refreshRecipeAggregatedRating(@PathVariable long recipeId) {
        try {
            RecipeRecord recipe = reviewService.refreshRecipeAggregatedRating(recipeId);
            return ResponseEntity.ok(recipe);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
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
}

