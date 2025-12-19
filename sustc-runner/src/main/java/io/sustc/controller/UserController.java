package io.sustc.controller;

import io.sustc.dto.*;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/users")
@CrossOrigin(origins = "*")
@Slf4j
public class UserController {

    @Autowired
    private UserService userService;

    @PostMapping("/register")
    public ResponseEntity<?> register(@RequestBody RegisterUserReq req) {
        try {
            long userId = userService.register(req);
            if (userId == -1) {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", "Registration failed"));
            }
            return ResponseEntity.ok(Map.of("userId", userId));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/login")
    public ResponseEntity<?> login(@RequestBody AuthInfo auth) {
        try {
            long userId = userService.login(auth);
            if (userId == -1) {
                return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "Login failed"));
            }
            return ResponseEntity.ok(Map.of("userId", userId));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/{userId}")
    public ResponseEntity<?> getById(@PathVariable long userId) {
        try {
            UserRecord user = userService.getById(userId);
            return ResponseEntity.ok(user);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/follow")
    public ResponseEntity<?> follow(@RequestBody Map<String, Object> request) {
        try {
            AuthInfo auth = new AuthInfo();
            auth.setAuthorId(((Number) request.get("authorId")).longValue());
            auth.setPassword((String) request.get("password"));
            long followeeId = ((Number) request.get("followeeId")).longValue();
            
            boolean result = userService.follow(auth, followeeId);
            return ResponseEntity.ok(Map.of("success", result));
        } catch (SecurityException e) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/feed")
    public ResponseEntity<?> feed(@RequestBody Map<String, Object> request) {
        try {
            AuthInfo auth = new AuthInfo();
            auth.setAuthorId(((Number) request.get("authorId")).longValue());
            auth.setPassword((String) request.get("password"));
            int page = request.get("page") != null ? ((Number) request.get("page")).intValue() : 1;
            int size = request.get("size") != null ? ((Number) request.get("size")).intValue() : 10;
            String category = (String) request.get("category");
            
            PageResult<FeedItem> result = userService.feed(auth, page, size, category);
            return ResponseEntity.ok(result);
        } catch (SecurityException e) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @PutMapping("/profile")
    public ResponseEntity<?> updateProfile(@RequestBody Map<String, Object> request) {
        try {
            AuthInfo auth = new AuthInfo();
            auth.setAuthorId(((Number) request.get("authorId")).longValue());
            auth.setPassword((String) request.get("password"));
            String gender = (String) request.get("gender");
            Integer age = request.get("age") != null ? ((Number) request.get("age")).intValue() : null;
            
            userService.updateProfile(auth, gender, age);
            return ResponseEntity.ok(Map.of("success", true));
        } catch (SecurityException e) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of("error", e.getMessage()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @DeleteMapping("/{userId}")
    public ResponseEntity<?> deleteAccount(@RequestBody AuthInfo auth, @PathVariable long userId) {
        try {
            boolean result = userService.deleteAccount(auth, userId);
            return ResponseEntity.ok(Map.of("success", result));
        } catch (SecurityException e) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of("error", e.getMessage()));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/highest-follow-ratio")
    public ResponseEntity<?> getUserWithHighestFollowRatio() {
        try {
            Map<String, Object> result = userService.getUserWithHighestFollowRatio();
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }
}

