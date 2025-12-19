package io.sustc.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Redis 缓存服务
 * 用于高并发场景下的数据缓存
 * 如果 Redis 不可用，会自动降级到直接查询数据库
 */
@Service
@Slf4j
public class CacheService {

    @Autowired(required = false)
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    // 缓存键前缀
    private static final String RECIPE_PREFIX = "recipe:";
    private static final String RECIPE_SEARCH_PREFIX = "recipe:search:";
    private static final String REVIEW_PREFIX = "review:recipe:";

    // 缓存过期时间（分钟）
    private static final long RECIPE_CACHE_TIME = 60; // 食谱详情缓存1小时
    private static final long SEARCH_CACHE_TIME = 10; // 搜索结果缓存10分钟
    private static final long REVIEW_CACHE_TIME = 30; // 评论列表缓存30分钟

    /**
     * 获取食谱详情缓存
     */
    public <T> T getRecipe(Long recipeId, Class<T> clazz) {
        String key = RECIPE_PREFIX + recipeId;
        return get(key, clazz);
    }

    /**
     * 设置食谱详情缓存
     */
    public void setRecipe(Long recipeId, Object value) {
        String key = RECIPE_PREFIX + recipeId;
        set(key, value, RECIPE_CACHE_TIME);
    }

    /**
     * 删除食谱详情缓存
     */
    public void deleteRecipe(Long recipeId) {
        String key = RECIPE_PREFIX + recipeId;
        delete(key);
    }

    /**
     * 获取搜索结果缓存
     */
    @SuppressWarnings("unchecked")
    public <T> T getSearchResult(String keyword, String category, Double minRating, 
                                 Integer page, Integer size, String sort, Class<T> clazz) {
        if (redisTemplate == null) {
            return null; // Redis 不可用，直接返回 null
        }
        String key = buildSearchKey(keyword, category, minRating, page, size, sort);
        try {
            Object value = redisTemplate.opsForValue().get(RECIPE_SEARCH_PREFIX + key);
            if (value == null) {
                return null;
            }
            if (clazz.isInstance(value)) {
                return (T) value;
            }
            return (T) objectMapper.convertValue(value, clazz);
        } catch (Exception e) {
            log.error("Redis getSearchResult error, key: {}", key, e);
            return null;
        }
    }

    /**
     * 设置搜索结果缓存
     */
    public void setSearchResult(String keyword, String category, Double minRating,
                               Integer page, Integer size, String sort, Object value) {
        String key = buildSearchKey(keyword, category, minRating, page, size, sort);
        set(RECIPE_SEARCH_PREFIX + key, value, SEARCH_CACHE_TIME);
    }

    /**
     * 删除所有搜索结果缓存（当有新的食谱创建或更新时）
     */
    public void deleteAllSearchResults() {
        if (redisTemplate == null) {
            return; // Redis 不可用，直接返回
        }
        try {
            Set<String> keys = redisTemplate.keys(RECIPE_SEARCH_PREFIX + "*");
            if (keys != null && !keys.isEmpty()) {
                redisTemplate.delete(keys);
            }
        } catch (Exception e) {
            log.error("Redis deleteAllSearchResults error", e);
        }
    }

    /**
     * 获取评论列表缓存
     */
    @SuppressWarnings("unchecked")
    public <T> T getReviews(Long recipeId, Integer page, Integer size, String sort, Class<T> clazz) {
        String key = buildReviewKey(recipeId, page, size, sort);
        return get(REVIEW_PREFIX + key, clazz);
    }

    /**
     * 设置评论列表缓存
     */
    public void setReviews(Long recipeId, Integer page, Integer size, String sort, Object value) {
        String key = buildReviewKey(recipeId, page, size, sort);
        set(REVIEW_PREFIX + key, value, REVIEW_CACHE_TIME);
    }

    /**
     * 删除某个食谱的所有评论缓存（当有新评论时）
     */
    public void deleteReviews(Long recipeId) {
        if (redisTemplate == null) {
            return; // Redis 不可用，直接返回
        }
        try {
            Set<String> keys = redisTemplate.keys(REVIEW_PREFIX + recipeId + ":*");
            if (keys != null && !keys.isEmpty()) {
                redisTemplate.delete(keys);
            }
        } catch (Exception e) {
            log.error("Redis deleteReviews error, recipeId: {}", recipeId, e);
        }
    }

    /**
     * 通用获取方法
     */
    @SuppressWarnings("unchecked")
    private <T> T get(String key, Class<T> clazz) {
        if (redisTemplate == null) {
            return null; // Redis 不可用，直接返回 null，触发数据库查询
        }
        try {
            Object value = redisTemplate.opsForValue().get(key);
            if (value == null) {
                return null;
            }
            if (clazz.isInstance(value)) {
                return (T) value;
            }
            // 如果是 JSON 字符串，需要反序列化
            return objectMapper.convertValue(value, clazz);
        } catch (Exception e) {
            log.error("Redis get error, key: {}", key, e);
            return null;
        }
    }

    /**
     * 通用设置方法
     */
    private void set(String key, Object value, long timeoutMinutes) {
        if (redisTemplate == null) {
            return; // Redis 不可用，直接返回
        }
        try {
            redisTemplate.opsForValue().set(key, value, timeoutMinutes, TimeUnit.MINUTES);
        } catch (Exception e) {
            log.error("Redis set error, key: {}", key, e);
        }
    }

    /**
     * 通用删除方法
     */
    private void delete(String key) {
        if (redisTemplate == null) {
            return; // Redis 不可用，直接返回
        }
        try {
            redisTemplate.delete(key);
        } catch (Exception e) {
            log.error("Redis delete error, key: {}", key, e);
        }
    }

    /**
     * 构建搜索缓存键
     */
    private String buildSearchKey(String keyword, String category, Double minRating,
                                  Integer page, Integer size, String sort) {
        StringBuilder sb = new StringBuilder();
        sb.append(keyword != null ? keyword : "null");
        sb.append(":");
        sb.append(category != null ? category : "null");
        sb.append(":");
        sb.append(minRating != null ? minRating : "null");
        sb.append(":");
        sb.append(page);
        sb.append(":");
        sb.append(size);
        sb.append(":");
        sb.append(sort != null ? sort : "null");
        return sb.toString();
    }

    /**
     * 构建评论缓存键
     */
    private String buildReviewKey(Long recipeId, Integer page, Integer size, String sort) {
        return recipeId + ":" + page + ":" + size + ":" + (sort != null ? sort : "null");
    }
}
