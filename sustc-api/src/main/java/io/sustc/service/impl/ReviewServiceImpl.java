package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PageResult;
import io.sustc.dto.RecipeRecord;
import io.sustc.dto.ReviewRecord;
import io.sustc.service.RecipeService;
import io.sustc.service.ReviewService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.util.*;

@Service
@Slf4j
public class ReviewServiceImpl implements ReviewService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private RecipeService recipeService;

    /**
     * 校验 auth 对应的用户是否存在且未被软删除，返回用户 ID
     */
    private long requireActiveUser(AuthInfo auth) {
        if (auth == null) {
            throw new SecurityException("auth is null");
        }
        long userId = auth.getAuthorId();
        try {
            Boolean isDeleted = jdbcTemplate.queryForObject(
                    "SELECT IsDeleted FROM users WHERE AuthorId = ?",
                    Boolean.class,
                    userId
            );
            if (isDeleted == null || isDeleted) {
                throw new SecurityException("user is inactive");
            }
            return userId;
        } catch (EmptyResultDataAccessException e) {
            throw new SecurityException("user does not exist", e);
        }
    }

    /**
     * 校验 auth 对应的用户是否存在、未被软删除且密码正确，返回用户 ID
     * 用于 likeReview 和 unlikeReview，需要验证密码
     */
    private long requireActiveUserWithPassword(AuthInfo auth) {
        if (auth == null) {
            throw new SecurityException("auth is null");
        }
        long userId = auth.getAuthorId();
        String password = auth.getPassword();
        try {
            Map<String, Object> row = jdbcTemplate.queryForMap(
                    "SELECT Password, IsDeleted FROM users WHERE AuthorId = ?",
                    userId
            );
            Map<String, Object> lowerCaseRow = new HashMap<>();
            row.forEach((k, v) -> lowerCaseRow.put(k.toLowerCase(), v));
            
            Boolean isDeleted = (Boolean) lowerCaseRow.get("isdeleted");
            if (isDeleted == null || isDeleted) {
                throw new SecurityException("user is inactive");
            }
            Object storedPwdObj = lowerCaseRow.get("password");
            String storedPwd = storedPwdObj == null ? null : storedPwdObj.toString();
            if (password == null || !password.equals(storedPwd)) {
                throw new SecurityException("password mismatch");
            }
            return userId;
        } catch (EmptyResultDataAccessException e) {
            throw new SecurityException("user does not exist", e);
        }
    }

    @Override
    @Transactional
    public long addReview(AuthInfo auth, long recipeId, int rating, String review) {
        long authorId = requireActiveUser(auth);

        if (rating < 1 || rating > 5) {
            throw new IllegalArgumentException("rating must be between 1 and 5");
        }

        // 检查菜谱是否存在
        Boolean recipeExists = jdbcTemplate.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM recipes WHERE RecipeId = ?)",
                Boolean.class,
                recipeId
        );
        if (recipeExists == null || !recipeExists) {
            throw new IllegalArgumentException("recipe does not exist");
        }

        // 生成新的 ReviewId
        Long newReviewId = jdbcTemplate.queryForObject(
                "SELECT COALESCE(MAX(ReviewId), 0) + 1 FROM reviews",
                Long.class
        );

        Timestamp now = new Timestamp(System.currentTimeMillis());

        // 插入评论
        jdbcTemplate.update(
                "INSERT INTO reviews (ReviewId, RecipeId, AuthorId, Rating, Review, DateSubmitted, DateModified) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                newReviewId,
                recipeId,
                authorId,
                rating,
                review,
                now,
                now
        );

        // 刷新菜谱评分
        refreshRecipeAggregatedRating(recipeId);

        return newReviewId;
    }

    @Override
    @Transactional
    public void editReview(AuthInfo auth, long recipeId, long reviewId, int rating, String review) {
        long operatorId = requireActiveUser(auth);

        if (rating < 1 || rating > 5) {
            throw new IllegalArgumentException("rating must be between 1 and 5");
        }

        // 检查评论是否存在且属于该菜谱
        Long reviewAuthorId;
        try {
            reviewAuthorId = jdbcTemplate.queryForObject(
                    "SELECT AuthorId FROM reviews WHERE ReviewId = ? AND RecipeId = ?",
                    Long.class,
                    reviewId,
                    recipeId
            );
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("review does not belong to recipe");
        }

        // 检查权限
        if (reviewAuthorId != operatorId) {
            throw new SecurityException("only review author can edit review");
        }

        // 更新评论
        Timestamp now = new Timestamp(System.currentTimeMillis());
        jdbcTemplate.update(
                "UPDATE reviews SET Rating = ?, Review = ?, DateModified = ? WHERE ReviewId = ?",
                rating,
                review,
                now,
                reviewId
        );

        // 刷新菜谱评分
        refreshRecipeAggregatedRating(recipeId);
    }

    @Override
    @Transactional
    public void deleteReview(AuthInfo auth, long recipeId, long reviewId) {
        long operatorId = requireActiveUser(auth);

        // 检查评论是否存在且属于该菜谱
        Long reviewAuthorId;
        try {
            reviewAuthorId = jdbcTemplate.queryForObject(
                    "SELECT AuthorId FROM reviews WHERE ReviewId = ? AND RecipeId = ?",
                    Long.class,
                    reviewId,
                    recipeId
            );
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("review does not belong to recipe");
        }

        // 检查权限
        if (reviewAuthorId != operatorId) {
            throw new SecurityException("only review author can delete review");
        }

        // 删除点赞
        jdbcTemplate.update("DELETE FROM review_likes WHERE ReviewId = ?", reviewId);

        // 删除评论
        jdbcTemplate.update("DELETE FROM reviews WHERE ReviewId = ?", reviewId);

        // 刷新菜谱评分
        refreshRecipeAggregatedRating(recipeId);
    }

    @Override
    @Transactional
    public long likeReview(AuthInfo auth, long reviewId) {
        long userId = requireActiveUserWithPassword(auth);

        // 检查评论是否存在
        Long reviewAuthorId;
        try {
            reviewAuthorId = jdbcTemplate.queryForObject(
                    "SELECT AuthorId FROM reviews WHERE ReviewId = ?",
                    Long.class,
                    reviewId
            );
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("review does not exist");
        }

        // 不能给自己的评论点赞
        if (reviewAuthorId == userId) {
            throw new SecurityException("cannot like own review");
        }

        // 插入点赞（如果已存在则忽略）
        jdbcTemplate.update(
                "INSERT INTO review_likes (ReviewId, AuthorId) VALUES (?, ?) " +
                        "ON CONFLICT (ReviewId, AuthorId) DO NOTHING",
                reviewId,
                userId
        );

        // 返回当前总点赞数
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM review_likes WHERE ReviewId = ?",
                Integer.class,
                reviewId
        );
        return count == null ? 0 : count;
    }

    @Override
    @Transactional
    public long unlikeReview(AuthInfo auth, long reviewId) {
        long userId = requireActiveUserWithPassword(auth);

        // 检查评论是否存在
        Boolean reviewExists = jdbcTemplate.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM reviews WHERE ReviewId = ?)",
                Boolean.class,
                reviewId
        );
        if (reviewExists == null || !reviewExists) {
            throw new IllegalArgumentException("review does not exist");
        }

        // 删除点赞（如果不存在则忽略）
        jdbcTemplate.update(
                "DELETE FROM review_likes WHERE ReviewId = ? AND AuthorId = ?",
                reviewId,
                userId
        );

        // 返回当前总点赞数
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM review_likes WHERE ReviewId = ?",
                Integer.class,
                reviewId
        );
        return count == null ? 0 : count;
    }

    @Override
    public PageResult<ReviewRecord> listByRecipe(long recipeId, int page, int size, String sort) {
        if (page < 1) {
            throw new IllegalArgumentException("page must be >= 1");
        }
        if (size <= 0) {
            throw new IllegalArgumentException("size must be > 0");
        }

        // 统计总数
        Long total = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM reviews WHERE RecipeId = ?",
                Long.class,
                recipeId
        );
        if (total == null) total = 0L;

        // 构建排序
        String orderBy = "ORDER BY r.DateModified DESC";
        String fromClause = "FROM reviews r " +
                "JOIN users u ON r.AuthorId = u.AuthorId ";
        if (sort != null && "likes_desc".equals(sort)) {
            fromClause = "FROM reviews r " +
                    "JOIN users u ON r.AuthorId = u.AuthorId " +
                    "LEFT JOIN review_likes rl ON r.ReviewId = rl.ReviewId ";
            orderBy = "ORDER BY COUNT(rl.AuthorId) DESC, r.DateModified DESC";
        } else if (sort != null && "date_desc".equals(sort)) {
            orderBy = "ORDER BY r.DateModified DESC";
        }

        // 分页查询
        int offset = (page - 1) * size;
        String sql = "SELECT r.ReviewId, r.RecipeId, r.AuthorId, u.AuthorName, r.Rating, r.Review, " +
                "r.DateSubmitted, r.DateModified " +
                fromClause +
                "WHERE r.RecipeId = ? " +
                (sort != null && "likes_desc".equals(sort) ? "GROUP BY r.ReviewId, r.RecipeId, r.AuthorId, u.AuthorName, r.Rating, r.Review, r.DateSubmitted, r.DateModified " : "") +
                orderBy + " LIMIT ? OFFSET ?";

        List<ReviewRecord> reviews = jdbcTemplate.query(sql, (rs, rowNum) -> {
            ReviewRecord rec = new ReviewRecord();
            rec.setReviewId(rs.getLong("ReviewId"));
            rec.setRecipeId(rs.getLong("RecipeId"));
            rec.setAuthorId(rs.getLong("AuthorId"));
            rec.setAuthorName(rs.getString("AuthorName"));
            rec.setRating(rs.getFloat("Rating"));
            rec.setReview(rs.getString("Review"));
            rec.setDateSubmitted(rs.getTimestamp("DateSubmitted"));
            rec.setDateModified(rs.getTimestamp("DateModified"));
            return rec;
        }, recipeId, size, offset);

        // 为每个评论查询点赞列表
        for (ReviewRecord rec : reviews) {
            List<Long> likes = jdbcTemplate.queryForList(
                    "SELECT AuthorId FROM review_likes WHERE ReviewId = ?",
                    Long.class,
                    rec.getReviewId()
            );
            long[] likesArray = new long[likes.size()];
            for (int i = 0; i < likes.size(); i++) {
                likesArray[i] = likes.get(i);
            }
            rec.setLikes(likesArray);
        }

        PageResult<ReviewRecord> result = new PageResult<>();
        result.setItems(reviews);
        result.setPage(page);
        result.setSize(size);
        result.setTotal(total);
        return result;
    }

    @Override
    @Transactional
    public RecipeRecord refreshRecipeAggregatedRating(long recipeId) {
        // 检查菜谱是否存在
        Boolean recipeExists = jdbcTemplate.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM recipes WHERE RecipeId = ?)",
                Boolean.class,
                recipeId
        );
        if (recipeExists == null || !recipeExists) {
            throw new IllegalArgumentException("recipe does not exist");
        }

        // 计算平均评分和评论数
        Map<String, Object> stats = jdbcTemplate.queryForMap(
                "SELECT " +
                        "COUNT(*) AS ReviewCount, " +
                        "ROUND(AVG(Rating)::numeric, 2) AS AvgRating " +
                        "FROM reviews WHERE RecipeId = ?",
                recipeId
        );

        Integer reviewCount = ((Number) stats.get("reviewcount")).intValue();
        Object avgRatingObj = stats.get("avgrating");

        // 更新菜谱
        if (reviewCount == 0 || avgRatingObj == null) {
            jdbcTemplate.update(
                    "UPDATE recipes SET AggregatedRating = NULL, ReviewCount = 0 WHERE RecipeId = ?",
                    recipeId
            );
        } else {
            Double avgRating = ((Number) avgRatingObj).doubleValue();
            jdbcTemplate.update(
                    "UPDATE recipes SET AggregatedRating = ?, ReviewCount = ? WHERE RecipeId = ?",
                    avgRating,
                    reviewCount,
                    recipeId
            );
        }

        // 返回更新后的菜谱记录
        return recipeService.getRecipeById(recipeId);
    }
}
