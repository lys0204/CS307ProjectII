package io.sustc.service.impl;

import io.sustc.dto.ReviewRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.RecipeRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
     */
    @Autowired
    private DataSource dataSource;

    @Override
    public List<Integer> getGroupMembers() {
        return Arrays.asList(12412308, 12412310);
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    @Transactional
    public void importData(
            List<ReviewRecord> reviewRecords,
            List<UserRecord> userRecords,
            List<RecipeRecord> recipeRecords) {

        // ddl to create tables.
        createTables();

        // 清空旧数据（按外键从子表到父表的顺序）
        jdbcTemplate.update("DELETE FROM review_likes");
        jdbcTemplate.update("DELETE FROM reviews");
        jdbcTemplate.update("DELETE FROM recipe_ingredients");
        jdbcTemplate.update("DELETE FROM user_follows");
        jdbcTemplate.update("DELETE FROM recipes");
        jdbcTemplate.update("DELETE FROM users");

        final int batchSize = 1000; // 定义批处理大小

        // 1. 批量插入 users (分批)
        if (userRecords != null && !userRecords.isEmpty()) {
            String userSql = "INSERT INTO users " +
                    "(AuthorId, AuthorName, Gender, Age, Followers, Following, Password, IsDeleted) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

            for (int start = 0; start < userRecords.size(); start += batchSize) {
                final int from = start;
                final int to = Math.min(from + batchSize, userRecords.size());
                jdbcTemplate.batchUpdate(userSql, new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        UserRecord u = userRecords.get(from + i);
                        ps.setLong(1, u.getAuthorId());
                        ps.setString(2, u.getAuthorName());
                        ps.setString(3, u.getGender());
                        ps.setInt(4, u.getAge());
                        ps.setInt(5, u.getFollowers());
                        ps.setInt(6, u.getFollowing());
                        ps.setString(7, u.getPassword());
                        ps.setBoolean(8, u.isDeleted());
                    }

                    @Override
                    public int getBatchSize() {
                        return to - from;
                    }
                });
            }
        }

        // 2. 批量插入 recipes (分批)
        if (recipeRecords != null && !recipeRecords.isEmpty()) {
            String recipeSql = "INSERT INTO recipes " +
                    "(RecipeId, Name, AuthorId, CookTime, PrepTime, TotalTime, DatePublished, Description, " +
                    "RecipeCategory, AggregatedRating, ReviewCount, Calories, FatContent, SaturatedFatContent, " +
                    "CholesterolContent, SodiumContent, CarbohydrateContent, FiberContent, SugarContent, " +
                    "ProteinContent, RecipeServings, RecipeYield) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            for (int start = 0; start < recipeRecords.size(); start += batchSize) {
                final int from = start;
                final int to = Math.min(from + batchSize, recipeRecords.size());
                jdbcTemplate.batchUpdate(recipeSql, new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        RecipeRecord r = recipeRecords.get(from + i);
                        ps.setLong(1, r.getRecipeId());
                        ps.setString(2, r.getName());
                        ps.setLong(3, r.getAuthorId());
                        ps.setString(4, r.getCookTime());
                        ps.setString(5, r.getPrepTime());
                        ps.setString(6, r.getTotalTime());
                        ps.setTimestamp(7, r.getDatePublished());
                        ps.setString(8, r.getDescription());
                        ps.setString(9, r.getRecipeCategory());
                        Object aggRating = r.getAggregatedRating();
                        ps.setObject(10, aggRating);
                        ps.setInt(11, r.getReviewCount());
                        Object calories = r.getCalories();
                        ps.setObject(12, calories);
                        Object fatContent = r.getFatContent();
                        ps.setObject(13, fatContent);
                        Object saturatedFatContent = r.getSaturatedFatContent();
                        ps.setObject(14, saturatedFatContent);
                        Object cholesterolContent = r.getCholesterolContent();
                        ps.setObject(15, cholesterolContent);
                        Object sodiumContent = r.getSodiumContent();
                        ps.setObject(16, sodiumContent);
                        Object carbohydrateContent = r.getCarbohydrateContent();
                        ps.setObject(17, carbohydrateContent);
                        Object fiberContent = r.getFiberContent();
                        ps.setObject(18, fiberContent);
                        Object sugarContent = r.getSugarContent();
                        ps.setObject(19, sugarContent);
                        Object proteinContent = r.getProteinContent();
                        ps.setObject(20, proteinContent);
                        Object servings = r.getRecipeServings();
                        if (servings instanceof String) {
                            try {
                                ps.setInt(21, Integer.parseInt((String) servings));
                            } catch (NumberFormatException e) {
                                ps.setNull(21, java.sql.Types.INTEGER);
                            }
                        } else if (servings instanceof Number) {
                            ps.setInt(21, ((Number) servings).intValue());
                        } else {
                            ps.setNull(21, java.sql.Types.INTEGER);
                        }
                        ps.setString(22, r.getRecipeYield());
                    }

                    @Override
                    public int getBatchSize() {
                        return to - from;
                    }
                });
            }
        }

        // 3. 批量插入 recipe_ingredients (分批，使用 ON CONFLICT 处理重复)
        if (recipeRecords != null && !recipeRecords.isEmpty()) {
            // 预处理：收集所有配料，保持原始大小写
            // 使用 LinkedHashSet 保持顺序，同时去重（大小写敏感）
            Map<Long, Set<String>> recipeIngredientsMap = new HashMap<>();
            for (RecipeRecord recipe : recipeRecords) {
                if (recipe != null && recipe.getRecipeIngredientParts() != null) {
                    long recipeId = recipe.getRecipeId();
                    Set<String> ingredients = recipeIngredientsMap.computeIfAbsent(recipeId, k -> new LinkedHashSet<>());
                    for (String ingredient : recipe.getRecipeIngredientParts()) {
                        if (ingredient != null && !ingredient.trim().isEmpty()) {
                            // 保持原始大小写，只去除首尾空格
                            ingredients.add(ingredient.trim());
                        }
                    }
                }
            }

            // 批量插入（保持原始大小写）
            List<Object[]> ingredientBatch = new ArrayList<>();
            for (Map.Entry<Long, Set<String>> entry : recipeIngredientsMap.entrySet()) {
                long recipeId = entry.getKey();
                for (String ingredient : entry.getValue()) {
                    ingredientBatch.add(new Object[]{recipeId, ingredient});
                }
            }

            if (!ingredientBatch.isEmpty()) {
                String ingredientSql = "INSERT INTO recipe_ingredients (RecipeId, IngredientPart) " +
                        "VALUES (?, ?) ON CONFLICT (RecipeId, IngredientPart) DO NOTHING";

                for (int start = 0; start < ingredientBatch.size(); start += batchSize) {
                    final int from = start;
                    final int to = Math.min(from + batchSize, ingredientBatch.size());
                    jdbcTemplate.batchUpdate(ingredientSql, new BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            Object[] row = ingredientBatch.get(from + i);
                            ps.setLong(1, ((Number) row[0]).longValue());
                            ps.setString(2, (String) row[1]);
                        }

                        @Override
                        public int getBatchSize() {
                            return to - from;
                        }
                    });
                }
            }
        }

        // 4. 批量插入 reviews (分批)
        if (reviewRecords != null && !reviewRecords.isEmpty()) {
            String reviewSql = "INSERT INTO reviews " +
                    "(ReviewId, RecipeId, AuthorId, Rating, Review, DateSubmitted, DateModified) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)";

            for (int start = 0; start < reviewRecords.size(); start += batchSize) {
                final int from = start;
                final int to = Math.min(from + batchSize, reviewRecords.size());
                jdbcTemplate.batchUpdate(reviewSql, new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        ReviewRecord r = reviewRecords.get(from + i);
                        ps.setLong(1, r.getReviewId());
                        ps.setLong(2, r.getRecipeId());
                        ps.setLong(3, r.getAuthorId());
                        ps.setInt(4, (int) r.getRating());
                        ps.setString(5, r.getReview());
                        ps.setTimestamp(6, r.getDateSubmitted());
                        ps.setTimestamp(7, r.getDateModified());
                    }

                    @Override
                    public int getBatchSize() {
                        return to - from;
                    }
                });
            }
        }

        // 5. 批量插入 review_likes (从 ReviewRecord 中提取)
        if (reviewRecords != null && !reviewRecords.isEmpty()) {
            List<Object[]> likeBatch = new ArrayList<>();
            for (ReviewRecord review : reviewRecords) {
                if (review != null && review.getLikes() != null) {
                    long reviewId = review.getReviewId();
                    for (long authorId : review.getLikes()) {
                        likeBatch.add(new Object[]{reviewId, authorId});
                    }
                }
            }

            if (!likeBatch.isEmpty()) {
                String likeSql = "INSERT INTO review_likes (ReviewId, AuthorId) " +
                        "VALUES (?, ?) ON CONFLICT (ReviewId, AuthorId) DO NOTHING";

                for (int start = 0; start < likeBatch.size(); start += batchSize) {
                    final int from = start;
                    final int to = Math.min(from + batchSize, likeBatch.size());
                    jdbcTemplate.batchUpdate(likeSql, new BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            Object[] row = likeBatch.get(from + i);
                            ps.setLong(1, ((Number) row[0]).longValue());
                            ps.setLong(2, ((Number) row[1]).longValue());
                        }

                        @Override
                        public int getBatchSize() {
                            return to - from;
                        }
                    });
                }
            }
        }

        // 6. 批量插入 user_follows (从 UserRecord 中提取)
        if (userRecords != null && !userRecords.isEmpty()) {
            List<Object[]> followBatch = new ArrayList<>();
            for (UserRecord user : userRecords) {
                if (user != null) {
                    long userId = user.getAuthorId();
                    // 处理 followerUsers（关注我的人）
                    if (user.getFollowerUsers() != null) {
                        for (long followerId : user.getFollowerUsers()) {
                            if (followerId != userId) { // 不能关注自己
                                followBatch.add(new Object[]{followerId, userId});
                            }
                        }
                    }
                    // 处理 followingUsers（我关注的人）
                    if (user.getFollowingUsers() != null) {
                        for (long followingId : user.getFollowingUsers()) {
                            if (followingId != userId) { // 不能关注自己
                                followBatch.add(new Object[]{userId, followingId});
                            }
                        }
                    }
                }
            }

            if (!followBatch.isEmpty()) {
                String followSql = "INSERT INTO user_follows (FollowerId, FollowingId) " +
                        "VALUES (?, ?) ON CONFLICT (FollowerId, FollowingId) DO NOTHING";

                for (int start = 0; start < followBatch.size(); start += batchSize) {
                    final int from = start;
                    final int to = Math.min(from + batchSize, followBatch.size());
                    jdbcTemplate.batchUpdate(followSql, new BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            Object[] row = followBatch.get(from + i);
                            ps.setLong(1, ((Number) row[0]).longValue());
                            ps.setLong(2, ((Number) row[1]).longValue());
                        }

                        @Override
                        public int getBatchSize() {
                            return to - from;
                        }
                    });
                }
            }
        }
    }


    private void createTables() {
        String[] createTableSQLs = {
                // 创建users表
                "CREATE TABLE IF NOT EXISTS users (" +
                        "    AuthorId BIGINT PRIMARY KEY, " +
                        "    AuthorName VARCHAR(255) NOT NULL, " +
                        "    Gender VARCHAR(10) CHECK (Gender IN ('Male', 'Female')), " +
                        "    Age INTEGER CHECK (Age > 0), " +
                        "    Followers INTEGER DEFAULT 0 CHECK (Followers >= 0), " +
                        "    Following INTEGER DEFAULT 0 CHECK (Following >= 0), " +
                        "    Password VARCHAR(255), " +
                        "    IsDeleted BOOLEAN DEFAULT FALSE" +
                        ")",

                // 创建recipes表
                "CREATE TABLE IF NOT EXISTS recipes (" +
                        "    RecipeId BIGINT PRIMARY KEY, " +
                        "    Name VARCHAR(500) NOT NULL, " +
                        "    AuthorId BIGINT NOT NULL, " +
                        "    CookTime VARCHAR(50), " +
                        "    PrepTime VARCHAR(50), " +
                        "    TotalTime VARCHAR(50), " +
                        "    DatePublished TIMESTAMP, " +
                        "    Description TEXT, " +
                        "    RecipeCategory VARCHAR(255), " +
                        "    AggregatedRating DECIMAL(3,2) CHECK (AggregatedRating >= 0 AND AggregatedRating <= 5), " +
                        "    ReviewCount INTEGER DEFAULT 0 CHECK (ReviewCount >= 0), " +
                        "    Calories DECIMAL(10,2), " +
                        "    FatContent DECIMAL(10,2), " +
                        "    SaturatedFatContent DECIMAL(10,2), " +
                        "    CholesterolContent DECIMAL(10,2), " +
                        "    SodiumContent DECIMAL(10,2), " +
                        "    CarbohydrateContent DECIMAL(10,2), " +
                        "    FiberContent DECIMAL(10,2), " +
                        "    SugarContent DECIMAL(10,2), " +
                        "    ProteinContent DECIMAL(10,2), " +
                        "    RecipeServings VARCHAR(100), " +
                        "    RecipeYield VARCHAR(100), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",

                // 创建reviews表
                "CREATE TABLE IF NOT EXISTS reviews (" +
                        "    ReviewId BIGINT PRIMARY KEY, " +
                        "    RecipeId BIGINT NOT NULL, " +
                        "    AuthorId BIGINT NOT NULL, " +
                        "    Rating INTEGER, " +
                        "    Review TEXT, " +
                        "    DateSubmitted TIMESTAMP, " +
                        "    DateModified TIMESTAMP, " +
                        "    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",

                // 创建recipe_ingredients表
                "CREATE TABLE IF NOT EXISTS recipe_ingredients (" +
                        "    RecipeId BIGINT, " +
                        "    IngredientPart VARCHAR(500), " +
                        "    PRIMARY KEY (RecipeId, IngredientPart), " +
                        "    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId)" +
                        ")",

                // 创建review_likes表
                "CREATE TABLE IF NOT EXISTS review_likes (" +
                        "    ReviewId BIGINT, " +
                        "    AuthorId BIGINT, " +
                        "    PRIMARY KEY (ReviewId, AuthorId), " +
                        "    FOREIGN KEY (ReviewId) REFERENCES reviews(ReviewId), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",

                // 创建user_follows表
                "CREATE TABLE IF NOT EXISTS user_follows (" +
                        "    FollowerId BIGINT, " +
                        "    FollowingId BIGINT, " +
                        "    PRIMARY KEY (FollowerId, FollowingId), " +
                        "    FOREIGN KEY (FollowerId) REFERENCES users(AuthorId), " +
                        "    FOREIGN KEY (FollowingId) REFERENCES users(AuthorId), " +
                        "    CHECK (FollowerId != FollowingId)" +
                        ")"
        };

        // 创建表
        for (String sql : createTableSQLs) {
            jdbcTemplate.execute(sql);
        }

        // 创建索引以优化查询性能
        createIndexes();
    }

    /**
     * 创建索引以优化查询性能
     * 根据实际查询模式分析，添加必要的索引
     */
    private void createIndexes() {
        String[] createIndexSQLs = {
                // users 表索引
                // AuthorName 索引（用于用户名查询，虽然不常用，但如果有注册时检查用户名重复的需求）
                "CREATE INDEX IF NOT EXISTS idx_users_authorname ON users(AuthorName)",
                // IsDeleted 索引（用于过滤活跃用户）
                "CREATE INDEX IF NOT EXISTS idx_users_isdeleted ON users(IsDeleted)",

                // recipes 表索引
                // AuthorId 索引（用于查询用户的食谱、feed 查询）
                "CREATE INDEX IF NOT EXISTS idx_recipes_authorid ON recipes(AuthorId)",
                // RecipeCategory 索引（用于分类筛选）
                "CREATE INDEX IF NOT EXISTS idx_recipes_category ON recipes(RecipeCategory)",
                // DatePublished 索引（用于时间排序）
                "CREATE INDEX IF NOT EXISTS idx_recipes_datepublished ON recipes(DatePublished DESC NULLS LAST)",
                // AggregatedRating 索引（用于评分排序）
                "CREATE INDEX IF NOT EXISTS idx_recipes_rating ON recipes(AggregatedRating DESC NULLS LAST)",
                // Calories 索引（用于卡路里排序）
                "CREATE INDEX IF NOT EXISTS idx_recipes_calories ON recipes(Calories ASC NULLS LAST)",
                // 复合索引：用于 feed 查询（AuthorId + RecipeCategory + DatePublished）
                "CREATE INDEX IF NOT EXISTS idx_recipes_feed ON recipes(AuthorId, RecipeCategory, DatePublished DESC NULLS LAST)",
                // 复合索引：用于搜索和排序（RecipeCategory + AggregatedRating）
                "CREATE INDEX IF NOT EXISTS idx_recipes_category_rating ON recipes(RecipeCategory, AggregatedRating DESC NULLS LAST)",
                // Name 和 Description 的全文搜索索引（PostgreSQL 使用 GIN 索引）
                // 注意：需要先创建扩展，这里使用普通索引作为备选
                "CREATE INDEX IF NOT EXISTS idx_recipes_name_lower ON recipes(LOWER(Name))",
                "CREATE INDEX IF NOT EXISTS idx_recipes_description_lower ON recipes(LOWER(Description))",

                // reviews 表索引
                // RecipeId 索引（用于查询食谱的评论列表）
                "CREATE INDEX IF NOT EXISTS idx_reviews_recipeid ON reviews(RecipeId)",
                // AuthorId 索引（用于查询用户的评论）
                "CREATE INDEX IF NOT EXISTS idx_reviews_authorid ON reviews(AuthorId)",
                // DateModified 索引（用于评论时间排序）
                "CREATE INDEX IF NOT EXISTS idx_reviews_datemodified ON reviews(DateModified DESC)",
                // 复合索引：用于评论列表查询（RecipeId + DateModified）
                "CREATE INDEX IF NOT EXISTS idx_reviews_recipe_date ON reviews(RecipeId, DateModified DESC)",

                // recipe_ingredients 表索引
                // RecipeId 索引（用于查询食谱的配料，主键已包含，但单独索引可能更快）
                "CREATE INDEX IF NOT EXISTS idx_recipe_ingredients_recipeid ON recipe_ingredients(RecipeId)",

                // review_likes 表索引
                // ReviewId 索引（用于统计点赞数，主键已包含，但单独索引可能更快）
                "CREATE INDEX IF NOT EXISTS idx_review_likes_reviewid ON review_likes(ReviewId)",
                // AuthorId 索引（用于查询用户点赞的评论）
                "CREATE INDEX IF NOT EXISTS idx_review_likes_authorid ON review_likes(AuthorId)",

                // user_follows 表索引（关键优化）
                // FollowerId 索引（用于查询用户的关注列表、feed 查询）
                "CREATE INDEX IF NOT EXISTS idx_user_follows_followerid ON user_follows(FollowerId)",
                // FollowingId 索引（用于查询用户的粉丝列表、统计关注数）
                "CREATE INDEX IF NOT EXISTS idx_user_follows_followingid ON user_follows(FollowingId)"
        };

        // 创建索引（忽略已存在的错误）
        for (String sql : createIndexSQLs) {
            try {
                jdbcTemplate.execute(sql);
            } catch (Exception e) {
                // 索引可能已存在，忽略错误
                log.debug("Index creation skipped (may already exist): {}", e.getMessage());
            }
        }
    }



    /*
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     *
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */

    @Override
    public void drop() {
        // You can use the default drop script provided by us in most cases,
        // but if it doesn't work properly, you may need to modify it.
        // This method will delete all the tables in the public schema.

        String sql = "DO $$\n" +
                "DECLARE\n" +
                "    tables CURSOR FOR\n" +
                "        SELECT tablename\n" +
                "        FROM pg_tables\n" +
                "        WHERE schemaname = 'public';\n" +
                "BEGIN\n" +
                "    FOR t IN tables\n" +
                "    LOOP\n" +
                "        EXECUTE 'DROP TABLE IF EXISTS ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
                "    END LOOP;\n" +
                "END $$;\n";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
