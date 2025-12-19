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

@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

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

        createTables();

        String[] deleteTables = {
                "user_favorite_recipes",
                "recipe_keywords",
                "keywords",
                "recipe_ingredients_normalized",
                "ingredients",
                "instructions",
                "nutrition",
                "review_likes",
                "reviews",
                "recipe_ingredients",
                "user_follows",
                "recipes",
                "users"
        };
        
        for (String tableName : deleteTables) {
            try {
                jdbcTemplate.update("DELETE FROM " + tableName);
            } catch (Exception e) {
                log.debug("Table {} may not exist, skipping delete: {}", tableName, e.getMessage());
            }
        }

        final int batchSize = 1000;

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

        if (recipeRecords != null && !recipeRecords.isEmpty()) {
            String recipeSql = "INSERT INTO recipes " +
                    "(RecipeId, Name, AuthorId, CookTime, PrepTime, TotalTime, DatePublished, Description, " +
                    "RecipeCategory, AggregatedRating, ReviewCount, RecipeServings, RecipeYield) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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
                        Object servings = r.getRecipeServings();
                        if (servings instanceof String) {
                            try {
                                ps.setInt(12, Integer.parseInt((String) servings));
                            } catch (NumberFormatException e) {
                                ps.setNull(12, java.sql.Types.INTEGER);
                            }
                        } else if (servings instanceof Number) {
                            ps.setInt(12, ((Number) servings).intValue());
                        } else {
                            ps.setNull(12, java.sql.Types.INTEGER);
                        }
                        ps.setString(13, r.getRecipeYield());
                    }

                    @Override
                    public int getBatchSize() {
                        return to - from;
                    }
                });
            }

            List<Object[]> nutritionBatch = new ArrayList<>();
            for (RecipeRecord r : recipeRecords) {
                if (r != null && r.getCalories() > 0) {
                    nutritionBatch.add(new Object[]{
                            r.getRecipeId(),
                            r.getCalories(),
                            r.getFatContent(),
                            r.getSaturatedFatContent(),
                            r.getCholesterolContent(),
                            r.getSodiumContent(),
                            r.getCarbohydrateContent(),
                            r.getFiberContent(),
                            r.getSugarContent(),
                            r.getProteinContent()
                    });
                }
            }

            if (!nutritionBatch.isEmpty()) {
                String nutritionSql = "INSERT INTO nutrition " +
                        "(RecipeId, Calories, FatContent, SaturatedFatContent, CholesterolContent, " +
                        "SodiumContent, CarbohydrateContent, FiberContent, SugarContent, ProteinContent) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (RecipeId) DO NOTHING";

                for (int start = 0; start < nutritionBatch.size(); start += batchSize) {
                    final int from = start;
                    final int to = Math.min(from + batchSize, nutritionBatch.size());
                    jdbcTemplate.batchUpdate(nutritionSql, new BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            Object[] row = nutritionBatch.get(from + i);
                            ps.setLong(1, ((Number) row[0]).longValue());
                            ps.setObject(2, row[1]);
                            ps.setObject(3, row[2]);
                            ps.setObject(4, row[3]);
                            ps.setObject(5, row[4]);
                            ps.setObject(6, row[5]);
                            ps.setObject(7, row[6]);
                            ps.setObject(8, row[7]);
                            ps.setObject(9, row[8]);
                            ps.setObject(10, row[9]);
                        }

                        @Override
                        public int getBatchSize() {
                            return to - from;
                        }
                    });
                }
            }
        }

        if (recipeRecords != null && !recipeRecords.isEmpty()) {
            //+保持原始大小写，使用LinkedHashSet去重
            Map<Long, Set<String>> recipeIngredientsMap = new HashMap<>();
            for (RecipeRecord recipe : recipeRecords) {
                if (recipe != null && recipe.getRecipeIngredientParts() != null) {
                    long recipeId = recipe.getRecipeId();
                    Set<String> ingredients = recipeIngredientsMap.computeIfAbsent(recipeId, k -> new LinkedHashSet<>());
                    for (String ingredient : recipe.getRecipeIngredientParts()) {
                        if (ingredient != null && !ingredient.trim().isEmpty()) {
                            ingredients.add(ingredient.trim());
                        }
                    }
                }
            }

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

        //+记录成功插入的reviewId用于外键验证
        Set<Long> validReviewIds = new HashSet<>();
        
        if (reviewRecords != null && !reviewRecords.isEmpty()) {
            //+Rating=0保留用于likeReview，但计算AggregatedRating时排除
            List<ReviewRecord> validReviews = new ArrayList<>();
            for (ReviewRecord r : reviewRecords) {
                if (r != null) {
                    float rating = r.getRating();
                    if (rating < 0.0f) {
                        log.debug("Converting review {} with rating {} to 0", r.getReviewId(), rating);
                        ReviewRecord modified = ReviewRecord.builder()
                                .reviewId(r.getReviewId())
                                .recipeId(r.getRecipeId())
                                .authorId(r.getAuthorId())
                                .rating(0.0f)
                                .review(r.getReview())
                                .dateSubmitted(r.getDateSubmitted())
                                .dateModified(r.getDateModified())
                                .build();
                        validReviews.add(modified);
                    } else if (rating > 5.0f) {
                        log.debug("Converting review {} with rating {} to 5", r.getReviewId(), rating);
                        ReviewRecord modified = ReviewRecord.builder()
                                .reviewId(r.getReviewId())
                                .recipeId(r.getRecipeId())
                                .authorId(r.getAuthorId())
                                .rating(5.0f)
                                .review(r.getReview())
                                .dateSubmitted(r.getDateSubmitted())
                                .dateModified(r.getDateModified())
                                .build();
                        validReviews.add(modified);
                    } else {
                        validReviews.add(r);
                    }
                    validReviewIds.add(r.getReviewId());
                }
            }

            if (!validReviews.isEmpty()) {
                String reviewSql = "INSERT INTO reviews " +
                        "(ReviewId, RecipeId, AuthorId, Rating, Review, DateSubmitted, DateModified) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (ReviewId) DO NOTHING";

                for (int start = 0; start < validReviews.size(); start += batchSize) {
                    final int from = start;
                    final int to = Math.min(from + batchSize, validReviews.size());
                    jdbcTemplate.batchUpdate(reviewSql, new BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            ReviewRecord r = validReviews.get(from + i);
                            ps.setLong(1, r.getReviewId());
                            ps.setLong(2, r.getRecipeId());
                            ps.setLong(3, r.getAuthorId());
                            float ratingFloat = r.getRating();
                            int rating = Math.round(ratingFloat);
                            if (rating < 0) rating = 0;
                            if (rating > 5) rating = 5;
                            ps.setInt(4, rating);
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
        }

        //+只插入有效的reviewId
        if (reviewRecords != null && !reviewRecords.isEmpty() && !validReviewIds.isEmpty()) {
            List<Object[]> likeBatch = new ArrayList<>();
            for (ReviewRecord review : reviewRecords) {
                if (review != null && review.getLikes() != null) {
                    long reviewId = review.getReviewId();
                    if (validReviewIds.contains(reviewId)) {
                        for (long authorId : review.getLikes()) {
                            likeBatch.add(new Object[]{reviewId, authorId});
                        }
                    }
                }
            }

            if (!likeBatch.isEmpty()) {
                String likeSql = "INSERT INTO review_likes (ReviewId, AuthorId) " +
                        "VALUES (?, ?) ON CONFLICT (ReviewId, AuthorId) DO NOTHING";

                for (int start = 0; start < likeBatch.size(); start += batchSize) {
                    final int from = start;
                    final int to = Math.min(from + batchSize, likeBatch.size());
                    try {
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
                    } catch (Exception e) {
                        log.warn("Failed to insert some review_likes (foreign key constraint): {}", e.getMessage());
                    }
                }
            }
        }

        if (userRecords != null && !userRecords.isEmpty()) {
            List<Object[]> followBatch = new ArrayList<>();
            for (UserRecord user : userRecords) {
                if (user != null) {
                    long userId = user.getAuthorId();
                    if (user.getFollowerUsers() != null) {
                        for (long followerId : user.getFollowerUsers()) {
                            if (followerId != userId) {
                                followBatch.add(new Object[]{followerId, userId});
                            }
                        }
                    }
                    if (user.getFollowingUsers() != null) {
                        for (long followingId : user.getFollowingUsers()) {
                            if (followingId != userId) {
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
        String[] dropTableSQLs = {
                "DROP TABLE IF EXISTS user_favorite_recipes CASCADE",
                "DROP TABLE IF EXISTS recipe_keywords CASCADE",
                "DROP TABLE IF EXISTS keywords CASCADE",
                "DROP TABLE IF EXISTS recipe_ingredients_normalized CASCADE",
                "DROP TABLE IF EXISTS ingredients CASCADE",
                "DROP TABLE IF EXISTS instructions CASCADE",
                "DROP TABLE IF EXISTS nutrition CASCADE",
                "DROP TABLE IF EXISTS review_likes CASCADE",
                "DROP TABLE IF EXISTS reviews CASCADE",
                "DROP TABLE IF EXISTS recipe_ingredients CASCADE",
                "DROP TABLE IF EXISTS user_follows CASCADE",
                "DROP TABLE IF EXISTS recipes CASCADE",
                "DROP TABLE IF EXISTS users CASCADE"
        };
        
        for (String sql : dropTableSQLs) {
            try {
                jdbcTemplate.execute(sql);
            } catch (Exception e) {
                log.debug("Drop table error (may not exist): {}", e.getMessage());
            }
        }
        
        String[] dropTriggerSQLs = {
                "DROP TRIGGER IF EXISTS trigger_update_recipe_rating_insert ON reviews",
                "DROP TRIGGER IF EXISTS trigger_update_recipe_rating_update ON reviews",
                "DROP TRIGGER IF EXISTS trigger_update_recipe_rating_delete ON reviews"
        };
        for (String sql : dropTriggerSQLs) {
            try {
                jdbcTemplate.execute(sql);
            } catch (Exception e) {
                log.debug("Drop trigger error: {}", e.getMessage());
            }
        }
        
        try {
            jdbcTemplate.execute("DROP FUNCTION IF EXISTS update_recipe_rating() CASCADE");
        } catch (Exception e) {
            log.debug("Drop function error: {}", e.getMessage());
        }
        
        String[] createTableSQLs = {
                "CREATE TABLE IF NOT EXISTS users (" +
                        "    AuthorId BIGINT PRIMARY KEY, " +
                        "    AuthorName TEXT NOT NULL, " +
                        "    Gender VARCHAR(10) CHECK (Gender IN ('Male', 'Female')), " +
                        "    Age INTEGER CHECK (Age > 0), " +
                        "    Password TEXT, " +
                        "    IsDeleted BOOLEAN DEFAULT FALSE, " +
                        "    Followers INTEGER DEFAULT 0 CHECK (Followers >= 0), " +
                        "    Following INTEGER DEFAULT 0 CHECK (Following >= 0)" +
                        ")",

                "CREATE TABLE IF NOT EXISTS recipes (" +
                        "    RecipeId BIGINT PRIMARY KEY, " +
                        "    AuthorId BIGINT NOT NULL, " +
                        "    Name TEXT NOT NULL, " +
                        "    CookTime TEXT, " +
                        "    PrepTime TEXT, " +
                        "    TotalTime TEXT, " +
                        "    DatePublished TIMESTAMP, " +
                        "    Description TEXT, " +
                        "    RecipeCategory TEXT, " +
                        "    RecipeServings INTEGER, " +
                        "    RecipeYield TEXT, " +
                        "    AggregatedRating DECIMAL(3,2) CHECK (AggregatedRating >= 0 AND AggregatedRating <= 5), " +
                        "    ReviewCount INTEGER DEFAULT 0 CHECK (ReviewCount >= 0), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",

                "CREATE TABLE IF NOT EXISTS reviews (" +
                        "    ReviewId BIGINT PRIMARY KEY, " +
                        "    RecipeId BIGINT NOT NULL, " +
                        "    AuthorId BIGINT NOT NULL, " +
                        "    Rating INTEGER NOT NULL CHECK (Rating >= 0 AND Rating <= 5), " +
                        "    Review TEXT, " +
                        "    DateSubmitted TIMESTAMP, " +
                        "    DateModified TIMESTAMP, " +
                        "    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId) ON DELETE CASCADE, " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",

                "CREATE TABLE IF NOT EXISTS recipe_ingredients (" +
                        "    RecipeId BIGINT, " +
                        "    IngredientPart TEXT, " +
                        "    PRIMARY KEY (RecipeId, IngredientPart), " +
                        "    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId) ON DELETE CASCADE" +
                        ")",

                "CREATE TABLE IF NOT EXISTS review_likes (" +
                        "    ReviewId BIGINT, " +
                        "    AuthorId BIGINT, " +
                        "    PRIMARY KEY (ReviewId, AuthorId), " +
                        "    FOREIGN KEY (ReviewId) REFERENCES reviews(ReviewId) ON DELETE CASCADE, " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",

                "CREATE TABLE IF NOT EXISTS user_follows (" +
                        "    FollowerId BIGINT, " +
                        "    FollowingId BIGINT, " +
                        "    PRIMARY KEY (FollowerId, FollowingId), " +
                        "    FOREIGN KEY (FollowerId) REFERENCES users(AuthorId), " +
                        "    FOREIGN KEY (FollowingId) REFERENCES users(AuthorId), " +
                        "    CHECK (FollowerId != FollowingId)" +
                        ")",

                "CREATE TABLE IF NOT EXISTS nutrition (" +
                        "    RecipeId BIGINT PRIMARY KEY, " +
                        "    Calories NUMERIC(10, 2) NOT NULL, " +
                        "    FatContent NUMERIC(10, 2), " +
                        "    SaturatedFatContent NUMERIC(10, 2), " +
                        "    CholesterolContent NUMERIC(10, 2), " +
                        "    SodiumContent NUMERIC(10, 2), " +
                        "    CarbohydrateContent NUMERIC(10, 2), " +
                        "    FiberContent NUMERIC(10, 2), " +
                        "    SugarContent NUMERIC(10, 2), " +
                        "    ProteinContent NUMERIC(10, 2), " +
                        "    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId) ON DELETE CASCADE" +
                        ")",

                "CREATE TABLE IF NOT EXISTS instructions (" +
                        "    RecipeId BIGINT, " +
                        "    StepNumber INTEGER, " +
                        "    InstructionText TEXT NOT NULL, " +
                        "    PRIMARY KEY (RecipeId, StepNumber), " +
                        "    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId) ON DELETE CASCADE" +
                        ")",

                "CREATE TABLE IF NOT EXISTS ingredients (" +
                        "    IngredientId BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, " +
                        "    IngredientName TEXT NOT NULL UNIQUE, " +
                        "    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                        ")",

                "CREATE TABLE IF NOT EXISTS recipe_ingredients_normalized (" +
                        "    RecipeId BIGINT, " +
                        "    IngredientId BIGINT, " +
                        "    Quantity TEXT, " +
                        "    Unit TEXT, " +
                        "    PRIMARY KEY (RecipeId, IngredientId), " +
                        "    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId) ON DELETE CASCADE, " +
                        "    FOREIGN KEY (IngredientId) REFERENCES ingredients(IngredientId) ON DELETE CASCADE" +
                        ")",

                "CREATE TABLE IF NOT EXISTS keywords (" +
                        "    KeywordId BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, " +
                        "    KeywordText TEXT NOT NULL UNIQUE, " +
                        "    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP" +
                        ")",

                "CREATE TABLE IF NOT EXISTS recipe_keywords (" +
                        "    RecipeId BIGINT, " +
                        "    KeywordId BIGINT, " +
                        "    PRIMARY KEY (RecipeId, KeywordId), " +
                        "    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId) ON DELETE CASCADE, " +
                        "    FOREIGN KEY (KeywordId) REFERENCES keywords(KeywordId) ON DELETE CASCADE" +
                        ")",

                "CREATE TABLE IF NOT EXISTS user_favorite_recipes (" +
                        "    AuthorId BIGINT, " +
                        "    RecipeId BIGINT, " +
                        "    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
                        "    PRIMARY KEY (AuthorId, RecipeId), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId), " +
                        "    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId) ON DELETE CASCADE" +
                        ")"
        };

        for (String sql : createTableSQLs) {
            try {
                jdbcTemplate.execute(sql);
                log.debug("Table created successfully");
            } catch (Exception e) {
                log.warn("Table creation error (may already exist): {}", e.getMessage());
                if (e.getMessage() != null && e.getMessage().contains("不存在")) {
                    log.error("Table creation failed due to missing dependency: {}", e.getMessage());
                }
            }
        }

        try {
            createIndexes();
        } catch (Exception e) {
            log.warn("Index creation failed, but tables are created: {}", e.getMessage());
        }

        try {
            createViews();
        } catch (Exception e) {
            log.warn("View creation failed, but tables are created: {}", e.getMessage());
        }
    }

    private void createIndexes() {
        String[] createIndexSQLs = {
                "CREATE INDEX IF NOT EXISTS idx_users_authorname ON users(AuthorName)",
                "CREATE INDEX IF NOT EXISTS idx_users_isdeleted ON users(IsDeleted) WHERE IsDeleted = FALSE",

                "CREATE INDEX IF NOT EXISTS idx_recipes_authorid ON recipes(AuthorId)",
                "CREATE INDEX IF NOT EXISTS idx_recipes_category ON recipes(RecipeCategory)",
                "CREATE INDEX IF NOT EXISTS idx_recipes_datepublished ON recipes(DatePublished DESC NULLS LAST)",
                "CREATE INDEX IF NOT EXISTS idx_recipes_rating ON recipes(AggregatedRating DESC NULLS LAST)",
                "CREATE INDEX IF NOT EXISTS idx_recipes_reviewcount ON recipes(ReviewCount DESC)",
                "CREATE INDEX IF NOT EXISTS idx_recipes_feed ON recipes(AuthorId, RecipeCategory, DatePublished DESC NULLS LAST)",
                "CREATE INDEX IF NOT EXISTS idx_recipes_category_rating ON recipes(RecipeCategory, AggregatedRating DESC NULLS LAST)",
                "CREATE INDEX IF NOT EXISTS idx_recipes_name_lower ON recipes(LOWER(Name))",
                "CREATE INDEX IF NOT EXISTS idx_recipes_description_lower ON recipes(LOWER(Description))",

                "CREATE INDEX IF NOT EXISTS idx_reviews_recipeid ON reviews(RecipeId)",
                "CREATE INDEX IF NOT EXISTS idx_reviews_authorid ON reviews(AuthorId)",
                "CREATE INDEX IF NOT EXISTS idx_reviews_datemodified ON reviews(DateModified DESC)",
                "CREATE INDEX IF NOT EXISTS idx_reviews_rating ON reviews(Rating)",
                "CREATE INDEX IF NOT EXISTS idx_reviews_recipe_date ON reviews(RecipeId, DateModified DESC)",

                "CREATE INDEX IF NOT EXISTS idx_recipe_ingredients_recipeid ON recipe_ingredients(RecipeId)",
                "CREATE INDEX IF NOT EXISTS idx_recipe_ingredients_part_lower ON recipe_ingredients(LOWER(IngredientPart))",

                "CREATE INDEX IF NOT EXISTS idx_review_likes_reviewid ON review_likes(ReviewId)",
                "CREATE INDEX IF NOT EXISTS idx_review_likes_authorid ON review_likes(AuthorId)",

                "CREATE INDEX IF NOT EXISTS idx_user_follows_followerid ON user_follows(FollowerId)",
                "CREATE INDEX IF NOT EXISTS idx_user_follows_followingid ON user_follows(FollowingId)",

                "CREATE INDEX IF NOT EXISTS idx_nutrition_calories ON nutrition(Calories ASC NULLS LAST)",

                "CREATE INDEX IF NOT EXISTS idx_instructions_recipeid ON instructions(RecipeId)",

                "CREATE INDEX IF NOT EXISTS idx_recipe_ingredients_norm_recipeid ON recipe_ingredients_normalized(RecipeId)",
                "CREATE INDEX IF NOT EXISTS idx_recipe_ingredients_norm_ingredientid ON recipe_ingredients_normalized(IngredientId)",
                "CREATE INDEX IF NOT EXISTS idx_ingredients_name_lower ON ingredients(LOWER(IngredientName))",

                "CREATE INDEX IF NOT EXISTS idx_recipe_keywords_recipeid ON recipe_keywords(RecipeId)",
                "CREATE INDEX IF NOT EXISTS idx_recipe_keywords_keywordid ON recipe_keywords(KeywordId)",
                "CREATE INDEX IF NOT EXISTS idx_keywords_text_lower ON keywords(LOWER(KeywordText))",

                "CREATE INDEX IF NOT EXISTS idx_user_favorite_recipes_authorid ON user_favorite_recipes(AuthorId)",
                "CREATE INDEX IF NOT EXISTS idx_user_favorite_recipes_recipeid ON user_favorite_recipes(RecipeId)",
                "CREATE INDEX IF NOT EXISTS idx_user_favorite_recipes_created ON user_favorite_recipes(CreatedAt DESC)"
        };

        for (String sql : createIndexSQLs) {
            try {
                jdbcTemplate.execute(sql);
            } catch (Exception e) {
                log.debug("Index creation skipped (may already exist): {}", e.getMessage());
            }
        }
    }

    private void createTriggers() {
        String triggerFunction = "CREATE OR REPLACE FUNCTION update_recipe_rating() " +
                "RETURNS TRIGGER AS $$ " +
                "BEGIN " +
                "    UPDATE recipes " +
                "    SET " +
                "        AggregatedRating = ( " +
                "            SELECT COALESCE(AVG(Rating), 0) " +
                "            FROM reviews " +
                "            WHERE RecipeId = COALESCE(NEW.RecipeId, OLD.RecipeId) " +
                "        ), " +
                "        ReviewCount = ( " +
                "            SELECT COUNT(*) " +
                "            FROM reviews " +
                "            WHERE RecipeId = COALESCE(NEW.RecipeId, OLD.RecipeId) " +
                "        ) " +
                "    WHERE RecipeId = COALESCE(NEW.RecipeId, OLD.RecipeId); " +
                "    RETURN COALESCE(NEW, OLD); " +
                "END; " +
                "$$ LANGUAGE plpgsql";

        try {
            jdbcTemplate.execute(triggerFunction);
        } catch (Exception e) {
            log.warn("Trigger function creation error: {}", e.getMessage());
        }

        String[] triggerSQLs = {
                "DROP TRIGGER IF EXISTS trigger_update_recipe_rating_insert ON reviews",
                "CREATE TRIGGER trigger_update_recipe_rating_insert " +
                        "    AFTER INSERT ON reviews " +
                        "    FOR EACH ROW " +
                        "    EXECUTE FUNCTION update_recipe_rating()",
                "DROP TRIGGER IF EXISTS trigger_update_recipe_rating_update ON reviews",
                "CREATE TRIGGER trigger_update_recipe_rating_update " +
                        "    AFTER UPDATE OF Rating ON reviews " +
                        "    FOR EACH ROW " +
                        "    EXECUTE FUNCTION update_recipe_rating()",
                "DROP TRIGGER IF EXISTS trigger_update_recipe_rating_delete ON reviews",
                "CREATE TRIGGER trigger_update_recipe_rating_delete " +
                        "    AFTER DELETE ON reviews " +
                        "    FOR EACH ROW " +
                        "    EXECUTE FUNCTION update_recipe_rating()"
        };

        for (String sql : triggerSQLs) {
            try {
                jdbcTemplate.execute(sql);
            } catch (Exception e) {
                log.debug("Trigger creation skipped: {}", e.getMessage());
            }
        }
    }

    private void createViews() {
        String viewSQL = "CREATE OR REPLACE VIEW recipe_full AS " +
                "SELECT " +
                "    r.RecipeId, " +
                "    r.AuthorId, " +
                "    r.Name, " +
                "    r.CookTime, " +
                "    r.PrepTime, " +
                "    r.TotalTime, " +
                "    r.DatePublished, " +
                "    r.Description, " +
                "    r.RecipeCategory, " +
                "    r.RecipeServings, " +
                "    r.RecipeYield, " +
                "    r.AggregatedRating, " +
                "    r.ReviewCount, " +
                "    n.Calories, " +
                "    n.FatContent, " +
                "    n.SaturatedFatContent, " +
                "    n.CholesterolContent, " +
                "    n.SodiumContent, " +
                "    n.CarbohydrateContent, " +
                "    n.FiberContent, " +
                "    n.SugarContent, " +
                "    n.ProteinContent " +
                "FROM recipes r " +
                "LEFT JOIN nutrition n ON r.RecipeId = n.RecipeId";

        try {
            jdbcTemplate.execute(viewSQL);
        } catch (Exception e) {
            log.warn("View creation error: {}", e.getMessage());
        }
    }



    @Override
    public void drop() {
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
