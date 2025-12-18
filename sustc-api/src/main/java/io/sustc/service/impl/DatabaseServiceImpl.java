package io.sustc.service.impl;

import io.sustc.dto.ReviewRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.RecipeRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

        // 1. 创建表（如果不存在）
        createTables();

        // 2. 清空旧数据（按外键从子表到父表的顺序）
        jdbcTemplate.update("DELETE FROM review_likes");
        jdbcTemplate.update("DELETE FROM reviews");
        jdbcTemplate.update("DELETE FROM recipe_ingredients");
        jdbcTemplate.update("DELETE FROM user_follows");
        jdbcTemplate.update("DELETE FROM recipes");
        jdbcTemplate.update("DELETE FROM users");

        // 3. 批量插入 users（分批，降低单次 batch 体积）
        if (userRecords != null && !userRecords.isEmpty()) {
            String userSql = "INSERT INTO users " +
                    "(AuthorId, AuthorName, Gender, Age, Followers, Following, Password, IsDeleted) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            final int batchSize = 1000;

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

        // 4. 批量插入 recipes（分批）
        if (recipeRecords != null && !recipeRecords.isEmpty()) {
            String recipeSql = "INSERT INTO recipes (" +
                    "RecipeId, Name, AuthorId, CookTime, PrepTime, TotalTime, DatePublished, " +
                    "Description, RecipeCategory, AggregatedRating, ReviewCount, " +
                    "Calories, FatContent, SaturatedFatContent, CholesterolContent, SodiumContent, " +
                    "CarbohydrateContent, FiberContent, SugarContent, ProteinContent, " +
                    "RecipeServings, RecipeYield" +
                    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            final int batchSize = 1000;

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
                        ps.setFloat(10, r.getAggregatedRating());
                        ps.setInt(11, r.getReviewCount());
                        ps.setFloat(12, r.getCalories());
                        ps.setFloat(13, r.getFatContent());
                        ps.setFloat(14, r.getSaturatedFatContent());
                        ps.setFloat(15, r.getCholesterolContent());
                        ps.setFloat(16, r.getSodiumContent());
                        ps.setFloat(17, r.getCarbohydrateContent());
                        ps.setFloat(18, r.getFiberContent());
                        ps.setFloat(19, r.getSugarContent());
                        ps.setFloat(20, r.getProteinContent());
                        ps.setString(21, String.valueOf(r.getRecipeServings()));
                        ps.setString(22, r.getRecipeYield());
                    }

                    @Override
                    public int getBatchSize() {
                        return to - from;
                    }
                });
            }
        }

        // 5. 批量插入 recipe_ingredients
        if (recipeRecords != null && !recipeRecords.isEmpty()) {
            List<Long> ingredientRecipeIds = new ArrayList<>();
            List<String> ingredientParts = new ArrayList<>();
            for (RecipeRecord r : recipeRecords) {
                String[] ingredients = r.getRecipeIngredientParts();
                if (ingredients == null) continue;
                for (String ing : ingredients) {
                    if (ing == null) continue;
                    ingredientRecipeIds.add(r.getRecipeId());
                    ingredientParts.add(ing);
                }
            }

            if (!ingredientRecipeIds.isEmpty()) {
                String ingredientSql = "INSERT INTO recipe_ingredients (RecipeId, IngredientPart) VALUES (?, ?)";
                final int batchSize = 1000;
                for (int start = 0; start < ingredientRecipeIds.size(); start += batchSize) {
                    final int from = start;
                    final int to = Math.min(from + batchSize, ingredientRecipeIds.size());
                    jdbcTemplate.batchUpdate(ingredientSql, new BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            ps.setLong(1, ingredientRecipeIds.get(from + i));
                            ps.setString(2, ingredientParts.get(from + i));
                        }

                        @Override
                        public int getBatchSize() {
                            return to - from;
                        }
                    });
                }
            }
        }

        // 6. 批量插入 reviews（分批）
        if (reviewRecords != null && !reviewRecords.isEmpty()) {
            String reviewSql = "INSERT INTO reviews (" +
                    "ReviewId, RecipeId, AuthorId, Rating, Review, DateSubmitted, DateModified" +
                    ") VALUES (?,?,?,?,?,?,?)";
            final int batchSize = 1000;

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

        // 7. 批量插入 review_likes（分批）
        if (reviewRecords != null && !reviewRecords.isEmpty()) {
            List<Long> likeReviewIds = new ArrayList<>();
            List<Long> likeUserIds = new ArrayList<>();

            for (ReviewRecord r : reviewRecords) {
                long[] likes = r.getLikes();
                if (likes == null) continue;
                for (long uid : likes) {
                    likeReviewIds.add(r.getReviewId());
                    likeUserIds.add(uid);
                }
            }

            if (!likeReviewIds.isEmpty()) {
                String likeSql = "INSERT INTO review_likes (ReviewId, AuthorId) VALUES (?, ?)";
                final int batchSize = 1000;
                for (int start = 0; start < likeReviewIds.size(); start += batchSize) {
                    final int from = start;
                    final int to = Math.min(from + batchSize, likeReviewIds.size());
                    jdbcTemplate.batchUpdate(likeSql, new BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            ps.setLong(1, likeReviewIds.get(from + i));
                            ps.setLong(2, likeUserIds.get(from + i));
                        }

                        @Override
                        public int getBatchSize() {
                            return to - from;
                        }
                    });
                }
            }
        }

        // 8. 批量插入 user_follows（使用 followingUsers，避免重复，分批）
        if (userRecords != null && !userRecords.isEmpty()) {
            List<Long> followerIds = new ArrayList<>();
            List<Long> followingIds = new ArrayList<>();

            for (UserRecord u : userRecords) {
                long[] followingUsers = u.getFollowingUsers();
                if (followingUsers == null) continue;
                for (long fid : followingUsers) {
                    followerIds.add(u.getAuthorId());
                    followingIds.add(fid);
                }
            }

            if (!followerIds.isEmpty()) {
                String followSql = "INSERT INTO user_follows (FollowerId, FollowingId) VALUES (?, ?)";
                final int batchSize = 1000;
                for (int start = 0; start < followerIds.size(); start += batchSize) {
                    final int from = start;
                    final int to = Math.min(from + batchSize, followerIds.size());
                    jdbcTemplate.batchUpdate(followSql, new BatchPreparedStatementSetter() {
                        @Override
                        public void setValues(PreparedStatement ps, int i) throws SQLException {
                            ps.setLong(1, followerIds.get(from + i));
                            ps.setLong(2, followingIds.get(from + i));
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

        for (String sql : createTableSQLs) {
            jdbcTemplate.execute(sql);
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
