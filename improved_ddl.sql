-- ============================================
-- SUSTC 数据库 DDL - 改进版
-- 结合新设计的优点和现有代码的兼容性需求
-- ============================================

-- ============================================
-- 1. 核心表（保持兼容性）
-- ============================================

-- 用户表
CREATE TABLE IF NOT EXISTS users (
    AuthorId BIGINT PRIMARY KEY,
    AuthorName TEXT NOT NULL,
    Gender VARCHAR(10) CHECK (Gender IN ('Male', 'Female')),
    Age INTEGER CHECK (Age > 0),
    Password TEXT,  -- 必需：用于登录验证
    IsDeleted BOOLEAN DEFAULT FALSE,  -- 必需：软删除支持
    Followers INTEGER DEFAULT 0 CHECK (Followers >= 0),  -- 冗余字段，可优化但保留兼容性
    Following INTEGER DEFAULT 0 CHECK (Following >= 0)   -- 冗余字段，可优化但保留兼容性
);

-- 食谱表（保留核心字段，营养信息分离）
CREATE TABLE IF NOT EXISTS recipes (
    RecipeId BIGINT PRIMARY KEY,
    AuthorId BIGINT NOT NULL,  -- 不允许 NULL，使用软删除而非级联删除
    Name TEXT NOT NULL,
    CookTime TEXT,  -- ISO 8601 格式
    PrepTime TEXT,  -- ISO 8601 格式
    TotalTime TEXT,  -- ISO 8601 格式（必需字段）
    DatePublished TIMESTAMP,
    Description TEXT,
    RecipeCategory TEXT,
    RecipeServings INTEGER,  -- 改为 INTEGER 类型
    RecipeYield TEXT,
    -- 聚合字段（必需：用于快速查询和排序）
    AggregatedRating DECIMAL(3,2) CHECK (AggregatedRating >= 0 AND AggregatedRating <= 5),
    ReviewCount INTEGER DEFAULT 0 CHECK (ReviewCount >= 0),
    -- 外键约束：使用软删除，不级联删除
    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)
);

-- 评论表
CREATE TABLE IF NOT EXISTS reviews (
    ReviewId BIGINT PRIMARY KEY,
    RecipeId BIGINT NOT NULL,
    AuthorId BIGINT NOT NULL,
    Rating INTEGER NOT NULL CHECK (Rating >= 1 AND Rating <= 5),
    Review TEXT,
    DateSubmitted TIMESTAMP,
    DateModified TIMESTAMP,
    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId) ON DELETE CASCADE,  -- 删除食谱时删除评论
    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)  -- 不级联删除，使用软删除
);

-- ============================================
-- 2. 关联表（保持兼容性）
-- ============================================

-- 食谱配料表（兼容旧设计：直接存储字符串）
-- 同时支持新设计的规范化方式（见下方）
CREATE TABLE IF NOT EXISTS recipe_ingredients (
    RecipeId BIGINT,
    IngredientPart TEXT,  -- 改为 TEXT，支持更长的配料名
    PRIMARY KEY (RecipeId, IngredientPart),
    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId) ON DELETE CASCADE
);

-- 评论点赞表（保持表名兼容）
CREATE TABLE IF NOT EXISTS review_likes (
    ReviewId BIGINT,
    AuthorId BIGINT,
    PRIMARY KEY (ReviewId, AuthorId),
    FOREIGN KEY (ReviewId) REFERENCES reviews(ReviewId) ON DELETE CASCADE,
    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)  -- 不级联删除
);

-- 用户关注表
CREATE TABLE IF NOT EXISTS user_follows (
    FollowerId BIGINT,
    FollowingId BIGINT,
    PRIMARY KEY (FollowerId, FollowingId),
    FOREIGN KEY (FollowerId) REFERENCES users(AuthorId),  -- 不级联删除
    FOREIGN KEY (FollowingId) REFERENCES users(AuthorId),  -- 不级联删除
    CHECK (FollowerId != FollowingId)  -- 不能关注自己
);

-- ============================================
-- 3. 扩展表（新设计的功能）
-- ============================================

-- 营养信息表（规范化设计）
CREATE TABLE IF NOT EXISTS nutrition (
    RecipeId BIGINT PRIMARY KEY,
    Calories NUMERIC(10, 2) NOT NULL,
    FatContent NUMERIC(10, 2),
    SaturatedFatContent NUMERIC(10, 2),
    CholesterolContent NUMERIC(10, 2),
    SodiumContent NUMERIC(10, 2),
    CarbohydrateContent NUMERIC(10, 2),
    FiberContent NUMERIC(10, 2),
    SugarContent NUMERIC(10, 2),
    ProteinContent NUMERIC(10, 2),
    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId) ON DELETE CASCADE
);

-- 制作步骤表（新功能）
CREATE TABLE IF NOT EXISTS instructions (
    RecipeId BIGINT,
    StepNumber INTEGER,
    InstructionText TEXT NOT NULL,
    PRIMARY KEY (RecipeId, StepNumber),
    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId) ON DELETE CASCADE
);

-- 配料规范化表（可选：用于新功能）
CREATE TABLE IF NOT EXISTS ingredients (
    IngredientId BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    IngredientName TEXT NOT NULL UNIQUE,
    -- 可以添加其他属性，如单位、分类等
    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 食谱-配料关联表（规范化版本，可选）
-- 注意：如果使用此表，需要同步维护 recipe_ingredients 表以保持兼容性
CREATE TABLE IF NOT EXISTS recipe_ingredients_normalized (
    RecipeId BIGINT,
    IngredientId BIGINT,
    Quantity TEXT,  -- 可选：数量信息
    Unit TEXT,      -- 可选：单位
    PRIMARY KEY (RecipeId, IngredientId),
    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId) ON DELETE CASCADE,
    FOREIGN KEY (IngredientId) REFERENCES ingredients(IngredientId) ON DELETE CASCADE
);

-- 关键词表（新功能：标签系统）
CREATE TABLE IF NOT EXISTS keywords (
    KeywordId BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    KeywordText TEXT NOT NULL UNIQUE,
    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 食谱-关键词关联表
CREATE TABLE IF NOT EXISTS recipe_keywords (
    RecipeId BIGINT,
    KeywordId BIGINT,
    PRIMARY KEY (RecipeId, KeywordId),
    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId) ON DELETE CASCADE,
    FOREIGN KEY (KeywordId) REFERENCES keywords(KeywordId) ON DELETE CASCADE
);

-- 用户收藏表（新功能）
CREATE TABLE IF NOT EXISTS user_favorite_recipes (
    AuthorId BIGINT,
    RecipeId BIGINT,
    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (AuthorId, RecipeId),
    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId),  -- 不级联删除
    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId) ON DELETE CASCADE
);

-- ============================================
-- 4. 索引优化（提升查询性能）
-- ============================================

-- users 表索引
CREATE INDEX IF NOT EXISTS idx_users_authorname ON users(AuthorName);
CREATE INDEX IF NOT EXISTS idx_users_isdeleted ON users(IsDeleted) WHERE IsDeleted = FALSE;

-- recipes 表索引
CREATE INDEX IF NOT EXISTS idx_recipes_authorid ON recipes(AuthorId);
CREATE INDEX IF NOT EXISTS idx_recipes_category ON recipes(RecipeCategory);
CREATE INDEX IF NOT EXISTS idx_recipes_datepublished ON recipes(DatePublished DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_recipes_rating ON recipes(AggregatedRating DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_recipes_reviewcount ON recipes(ReviewCount DESC);
-- 复合索引：用于 feed 查询
CREATE INDEX IF NOT EXISTS idx_recipes_feed ON recipes(AuthorId, RecipeCategory, DatePublished DESC NULLS LAST);
-- 复合索引：用于搜索和排序
CREATE INDEX IF NOT EXISTS idx_recipes_category_rating ON recipes(RecipeCategory, AggregatedRating DESC NULLS LAST);
-- 全文搜索索引（PostgreSQL）
CREATE INDEX IF NOT EXISTS idx_recipes_name_lower ON recipes(LOWER(Name));
CREATE INDEX IF NOT EXISTS idx_recipes_description_lower ON recipes(LOWER(Description));

-- reviews 表索引
CREATE INDEX IF NOT EXISTS idx_reviews_recipeid ON reviews(RecipeId);
CREATE INDEX IF NOT EXISTS idx_reviews_authorid ON reviews(AuthorId);
CREATE INDEX IF NOT EXISTS idx_reviews_datemodified ON reviews(DateModified DESC);
CREATE INDEX IF NOT EXISTS idx_reviews_rating ON reviews(Rating);
-- 复合索引：用于评论列表查询
CREATE INDEX IF NOT EXISTS idx_reviews_recipe_date ON reviews(RecipeId, DateModified DESC);

-- recipe_ingredients 表索引
CREATE INDEX IF NOT EXISTS idx_recipe_ingredients_recipeid ON recipe_ingredients(RecipeId);
CREATE INDEX IF NOT EXISTS idx_recipe_ingredients_part_lower ON recipe_ingredients(LOWER(IngredientPart));

-- review_likes 表索引
CREATE INDEX IF NOT EXISTS idx_review_likes_reviewid ON review_likes(ReviewId);
CREATE INDEX IF NOT EXISTS idx_review_likes_authorid ON review_likes(AuthorId);

-- user_follows 表索引（关键优化）
CREATE INDEX IF NOT EXISTS idx_user_follows_followerid ON user_follows(FollowerId);
CREATE INDEX IF NOT EXISTS idx_user_follows_followingid ON user_follows(FollowingId);

-- nutrition 表索引
CREATE INDEX IF NOT EXISTS idx_nutrition_calories ON nutrition(Calories ASC NULLS LAST);

-- instructions 表索引
CREATE INDEX IF NOT EXISTS idx_instructions_recipeid ON instructions(RecipeId);

-- 规范化配料表索引
CREATE INDEX IF NOT EXISTS idx_recipe_ingredients_norm_recipeid ON recipe_ingredients_normalized(RecipeId);
CREATE INDEX IF NOT EXISTS idx_recipe_ingredients_norm_ingredientid ON recipe_ingredients_normalized(IngredientId);
CREATE INDEX IF NOT EXISTS idx_ingredients_name_lower ON ingredients(LOWER(IngredientName));

-- 关键词表索引
CREATE INDEX IF NOT EXISTS idx_recipe_keywords_recipeid ON recipe_keywords(RecipeId);
CREATE INDEX IF NOT EXISTS idx_recipe_keywords_keywordid ON recipe_keywords(KeywordId);
CREATE INDEX IF NOT EXISTS idx_keywords_text_lower ON keywords(LOWER(KeywordText));

-- 用户收藏表索引
CREATE INDEX IF NOT EXISTS idx_user_favorite_recipes_authorid ON user_favorite_recipes(AuthorId);
CREATE INDEX IF NOT EXISTS idx_user_favorite_recipes_recipeid ON user_favorite_recipes(RecipeId);
CREATE INDEX IF NOT EXISTS idx_user_favorite_recipes_created ON user_favorite_recipes(CreatedAt DESC);

-- ============================================
-- 5. 触发器（自动维护聚合字段）
-- ============================================

-- 自动更新 recipes 的 AggregatedRating 和 ReviewCount
CREATE OR REPLACE FUNCTION update_recipe_rating()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE recipes
    SET 
        AggregatedRating = (
            SELECT COALESCE(AVG(Rating), 0)
            FROM reviews
            WHERE RecipeId = COALESCE(NEW.RecipeId, OLD.RecipeId)
        ),
        ReviewCount = (
            SELECT COUNT(*)
            FROM reviews
            WHERE RecipeId = COALESCE(NEW.RecipeId, OLD.RecipeId)
        )
    WHERE RecipeId = COALESCE(NEW.RecipeId, OLD.RecipeId);
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- 当评论被插入、更新或删除时触发
CREATE TRIGGER trigger_update_recipe_rating_insert
    AFTER INSERT ON reviews
    FOR EACH ROW
    EXECUTE FUNCTION update_recipe_rating();

CREATE TRIGGER trigger_update_recipe_rating_update
    AFTER UPDATE OF Rating ON reviews
    FOR EACH ROW
    EXECUTE FUNCTION update_recipe_rating();

CREATE TRIGGER trigger_update_recipe_rating_delete
    AFTER DELETE ON reviews
    FOR EACH ROW
    EXECUTE FUNCTION update_recipe_rating();

-- ============================================
-- 6. 视图（便于查询）
-- ============================================

-- 完整的食谱视图（包含营养信息）
CREATE OR REPLACE VIEW recipe_full AS
SELECT 
    r.RecipeId,
    r.AuthorId,
    r.Name,
    r.CookTime,
    r.PrepTime,
    r.TotalTime,
    r.DatePublished,
    r.Description,
    r.RecipeCategory,
    r.RecipeServings,
    r.RecipeYield,
    r.AggregatedRating,
    r.ReviewCount,
    n.Calories,
    n.FatContent,
    n.SaturatedFatContent,
    n.CholesterolContent,
    n.SodiumContent,
    n.CarbohydrateContent,
    n.FiberContent,
    n.SugarContent,
    n.ProteinContent
FROM recipes r
LEFT JOIN nutrition n ON r.RecipeId = n.RecipeId;

-- ============================================
-- 7. 注释说明
-- ============================================

COMMENT ON TABLE users IS '用户表，支持软删除';
COMMENT ON TABLE recipes IS '食谱表，包含核心信息和聚合字段';
COMMENT ON TABLE nutrition IS '营养信息表，规范化设计';
COMMENT ON TABLE reviews IS '评论表';
COMMENT ON TABLE recipe_ingredients IS '食谱配料表（兼容旧设计）';
COMMENT ON TABLE ingredients IS '配料规范化表（新设计）';
COMMENT ON TABLE instructions IS '制作步骤表';
COMMENT ON TABLE keywords IS '关键词/标签表';
COMMENT ON TABLE user_favorite_recipes IS '用户收藏表';
COMMENT ON COLUMN users.IsDeleted IS '软删除标记，TRUE表示已删除';
COMMENT ON COLUMN recipes.AggregatedRating IS '聚合评分，由触发器自动更新';
COMMENT ON COLUMN recipes.ReviewCount IS '评论数量，由触发器自动更新';

