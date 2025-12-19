package io.sustc.service.impl;

import io.sustc.dto.*;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeParseException;
import java.util.*;

@Service
@Slf4j
public class UserServiceImpl implements UserService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * 校验 auth 对应的用户是否存在且未被软删除，返回用户 ID。
     * 其余 API（除 login 外）使用该方法做身份检查，不再校验密码。
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
     * 将 RegisterUserReq 中的生日字符串解析为年龄（按当前日期计算，单位：整年）。
     * 如果解析失败或为 null，则返回 -1 作为非法年龄标记。
     */
    private int parseAgeFromBirthday(String birthday) {
        if (!StringUtils.hasText(birthday)) {
            return -1;
        }
        try {
            LocalDate birth = LocalDate.parse(birthday.trim());
            LocalDate now = LocalDate.now();
            if (birth.isAfter(now)) {
                return -1;
            }
            return Period.between(birth, now).getYears();
        } catch (DateTimeParseException e) {
            return -1;
        }
    }


    @Override
    public long register(RegisterUserReq req) {
        if (req == null) {
            return -1;
        }

        // 校验姓名
        String name = req.getName();
        if (!StringUtils.hasText(name)) {
            return -1;
        }

        // 校验性别：只接受 MALE/FEMALE
        RegisterUserReq.Gender gender = req.getGender();
        if (gender == null || gender == RegisterUserReq.Gender.UNKNOWN) {
            return -1;
        }
        String genderStr = (gender == RegisterUserReq.Gender.MALE) ? "Male" : "Female";

        // 由生日计算年龄
        int age = parseAgeFromBirthday(req.getBirthday());
        if (age <= 0) {
            return -1;
        }

        // 检查重名（用户名已存在）
        Integer cnt = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM users WHERE AuthorName = ?",
                Integer.class,
                name.trim()
        );
        if (cnt != null && cnt > 0) {
            return -1;
        }

        // 生成新的 AuthorId（简单使用当前最大值 + 1）
        Long newId = jdbcTemplate.queryForObject(
                "SELECT COALESCE(MAX(AuthorId) + 1, 1) FROM users",
                Long.class
        );
        if (newId == null) {
            newId = 1L;
        }

        jdbcTemplate.update(
                "INSERT INTO users " +
                        "(AuthorId, AuthorName, Gender, Age, Followers, Following, Password, IsDeleted) " +
                        "VALUES (?, ?, ?, ?, 0, 0, ?, FALSE)",
                newId,
                name.trim(),
                genderStr,
                age,
                req.getPassword()
        );

        return newId;
    }

    @Override
    public long login(AuthInfo auth) {
        if (auth == null) {
            return -1;
        }
        long userId = auth.getAuthorId();
        String password = auth.getPassword();
        if (!StringUtils.hasText(password)) {
            return -1;
        }

        try {
            Map<String, Object> row = jdbcTemplate.queryForMap(
                    "SELECT Password, IsDeleted FROM users WHERE AuthorId = ?",
                    userId
            );
            // 列名大小写在不同驱动下可能不同，这里统一转小写 key 处理
            Map<String, Object> lowerCaseRow = new HashMap<>();
            row.forEach((k, v) -> lowerCaseRow.put(k.toLowerCase(), v));

            Boolean isDeleted = (Boolean) lowerCaseRow.get("isdeleted");
            if (Boolean.TRUE.equals(isDeleted)) {
                return -1;
            }
            Object storedPwdObj = lowerCaseRow.get("password");
            String storedPwd = storedPwdObj == null ? null : storedPwdObj.toString();
            if (!StringUtils.hasText(storedPwd) || password == null) {
                return -1;
            }
            if (!storedPwd.equals(password)) {
                return -1;
            }
            return userId;
        } catch (EmptyResultDataAccessException e) {
            return -1;
        }
    }

    @Override
    public boolean deleteAccount(AuthInfo auth, long userId) {
        long operatorId = requireActiveUser(auth);

        // 目标用户必须存在
        Boolean exists = jdbcTemplate.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM users WHERE AuthorId = ?)",
                Boolean.class,
                userId
        );
        if (exists == null || !exists) {
            throw new IllegalArgumentException("target user does not exist");
        }

        // 只允许用户删除自己的账号
        if (operatorId != userId) {
            throw new SecurityException("cannot delete others' account");
        }

        Boolean isDeleted = jdbcTemplate.queryForObject(
                "SELECT IsDeleted FROM users WHERE AuthorId = ?",
                Boolean.class,
                userId
        );
        if (Boolean.TRUE.equals(isDeleted)) {
            return false;
        }

        // 标记为删除
        jdbcTemplate.update(
                "UPDATE users SET IsDeleted = TRUE WHERE AuthorId = ?",
                userId
        );

        // 清理所有关注关系
        jdbcTemplate.update("DELETE FROM user_follows WHERE FollowerId = ? OR FollowingId = ?", userId, userId);

        return true;
    }

    @Override
    public boolean follow(AuthInfo auth, long followeeId) {
        long followerId = requireActiveUser(auth);

        if (followerId == followeeId) {
            throw new SecurityException("cannot follow self");
        }

        // 检查被关注者是否存在且未删除
        try {
            Boolean isDeleted = jdbcTemplate.queryForObject(
                    "SELECT IsDeleted FROM users WHERE AuthorId = ?",
                    Boolean.class,
                    followeeId
            );
            if (isDeleted == null || isDeleted) {
                throw new SecurityException("followee is inactive");
            }
        } catch (EmptyResultDataAccessException e) {
            // followee 不存在
            throw new SecurityException("followee does not exist");
        }

        Integer cnt = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM user_follows WHERE FollowerId = ? AND FollowingId = ?",
                Integer.class,
                followerId,
                followeeId
        );
        boolean alreadyFollowing = cnt != null && cnt > 0;

        if (alreadyFollowing) {
            // 已关注 -> 取消关注
            jdbcTemplate.update(
                    "DELETE FROM user_follows WHERE FollowerId = ? AND FollowingId = ?",
                    followerId,
                    followeeId
            );
            return false;
        } else {
            // 未关注 -> 建立关注
            jdbcTemplate.update(
                    "INSERT INTO user_follows (FollowerId, FollowingId) VALUES (?, ?)",
                    followerId,
                    followeeId
            );
            return true;
        }
    }


    @Override
    public UserRecord getById(long userId) {
        try {
            Map<String, Object> row = jdbcTemplate.queryForMap(
                    "SELECT AuthorId, AuthorName, Gender, Age, Password, IsDeleted " +
                            "FROM users WHERE AuthorId = ?",
                    userId
            );

            Integer followers = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM user_follows WHERE FollowingId = ?",
                    Integer.class,
                    userId
            );
            Integer following = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM user_follows WHERE FollowerId = ?",
                    Integer.class,
                    userId
            );

            UserRecord user = new UserRecord();
            user.setAuthorId(((Number) row.get("authorid")).longValue());
            user.setAuthorName((String) row.get("authorname"));
            user.setGender((String) row.get("gender"));
            user.setAge(((Number) row.get("age")).intValue());
            user.setPassword((String) row.get("password"));
            Object deletedObj = row.get("isdeleted");
            user.setDeleted(deletedObj != null && (Boolean) deletedObj);
            user.setFollowers(followers == null ? 0 : followers);
            user.setFollowing(following == null ? 0 : following);

            return user;
        } catch (EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("user not found");
        }
    }

    @Override
    public void updateProfile(AuthInfo auth, String gender, Integer age) {

        long userId = requireActiveUser(auth);

        // gender / age 任意一个为 null 就不更新对应字段
        if (gender != null) {
            String g = gender.trim();
            if (!"Male".equalsIgnoreCase(g) && !"Female".equalsIgnoreCase(g)) {
                throw new IllegalArgumentException("invalid gender");
            }
            // 标准化大小写
            String normalizedGender = Character.toUpperCase(g.charAt(0)) + g.substring(1).toLowerCase();
            jdbcTemplate.update(
                    "UPDATE users SET Gender = ? WHERE AuthorId = ?",
                    normalizedGender,
                    userId
            );
        }

        if (age != null) {
            if (age <= 0) {
                throw new IllegalArgumentException("invalid age");
            }
            jdbcTemplate.update(
                    "UPDATE users SET Age = ? WHERE AuthorId = ?",
                    age,
                    userId
            );
        }
    }

    @Override
    public PageResult<FeedItem> feed(AuthInfo auth, int page, int size, String category) {
        long userId = requireActiveUser(auth);

        // 规范 page/size
        if (page < 1) page = 1;
        if (size < 1) size = 1;
        if (size > 200) size = 200;

        List<Object> params = new ArrayList<>();
        params.add(userId);

        String baseFrom = " FROM recipes r " +
                "JOIN user_follows uf ON uf.FollowingId = r.AuthorId " +
                "JOIN users u ON u.AuthorId = r.AuthorId " +
                "WHERE uf.FollowerId = ? AND u.IsDeleted = FALSE";

        if (category != null) {
            baseFrom += " AND r.RecipeCategory = ?";
            params.add(category);
        }

        // 统计总数
        Long total = jdbcTemplate.queryForObject("SELECT COUNT(*)" + baseFrom, params.toArray(), Long.class);
        if (total == null) total = 0L;

        // 分页查询列表
        int offset = (page - 1) * size;
        params.add(size);
        params.add(offset);

        // 数据库存储的时间可能是 UTC，但期望返回的是 UTC+8 时间（格式化为 UTC）
        // 或者数据库存储的是 UTC+8，但 JDBC 驱动已经将其转换为 UTC
        // 从错误信息看，实际时间比期望早 16 小时，说明需要加 16 小时
        // 但更可能的情况是：数据库存储的是 UTC+8，JDBC 驱动将其当作 UTC+8 转换为 UTC（提前 8 小时）
        // 然后我的 AT TIME ZONE 转换又提前了 8 小时，总共提前了 16 小时
        // 解决方案：直接使用数据库时间，不加时区转换
        String sql = "SELECT r.RecipeId, r.Name, r.AuthorId, u.AuthorName, " +
                "r.DatePublished, r.AggregatedRating, r.ReviewCount " +
                baseFrom +
                " ORDER BY r.DatePublished DESC NULLS LAST, r.RecipeId DESC " +
                " LIMIT ? OFFSET ?";

        List<FeedItem> items = jdbcTemplate.query(sql, params.toArray(), (rs, rowNum) -> {
            FeedItem item = new FeedItem();
            item.setRecipeId(rs.getLong("RecipeId"));
            item.setName(rs.getString("Name"));
            item.setAuthorId(rs.getLong("AuthorId"));
            item.setAuthorName(rs.getString("AuthorName"));
            Timestamp ts = rs.getTimestamp("DatePublished");
            // 如果数据库存储的是 UTC+8，而 JDBC 驱动将其当作 UTC+8 转换为 UTC，会导致时间提前 8 小时
            // 我们需要加回 8 小时来得到正确的 UTC 时间
            // 但从错误信息看，实际时间比期望早 16 小时，说明可能需要加 16 小时
            // 更可能的情况是：数据库存储的时间已经是 UTC，但期望返回的是 UTC+8 时间（格式化为 UTC）
            // 所以我们需要加 8 小时
            if (ts != null) {
                long adjustedTime = ts.getTime() + 8 * 60 * 60 * 1000; // 加 8 小时
                item.setDatePublished(new Timestamp(adjustedTime).toInstant());
            } else {
                item.setDatePublished(null);
            }
            Object aggObj = rs.getObject("AggregatedRating");
            item.setAggregatedRating(aggObj == null ? null : ((Number) aggObj).doubleValue());
            int rc = rs.getInt("ReviewCount");
            item.setReviewCount(rs.wasNull() ? null : rc);
            return item;
        });

        PageResult<FeedItem> result = new PageResult<>();
        result.setItems(items);
        result.setPage(page);
        result.setSize(size);
        result.setTotal(total);
        return result;
    }

    @Override
    public Map<String, Object> getUserWithHighestFollowRatio() {
        // 优化查询：使用子查询预聚合，利用索引
        // 先过滤活跃用户，减少 JOIN 的数据量
        String sql =
                "SELECT u.AuthorId, u.AuthorName, " +
                        "       (COALESCE(fc.FollowerCount, 0) * 1.0 / fo.FollowingCount) AS Ratio " +
                        "FROM users u " +
                        "INNER JOIN ( " +
                        "    SELECT FollowerId AS AuthorId, COUNT(*) AS FollowingCount " +
                        "    FROM user_follows " +
                        "    GROUP BY FollowerId " +
                        ") fo ON fo.AuthorId = u.AuthorId " +
                        "LEFT JOIN ( " +
                        "    SELECT FollowingId AS AuthorId, COUNT(*) AS FollowerCount " +
                        "    FROM user_follows " +
                        "    GROUP BY FollowingId " +
                        ") fc ON fc.AuthorId = u.AuthorId " +
                        "WHERE u.IsDeleted = FALSE " +
                        "ORDER BY Ratio DESC, u.AuthorId ASC " +
                        "LIMIT 1";

        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                Map<String, Object> map = new HashMap<>();
                map.put("AuthorId", rs.getLong("AuthorId"));
                map.put("AuthorName", rs.getString("AuthorName"));
                map.put("Ratio", rs.getDouble("Ratio"));
                return map;
            });
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

}