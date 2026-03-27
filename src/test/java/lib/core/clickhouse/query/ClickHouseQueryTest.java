package lib.core.clickhouse.query;


import lib.core.clickhouse.expression.CH;
import lib.core.clickhouse.query.builder.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

import java.time.Instant;
import java.util.List;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;


import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link ClickHouseQuery} fluent builder.
 */
class ClickHouseQueryTest {

    // ── SELECT ───────────────────────────────────────────────────────────

    @Nested
    @DisplayName("SELECT")
    class SelectTests {

        @Test
        @DisplayName("Basic SELECT FROM")
        void basicSelect() {
            String sql = ClickHouseQuery
                    .select("user_id", "sum(amount) AS total")
                    .from("orders")
                    .toSql();

            assertTrue(sql.contains("SELECT user_id"));
            assertTrue(sql.contains("sum(amount) AS total"));
            assertTrue(sql.contains("FROM orders"));
        }

        @Test
        @DisplayName("SELECT DISTINCT")
        void selectDistinct() {
            String sql = ClickHouseQuery
                    .selectDistinct("user_id", "product_id")
                    .from("orders")
                    .toSql();

            assertTrue(sql.startsWith("SELECT DISTINCT"));
            assertTrue(sql.contains("user_id"));
        }
    }

    // ── WHERE ────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("WHERE clauses")
    class WhereTests {

        @Test
        @DisplayName("where(col).eq(value) generates correct SQL and params")
        void whereEq() {
            ClickHouseQuery q = ClickHouseQuery
                    .select("*")
                    .from("orders")
                    .where("tenant_id").eq("op-123");

            assertTrue(q.toSql().contains("WHERE tenant_id = :tenantId"));
            assertEquals("op-123", q.toParams().getValue("tenantId"));
        }

        @Test
        @DisplayName("where(col).eqIfNotBlank skips blank value")
        void eqIfNotBlank_blank() {
            String sql = ClickHouseQuery
                    .select("*")
                    .from("t")
                    .where("status").eqIfNotBlank("  ")
                    .toSql();

            assertFalse(sql.contains("status"));
        }

        @Test
        @DisplayName("where(col).eqIfNotBlank adds non-blank value")
        void eqIfNotBlank_valid() {
            ClickHouseQuery q = ClickHouseQuery
                    .select("*")
                    .from("t")
                    .where("order_status").eqIfNotBlank("ACTIVE");

            assertTrue(q.toSql().contains("order_status = :orderStatus"));
            assertEquals("ACTIVE", q.toParams().getValue("orderStatus"));
        }

        @Test
        @DisplayName("where(col).eqIf(false) skips")
        void eqIf_false() {
            String sql = ClickHouseQuery
                    .select("*")
                    .from("t")
                    .where("status").eqIf(false, "ACTIVE")
                    .toSql();

            assertFalse(sql.contains("status"));
        }

        @Test
        @DisplayName("where(col).eqIf(true) adds clause")
        void eqIf_true() {
            ClickHouseQuery q = ClickHouseQuery
                    .select("*")
                    .from("t")
                    .where("status").eqIf(true, "ACTIVE");

            assertTrue(q.toSql().contains("status = :status"));
            assertEquals("ACTIVE", q.toParams().getValue("status"));
        }

        @Test
        @DisplayName("where(col).ne(value)")
        void whereNe() {
            ClickHouseQuery q = ClickHouseQuery
                    .select("*")
                    .from("t")
                    .where("status").ne("DELETED");

            assertTrue(q.toSql().contains("status != :statusNe"));
            assertEquals("DELETED", q.toParams().getValue("statusNe"));
        }

        @Test
        @DisplayName("where(col).gt(value)")
        void whereGt() {
            ClickHouseQuery q = ClickHouseQuery
                    .select("*")
                    .from("t")
                    .where("amount").gt(100);

            assertTrue(q.toSql().contains("amount > :amountGt"));
            assertEquals(100, q.toParams().getValue("amountGt"));
        }

        @Test
        @DisplayName("where(col).gte(value)")
        void whereGte() {
            ClickHouseQuery q = ClickHouseQuery
                    .select("*")
                    .from("t")
                    .where("amount").gte(50);

            assertTrue(q.toSql().contains("amount >= :amountGte"));
            assertEquals(50, q.toParams().getValue("amountGte"));
        }

        @Test
        @DisplayName("where(col).lt(value)")
        void whereLt() {
            ClickHouseQuery q = ClickHouseQuery
                    .select("*")
                    .from("t")
                    .where("score").lt(10);

            assertTrue(q.toSql().contains("score < :scoreLt"));
            assertEquals(10, q.toParams().getValue("scoreLt"));
        }

        @Test
        @DisplayName("where(col).lte(value)")
        void whereLte() {
            ClickHouseQuery q = ClickHouseQuery
                    .select("*")
                    .from("t")
                    .where("score").lte(99);

            assertTrue(q.toSql().contains("score <= :scoreLte"));
            assertEquals(99, q.toParams().getValue("scoreLte"));
        }

        @Test
        @DisplayName("where(col).in generates expanded IN clause")
        void whereIn() {
            ClickHouseQuery q = ClickHouseQuery
                    .select("*")
                    .from("orders")
                    .where("product_id").in(List.of("g1", "g2", "g3"));

            String sql = q.toSql();
            assertTrue(sql.contains("product_id IN (:productId0, :productId1, :productId2)"));
            assertEquals("g1", q.toParams().getValue("productId0"));
            assertEquals("g2", q.toParams().getValue("productId1"));
            assertEquals("g3", q.toParams().getValue("productId2"));
        }

        @Test
        @DisplayName("null value in eq/ne/gt/gte/lt/lte is silently skipped")
        void whereNullSkipped() {
            String sql = ClickHouseQuery
                    .select("*").from("t")
                    .where("col1").eq(null)
                    .where("col2").ne(null)
                    .where("col3").gt(null)
                    .where("col4").gte(null)
                    .where("col5").lt(null)
                    .where("col6").lte(null)
                    .toSql();

            assertFalse(sql.contains("WHERE"), "No WHERE clause when all values are null");
        }

        @Test
        @DisplayName("null values mixed with non-null — only non-null applied")
        void whereNullMixed() {
            String sql = ClickHouseQuery
                    .select("*").from("t")
                    .where("status").eq(null)        // skipped
                    .where("amount").gt(100)          // applied
                    .where("category").eq(null)       // skipped
                    .toSql();

            assertFalse(sql.contains("status"));
            assertFalse(sql.contains("category"));
            assertTrue(sql.contains("amount > :amountGt"));
        }

        @Test
        @DisplayName("where(col).in with empty list is skipped")
        void whereIn_empty() {
            String sql = ClickHouseQuery
                    .select("*")
                    .from("t")
                    .where("product_id").in(List.of())
                    .toSql();

            assertFalse(sql.contains("IN"));
        }

        @Test
        @DisplayName("where(col).notIn generates NOT IN clause")
        void whereNotIn() {
            ClickHouseQuery q = ClickHouseQuery
                    .select("*")
                    .from("t")
                    .where("status").notIn(List.of("DELETED", "BANNED"));

            String sql = q.toSql();
            assertTrue(sql.contains("NOT IN"));
        }

        @Test
        @DisplayName("where(col).isNull generates IS NULL")
        void whereIsNull() {
            String sql = ClickHouseQuery
                    .select("*")
                    .from("t")
                    .where("deleted_at").isNull()
                    .toSql();

            assertTrue(sql.contains("deleted_at IS NULL"));
        }

        @Test
        @DisplayName("where(col).isNotNull generates IS NOT NULL")
        void whereIsNotNull() {
            String sql = ClickHouseQuery
                    .select("*")
                    .from("t")
                    .where("error_code").isNotNull()
                    .toSql();

            assertTrue(sql.contains("error_code IS NOT NULL"));
        }

        @Test
        @DisplayName("where(col).inSubQuery generates IN (subquery)")
        void whereInSubQuery() {
            String sql = ClickHouseQuery
                    .select("*")
                    .from("t")
                    .where("product_id").inSubQuery("SELECT id FROM games WHERE active = 1")
                    .toSql();

            assertTrue(sql.contains("product_id IN (SELECT id FROM games WHERE active = 1)"));
        }

        @Test
        @DisplayName("where(col).notInSubQuery generates NOT IN (subquery)")
        void whereNotInSubQuery() {
            String sql = ClickHouseQuery
                    .select("*")
                    .from("t")
                    .where("user_id").notInSubQuery("SELECT id FROM banned_users")
                    .toSql();

            assertTrue(sql.contains("user_id NOT IN (SELECT id FROM banned_users)"));
        }

        @Test
        @DisplayName("Fluent where(col).in(ClickHouseQuery subQuery)")
        void whereInSubQueryFluent() {
            String sql = ClickHouseQuery
                    .select("*")
                    .from("orders")
                    .where("product_id").in(
                            ClickHouseQuery.select("id").from("games").where("active").eq(1)
                    )
                    .toSql();

            assertTrue(sql.contains("product_id IN (SELECT id"));
            assertTrue(sql.contains("FROM games"));
            assertTrue(sql.contains("active = :active"));
        }

        @Test
        @DisplayName("Fluent where(col).notIn(ClickHouseQuery subQuery)")
        void whereNotInSubQueryFluent() {
            String sql = ClickHouseQuery
                    .select("*")
                    .from("orders")
                    .where("user_id").notIn(
                            ClickHouseQuery.select("id").from("banned_users").where("status").eq("BANNED")
                    )
                    .toSql();

            assertTrue(sql.contains("user_id NOT IN (SELECT id"));
            assertTrue(sql.contains("FROM banned_users"));
            assertTrue(sql.contains("status = :status"));
        }

        @Test
        @DisplayName("where(col).between generates date range")
        void between() {
            Instant from = Instant.parse("2026-01-01T00:00:00Z");
            Instant to = Instant.parse("2026-12-31T23:59:59Z");

            ClickHouseQuery q = ClickHouseQuery
                    .select("*")
                    .from("order_items")
                    .where("created_at").between(from, to);

            String sql = q.toSql();
            assertTrue(sql.contains("created_at >= :createdAtFrom"));
            assertTrue(sql.contains("created_at <= :createdAtTo"));
            assertEquals("2026-01-01 00:00:00.000", q.toParams().getValue("createdAtFrom"));
            assertEquals("2026-12-31 23:59:59.000", q.toParams().getValue("createdAtTo"));
        }

        @Test
        @DisplayName("where(col).between with null from skips lower bound")
        void between_nullFrom() {
            Instant to = Instant.parse("2026-12-31T23:59:59Z");

            String sql = ClickHouseQuery
                    .select("*")
                    .from("t")
                    .where("created_at").between(null, to)
                    .toSql();

            assertFalse(sql.contains(">="));
            assertTrue(sql.contains("<="));
        }

        @Test
        @DisplayName("whereILike(kw).on(cols) generates ILIKE OR")
        void whereILike() {
            ClickHouseQuery q = ClickHouseQuery
                    .select("*")
                    .from("order_items")
                    .whereILike("test").on("order_id", "user_id");

            String sql = q.toSql();
            assertTrue(sql.contains("(order_id ILIKE :_keyword OR user_id ILIKE :_keyword)"));
            assertEquals("%test%", q.toParams().getValue("_keyword"));
        }

        @Test
        @DisplayName("whereILike(null).on(cols) is skipped")
        void whereILike_null() {
            String sql = ClickHouseQuery
                    .select("*")
                    .from("order_items")
                    .whereILike(null).on("order_id", "user_id")
                    .toSql();

            assertFalse(sql.contains("ILIKE"));
        }

        @Test
        @DisplayName("whereILike(blank).on(cols) is skipped")
        void whereILike_blank() {
            String sql = ClickHouseQuery
                    .select("*")
                    .from("t")
                    .whereILike("  ").on("col1")
                    .toSql();

            assertFalse(sql.contains("ILIKE"));
        }

        @Test
        @DisplayName("whereILike(kw).onPrefix(cols) generates prefix ILIKE (keyword%)")
        void whereILike_prefix() {
            ClickHouseQuery q = ClickHouseQuery
                    .select("*")
                    .from("users")
                    .whereILike("john").onPrefix("name", "email");

            String sql = q.toSql();
            assertTrue(sql.contains("name ILIKE :_prefixKeyword"));
            assertTrue(sql.contains("email ILIKE :_prefixKeyword"));
            assertEquals("john%", q.toParams().getValue("_prefixKeyword"));
        }

        @Test
        @DisplayName("whereLike(kw).onPrefix(cols) generates prefix LIKE (keyword%)")
        void whereLike_prefix() {
            ClickHouseQuery q = ClickHouseQuery
                    .select("*")
                    .from("users")
                    .whereLike("TXN-").onPrefix("transaction_id");

            String sql = q.toSql();
            assertTrue(sql.contains("transaction_id LIKE :_likePrefixKeyword"));
            assertEquals("TXN-%", q.toParams().getValue("_likePrefixKeyword"));
        }

        @Test
        @DisplayName("whereILike(null).onPrefix(cols) is skipped")
        void whereILike_prefix_null() {
            String sql = ClickHouseQuery
                    .select("*").from("t")
                    .whereILike(null).onPrefix("name")
                    .toSql();

            assertFalse(sql.contains("ILIKE"));
        }

        @Test
        @DisplayName("whereOr generates OR group")
        void whereOr() {
            ClickHouseQuery q = ClickHouseQuery
                    .select("*")
                    .from("t")
                    .whereOr(or -> or
                            .addEq("status", "ACTIVE")
                            .addEq("status", "PENDING")
                    );

            String sql = q.toSql();
            assertTrue(sql.contains("OR"));
            assertTrue(sql.contains("status = :"));
        }

        @Test
        @DisplayName("whereOr fluent — eq, ne, gt, gte, lt, lte")
        void whereOrFluentComparison() {
            String sql = ClickHouseQuery
                    .select("*").from("t")
                    .whereOr(or -> or
                            .where("status").eq("ACTIVE")
                            .where("status").ne("DELETED")
                            .where("amount").gt(100)
                            .where("amount").gte(50)
                            .where("score").lt(10)
                            .where("score").lte(5)
                    ).toSql();

            assertTrue(sql.contains("status = :_or0"));
            assertTrue(sql.contains("status != :_or1"));
            assertTrue(sql.contains("amount > :_or2"));
            assertTrue(sql.contains("amount >= :_or3"));
            assertTrue(sql.contains("score < :_or4"));
            assertTrue(sql.contains("score <= :_or5"));
            assertTrue(sql.contains(" OR "));
        }

        @Test
        @DisplayName("whereOr fluent — in() with collection")
        void whereOrFluentIn() {
            String sql = ClickHouseQuery
                    .select("*").from("t")
                    .whereOr(or -> or
                            .where("status").in(List.of("ACTIVE", "PENDING"))
                            .where("amount").gt(1000)
                    ).toSql();

            assertTrue(sql.contains("status IN (:_or0, :_or1)"));
            assertTrue(sql.contains("amount > :_or2"));
        }

        @Test
        @DisplayName("whereOr fluent — notIn() with collection")
        void whereOrFluentNotIn() {
            String sql = ClickHouseQuery
                    .select("*").from("t")
                    .whereOr(or -> or
                            .where("type").notIn(List.of("TEST", "DEMO"))
                    ).toSql();

            assertTrue(sql.contains("type NOT IN (:_or0, :_or1)"));
        }

        @Test
        @DisplayName("whereOr fluent — isNull / isNotNull")
        void whereOrFluentNullChecks() {
            String sql = ClickHouseQuery
                    .select("*").from("t")
                    .whereOr(or -> or
                            .where("deleted_at").isNull()
                            .where("status").isNotNull()
                    ).toSql();

            assertTrue(sql.contains("deleted_at IS NULL"));
            assertTrue(sql.contains("status IS NOT NULL"));
        }

        @Test
        @DisplayName("whereOr fluent — ilike / like")
        void whereOrFluentLike() {
            String sql = ClickHouseQuery
                    .select("*").from("t")
                    .whereOr(or -> or
                            .where("name").ilike("john")
                            .where("email").like("gmail")
                    ).toSql();

            assertTrue(sql.contains("name ILIKE :_or0"));
            assertTrue(sql.contains("email LIKE :_or1"));
        }

        @Test
        @DisplayName("whereOr fluent — subquery in()")
        void whereOrFluentSubqueryIn() {
            String sql = ClickHouseQuery
                    .select("*").from("orders")
                    .whereOr(or -> or
                            .where("user_id").in(
                                ClickHouseQuery.select("id").from("vip_users")
                            )
                            .where("amount").gt(5000)
                    ).toSql();

            assertTrue(sql.contains("user_id IN (SELECT id"));
            assertTrue(sql.contains("FROM vip_users)"));
            assertTrue(sql.contains("amount > :_or0"));
        }

        @Test
        @DisplayName("whereOr fluent — null values are skipped")
        void whereOrFluentSkipsNull() {
            String sql = ClickHouseQuery
                    .select("*").from("t")
                    .whereOr(or -> or
                            .where("status").eq(null)       // skipped
                            .where("amount").gt(100)         // kept
                            .where("type").in((List<?>) null) // skipped
                    ).toSql();

            assertFalse(sql.contains("status"));
            assertTrue(sql.contains("amount > :_or0"));
        }

        @Test
        @DisplayName("whereOr fluent — mixed with AND where")
        void whereOrFluentWithAnd() {
            String sql = ClickHouseQuery
                    .select("*").from("orders")
                    .where("tenant_id").eq("op-1")
                    .whereOr(or -> or
                            .where("status").eq("ACTIVE")
                            .where("status").eq("PENDING")
                    )
                    .toSql();

            assertTrue(sql.contains("tenant_id = :tenantId"));
            assertTrue(sql.contains("(status = :_or0 OR status = :_or1)"));
        }

        @Test
        @DisplayName("whereRaw adds raw condition")
        void whereRaw() {
            String sql = ClickHouseQuery
                    .select("*")
                    .from("t")
                    .whereRaw("1=1")
                    .toSql();

            assertTrue(sql.contains("1=1"));
        }
    }

    // ── GROUP BY / HAVING ────────────────────────────────────────────────

    @Nested
    @DisplayName("GROUP BY & HAVING")
    class GroupByHavingTests {

        @Test
        @DisplayName("GROUP BY generates correct SQL")
        void groupBy() {
            String sql = ClickHouseQuery
                    .select("user_id", "count(*) AS cnt")
                    .from("order_items")
                    .groupBy("user_id", "order_id")
                    .toSql();

            assertTrue(sql.contains("GROUP BY user_id, order_id"));
        }

        @Test
        @DisplayName("HAVING with CH.Expr")
        void havingExpr() {
            String sql = ClickHouseQuery
                    .select("user_id", "sum(amount) AS total")
                    .from("t")
                    .groupBy("user_id")
                    .having(CH.sum("amount")).gt(100)
                    .toSql();

            assertTrue(sql.contains("HAVING sum(amount) > :"));
        }

        @Test
        @DisplayName("HAVING with raw string")
        void havingRaw() {
            String sql = ClickHouseQuery
                    .select("user_id", "count(*) AS cnt")
                    .from("t")
                    .groupBy("user_id")
                    .havingRaw("count(*)").gte(5)
                    .toSql();

            assertTrue(sql.contains("HAVING count(*) >= :"));
        }

        @Test
        @DisplayName("HAVING supports all operators")
        void havingOperators() {
            ClickHouseQuery q = ClickHouseQuery.select("x").from("t").groupBy("x");
            q.havingRaw("a").eq(1);
            q.havingRaw("b").ne(2);
            q.havingRaw("c").lt(3);
            q.havingRaw("d").lte(4);
            q.havingRaw("e").between(5, 6);
            
            String sql = q.toSql();
            assertTrue(sql.contains("HAVING a = :"));
            assertTrue(sql.contains("AND b != :"));
            assertTrue(sql.contains("AND c < :"));
            assertTrue(sql.contains("AND d <= :"));
            assertTrue(sql.contains("AND e >= :") && sql.contains("AND e <= :"));
        }
    }

    // ── Execute Tests ───────────────────────────────────────────────────

    @Nested
    @DisplayName("Execution Tests")
    class ExecutionTests {

        @Test
        @DisplayName("query() executes namedJdbc.query")
        @SuppressWarnings("unchecked")
        void query() {
            NamedParameterJdbcTemplate jdbc = mock(NamedParameterJdbcTemplate.class);
            RowMapper<String> mapper = mock(RowMapper.class);
            List<String> expected = List.of("A");
            when(jdbc.query(anyString(), any(SqlParameterSource.class), eq(mapper))).thenReturn(expected);

            List<String> result = ClickHouseQuery.select("*").from("t").query(jdbc, mapper);
            assertEquals(expected, result);
        }

        @Test
        @DisplayName("queryForObject() executes namedJdbc.queryForObject")
        void queryForObject() {
            NamedParameterJdbcTemplate jdbc = mock(NamedParameterJdbcTemplate.class);
            when(jdbc.queryForObject(anyString(), any(SqlParameterSource.class), eq(Long.class))).thenReturn(10L);

            Long result = ClickHouseQuery.select("count(*)").from("t").queryForObject(jdbc, Long.class);
            assertEquals(10L, result);
        }

        @Test
        @DisplayName("count() executes count subquery")
        void count() {
            NamedParameterJdbcTemplate jdbc = mock(NamedParameterJdbcTemplate.class);
            when(jdbc.queryForObject(anyString(), any(SqlParameterSource.class), eq(Long.class))).thenReturn(5L);

            long result = ClickHouseQuery.select("*").from("t").count(jdbc);
            assertEquals(5L, result);
        }
    }

    // ── ORDER BY / LIMIT ────────────────────────────────────────────────

    @Nested
    @DisplayName("ORDER BY & LIMIT")
    class OrderByLimitTests {

        @Test
        @DisplayName("ORDER BY ASC")
        void orderByAsc() {
            String sql = ClickHouseQuery
                    .select("*")
                    .from("t")
                    .orderBy("created_at")
                    .toSql();

            assertTrue(sql.contains("ORDER BY created_at ASC"));
        }

        @Test
        @DisplayName("LIMIT / OFFSET")
        void limitOffset() {
            ClickHouseQuery q = ClickHouseQuery
                    .select("*")
                    .from("t")
                    .limit(10).offset(20);

            String sql = q.toSql();
            assertTrue(sql.contains("LIMIT :_limit"));
            assertTrue(sql.contains("OFFSET :_offset"));
            assertEquals(10, q.toParams().getValue("_limit"));
            assertEquals(20L, q.toParams().getValue("_offset"));
        }
    }

    // ── JOIN ─────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("JOIN")
    class JoinTests {

        @Test
        @DisplayName("Fluent INNER JOIN with .on(left, right)")
        void innerJoin() {
            String sql = ClickHouseQuery
                    .select("u.name", "t.amount")
                    .from("orders t")
                    .join("users u").on("u.id", "t.user_id")
                    .toSql();

            assertTrue(sql.contains("JOIN users u ON u.id = t.user_id"));
        }

        @Test
        @DisplayName("Fluent LEFT JOIN with .on(left, right)")
        void leftJoin() {
            String sql = ClickHouseQuery
                    .select("*")
                    .from("t")
                    .leftJoin("settings s").on("s.user_id", "t.user_id")
                    .toSql();

            assertTrue(sql.contains("LEFT JOIN settings s ON s.user_id = t.user_id"));
        }

        @Test
        @DisplayName("Fluent RIGHT JOIN with .on(left, right)")
        void rightJoin() {
            String sql = ClickHouseQuery
                    .select("*")
                    .from("t")
                    .rightJoin("other o").on("o.id", "t.other_id")
                    .toSql();

            assertTrue(sql.contains("RIGHT JOIN other o ON o.id = t.other_id"));
        }

        @Test
        @DisplayName("Multiple JOINs")
        void multipleJoins() {
            String sql = ClickHouseQuery
                    .select("*")
                    .from("orders t")
                    .join("users u").on("u.id", "t.user_id")
                    .leftJoin("product_config g").on("g.id", "t.product_id")
                    .toSql();

            assertTrue(sql.contains("JOIN users u ON u.id = t.user_id"));
            assertTrue(sql.contains("LEFT JOIN product_config g ON g.id = t.product_id"));
        }

        @Test
        @DisplayName("Raw ON condition for complex cases")
        void rawOnCondition() {
            String sql = ClickHouseQuery
                    .select("*")
                    .from("orders t")
                    .join("users u").on("u.id = t.user_id AND u.active = 1")
                    .toSql();

            assertTrue(sql.contains("JOIN users u ON u.id = t.user_id AND u.active = 1"));
        }
    }

    // ── Phase Validation ─────────────────────────────────────────────────

    @Nested
    @DisplayName("Phase Validation")
    class PhaseValidationTests {

        @Test
        @DisplayName("Correct order: SELECT → FROM → WHERE → GROUP_BY → ORDER_BY → LIMIT")
        void correctOrder() {
            // Should NOT throw
            assertDoesNotThrow(() ->
                ClickHouseQuery
                    .select("user_id")
                    .from("t")
                    .where("status").eq("ACTIVE")
                    .groupBy("user_id")
                    .orderBy("user_id", SortOrder.ASC)
                    .limit(10).offset(0)
                    .toSql()
            );
        }

        @Test
        @DisplayName("Multiple WHERE calls allowed (same phase)")
        void multipleWheres() {
            assertDoesNotThrow(() ->
                ClickHouseQuery
                    .select("*")
                    .from("t")
                    .where("a").eq(1)
                    .where("b").eq(2)
                    .where("c").eqIfNotBlank("x")
                    .whereILike("kw").on("d")
                    .toSql()
            );
        }

        @Test
        @DisplayName("WHERE after GROUP_BY throws IllegalStateException")
        void whereAfterGroupBy() {
            IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                ClickHouseQuery
                    .select("user_id")
                    .from("t")
                    .groupBy("user_id")
                    .where("status").eq("ACTIVE")
            );

            assertTrue(ex.getMessage().contains("cannot call WHERE after GROUP_BY"));
        }

        @Test
        @DisplayName("FROM after WHERE throws IllegalStateException")
        void fromAfterWhere() {
            IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                ClickHouseQuery
                    .select("*")
                    .from("t")
                    .where("a").eq(1)
                    .from("t2")
            );

            assertTrue(ex.getMessage().contains("cannot call FROM after WHERE"));
        }

        @Test
        @DisplayName("WHERE after ORDER_BY throws IllegalStateException")
        void whereAfterOrderBy() {
            assertThrows(IllegalStateException.class, () ->
                ClickHouseQuery
                    .select("*")
                    .from("t")
                    .orderBy("id", SortOrder.ASC)
                    .where("status").eq("ACTIVE")
            );
        }

        @Test
        @DisplayName("GROUP_BY after ORDER_BY throws IllegalStateException")
        void groupByAfterOrderBy() {
            assertThrows(IllegalStateException.class, () ->
                ClickHouseQuery
                    .select("*")
                    .from("t")
                    .orderBy("id")
                    .groupBy("user_id")
            );
        }

        @Test
        @DisplayName("HAVING after ORDER_BY throws IllegalStateException")
        void havingAfterOrderBy() {
            assertThrows(IllegalStateException.class, () ->
                ClickHouseQuery
                    .select("*")
                    .from("t")
                    .groupBy("user_id")
                    .orderBy("user_id")
                    .havingRaw("count(*)").gt(1)
            );
        }

        @Test
        @DisplayName("ORDER_BY after LIMIT throws IllegalStateException")
        void orderByAfterLimit() {
            assertThrows(IllegalStateException.class, () ->
                ClickHouseQuery
                    .select("*")
                    .from("t")
                    .limit(10)
                    .orderBy("id")
            );
        }

        @Test
        @DisplayName("JOIN after WHERE throws IllegalStateException")
        void joinAfterWhere() {
            assertThrows(IllegalStateException.class, () ->
                ClickHouseQuery
                    .select("*")
                    .from("t")
                    .where("a").eq(1)
                    .join("other o").on("o.id = t.oid")
            );
        }

        @Test
        @DisplayName("Skipping phases is allowed (SELECT → FROM → ORDER_BY)")
        void skippingPhasesAllowed() {
            assertDoesNotThrow(() ->
                ClickHouseQuery
                    .select("*")
                    .from("t")
                    .orderBy("id")
                    .toSql()
            );
        }

        @Test
        @DisplayName("Error message contains expected order")
        void errorMessageContainsExpectedOrder() {
            IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                ClickHouseQuery
                    .select("*")
                    .from("t")
                    .groupBy("user_id")
                    .where("x").eq(1)
            );

            assertTrue(ex.getMessage().contains("SELECT → FROM → JOIN → WHERE → GROUP_BY → HAVING → ORDER_BY → LIMIT"));
        }

        @Test
        @DisplayName("Subquery in WHERE also validates phase order independently")
        void subQueryValidatesIndependently() {
            // Subquery is a separate ClickHouseQuery instance → has its own phase tracking
            assertDoesNotThrow(() ->
                ClickHouseQuery
                    .select("*")
                    .from("orders")
                    .where("product_id").in(
                        ClickHouseQuery.select("id")
                            .from("games")
                            .where("active").eq(1)
                            .groupBy("id")
                    )
                    .toSql()
            );
        }

        @Test
        @DisplayName("Subquery with bad phase order throws IllegalStateException")
        void subQueryBadOrderThrows() {
            assertThrows(IllegalStateException.class, () ->
                ClickHouseQuery
                    .select("*")
                    .from("orders")
                    .where("product_id").in(
                        ClickHouseQuery.select("id")
                            .from("games")
                            .groupBy("id")
                            .where("active").eq(1)  // WHERE after GROUP_BY → ERROR in subquery!
                    )
            );
        }

        @Test
        @DisplayName("count(subQuery) with bad order throws IllegalStateException")
        void countSubQueryBadOrderThrows() {
            assertThrows(IllegalStateException.class, () ->
                ClickHouseQuery.count(
                    ClickHouseQuery.select("user_id")
                        .from("t")
                        .orderBy("user_id")
                        .where("status").eq("X")  // WHERE after ORDER_BY → ERROR!
                )
            );
        }

        @Test
        @DisplayName("notIn(subQuery) with bad order throws IllegalStateException")
        void notInSubQueryBadOrderThrows() {
            assertThrows(IllegalStateException.class, () ->
                ClickHouseQuery
                    .select("*")
                    .from("orders")
                    .where("user_id").notIn(
                        ClickHouseQuery.select("id")
                            .from("banned_users")
                            .limit(10)
                            .where("active").eq(1)  // WHERE after LIMIT → ERROR!
                    )
            );
        }

        @Test
        @DisplayName("JOIN after GROUP_BY throws IllegalStateException")
        void joinAfterGroupByThrows() {
            assertThrows(IllegalStateException.class, () ->
                ClickHouseQuery.select("*")
                    .from("orders")
                    .groupBy("user_id")
                    .join("users u").on("u.id", "orders.user_id")
            );
        }

        @Test
        @DisplayName("WHERE after HAVING throws IllegalStateException")
        void whereAfterHavingThrows() {
            assertThrows(IllegalStateException.class, () ->
                ClickHouseQuery.select("user_id", "sum(amount) AS total")
                    .from("orders")
                    .groupBy("user_id")
                    .havingRaw("sum(amount)").gt(100)
                    .where("status").eq("ACTIVE")
            );
        }

        @Test
        @DisplayName("FROM(subQuery) after WHERE throws IllegalStateException")
        void fromSubQueryAfterWhereThrows() {
            assertThrows(IllegalStateException.class, () ->
                ClickHouseQuery.select("*")
                    .from("t")
                    .where("status").eq("X")
                    .from(ClickHouseQuery.select("id").from("other")).as("sub")
            );
        }

        @Test
        @DisplayName("groupBy after LIMIT throws IllegalStateException")
        void groupByAfterLimitThrows() {
            assertThrows(IllegalStateException.class, () ->
                ClickHouseQuery.select("*")
                    .from("t")
                    .limit(10)
                    .groupBy("user_id")
            );
        }

        @Test
        @DisplayName("UNION ALL + ORDER BY + LIMIT is allowed")
        void unionAllWithOrderByLimitAllowed() {
            // Should NOT throw — ORDER BY/LIMIT after UNION ALL is valid SQL
            String sql = ClickHouseQuery
                    .select("user_id", "amount").from("orders_2024")
                    .where("status").eq("ACTIVE")
                    .unionAll(
                        ClickHouseQuery.select("user_id", "amount").from("orders_2025")
                    )
                    .orderBy("amount", SortOrder.DESC)
                    .limit(10)
                    .toSql();

            assertTrue(sql.contains("UNION ALL"));
            assertTrue(sql.contains("ORDER BY amount DESC"));
            assertTrue(sql.contains("LIMIT :_limit"));
        }

        @Test
        @DisplayName("CTE inner query validates phase order")
        void cteInnerQueryValidatesPhase() {
            assertThrows(IllegalStateException.class, () ->
                ClickHouseQuery.with("bad_cte",
                    ClickHouseQuery.select("id")
                        .from("users")
                        .orderBy("id")
                        .where("active").eq(1)  // WHERE after ORDER_BY → ERROR inside CTE!
                ).select("*").from("bad_cte").toSql()
            );
        }

        @Test
        @DisplayName("Subquery FROM inner query validates phase order")
        void subqueryFromInnerQueryValidatesPhase() {
            assertThrows(IllegalStateException.class, () ->
                ClickHouseQuery.select("*")
                    .from(
                        ClickHouseQuery.select("id")
                            .from("users")
                            .limit(10)
                            .where("active").eq(1)  // WHERE after LIMIT → ERROR inside subquery!
                    ).as("sub").toSql()
            );
        }

        @Test
        @DisplayName("Multiple same-phase calls are allowed")
        void multipleSamePhaseAllowed() {
            // Multiple WHERE, JOIN, ORDER BY — should all work
            String sql = ClickHouseQuery.select("*")
                    .from("orders o")
                    .join("users u").on("u.id", "o.user_id")
                    .leftJoin("products p").on("p.id", "o.product_id")
                    .where("o.status").eq("ACTIVE")
                    .where("o.amount").gt(100)
                    .where("o.deleted_at").isNull()
                    .groupBy("o.user_id")
                    .havingRaw("sum(o.amount)").gt(500)
                    .orderBy("o.amount", SortOrder.DESC)
                    .orderBy("o.created_at", SortOrder.ASC)
                    .limit(10)
                    .toSql();

            assertTrue(sql.contains("JOIN users u"));
            assertTrue(sql.contains("LEFT JOIN products p"));
            assertTrue(sql.contains("o.status = :o.status"));
            assertTrue(sql.contains("o.amount > :o.amountGt"));
            assertTrue(sql.contains("ORDER BY o.amount DESC, o.created_at ASC"));
        }

        @Test
        @DisplayName("Skipping multiple phases is allowed (SELECT → FROM → ORDER_BY → LIMIT, no WHERE)")
        void skippingMultiplePhasesAllowed() {
            // SELECT → FROM → ORDER_BY (skipping JOIN, WHERE, GROUP_BY, HAVING)
            String sql = ClickHouseQuery.select("*")
                    .from("orders")
                    .orderBy("created_at", SortOrder.DESC)
                    .limit(10)
                    .toSql();

            assertTrue(sql.contains("FROM orders"));
            assertTrue(sql.contains("ORDER BY created_at DESC"));
            assertFalse(sql.contains("WHERE"));
        }
    }

    // ── CountQuery ───────────────────────────────────────────────────────

    @Nested
    @DisplayName("CountQuery")
    class CountQueryTests {

        @Test
        @DisplayName("Static count(subQuery) generates correct SQL")
        void countSubQuery_sql() {
            CountQuery countQuery = ClickHouseQuery.count(
                    ClickHouseQuery.select("user_id", "order_id")
                            .from("order_items")
                            .where("status").eq("CLOSED")
                            .groupBy("user_id", "order_id")
            );

            String sql = countQuery.toSql();
            assertTrue(sql.startsWith("SELECT count(*) FROM ("));
            assertTrue(sql.contains("SELECT user_id"));
            assertTrue(sql.contains("FROM order_items"));
            assertTrue(sql.contains("WHERE status = :status"));
            assertTrue(sql.contains("GROUP BY user_id, order_id"));
            assertTrue(sql.endsWith(")"));
        }

        @Test
        @DisplayName("CountQuery with filters and keyword search")
        void countWithFilters() {
            Instant from = Instant.parse("2026-01-01T00:00:00Z");
            Instant to = Instant.parse("2026-12-31T23:59:59Z");

            CountQuery countQuery = ClickHouseQuery.count(
                    ClickHouseQuery.select("user_id", "order_id", "order_status")
                            .from("order_items")
                            .where("created_at").between(from, to)
                            .where("order_status").eqIfNotBlank("ACTIVE")
                            .whereILike("search").on("order_id", "user_id")
                            .groupBy("user_id", "order_id", "order_status")
            );

            String sql = countQuery.toSql();
            assertTrue(sql.contains("SELECT count(*) FROM ("));
            assertTrue(sql.contains("created_at >= :createdAtFrom"));
            assertTrue(sql.contains("order_status = :orderStatus"));
            assertTrue(sql.contains("ILIKE :_keyword"));
            assertTrue(sql.contains("GROUP BY user_id, order_id, order_status"));
        }

        @Test
        @DisplayName("CountQuery execute triggers JDBC queryForObject")
        void countExecute() {
            NamedParameterJdbcTemplate jdbc = mock(NamedParameterJdbcTemplate.class);
            when(jdbc.queryForObject(anyString(), any(SqlParameterSource.class), eq(Long.class))).thenReturn(99L);

            long result = ClickHouseQuery.count(ClickHouseQuery.select("*").from("t")).execute(jdbc);
            assertEquals(99L, result);
        }
    }

    // ── Formatting ───────────────────────────────────────────────────────

    @Nested
    @DisplayName("SQL Formatting")
    class FormattingTests {

        @Test
        @DisplayName("SQL is formatted with newlines")
        void formattedSql() {
            String sql = ClickHouseQuery
                    .select("user_id", "sum(bet_amount) AS total_bet")
                    .from("order_items")
                    .where("tenant_id").eq("op-1")
                    .where("status").eqIf(true, "ACTIVE")
                    .groupBy("user_id")
                    .orderBy("total_bet", SortOrder.DESC)
                    .limit(10).offset(0)
                    .toSql();

            assertTrue(sql.contains("\nFROM "));
            assertTrue(sql.contains("\nWHERE "));
            assertTrue(sql.contains("\n  AND "));
            assertTrue(sql.contains("\nGROUP BY "));
            assertTrue(sql.contains("\nORDER BY "));
            assertTrue(sql.contains("\nLIMIT "));
        }
    }

    // ── Utilities ────────────────────────────────────────────────────────

    @Nested
    @DisplayName("Utilities")
    class UtilTests {

        @Test
        @DisplayName("toCamelCase converts snake_case correctly")
        void toCamelCase() {
            assertEquals("createdAt", ClickHouseQuery.toCamelCase("created_at"));
            assertEquals("orderStatus", ClickHouseQuery.toCamelCase("order_status"));
            assertEquals("tenantId", ClickHouseQuery.toCamelCase("tenant_id"));
            assertEquals("status", ClickHouseQuery.toCamelCase("status"));
        }
    }

    // ── Complex Query ────────────────────────────────────────────────────

    @Nested
    @DisplayName("Complex Queries")
    class ComplexTests {

        @Test
        @DisplayName("Full complex query with all clauses")
        void complexQuery() {
            Instant from = Instant.parse("2026-01-01T00:00:00Z");
            Instant to = Instant.parse("2026-12-31T23:59:59Z");

            ClickHouseQuery q = ClickHouseQuery
                    .select("product_id", "count(*) AS cnt")
                    .from("orders")
                    .where("tenant_id").eq("op-1")
                    .where("product_id").in(List.of("g1", "g2"))
                    .where("created_at").between(from, to)
                    .where("status").eqIfNotBlank("ACTIVE")
                    .whereILike("search").on("user_id", "round_id")
                    .groupBy("product_id")
                    .orderBy("cnt", SortOrder.DESC)
                    .limit(10).offset(0);

            String sql = q.toSql();
            assertTrue(sql.contains("tenant_id = :tenantId"));
            assertTrue(sql.contains("product_id IN (:productId0, :productId1)"));
            assertTrue(sql.contains("created_at >= :createdAtFrom"));
            assertTrue(sql.contains("status = :status"));
            assertTrue(sql.contains("ILIKE :_keyword"));
            assertTrue(sql.contains("GROUP BY product_id"));
            assertTrue(sql.contains("ORDER BY cnt DESC"));
        }

        @Test
        @DisplayName("Query with JOIN and HAVING")
        void joinAndHaving() {
            String sql = ClickHouseQuery
                    .select("u.name", CH.sum("t.amount").as("total"))
                    .from("orders t")
                    .join("users u").on("u.id = t.user_id")
                    .where("t.tenant_id").eq("op-1")
                    .groupBy("u.name")
                    .having(CH.sum("t.amount")).gt(1000)
                    .orderBy("total", SortOrder.DESC)
                    .toSql();

            assertTrue(sql.contains("JOIN users u ON u.id = t.user_id"));
            assertTrue(sql.contains("GROUP BY u.name"));
            assertTrue(sql.contains("HAVING sum(t.amount) > :"));
            assertTrue(sql.contains("ORDER BY total DESC"));
        }
    }

    // ── LIKE (case-sensitive) ───────────────────────────────────────────

    @Nested
    @DisplayName("LIKE (case-sensitive)")
    class LikeTests {

        @Test
        @DisplayName("whereLike generates LIKE (not ILIKE)")
        void whereLike_caseSensitive() {
            String sql = ClickHouseQuery.select("*")
                    .from("orders")
                    .whereLike("test").on("order_id", "user_id")
                    .toSql();

            assertTrue(sql.contains("order_id LIKE :_likeKeyword"));
            assertTrue(sql.contains("user_id LIKE :_likeKeyword"));
            assertFalse(sql.contains("ILIKE"));
        }

        @Test
        @DisplayName("whereLike skipped when blank")
        void whereLike_blankSkipped() {
            String sql = ClickHouseQuery.select("*")
                    .from("orders")
                    .whereLike("").on("order_id")
                    .toSql();

            assertFalse(sql.contains("LIKE"));
        }

        @Test
        @DisplayName("whereILike still works (case-insensitive)")
        void whereILike_stillWorks() {
            String sql = ClickHouseQuery.select("*")
                    .from("orders")
                    .whereILike("test").on("name")
                    .toSql();

            assertTrue(sql.contains("name ILIKE :_keyword"));
        }
    }

    // ── Multiple ORDER BY ───────────────────────────────────────────────

    @Nested
    @DisplayName("Multiple ORDER BY")
    class MultipleOrderByTests {

        @Test
        @DisplayName("Multiple orderBy generates comma-separated ORDER BY")
        void multipleOrderBy() {
            String sql = ClickHouseQuery.select("*")
                    .from("orders")
                    .orderBy("total", SortOrder.DESC)
                    .orderBy("user_id", SortOrder.ASC)
                    .toSql();

            assertTrue(sql.contains("ORDER BY total DESC, user_id ASC"));
        }

        @Test
        @DisplayName("Three columns in ORDER BY")
        void threeColumns() {
            String sql = ClickHouseQuery.select("*")
                    .from("orders")
                    .orderBy("created_at", SortOrder.DESC)
                    .orderBy("amount", SortOrder.DESC)
                    .orderBy("user_id", SortOrder.ASC)
                    .toSql();

            assertTrue(sql.contains("ORDER BY created_at DESC, amount DESC, user_id ASC"));
        }

        @Test
        @DisplayName("Single orderBy still works")
        void singleOrderBy() {
            String sql = ClickHouseQuery.select("*")
                    .from("orders")
                    .orderBy("total", SortOrder.DESC)
                    .toSql();

            assertTrue(sql.contains("ORDER BY total DESC"));
        }
    }

    // ── Subquery FROM ──────────────────────────────────────────────────

    @Nested
    @DisplayName("Subquery FROM")
    class SubqueryFromTests {

        @Test
        @DisplayName("Basic subquery FROM")
        void basicSubqueryFrom() {
            String sql = ClickHouseQuery
                    .select("user_id", "total")
                    .from(
                            ClickHouseQuery.select("user_id", "sum(amount) AS total")
                                    .from("orders")
                                    .groupBy("user_id"),
                            "sub"
                    )
                    .toSql();

            assertTrue(sql.contains("FROM ("));
            assertTrue(sql.contains("SELECT user_id"));
            assertTrue(sql.contains("sum(amount) AS total"));
            assertTrue(sql.contains("GROUP BY user_id"));
            assertTrue(sql.contains(") AS sub"));
        }

        @Test
        @DisplayName("Subquery FROM with WHERE on outer query")
        void subqueryFromWithWhere() {
            ClickHouseQuery q = ClickHouseQuery
                    .select("user_id", "total")
                    .from(
                            ClickHouseQuery.select("user_id", "sum(amount) AS total")
                                    .from("orders")
                                    .where("tenant_id").eq("op-1")
                                    .groupBy("user_id"),
                            "sub"
                    )
                    .where("total").gt(1000);

            String sql = q.toSql();
            assertTrue(sql.contains("FROM ("));
            assertTrue(sql.contains(") AS sub"));
            assertTrue(sql.contains("total > :totalGt"));
            // Subquery params are merged
            assertEquals("op-1", q.toParams().getValue("tenantId"));
            assertEquals(1000, q.toParams().getValue("totalGt"));
        }

        @Test
        @DisplayName("Subquery FROM with ORDER BY and LIMIT")
        void subqueryFromWithOrderByLimit() {
            String sql = ClickHouseQuery
                    .select("*")
                    .from(
                            ClickHouseQuery.select("user_id", "count(*) AS cnt")
                                    .from("orders")
                                    .groupBy("user_id"),
                            "ranked"
                    )
                    .orderBy("cnt", SortOrder.DESC)
                    .limit(10)
                    .toSql();

            assertTrue(sql.contains(") AS ranked"));
            assertTrue(sql.contains("ORDER BY cnt DESC"));
            assertTrue(sql.contains("LIMIT :_limit"));
        }
    }

    // ── UNION ALL ───────────────────────────────────────────────────────

    @Nested
    @DisplayName("UNION ALL")
    class UnionAllTests {

        @Test
        @DisplayName("Basic UNION ALL with two queries")
        void basicUnionAll() {
            String sql = ClickHouseQuery
                    .select("user_id", "amount")
                    .from("orders_2024")
                    .unionAll(
                            ClickHouseQuery.select("user_id", "amount").from("orders_2025")
                    )
                    .toSql();

            assertTrue(sql.contains("FROM orders_2024"));
            assertTrue(sql.contains("UNION ALL"));
            assertTrue(sql.contains("FROM orders_2025"));
        }

        @Test
        @DisplayName("UNION ALL with three queries")
        void tripleUnionAll() {
            String sql = ClickHouseQuery
                    .select("user_id", "amount").from("orders_2023")
                    .unionAll(ClickHouseQuery.select("user_id", "amount").from("orders_2024"))
                    .unionAll(ClickHouseQuery.select("user_id", "amount").from("orders_2025"))
                    .toSql();

            String[] parts = sql.split("UNION ALL");
            assertEquals(3, parts.length, "Should have 3 parts separated by UNION ALL");
        }

        @Test
        @DisplayName("UNION ALL with ORDER BY and LIMIT on combined result")
        void unionAllWithOrderByLimit() {
            String sql = ClickHouseQuery
                    .select("user_id", "amount").from("orders_2024")
                    .unionAll(
                            ClickHouseQuery.select("user_id", "amount").from("orders_2025")
                    )
                    .orderBy("amount", SortOrder.DESC)
                    .limit(10)
                    .toSql();

            assertTrue(sql.contains("UNION ALL"));
            assertTrue(sql.contains("ORDER BY amount DESC"));
            assertTrue(sql.contains("LIMIT :_limit"));
        }

        @Test
        @DisplayName("UNION ALL merges params from both queries")
        void unionAllMergesParams() {
            ClickHouseQuery q = ClickHouseQuery
                    .select("user_id", "amount").from("orders_2024")
                    .where("tenant_id").eq("op-1")
                    .unionAll(
                            ClickHouseQuery.select("user_id", "amount").from("orders_2025")
                                    .where("status").eq("ACTIVE")
                    );

            q.toSql(); // trigger param merge
            assertEquals("op-1", q.toParams().getValue("tenantId"));
            assertEquals("ACTIVE", q.toParams().getValue("status"));
        }
    }

    // ── WITH (CTE) ─────────────────────────────────────────────────────

    @Nested
    @DisplayName("WITH (CTE)")
    class CTETests {

        @Test
        @DisplayName("Single CTE")
        void singleCTE() {
            String sql = ClickHouseQuery
                    .with("active_users",
                            ClickHouseQuery.select("user_id").from("users").where("status").eq("ACTIVE"))
                    .select("au.user_id", "count(*) AS order_count")
                    .from("orders o")
                    .join("active_users au").on("au.user_id", "o.user_id")
                    .groupBy("au.user_id")
                    .toSql();

            assertTrue(sql.startsWith("WITH active_users AS ("));
            assertTrue(sql.contains("status = :status"));
            assertTrue(sql.contains("SELECT au.user_id"));
            assertTrue(sql.contains("JOIN active_users au ON au.user_id = o.user_id"));
        }

        @Test
        @DisplayName("Multiple CTEs")
        void multipleCTEs() {
            String sql = ClickHouseQuery
                    .with("cte1", ClickHouseQuery.select("id").from("table1"))
                    .with("cte2", ClickHouseQuery.select("id").from("table2"))
                    .select("*")
                    .from("cte1")
                    .join("cte2").on("cte2.id", "cte1.id")
                    .toSql();

            assertTrue(sql.contains("WITH cte1 AS ("));
            assertTrue(sql.contains("cte2 AS ("));
            assertTrue(sql.contains("FROM cte1"));
            assertTrue(sql.contains("JOIN cte2"));
        }

        @Test
        @DisplayName("CTE merges params into main query")
        void cteMergesParams() {
            ClickHouseQuery q = ClickHouseQuery
                    .with("filtered_orders",
                            ClickHouseQuery.select("*").from("orders").where("tenant_id").eq("op-1"))
                    .select("user_id", "sum(amount) AS total")
                    .from("filtered_orders")
                    .groupBy("user_id");

            q.toSql();
            assertEquals("op-1", q.toParams().getValue("tenantId"));
        }

        @Test
        @DisplayName("CTE with ORDER BY and LIMIT")
        void cteWithOrderByLimit() {
            String sql = ClickHouseQuery
                    .with("top_users",
                            ClickHouseQuery.select("user_id", "sum(amount) AS total")
                                    .from("orders").groupBy("user_id"))
                    .select("user_id", "total")
                    .from("top_users")
                    .orderBy("total", SortOrder.DESC)
                    .limit(10)
                    .toSql();

            assertTrue(sql.startsWith("WITH top_users AS ("));
            assertTrue(sql.contains("ORDER BY total DESC"));
            assertTrue(sql.contains("LIMIT :_limit"));
        }
    }

    // ── Alias ────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("Alias — type-safe table prefix")
    class AliasTests {

        @Test
        @DisplayName("Alias.of(table, alias) — ref() returns 'table alias'")
        void aliasRef() {
            Alias o = Alias.of("orders").as("o");
            assertEquals("orders o", o.ref());
            assertEquals("o", o.toString());
        }

        @Test
        @DisplayName("Alias.of(table) — uses table name as prefix, ref() returns just table")
        void aliasSimple() {
            Alias orders = Alias.of("orders");
            assertEquals("orders", orders.ref());
            assertEquals(orders.col("amount"), "orders.amount");
            assertEquals("orders", orders.toString());
        }

        @Test
        @DisplayName("Alias.col() prefixes column name")
        void aliasPrefixesColumn() {
            Alias o = Alias.of("orders").as("o");
            assertEquals(o.col("amount"), "o.amount");
            assertEquals(o.col("created_at"), "o.created_at");
        }

        @Test
        @DisplayName("Alias expression shortcuts")
        void aliasExpressionShortcuts() {
            Alias o = Alias.of("orders").as("o");
            assertEquals(o.sum("amount"), "sum(o.amount)");
            assertEquals(o.count("id"), "count(o.id)");
            assertEquals(o.countDistinct("user_id"), "countDistinct(o.user_id)");
            assertEquals(o.min("created_at"), "min(o.created_at)");
            assertEquals(o.max("created_at"), "max(o.created_at)");
            assertEquals(o.avg("score"), "avg(o.score)");
        }

        @Test
        @DisplayName("Alias conditional aggregate shortcuts")
        void aliasConditionalAggregates() {
            Alias o = Alias.of("orders").as("o");
            assertEquals(o.sumIfRaw("amount", "status = 'ACTIVE'"),
                    "sumIf(o.amount, status = 'ACTIVE')");
            assertEquals(o.countIfRaw("id", "type = 'SALE'"),
                    "countIf(o.id, type = 'SALE')");
        }

        @Test
        @DisplayName("Full query with Alias — zero manual prefix strings")
        void fullQueryWithAlias() {
            Alias o = Alias.of("orders").as("o");
            Alias u = Alias.of("users").as("u");
            Alias p = Alias.of("products").as("p");

            String sql = ClickHouseQuery.select(
                        u.col("name"),
                        o.sum("amount").as("total_revenue")
                    )
                    .from(o)                                        // FROM orders o
                    .join(u).on(u.col("id"), o.col("user_id"))      // JOIN users u
                    .leftJoin(p).on(p.col("id"), o.col("product_id"))// LEFT JOIN products p
                    .where(o.col("tenant_id")).eq("op-1")
                    .where(o.col("status")).eq("ACTIVE")
                    .groupBy(u.col("name"))
                    .orderBy("total_revenue", SortOrder.DESC)
                    .limit(10)
                    .toSql();

            assertTrue(sql.contains("FROM orders o"));
            assertTrue(sql.contains("JOIN users u ON u.id = o.user_id"));
            assertTrue(sql.contains("LEFT JOIN products p ON p.id = o.product_id"));
            assertTrue(sql.contains("o.tenant_id = :o.tenantId"));
            assertTrue(sql.contains("GROUP BY u.name"));
        }

        @Test
        @DisplayName("Full query with simple Alias.of(table) — no short alias needed")
        void fullQueryWithSimpleAlias() {
            Alias orders = Alias.of("orders");
            Alias users = Alias.of("users");

            String sql = ClickHouseQuery.select(
                        users.col("name"),
                        orders.sum("amount").as("total")
                    )
                    .from(orders)
                    .join(users).on(users.col("id"), orders.col("user_id"))
                    .where(orders.col("status")).eq("ACTIVE")
                    .toSql();

            assertTrue(sql.contains("FROM orders"));
            assertFalse(sql.contains("FROM orders orders"));  // no duplicate
            assertTrue(sql.contains("JOIN users ON users.id = orders.user_id"));
            assertTrue(sql.contains("orders.status"));
        }

        @Test
        @DisplayName("Alias.toString() returns alias prefix only")
        void aliasToString() {
            assertEquals("o", Alias.of("orders").as("o").toString());
            assertEquals("orders", Alias.of("orders").toString());
        }
    }

    // ── Page ─────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("Page — single-query pagination")
    class PageTests {

        @Test
        @DisplayName("Page helpers — totalPages, hasNext, hasPrevious, isEmpty")
        void pageHelpers() {
            Page<String> page = new Page<>(List.of("a", "b", "c"), 25, 0, 10);
            assertEquals(25, page.getTotal());
            assertEquals(0, page.getPage());
            assertEquals(10, page.getPageSize());
            assertEquals(3, page.getTotalPages());
            assertTrue(page.hasNext());
            assertFalse(page.hasPrevious());
            assertFalse(page.isEmpty());
        }

        @Test
        @DisplayName("Page — last page has no next")
        void lastPageHasNoNext() {
            Page<String> page = new Page<>(List.of("a"), 21, 2, 10);
            assertEquals(3, page.getTotalPages());
            assertFalse(page.hasNext());
            assertTrue(page.hasPrevious());
        }

        @Test
        @DisplayName("Page — empty result")
        void emptyPage() {
            Page<String> page = new Page<>(List.of(), 0, 0, 10);
            assertTrue(page.isEmpty());
            assertFalse(page.hasNext());
            assertFalse(page.hasPrevious());
            assertEquals(0, page.getTotalPages());
        }

        @Test
        @DisplayName("queryPage — SQL injects count(*) OVER() and LIMIT/OFFSET")
        void queryPageSqlGeneration() {
            // Build query and manually check the SQL that queryPage would generate
            ClickHouseQuery q = ClickHouseQuery.select("user_id", "amount")
                    .from("orders")
                    .where("tenant_id").eq("op-1")
                    .orderBy("amount", SortOrder.DESC);

            // Simulate what queryPage does: inject _total, set limit/offset
            // We test via toSql after manual injection to avoid needing a real DB
            q.selectColumns.add("count(*) OVER() AS _total");
            q.limit(10).offset(20);

            String sql = q.toSql();
            assertTrue(sql.contains("count(*) OVER() AS _total"));
            assertTrue(sql.contains("LIMIT :_limit"));
            assertTrue(sql.contains("OFFSET :_offset"));
            assertTrue(sql.contains("user_id"));
            assertTrue(sql.contains("amount"));
        }
    }

    // ── CASE WHEN ──────────────────────────────────────────────────────

    @Nested
    @DisplayName("CASE WHEN")
    class CaseWhenTests {

        @Test
        @DisplayName("Basic CASE WHEN with eq and orElse")
        void basicCaseWhen() {
            String result = CH.caseWhen("status")
                    .eq("ACTIVE").then("Active")
                    .orElse("Inactive")
                    .toString();

            assertTrue(result.contains("CASE WHEN status = 'ACTIVE' THEN 'Active' ELSE 'Inactive' END"));
        }

        @Test
        @DisplayName("CASE WHEN with multiple conditions")
        void multipleCaseWhen() {
            String result = CH.caseWhen("amount")
                    .gt(1000).then("HIGH")
                    .when("amount").gt(500).then("MEDIUM")
                    .orElse("LOW")
                    .toString();

            assertTrue(result.contains("WHEN amount > 1000 THEN 'HIGH'"));
            assertTrue(result.contains("WHEN amount > 500 THEN 'MEDIUM'"));
            assertTrue(result.contains("ELSE 'LOW' END"));
        }

        @Test
        @DisplayName("CASE WHEN with numeric then value")
        void numericThenValue() {
            String result = CH.caseWhen("action")
                    .eq("BET").then(1)
                    .orElse(0)
                    .toString();

            assertTrue(result.contains("THEN 1"));
            assertTrue(result.contains("ELSE 0 END"));
        }

        @Test
        @DisplayName("CASE WHEN with .as() alias")
        void caseWhenWithAlias() {
            String result = CH.caseWhen("score")
                    .gte(80).then("PASS")
                    .orElse("FAIL")
                    .as("result")
                    .toString();

            assertTrue(result.contains("END AS result"));
        }

        @Test
        @DisplayName("CASE WHEN with IN condition")
        void caseWhenWithIn() {
            String result = CH.caseWhen("action")
                    .in("BET", "DEBIT").then("OUT")
                    .orElse("IN")
                    .toString();

            assertTrue(result.contains("action IN ('BET', 'DEBIT')"));
        }

        @Test
        @DisplayName("CASE WHEN with isNull")
        void caseWhenIsNull() {
            String result = CH.caseWhen("error_code")
                    .isNull().then("OK")
                    .orElse("ERROR")
                    .toString();

            assertTrue(result.contains("error_code IS NULL"));
        }

        @Test
        @DisplayName("CASE WHEN with between")
        void caseWhenBetween() {
            String result = CH.caseWhen("score")
                    .between(50, 100).then("MEDIUM")
                    .orElse("OTHER")
                    .toString();

            assertTrue(result.contains("score BETWEEN 50 AND 100"));
        }

        @Test
        @DisplayName("CASE WHEN with ne, lt, lte")
        void caseWhenOperators() {
            String result = CH.caseWhen("status")
                    .ne("DELETED").then("VISIBLE")
                    .when("score").lt(0).then("NEGATIVE")
                    .when("score").lte(10).then("LOW")
                    .orElse("NORMAL")
                    .toString();

            assertTrue(result.contains("status != 'DELETED'"));
            assertTrue(result.contains("score < 0"));
            assertTrue(result.contains("score <= 10"));
        }

        @Test
        @DisplayName("CASE WHEN with end() (no ELSE)")
        void caseWhenNoElse() {
            String result = CH.caseWhen("status")
                    .eq("ACTIVE").then(1)
                    .end()
                    .toString();

            assertTrue(result.endsWith("END"));
            assertFalse(result.contains("ELSE"));
        }

        @Test
        @DisplayName("CASE WHEN with orElseRaw")
        void caseWhenElseRaw() {
            String result = CH.caseWhen("status")
                    .eq("ACTIVE").then("YES")
                    .orElseRaw("other_column")
                    .toString();

            assertTrue(result.contains("ELSE other_column END"));
        }

        @Test
        @DisplayName("CASE WHEN with raw condition")
        void caseWhenRaw() {
            String result = CH.caseWhen("col")
                    .raw("col > 10 AND col < 100").then("MID")
                    .orElse("OTHER")
                    .toString();

            assertTrue(result.contains("WHEN col > 10 AND col < 100 THEN 'MID'"));
        }

        @Test
        @DisplayName("CASE WHEN in SELECT query")
        void caseWhenInQuery() {
            String sql = ClickHouseQuery.select(
                    "user_id",
                    CH.caseWhen("amount").gt(0).then("WIN").orElse("LOSE").as("result"),
                    CH.caseWhen("action").eq("BET").then(1).orElse(0).as("is_bet")
            ).from("orders").toSql();

            assertTrue(sql.contains("CASE WHEN amount > 0 THEN 'WIN' ELSE 'LOSE' END AS result"));
            assertTrue(sql.contains("CASE WHEN action = 'BET' THEN 1 ELSE 0 END AS is_bet"));
        }

        @Test
        @DisplayName("Alias.caseWhen prefixes column")
        void aliasCaseWhen() {
            Alias o = Alias.of("orders").as("o");
            String result = o.caseWhen("amount")
                    .gt(5000).then("HIGH")
                    .orElse("LOW")
                    .as("tier")
                    .toString();

            assertTrue(result.contains("o.amount > 5000"));
        }

        @Test
        @DisplayName("CASE WHEN with thenRaw")
        void caseWhenThenRaw() {
            String result = CH.caseWhen("status")
                    .eq("ACTIVE").thenRaw("amount * 2")
                    .orElse(0)
                    .toString();

            assertTrue(result.contains("THEN amount * 2"));
        }
    }

    // ── Generic between (numbers) ──────────────────────────────────────

    @Nested
    @DisplayName("Generic between")
    class GenericBetweenTests {

        @Test
        @DisplayName("between(int, int) for numbers")
        void betweenNumbers() {
            ClickHouseQuery q = ClickHouseQuery.select("*")
                    .from("t")
                    .where("amount").between(100, 500);

            String sql = q.toSql();
            assertTrue(sql.contains("amount >= :amountFrom"));
            assertTrue(sql.contains("amount <= :amountTo"));
            assertEquals(100, q.toParams().getValue("amountFrom"));
            assertEquals(500, q.toParams().getValue("amountTo"));
        }

        @Test
        @DisplayName("between(null, value) for one-sided bound")
        void betweenOneSided() {
            String sql = ClickHouseQuery.select("*")
                    .from("t")
                    .where("score").between(null, 100)
                    .toSql();

            assertFalse(sql.contains(">="));
            assertTrue(sql.contains("score <= :scoreTo"));
        }

        @Test
        @DisplayName("between(value, null) for lower bound only")
        void betweenLowerOnly() {
            String sql = ClickHouseQuery.select("*")
                    .from("t")
                    .where("score").between(50, null)
                    .toSql();

            assertTrue(sql.contains("score >= :scoreFrom"));
            assertFalse(sql.contains("<="));
        }
    }

    // ── Multiply / Divide ──────────────────────────────────────────────

    @Nested
    @DisplayName("Expr multiply / divide")
    class MultiplyDivideTests {

        @Test
        @DisplayName("Expr.multiply(Expr)")
        void multiplyExpr() {
            String result = CH.sum("amount").multiply(CH.count()).toString();
            assertTrue(result.contains("(sum(amount)) * (count(*))"));
        }

        @Test
        @DisplayName("Expr.multiply(String)")
        void multiplyRaw() {
            String result = CH.sum("price").multiply("quantity").toString();
            assertTrue(result.contains("(sum(price)) * (quantity)"));
        }

        @Test
        @DisplayName("Expr.divide(Expr)")
        void divideExpr() {
            String result = CH.sum("total_bet").divide(CH.count()).as("avg_bet").toString();
            assertTrue(result.contains("(sum(total_bet)) / (count(*)) AS avg_bet"));
        }

        @Test
        @DisplayName("Expr.divide(String)")
        void divideRaw() {
            String result = CH.sum("amount").divide("100").toString();
            assertTrue(result.contains("(sum(amount)) / (100)"));
        }

        @Test
        @DisplayName("Chained arithmetic: (a - b) / c")
        void chainedArithmetic() {
            String result = CH.sum("bet").minus(CH.sum("win"))
                    .divide(CH.sum("bet"))
                    .as("margin")
                    .toString();

            assertTrue(result.contains("/ (sum(bet))"));
            assertTrue(result.contains("AS margin"));
        }
    }

    // ── FULL OUTER JOIN ────────────────────────────────────────────────

    @Nested
    @DisplayName("FULL OUTER JOIN")
    class FullJoinTests {

        @Test
        @DisplayName("fullJoin generates FULL OUTER JOIN")
        void fullJoin() {
            String sql = ClickHouseQuery.select("*")
                    .from("t")
                    .fullJoin("other o").on("o.id", "t.other_id")
                    .toSql();

            assertTrue(sql.contains("FULL OUTER JOIN other o ON o.id = t.other_id"));
        }

        @Test
        @DisplayName("fullJoin with Alias")
        void fullJoinAlias() {
            Alias t = Alias.of("orders").as("t");
            Alias o = Alias.of("other").as("o");

            String sql = ClickHouseQuery.select("*")
                    .from(t)
                    .fullJoin(o).on(o.col("id"), t.col("other_id"))
                    .toSql();

            assertTrue(sql.contains("FULL OUTER JOIN other o ON o.id = t.other_id"));
        }
    }

    // ── OrBuilder between ──────────────────────────────────────────────

    @Nested
    @DisplayName("OrBuilder between")
    class OrBuilderBetweenTests {

        @Test
        @DisplayName("whereOr with between(Instant)")
        void whereOrBetweenInstant() {
            Instant from = Instant.parse("2026-01-01T00:00:00Z");
            Instant to = Instant.parse("2026-12-31T23:59:59Z");

            String sql = ClickHouseQuery.select("*")
                    .from("t")
                    .whereOr(or -> or
                            .where("created_at").between(from, to)
                            .where("status").eq("ACTIVE")
                    ).toSql();

            assertTrue(sql.contains("created_at >= :_or0"));
            assertTrue(sql.contains("created_at <= :_or1"));
            assertTrue(sql.contains("status = :_or2"));
            assertTrue(sql.contains(" OR "));
        }

        @Test
        @DisplayName("whereOr between with null bounds")
        void whereOrBetweenNullBounds() {
            String sql = ClickHouseQuery.select("*")
                    .from("t")
                    .whereOr(or -> or
                            .where("created_at").between(null, null)
                            .where("status").eq("ACTIVE")
                    ).toSql();

            // null between is skipped, only status remains
            assertTrue(sql.contains("status = :_or0"));
        }
    }
}
