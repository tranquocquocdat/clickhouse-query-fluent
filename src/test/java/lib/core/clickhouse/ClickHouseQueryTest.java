package lib.core.clickhouse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

import java.time.Instant;
import java.util.List;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import lib.core.clickhouse.ClickHouseQuery.SortOrder;

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
        @DisplayName("whereOr generates OR group")
        void whereOr() {
            ClickHouseQuery q = ClickHouseQuery
                    .select("*")
                    .from("t")
                    .whereOr(or -> or
                            .add("status", "ACTIVE")
                            .add("status", "PENDING")
                    );

            String sql = q.toSql();
            assertTrue(sql.contains("OR"));
            assertTrue(sql.contains("status = :"));
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
                    .having("count(*)").gte(5)
                    .toSql();

            assertTrue(sql.contains("HAVING count(*) >= :"));
        }

        @Test
        @DisplayName("HAVING supports all operators")
        void havingOperators() {
            ClickHouseQuery q = ClickHouseQuery.select("x").from("t").groupBy("x");
            q.having("a").eq(1);
            q.having("b").ne(2);
            q.having("c").lt(3);
            q.having("d").lte(4);
            q.having("e").between(5, 6);
            
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
                    .having("count(*)").gt(1)
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
    }

    // ── CountQuery ───────────────────────────────────────────────────────

    @Nested
    @DisplayName("CountQuery")
    class CountQueryTests {

        @Test
        @DisplayName("Static count(subQuery) generates correct SQL")
        void countSubQuery_sql() {
            ClickHouseQuery.CountQuery countQuery = ClickHouseQuery.count(
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

            ClickHouseQuery.CountQuery countQuery = ClickHouseQuery.count(
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
}
