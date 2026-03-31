package lib.core.query;

import lib.core.clickhouse.query.ClickHouseQuery;
import org.junit.jupiter.api.Test;

import static lib.core.clickhouse.expression.CH.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test SQL clause ordering validation (Phase enforcement).
 * 
 * <p>Valid order: SELECT → FROM → JOIN → WHERE → GROUP_BY → HAVING → ORDER_BY → LIMIT
 */
class PhaseOrderingTest {

    @Test
    void testValidOrder_AllClauses_Success() {
        // Valid order: SELECT → FROM → JOIN → WHERE → GROUP BY → HAVING → ORDER BY → LIMIT
        assertDoesNotThrow(() -> {
            ClickHouseQuery.select("user_id", "sum(amount) AS total")
                .from("orders")
                .join("users u").on("u.id = orders.user_id")
                .where("status").eq("ACTIVE")
                .groupBy("user_id")
                .having(sum("amount")).gt(100)
                .orderBy("total")
                .limit(10)
                .toSql();
        });
    }

    @Test
    void testValidOrder_PartialClauses_Success() {
        // Valid order: SELECT → FROM → WHERE → LIMIT
        assertDoesNotThrow(() -> {
            ClickHouseQuery.select("*")
                .from("orders")
                .where("status").eq("ACTIVE")
                .limit(10)
                .toSql();
        });
    }

    @Test
    void testValidOrder_MultipleWhere_Success() {
        // Multiple WHERE calls are allowed (same phase)
        assertDoesNotThrow(() -> {
            ClickHouseQuery.select("*")
                .from("orders")
                .where("status").eq("ACTIVE")
                .where("amount").gt(100)
                .where("user_id").eq(123)
                .toSql();
        });
    }

    @Test
    void testInvalidOrder_WhereBeforeFrom_ThrowsException() {
        // Invalid: WHERE before FROM
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> {
            ClickHouseQuery.select("*")
                .where("status").eq("ACTIVE")  // ❌ WHERE before FROM
                .from("orders")
                .toSql();
        });
        
        assertTrue(ex.getMessage().contains("cannot call FROM after WHERE"));
        assertTrue(ex.getMessage().contains("Expected order: SELECT → FROM → JOIN → WHERE → GROUP_BY → HAVING → ORDER_BY → LIMIT"));
    }

    @Test
    void testInvalidOrder_JoinBeforeFrom_ThrowsException() {
        // Invalid: JOIN before FROM
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> {
            ClickHouseQuery.select("*")
                .join("users u").on("u.id = orders.user_id")  // ❌ JOIN before FROM
                .from("orders")
                .toSql();
        });
        
        assertTrue(ex.getMessage().contains("cannot call FROM after JOIN"));
    }

    @Test
    void testInvalidOrder_GroupByBeforeWhere_ThrowsException() {
        // Invalid: GROUP BY before WHERE
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> {
            ClickHouseQuery.select("user_id", "sum(amount)")
                .from("orders")
                .groupBy("user_id")  // GROUP BY first
                .where("status").eq("ACTIVE")  // ❌ WHERE after GROUP BY
                .toSql();
        });
        
        assertTrue(ex.getMessage().contains("cannot call WHERE after GROUP_BY"));
    }

    @Test
    void testInvalidOrder_HavingBeforeGroupBy_ThrowsException() {
        // Invalid: HAVING before GROUP BY
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> {
            ClickHouseQuery.select("user_id", "sum(amount) AS total")
                .from("orders")
                .having(sum("amount")).gt(100)  // ❌ HAVING before GROUP BY
                .groupBy("user_id")
                .toSql();
        });
        
        assertTrue(ex.getMessage().contains("cannot call GROUP_BY after HAVING"));
    }

    @Test
    void testInvalidOrder_OrderByBeforeGroupBy_ThrowsException() {
        // Invalid: ORDER BY before GROUP BY
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> {
            ClickHouseQuery.select("user_id", "sum(amount)")
                .from("orders")
                .orderBy("user_id")  // ORDER BY first
                .groupBy("user_id")  // ❌ GROUP BY after ORDER BY
                .toSql();
        });
        
        assertTrue(ex.getMessage().contains("cannot call GROUP_BY after ORDER_BY"));
    }

    @Test
    void testInvalidOrder_LimitBeforeOrderBy_ThrowsException() {
        // Invalid: LIMIT before ORDER BY
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> {
            ClickHouseQuery.select("*")
                .from("orders")
                .limit(10)  // LIMIT first
                .orderBy("created_at")  // ❌ ORDER BY after LIMIT
                .toSql();
        });
        
        assertTrue(ex.getMessage().contains("cannot call ORDER_BY after LIMIT"));
    }

    @Test
    void testInvalidOrder_FromAfterWhere_ThrowsException() {
        // Invalid: FROM after WHERE
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> {
            ClickHouseQuery.select("*")
                .from("orders")
                .where("status").eq("ACTIVE")
                .from("users")  // ❌ FROM again after WHERE
                .toSql();
        });
        
        assertTrue(ex.getMessage().contains("cannot call FROM after WHERE"));
    }

    @Test
    void testValidOrder_MultipleJoins_Success() {
        // Multiple JOINs are allowed (same phase)
        assertDoesNotThrow(() -> {
            ClickHouseQuery.select("*")
                .from("orders o")
                .join("users u").on("u.id = o.user_id")
                .leftJoin("products p").on("p.id = o.product_id")
                .rightJoin("categories c").on("c.id = p.category_id")
                .toSql();
        });
    }

    @Test
    void testValidOrder_MultipleGroupBy_Success() {
        // Multiple GROUP BY calls are allowed (same phase)
        assertDoesNotThrow(() -> {
            ClickHouseQuery.select("user_id", "product_id", "sum(amount)")
                .from("orders")
                .groupBy("user_id")
                .groupBy("product_id")
                .toSql();
        });
    }

    @Test
    void testValidOrder_MultipleHaving_Success() {
        // Multiple HAVING calls are allowed (same phase)
        assertDoesNotThrow(() -> {
            ClickHouseQuery.select("user_id", "sum(amount) AS total")
                .from("orders")
                .groupBy("user_id")
                .having(sum("amount")).gt(100)
                .having(count()).gte(5)
                .toSql();
        });
    }

    @Test
    void testValidOrder_MultipleOrderBy_Success() {
        // Multiple ORDER BY calls are allowed (same phase)
        assertDoesNotThrow(() -> {
            ClickHouseQuery.select("*")
                .from("orders")
                .orderBy("created_at")
                .orderBy("amount")
                .toSql();
        });
    }

    @Test
    void testInvalidOrder_ComplexScenario_ThrowsException() {
        // Complex invalid scenario: try to add WHERE after HAVING
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> {
            ClickHouseQuery.select("user_id", "sum(amount)")
                .from("orders")
                .groupBy("user_id")
                .having(sum("amount")).gt(100)
                .where("status").eq("ACTIVE")  // ❌ WHERE after HAVING
                .toSql();
        });
        
        assertTrue(ex.getMessage().contains("cannot call WHERE after HAVING"));
    }

    @Test
    void testValidOrder_WithCTE_Success() {
        // CTE (WITH) is allowed before SELECT
        assertDoesNotThrow(() -> {
            ClickHouseQuery.with("active_users",
                    ClickHouseQuery.select("user_id").from("users").where("status").eq("ACTIVE")
                )
                .select("*")
                .from("active_users")
                .toSql();
        });
    }

    @Test
    void testValidOrder_WithUnionAll_Success() {
        // UNION ALL is allowed after main query
        assertDoesNotThrow(() -> {
            ClickHouseQuery.select("*")
                .from("orders_2024")
                .unionAll(
                    ClickHouseQuery.select("*").from("orders_2025")
                )
                .orderBy("created_at")
                .limit(10)
                .toSql();
        });
    }
}
