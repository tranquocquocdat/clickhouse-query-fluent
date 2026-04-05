package lib.core.query.builder;

import lib.core.clickhouse.query.ClickHouseQuery;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests null and empty string safety in OrBuilder.
 */
class OrBuilderNullSafetyTest {

    @Test
    void testOrBuilder_eq_NullValue_Skipped() {
        String sql = ClickHouseQuery.select("*")
            .from("orders")
            .whereOr(or -> or
                .where("status").eq(null)
                .where("amount").gt(100)
            )
            .toSql();
        
        // Only amount condition should be present
        assertFalse(sql.contains("status"));
        assertTrue(sql.contains("amount > :"));
    }

    @Test
    void testOrBuilder_eq_EmptyString_Skipped() {
        String sql = ClickHouseQuery.select("*")
            .from("orders")
            .whereOr(or -> or
                .where("status").eq("")
                .where("amount").gt(100)
            )
            .toSql();
        
        // Only amount condition should be present
        assertFalse(sql.contains("status"));
        assertTrue(sql.contains("amount > :"));
    }

    @Test
    void testOrBuilder_ne_NullValue_Skipped() {
        String sql = ClickHouseQuery.select("*")
            .from("orders")
            .whereOr(or -> or
                .where("status").ne(null)
                .where("amount").gt(100)
            )
            .toSql();
        
        assertFalse(sql.contains("status"));
        assertTrue(sql.contains("amount > :"));
    }

    @Test
    void testOrBuilder_ne_EmptyString_Skipped() {
        String sql = ClickHouseQuery.select("*")
            .from("orders")
            .whereOr(or -> or
                .where("status").ne("")
                .where("amount").gt(100)
            )
            .toSql();
        
        assertFalse(sql.contains("status"));
        assertTrue(sql.contains("amount > :"));
    }

    @Test
    void testOrBuilder_gt_NullValue_Skipped() {
        String sql = ClickHouseQuery.select("*")
            .from("orders")
            .whereOr(or -> or
                .where("amount").gt(null)
                .where("status").eq("ACTIVE")
            )
            .toSql();
        
        assertFalse(sql.contains("amount"));
        assertTrue(sql.contains("status = :"));
    }

    @Test
    void testOrBuilder_gt_EmptyString_Skipped() {
        String sql = ClickHouseQuery.select("*")
            .from("orders")
            .whereOr(or -> or
                .where("amount").gt("")
                .where("status").eq("ACTIVE")
            )
            .toSql();
        
        assertFalse(sql.contains("amount"));
        assertTrue(sql.contains("status = :"));
    }

    @Test
    void testOrBuilder_gte_NullValue_Skipped() {
        String sql = ClickHouseQuery.select("*")
            .from("orders")
            .whereOr(or -> or
                .where("amount").gte(null)
                .where("status").eq("ACTIVE")
            )
            .toSql();
        
        assertFalse(sql.contains("amount"));
        assertTrue(sql.contains("status = :"));
    }

    @Test
    void testOrBuilder_gte_EmptyString_Skipped() {
        String sql = ClickHouseQuery.select("*")
            .from("orders")
            .whereOr(or -> or
                .where("amount").gte("")
                .where("status").eq("ACTIVE")
            )
            .toSql();
        
        assertFalse(sql.contains("amount"));
        assertTrue(sql.contains("status = :"));
    }

    @Test
    void testOrBuilder_lt_NullValue_Skipped() {
        String sql = ClickHouseQuery.select("*")
            .from("orders")
            .whereOr(or -> or
                .where("amount").lt(null)
                .where("status").eq("ACTIVE")
            )
            .toSql();
        
        assertFalse(sql.contains("amount"));
        assertTrue(sql.contains("status = :"));
    }

    @Test
    void testOrBuilder_lt_EmptyString_Skipped() {
        String sql = ClickHouseQuery.select("*")
            .from("orders")
            .whereOr(or -> or
                .where("amount").lt("")
                .where("status").eq("ACTIVE")
            )
            .toSql();
        
        assertFalse(sql.contains("amount"));
        assertTrue(sql.contains("status = :"));
    }

    @Test
    void testOrBuilder_lte_NullValue_Skipped() {
        String sql = ClickHouseQuery.select("*")
            .from("orders")
            .whereOr(or -> or
                .where("amount").lte(null)
                .where("status").eq("ACTIVE")
            )
            .toSql();
        
        assertFalse(sql.contains("amount"));
        assertTrue(sql.contains("status = :"));
    }

    @Test
    void testOrBuilder_lte_EmptyString_Skipped() {
        String sql = ClickHouseQuery.select("*")
            .from("orders")
            .whereOr(or -> or
                .where("amount").lte("")
                .where("status").eq("ACTIVE")
            )
            .toSql();
        
        assertFalse(sql.contains("amount"));
        assertTrue(sql.contains("status = :"));
    }

    @Test
    void testOrBuilder_AllNullOrEmpty_NoWhereClause() {
        String sql = ClickHouseQuery.select("*")
            .from("orders")
            .whereOr(or -> or
                .where("status").eq(null)
                .where("type").ne("")
                .where("amount").gt(null)
            )
            .toSql();
        
        // No WHERE clause should be generated
        assertFalse(sql.contains("WHERE"));
    }

    @Test
    void testOrBuilder_MixedNullAndValid_OnlyValidApplied() {
        String sql = ClickHouseQuery.select("*")
            .from("orders")
            .whereOr(or -> or
                .where("status").eq(null)        // skipped
                .where("type").eq("")            // skipped
                .where("amount").gt(100)         // applied
                .where("user_id").eq(null)       // skipped
                .where("category").eq("FOOD")    // applied
            )
            .toSql();
        
        assertFalse(sql.contains("status"));
        assertFalse(sql.contains("type"));
        assertFalse(sql.contains("user_id"));
        assertTrue(sql.contains("amount > :"));
        assertTrue(sql.contains("category = :"));
        assertTrue(sql.contains(" OR "));
    }

    @Test
    void testOrBuilder_in_NullCollection_Skipped() {
        String sql = ClickHouseQuery.select("*")
            .from("orders")
            .whereOr(or -> or
                .where("status").in((List<?>) null)
                .where("amount").gt(100)
            )
            .toSql();
        
        assertFalse(sql.contains("status"));
        assertFalse(sql.contains("IN"));
        assertTrue(sql.contains("amount > :"));
    }

    @Test
    void testOrBuilder_in_EmptyCollection_Skipped() {
        String sql = ClickHouseQuery.select("*")
            .from("orders")
            .whereOr(or -> or
                .where("status").in(List.of())
                .where("amount").gt(100)
            )
            .toSql();
        
        assertFalse(sql.contains("status"));
        assertFalse(sql.contains("IN"));
        assertTrue(sql.contains("amount > :"));
    }

    @Test
    void testOrBuilder_ValidValues_AllApplied() {
        String sql = ClickHouseQuery.select("*")
            .from("orders")
            .whereOr(or -> or
                .where("status").eq("ACTIVE")
                .where("amount").gt(100)
                .where("type").ne("TEST")
            )
            .toSql();
        
        assertTrue(sql.contains("status = :"));
        assertTrue(sql.contains("amount > :"));
        assertTrue(sql.contains("type != :"));
        assertTrue(sql.contains(" OR "));
    }
}
