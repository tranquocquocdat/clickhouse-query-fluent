package lib.core.query.builder;

import lib.core.clickhouse.query.ClickHouseQuery;
import lib.core.query.exception.InvalidRangeException;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static lib.core.clickhouse.expression.CH.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test range validation in WHERE and HAVING builders.
 */
class RangeValidationTest {

    @Test
    void testWhereBetween_ValidRange_Success() {
        // Valid ranges should not throw
        assertDoesNotThrow(() -> {
            ClickHouseQuery.select("*")
                .from("orders")
                .where("amount").between(100, 200)
                .toSql();
        });
    }

    @Test
    void testWhereBetween_InvalidRange_ThrowsException() {
        // from > to should throw InvalidRangeException
        InvalidRangeException ex = assertThrows(InvalidRangeException.class, () -> {
            ClickHouseQuery.select("*")
                .from("orders")
                .where("amount").between(200, 100)
                .toSql();
        });
        
        assertEquals("amount", ex.getColumn());
        assertEquals(200, ex.getFrom());
        assertEquals(100, ex.getTo());
        assertTrue(ex.getMessage().contains("from (200) must be less than or equal to to (100)"));
    }

    @Test
    void testWhereBetween_StringRange_Invalid() {
        // String range: "Z" > "A"
        assertThrows(InvalidRangeException.class, () -> {
            ClickHouseQuery.select("*")
                .from("users")
                .where("name").between("Z", "A")
                .toSql();
        });
    }

    @Test
    void testWhereBetween_StringRange_Valid() {
        // String range: "A" < "Z"
        assertDoesNotThrow(() -> {
            ClickHouseQuery.select("*")
                .from("users")
                .where("name").between("A", "Z")
                .toSql();
        });
    }

    @Test
    void testWhereBetween_DateRange_Invalid() {
        Instant from = Instant.parse("2024-12-31T00:00:00Z");
        Instant to = Instant.parse("2024-01-01T00:00:00Z");
        
        // from > to should throw
        assertThrows(InvalidRangeException.class, () -> {
            ClickHouseQuery.select("*")
                .from("orders")
                .where("created_at").between(from, to)
                .toSql();
        });
    }

    @Test
    void testWhereBetween_DateRange_Valid() {
        Instant from = Instant.parse("2024-01-01T00:00:00Z");
        Instant to = Instant.parse("2024-12-31T00:00:00Z");
        
        // from < to should succeed
        assertDoesNotThrow(() -> {
            ClickHouseQuery.select("*")
                .from("orders")
                .where("created_at").between(from, to)
                .toSql();
        });
    }

    @Test
    void testWhereBetween_EqualBounds_Valid() {
        // from == to is valid (inclusive range)
        assertDoesNotThrow(() -> {
            ClickHouseQuery.select("*")
                .from("orders")
                .where("amount").between(100, 100)
                .toSql();
        });
    }

    @Test
    void testWhereBetween_NullBounds_NoValidation() {
        // Null bounds should not trigger validation
        assertDoesNotThrow(() -> {
            ClickHouseQuery.select("*")
                .from("orders")
                .where("amount").between(null, 100)
                .toSql();
        });
        
        assertDoesNotThrow(() -> {
            ClickHouseQuery.select("*")
                .from("orders")
                .where("amount").between(100, null)
                .toSql();
        });
    }

    @Test
    void testWhereBetween_EmptyStringBounds_NoValidation() {
        // Empty string bounds should not trigger validation
        assertDoesNotThrow(() -> {
            ClickHouseQuery.select("*")
                .from("orders")
                .where("name").between("", "Z")
                .toSql();
        });
    }

    @Test
    void testHavingBetween_InvalidRange_ThrowsException() {
        // HAVING with invalid range should throw
        InvalidRangeException ex = assertThrows(InvalidRangeException.class, () -> {
            ClickHouseQuery.select("user_id", "sum(amount) AS total")
                .from("orders")
                .groupBy("user_id")
                .having(sum("amount")).between(1000, 500)
                .toSql();
        });
        
        assertTrue(ex.getMessage().contains("from (1000) must be less than or equal to to (500)"));
    }

    @Test
    void testHavingBetween_ValidRange_Success() {
        // HAVING with valid range should succeed
        assertDoesNotThrow(() -> {
            ClickHouseQuery.select("user_id", "sum(amount) AS total")
                .from("orders")
                .groupBy("user_id")
                .having(sum("amount")).between(500, 1000)
                .toSql();
        });
    }
}
