package lib.core.query.util;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link ColumnValidator}.
 */
class ColumnValidatorTest {

    @Test
    void testValidate_SimpleColumn_Valid() {
        assertDoesNotThrow(() -> ColumnValidator.validate("user_id"));
        assertDoesNotThrow(() -> ColumnValidator.validate("amount"));
        assertDoesNotThrow(() -> ColumnValidator.validate("created_at"));
    }

    @Test
    void testValidate_TableQualified_Valid() {
        assertDoesNotThrow(() -> ColumnValidator.validate("orders.user_id"));
        assertDoesNotThrow(() -> ColumnValidator.validate("o.amount"));
        assertDoesNotThrow(() -> ColumnValidator.validate("users.created_at"));
    }

    @Test
    void testValidate_FunctionCall_Valid() {
        assertDoesNotThrow(() -> ColumnValidator.validate("sum(amount)"));
        assertDoesNotThrow(() -> ColumnValidator.validate("count(*)"));
        assertDoesNotThrow(() -> ColumnValidator.validate("avg(revenue)"));
    }

    @Test
    void testValidate_Expression_Valid() {
        assertDoesNotThrow(() -> ColumnValidator.validate("amount * 1.1"));
        assertDoesNotThrow(() -> ColumnValidator.validate("(revenue - cost)"));
        assertDoesNotThrow(() -> ColumnValidator.validate("amount + 100"));
    }

    @Test
    void testValidate_NullOrEmpty_Invalid() {
        assertThrows(IllegalArgumentException.class, () -> 
            ColumnValidator.validate(null));
        assertThrows(IllegalArgumentException.class, () -> 
            ColumnValidator.validate(""));
        assertThrows(IllegalArgumentException.class, () -> 
            ColumnValidator.validate("   "));
    }

    @Test
    void testValidate_DangerousKeywords_Invalid() {
        assertThrows(IllegalArgumentException.class, () -> 
            ColumnValidator.validate("DROP TABLE users"));
        assertThrows(IllegalArgumentException.class, () -> 
            ColumnValidator.validate("DELETE FROM orders"));
        assertThrows(IllegalArgumentException.class, () -> 
            ColumnValidator.validate("UPDATE users SET"));
        assertThrows(IllegalArgumentException.class, () -> 
            ColumnValidator.validate("INSERT INTO orders"));
        assertThrows(IllegalArgumentException.class, () -> 
            ColumnValidator.validate("TRUNCATE TABLE"));
    }

    @Test
    void testValidate_SQLComments_Invalid() {
        assertThrows(IllegalArgumentException.class, () -> 
            ColumnValidator.validate("user_id -- comment"));
        assertThrows(IllegalArgumentException.class, () -> 
            ColumnValidator.validate("amount /* comment */"));
        assertThrows(IllegalArgumentException.class, () -> 
            ColumnValidator.validate("id */ OR 1=1 /*"));
    }

    @Test
    void testValidate_Semicolon_Invalid() {
        assertThrows(IllegalArgumentException.class, () -> 
            ColumnValidator.validate("user_id; DROP TABLE users"));
        assertThrows(IllegalArgumentException.class, () -> 
            ColumnValidator.validate("amount;"));
    }

    @Test
    void testValidated_ReturnsColumnWhenValid() {
        String column = "user_id";
        String result = ColumnValidator.validated(column);
        assertEquals(column, result);
    }

    @Test
    void testValidated_ThrowsWhenInvalid() {
        assertThrows(IllegalArgumentException.class, () -> 
            ColumnValidator.validated("DROP TABLE users"));
    }

    @Test
    void testValidate_CaseInsensitiveKeywords() {
        assertThrows(IllegalArgumentException.class, () -> 
            ColumnValidator.validate("drop table users"));
        assertThrows(IllegalArgumentException.class, () -> 
            ColumnValidator.validate("DeLeTe FROM orders"));
        assertThrows(IllegalArgumentException.class, () -> 
            ColumnValidator.validate("UpDaTe users"));
    }
}
