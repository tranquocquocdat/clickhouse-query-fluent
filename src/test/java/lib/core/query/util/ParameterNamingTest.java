package lib.core.query.util;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link ParameterNaming}.
 */
class ParameterNamingTest {

    @Test
    void testGenerate_SimpleColumn() {
        String param = ParameterNaming.generate("user_id");
        assertTrue(param.startsWith("userId_"));
        assertTrue(param.matches("userId_\\d+"));
    }

    @Test
    void testGenerate_TableQualifiedColumn() {
        String param = ParameterNaming.generate("orders.user_id");
        assertTrue(param.startsWith("ordersUserId_"));
        assertTrue(param.matches("ordersUserId_\\d+"));
    }

    @Test
    void testGenerate_ShortAliasColumn() {
        String param = ParameterNaming.generate("o.amount");
        assertTrue(param.startsWith("oAmount_"));
        assertTrue(param.matches("oAmount_\\d+"));
    }

    @Test
    void testGenerate_WithSuffix() {
        String param = ParameterNaming.generate("amount", "Gt");
        assertTrue(param.startsWith("amountGt_"));
        assertTrue(param.matches("amountGt_\\d+"));
    }

    @Test
    void testGenerate_TableQualifiedWithSuffix() {
        String param = ParameterNaming.generate("orders.amount", "From");
        assertTrue(param.startsWith("ordersAmountFrom_"));
        assertTrue(param.matches("ordersAmountFrom_\\d+"));
    }

    @Test
    void testGenerate_UniqueSequence() {
        String param1 = ParameterNaming.generate("user_id");
        String param2 = ParameterNaming.generate("user_id");
        
        assertNotEquals(param1, param2, "Sequential calls should generate unique parameter names");
    }

    @Test
    void testToCamelCase_SimpleSnakeCase() {
        assertEquals("userId", ParameterNaming.toCamelCase("user_id"));
        assertEquals("createdAt", ParameterNaming.toCamelCase("created_at"));
        assertEquals("amount", ParameterNaming.toCamelCase("amount"));
    }

    @Test
    void testToCamelCase_TableQualified() {
        assertEquals("ordersUserId", ParameterNaming.toCamelCase("orders.user_id"));
        assertEquals("oAmount", ParameterNaming.toCamelCase("o.amount"));
        assertEquals("usersCreatedAt", ParameterNaming.toCamelCase("users.created_at"));
    }

    @Test
    void testToCamelCase_NullAndEmpty() {
        assertNull(ParameterNaming.toCamelCase(null));
        assertEquals("", ParameterNaming.toCamelCase(""));
    }

    @Test
    void testToCamelCase_NoUnderscores() {
        assertEquals("amount", ParameterNaming.toCamelCase("amount"));
        assertEquals("id", ParameterNaming.toCamelCase("id"));
    }
}
