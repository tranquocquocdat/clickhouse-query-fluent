package lib.core.query.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Generates unique parameter names to avoid collisions.
 * 
 * <p>Handles table-qualified columns correctly:
 * <ul>
 *   <li>{@code "user_id"} → {@code "userId"}</li>
 *   <li>{@code "orders.user_id"} → {@code "ordersUserId"}</li>
 *   <li>{@code "o.user_id"} → {@code "oUserId"}</li>
 * </ul>
 * 
 * <p>Thread-safe: uses {@link AtomicInteger} for sequence generation.
 */
public final class ParameterNaming {
    
    private ParameterNaming() {}
    
    private static final AtomicInteger globalSequence = new AtomicInteger(0);
    
    /**
     * Generates a unique parameter name from a column name.
     * 
     * <p>Format: {@code <camelCaseColumn>_<sequence>}
     * 
     * <p>Examples:
     * <ul>
     *   <li>{@code "user_id"} → {@code "userId_0"}</li>
     *   <li>{@code "orders.user_id"} → {@code "ordersUserId_1"}</li>
     *   <li>{@code "o.amount"} → {@code "oAmount_2"}</li>
     * </ul>
     * 
     * @param column the column name (may include table prefix)
     * @return a unique parameter name
     */
    public static String generate(String column) {
        String base = toCamelCase(column);
        int seq = globalSequence.getAndIncrement();
        return base + "_" + seq;
    }
    
    /**
     * Generates a unique parameter name with a suffix.
     * 
     * <p>Format: {@code <camelCaseColumn><suffix>_<sequence>}
     * 
     * <p>Examples:
     * <ul>
     *   <li>{@code ("amount", "Gt")} → {@code "amountGt_0"}</li>
     *   <li>{@code ("orders.amount", "From")} → {@code "ordersAmountFrom_1"}</li>
     * </ul>
     * 
     * @param column the column name
     * @param suffix the suffix (e.g., "Gt", "From", "To")
     * @return a unique parameter name
     */
    public static String generate(String column, String suffix) {
        String base = toCamelCase(column);
        int seq = globalSequence.getAndIncrement();
        return base + suffix + "_" + seq;
    }
    
    /**
     * Converts a column name to camelCase, handling table prefixes.
     * 
     * <p>Examples:
     * <ul>
     *   <li>{@code "user_id"} → {@code "userId"}</li>
     *   <li>{@code "orders.user_id"} → {@code "ordersUserId"}</li>
     *   <li>{@code "o.user_id"} → {@code "oUserId"}</li>
     *   <li>{@code "created_at"} → {@code "createdAt"}</li>
     * </ul>
     * 
     * @param column the column name (may include table prefix with dot)
     * @return the camelCase version
     */
    public static String toCamelCase(String column) {
        if (column == null || column.isEmpty()) {
            return column;
        }
        
        // Handle table.column format
        if (column.contains(".")) {
            String[] parts = column.split("\\.");
            StringBuilder result = new StringBuilder();
            for (String part : parts) {
                result.append(convertSnakeToCamel(part));
            }
            return result.toString();
        }
        
        return convertSnakeToCamel(column);
    }
    
    /**
     * Converts snake_case to camelCase.
     * 
     * @param snake the snake_case string
     * @return the camelCase string
     */
    private static String convertSnakeToCamel(String snake) {
        if (snake == null || snake.isEmpty()) {
            return snake;
        }
        
        StringBuilder result = new StringBuilder();
        boolean capitalizeNext = false;
        
        for (char c : snake.toCharArray()) {
            if (c == '_') {
                capitalizeNext = true;
            } else {
                result.append(capitalizeNext ? Character.toUpperCase(c) : c);
                capitalizeNext = false;
            }
        }
        
        return result.toString();
    }
}
