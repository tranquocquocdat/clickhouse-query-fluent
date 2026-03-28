package lib.core.clickhouse.util;

/**
 * String utilities for ClickHouse query building.
 */
public final class CHStringUtils {

    private CHStringUtils() {}

    /**
     * Convert snake_case to camelCase.
     * Example: "user_id" → "userId", "created_at" → "createdAt"
     */
    public static String toCamelCase(String snake) {
        if (snake == null || snake.isEmpty()) return snake;
        StringBuilder sb = new StringBuilder();
        boolean nextUpper = false;
        for (char c : snake.toCharArray()) {
            if (c == '_') {
                nextUpper = true;
            } else if (nextUpper) {
                sb.append(Character.toUpperCase(c));
                nextUpper = false;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
