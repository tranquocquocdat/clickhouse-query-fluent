package lib.core.clickhouse.util;

import lib.core.query.util.StringUtils;

/**
 * String utilities for ClickHouse query building.
 *
 * @deprecated Use {@link StringUtils} instead (database-agnostic).
 */
@Deprecated
public final class CHStringUtils {

    private CHStringUtils() {}

    /**
     * Convert snake_case to camelCase.
     * Example: "user_id" → "userId", "created_at" → "createdAt"
     *
     * @deprecated Use {@link StringUtils#toCamelCase(String)} instead.
     */
    @Deprecated
    public static String toCamelCase(String snake) {
        return StringUtils.toCamelCase(snake);
    }
}
