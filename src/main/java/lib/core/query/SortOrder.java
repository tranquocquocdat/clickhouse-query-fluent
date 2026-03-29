package lib.core.query;

/**
 * Type-safe sort direction for ORDER BY clauses.
 *
 * <pre>{@code
 * .orderBy("total", SortOrder.DESC)
 * .orderBy("user_id", SortOrder.ASC)
 * }</pre>
 */
public enum SortOrder {
    ASC, DESC;

    /**
     * Parse a direction string ("ASC" / "DESC") case-insensitively.
     * Defaults to {@link #DESC} if the value is null or unrecognised.
     */
    public static SortOrder of(String direction) {
        if (direction != null && "ASC".equalsIgnoreCase(direction.trim())) {
            return ASC;
        }
        return DESC;
    }
}
