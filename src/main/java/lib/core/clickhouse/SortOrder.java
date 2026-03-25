package lib.core.clickhouse;

/**
 * Type-safe sort direction for ORDER BY clauses.
 *
 * <pre>{@code
 * .orderBy("total", SortOrder.DESC)
 * .orderBy("user_id", SortOrder.ASC)
 * }</pre>
 */
public enum SortOrder {
    ASC, DESC
}
