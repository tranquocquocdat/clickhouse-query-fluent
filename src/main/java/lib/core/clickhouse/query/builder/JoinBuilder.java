package lib.core.clickhouse.query.builder;

import lib.core.clickhouse.query.ClickHouseQuery;

/**
 * Fluent builder for JOIN conditions.
 * Created via {@link ClickHouseQuery#join(String)}, {@link ClickHouseQuery#leftJoin(String)},
 * or {@link ClickHouseQuery#rightJoin(String)}.
 *
 * <h3>Usage</h3>
 * <pre>{@code
 * // Fluent equality (most common)
 * .join("user_profile u").on("u.id", "t.user_id")
 *
 * // Raw condition (complex cases)
 * .join("user_profile u").on("u.id = t.user_id AND u.active = 1")
 * }</pre>
 */
public final class JoinBuilder {
    private final ClickHouseQuery query;
    private final String joinType;
    private final String table;

    public JoinBuilder(ClickHouseQuery query, String joinType, String table) {
        this.query = query;
        this.joinType = joinType;
        this.table = table;
    }

    /**
     * Fluent ON with equality: {@code ON leftCol = rightCol}.
     *
     * @param leftColumn  the left column (e.g. {@code "u.id"})
     * @param rightColumn the right column (e.g. {@code "t.user_id"})
     * @return the parent query builder
     */
    public ClickHouseQuery on(String leftColumn, String rightColumn) {
        query.joinClauses.add(joinType + " " + table + " ON " + leftColumn + " = " + rightColumn);
        return query;
    }

    /**
     * Raw ON condition for complex cases.
     *
     * @param condition the raw join condition
     * @return the parent query builder
     */
    public ClickHouseQuery on(String condition) {
        query.joinClauses.add(joinType + " " + table + " ON " + condition);
        return query;
    }
}
