package lib.core.clickhouse.query.builder;

import lib.core.clickhouse.query.ClickHouseQuery;
import lib.core.clickhouse.query.SortOrder;

/**
 * Fluent builder for chaining additional JOIN ON conditions via {@code .and()}.
 *
 * <pre>{@code
 * .join(wt).on(st.c("user_id"), wt.c("user_id"))
 *          .and(st.c("spin_id"), wt.c("round_id"))
 * }</pre>
 */
public final class JoinOnBuilder {
    private final ClickHouseQuery query;
    private final int joinIndex;

    public JoinOnBuilder(ClickHouseQuery query, int joinIndex) {
        this.query = query;
        this.joinIndex = joinIndex;
    }

    /**
     * Append an additional equality condition to the JOIN ON clause.
     * {@code AND leftCol = rightCol}
     */
    public JoinOnBuilder and(String leftColumn, String rightColumn) {
        String existing = query.joinClauses.get(joinIndex);
        query.joinClauses.set(joinIndex, existing + " AND " + leftColumn + " = " + rightColumn);
        return this;
    }

    // ── Transition methods (delegate to ClickHouseQuery) ────────────────

    /** Continue to WHERE phase. */
    public WhereBuilder where(String column) {
        return query.where(column);
    }

    /** Continue to ILIKE search phase. */
    public WhereILikeBuilder whereILike(String keyword) {
        return query.whereILike(keyword);
    }

    /** Continue to LIKE search phase. */
    public WhereILikeBuilder whereLike(String keyword) {
        return query.whereLike(keyword);
    }

    /** Continue to GROUP BY phase. */
    public ClickHouseQuery groupBy(String... columns) {
        return query.groupBy(columns);
    }

    /** Continue to ORDER BY phase. */
    public ClickHouseQuery orderBy(String column, SortOrder order) {
        return query.orderBy(column, order);
    }

    /** Continue to LIMIT phase. */
    public ClickHouseQuery limit(int n) {
        return query.limit(n);
    }

    /** Get the underlying query (e.g. for passing as a parameter). */
    public ClickHouseQuery query() {
        return query;
    }
}
