package lib.core.clickhouse.query.builder;

import lib.core.clickhouse.query.ClickHouseQuery;
import lib.core.clickhouse.query.SortOrder;

/**
 * Fluent builder for chaining additional JOIN ON conditions via {@code .and()}.
 *
 * <pre>{@code
 * .join(wt).on(st.col("user_id"), wt.col("user_id"))
 *          .and(st.col("spin_id"), wt.col("round_id"))
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

    /** Build the SQL string. */
    public String toSql() {
        return query.toSql();
    }

    /** Continue to another INNER JOIN. */
    public JoinBuilder join(String table) {
        return query.join(table);
    }

    /** Continue to another INNER JOIN (Alias). */
    public JoinBuilder join(lib.core.clickhouse.query.Alias alias) {
        return query.join(alias);
    }

    /** Continue to another LEFT JOIN. */
    public JoinBuilder leftJoin(String table) {
        return query.leftJoin(table);
    }

    /** Continue to another LEFT JOIN (Alias). */
    public JoinBuilder leftJoin(lib.core.clickhouse.query.Alias alias) {
        return query.leftJoin(alias);
    }

    /** Continue to another RIGHT JOIN. */
    public JoinBuilder rightJoin(String table) {
        return query.rightJoin(table);
    }

    /** Continue to another RIGHT JOIN (Alias). */
    public JoinBuilder rightJoin(lib.core.clickhouse.query.Alias alias) {
        return query.rightJoin(alias);
    }

    /** Expr-accepting overloads */
    public JoinOnBuilder and(lib.core.clickhouse.expression.CH.Expr left, lib.core.clickhouse.expression.CH.Expr right) {
        return and(left.toString(), right.toString());
    }

    public WhereBuilder where(lib.core.clickhouse.expression.CH.Expr column) {
        return query.where(column.toString());
    }

    public ClickHouseQuery groupBy(Object... columns) {
        String[] strs = new String[columns.length];
        for (int i = 0; i < columns.length; i++) strs[i] = columns[i].toString();
        return query.groupBy(strs);
    }
}
