package lib.core.query.builder;

import lib.core.query.BaseQuery;
import lib.core.query.SortOrder;
import lib.core.query.Alias;

/**
 * Fluent builder for chaining additional JOIN ON conditions via {@code .and()}.
 *
 * <pre>{@code
 * .join(wt).on(st.col("user_id"), wt.col("user_id"))
 *          .and(o.col("order_id"), wt.col("transaction_id"))
 * }</pre>
 */
public final class JoinOnBuilder<T extends BaseQuery<T>> {
    private final T query;
    private final int joinIndex;

    public JoinOnBuilder(T query, int joinIndex) {
        this.query = query;
        this.joinIndex = joinIndex;
    }

    /**
     * Append an additional equality condition to the JOIN ON clause.
     * {@code AND leftCol = rightCol}
     */
    public JoinOnBuilder<T> and(String leftColumn, String rightColumn) {
        String existing = query.joinClauses.get(joinIndex);
        query.joinClauses.set(joinIndex, existing + " AND " + leftColumn + " = " + rightColumn);
        return this;
    }

    // ── Transition methods (delegate to BaseQuery) ────────────────

    /** Continue to WHERE phase. */
    public WhereBuilder<T> where(String column) {
        return query.where(column);
    }

    /** Continue to ILIKE search phase. */
    public WhereILikeBuilder<T> whereILike(String keyword) {
        return query.whereILike(keyword);
    }

    /** Continue to OR-grouped WHERE phase. */
    public T whereOr(java.util.function.Consumer<OrBuilder<T>> consumer) {
        return query.whereOr(consumer);
    }

    /** Continue to LIKE search phase. */
    public WhereILikeBuilder<T> whereLike(String keyword) {
        return query.whereLike(keyword);
    }

    /** Continue to GROUP BY phase. */
    public T groupBy(String... columns) {
        return query.groupBy(columns);
    }

    /** Continue to ORDER BY phase. */
    public T orderBy(String column, SortOrder order) {
        return query.orderBy(column, order);
    }

    /** Continue to ORDER BY phase (Expr overload). */
    public T orderBy(Object column, SortOrder order) {
        return query.orderBy(column.toString(), order);
    }

    /** Continue to LIMIT phase. */
    public T limit(int n) {
        return query.limit(n);
    }

    /** Get the underlying query (e.g. for passing as a parameter). */
    public T query() {
        return query;
    }

    /** Build the SQL string. */
    public String toSql() {
        return query.toSql();
    }

    /** Continue to another INNER JOIN. */
    public JoinBuilder<T> join(String table) {
        return query.join(table);
    }

    /** Continue to another INNER JOIN (Alias). */
    public JoinBuilder<T> join(Alias alias) {
        return query.join(alias);
    }

    /** Continue to another LEFT JOIN. */
    public JoinBuilder<T> leftJoin(String table) {
        return query.leftJoin(table);
    }

    /** Continue to another LEFT JOIN (Alias). */
    public JoinBuilder<T> leftJoin(Alias alias) {
        return query.leftJoin(alias);
    }

    /** Continue to another RIGHT JOIN. */
    public JoinBuilder<T> rightJoin(String table) {
        return query.rightJoin(table);
    }

    /** Continue to another RIGHT JOIN (Alias). */
    public JoinBuilder<T> rightJoin(Alias alias) {
        return query.rightJoin(alias);
    }

    /** Expr-accepting overloads */
    public JoinOnBuilder<T> and(lib.core.query.expression.CommonFunctions.Expr left, lib.core.query.expression.CommonFunctions.Expr right) {
        return and(left.toString(), right.toString());
    }

    public WhereBuilder<T> where(lib.core.query.expression.CommonFunctions.Expr column) {
        return query.where(column.toString());
    }

    public T groupBy(Object... columns) {
        String[] strs = new String[columns.length];
        for (int i = 0; i < columns.length; i++) strs[i] = columns[i].toString();
        return query.groupBy(strs);
    }
}
