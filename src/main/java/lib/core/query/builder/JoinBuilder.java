package lib.core.query.builder;

import lib.core.query.BaseQuery;

/**
 * Fluent builder for JOIN conditions.
 * Created via {@link BaseQuery#join(String)}, {@link BaseQuery#leftJoin(String)},
 * or {@link BaseQuery#rightJoin(String)}.
 *
 * <h3>Usage</h3>
 * <pre>{@code
 * // Single equality condition
 * .join("user_profile u").on("u.id", "t.user_id")
 *
 * // Chained equality conditions (via .and())
 * .join("wallet_transaction wt")
 *     .on("st.user_id", "wt.user_id")
 *     .and("o.order_id", "wt.transaction_id")
 *
 * // Raw condition (complex cases)
 * .join("user_profile u").on("u.id = t.user_id AND u.active = 1")
 * }</pre>
 */
public final class JoinBuilder<T extends BaseQuery<T>> {
    private final T query;
    private final String joinType;
    private final String table;

    public JoinBuilder(T query, String joinType, String table) {
        this.query = query;
        this.joinType = joinType;
        this.table = table;
    }

    /**
     * Fluent ON with equality: {@code ON leftCol = rightCol}.
     * Returns a {@link JoinOnBuilder} to allow chaining additional conditions via {@code .and()}.
     *
     * @param leftColumn  the left column (e.g. {@code "u.id"})
     * @param rightColumn the right column (e.g. {@code "t.user_id"})
     * @return a {@link JoinOnBuilder} for chaining {@code .and()} or transitioning to the next phase
     */
    public JoinOnBuilder<T> on(String leftColumn, String rightColumn) {
        String joinClause = joinType + " " + table + " ON " + leftColumn + " = " + rightColumn;
        int index = query.joinClauses.size();
        query.joinClauses.add(joinClause);
        return new JoinOnBuilder<>(query, index);
    }

    /**
     * Raw ON condition for complex cases.
     *
     * @param condition the raw join condition
     * @return the parent query builder
     */
    public T on(String condition) {
        query.joinClauses.add(joinType + " " + table + " ON " + condition);
        return query;
    }

    /**
     * Expr-accepting overload for type-safe column references.
     */
    public JoinOnBuilder<T> on(lib.core.query.expression.CommonFunctions.Expr leftColumn, lib.core.query.expression.CommonFunctions.Expr rightColumn) {
        return on(leftColumn.toString(), rightColumn.toString());
    }
}

