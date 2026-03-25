package lib.core.clickhouse.query;

import lib.core.clickhouse.expression.CH;

/**
 * Type-safe table alias for avoiding hard-coded prefix strings.
 *
 * <pre>{@code
 * // Simple — table name is the prefix
 * Alias orders = Alias.of("orders");
 * Alias users  = Alias.of("users");
 *
 * ClickHouseQuery.select(users.col("name"), orders.sum("amount").as("total"))
 *     .from(orders)                                       // FROM orders
 *     .join(users).on(users.c("id"), orders.c("user_id")) // JOIN users ON ...
 *     .where(orders.c("tenant_id")).eq(tenantId)
 *
 * // Short alias — when you want o.amount instead of orders.amount
 * Alias o = Alias.of("orders", "o");
 * Alias u = Alias.of("users", "u");
 *
 * ClickHouseQuery.select(u.col("name"), o.sum("amount").as("total"))
 *     .from(o)                                    // FROM orders o
 *     .join(u).on(u.c("id"), o.c("user_id"))      // JOIN users u ON ...
 * }</pre>
 */
public final class Alias {

    private final String table;
    private final String alias;

    private Alias(String table, String alias) {
        this.table = table;
        this.alias = alias;
    }

    /**
     * Create an Alias with a custom short alias.
     * {@code Alias.of("orders", "o")} → {@code o.c("amount")} = {@code "o.amount"}
     *
     * @param table the table name (e.g. {@code "orders"})
     * @param alias the short alias (e.g. {@code "o"})
     * @return a reusable Alias instance
     */
    public static Alias of(String table, String alias) {
        return new Alias(table, alias);
    }

    /**
     * Create an Alias using the table name as the prefix.
     * {@code Alias.of("orders")} → {@code orders.c("amount")} = {@code "orders.amount"}
     *
     * @param table the table name (e.g. {@code "orders"})
     * @return a reusable Alias instance
     */
    public static Alias of(String table) {
        return new Alias(table, table);
    }

    /**
     * Returns the table reference for FROM/JOIN clauses.
     * <ul>
     *   <li>{@code Alias.of("orders")} → {@code "orders"}</li>
     *   <li>{@code Alias.of("orders", "o")} → {@code "orders o"}</li>
     * </ul>
     */
    public String ref() {
        return table.equals(alias) ? table : table + " " + alias;
    }

    /**
     * Prefix a column name with this alias.
     * {@code o.c("amount")} → {@code "o.amount"}
     *
     * @param column the column name
     * @return the fully-qualified column string
     */
    public String c(String column) {
        return alias + "." + column;
    }

    // ── CH expression shortcuts ─────────────────────────────────────────

    /** {@code col(alias.column)} → returns a plain string like "o.amount" */
    public String col(String column) {
        return CH.col(c(column));
    }

    /** {@code sum(alias.column)} */
    public CH.Expr sum(String column) {
        return CH.sum(c(column));
    }

    /** {@code count(alias.column)} */
    public CH.Expr count(String column) {
        return CH.count(c(column));
    }

    /** {@code countDistinct(alias.column)} */
    public CH.Expr countDistinct(String column) {
        return CH.countDistinct(c(column));
    }

    /** {@code min(alias.column)} */
    public CH.Expr min(String column) {
        return CH.min(c(column));
    }

    /** {@code max(alias.column)} */
    public CH.Expr max(String column) {
        return CH.max(c(column));
    }

    /** {@code avg(alias.column)} */
    public CH.Expr avg(String column) {
        return CH.avg(c(column));
    }

    /** {@code sumIf(alias.column, condition)} */
    public CH.Expr sumIf(String column, String condition) {
        return CH.sumIf(c(column), condition);
    }

    /** {@code countIf(alias.column, condition)} */
    public CH.Expr countIf(String column, String condition) {
        return CH.countIf(c(column), condition);
    }

    /** {@code minIf(alias.column, condition)} */
    public CH.Expr minIf(String column, String condition) {
        return CH.minIf(c(column), condition);
    }

    /** {@code maxIf(alias.column, condition)} */
    public CH.Expr maxIf(String column, String condition) {
        return CH.maxIf(c(column), condition);
    }

    /** {@code avgIf(alias.column, condition)} */
    public CH.Expr avgIf(String column, String condition) {
        return CH.avgIf(c(column), condition);
    }

    /** Returns the alias prefix (e.g. {@code "o"}) */
    @Override
    public String toString() {
        return alias;
    }
}
