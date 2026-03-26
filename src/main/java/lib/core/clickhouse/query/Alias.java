package lib.core.clickhouse.query;

import lib.core.clickhouse.expression.AggIfBuilder;
import lib.core.clickhouse.expression.CaseConditionBuilder;
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
 *     .from(orders)                                             // FROM orders
 *     .join(users).on(users.col("id"), orders.col("user_id"))   // JOIN users ON ...
 *     .where(orders.col("tenant_id")).eq(tenantId)
 *
 * // Short alias — when you want o.amount instead of orders.amount
 * Alias o = Alias.of("orders").as("o");
 * Alias u = Alias.of("users").as("u");
 *
 * ClickHouseQuery.select(u.col("name"), o.sum("amount").as("total"))
 *     .from(o)                                          // FROM orders o
 *     .join(u).on(u.col("id"), o.col("user_id"))        // JOIN users u ON ...
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
     * Create a short alias from this Alias (fluent chaining).
     * {@code Alias.of("orders").as("o")} → {@code o.c("amount")} = {@code "o.amount"}
     */
    public Alias as(String shortAlias) {
        return new Alias(this.table, shortAlias);
    }

    /**
     * Returns the table reference for FROM/JOIN clauses.
     * <ul>
     *   <li>{@code Alias.of("orders")} → {@code "orders"}</li>
     *   <li>{@code Alias.of("orders").as("o")} → {@code "orders o"}</li>
     * </ul>
     */
    public String ref() {
        return table.equals(alias) ? table : table + " " + alias;
    }

    /**
     * Prefix a column name with this alias (internal use).
     * {@code qualified("amount")} → {@code "o.amount"}
     *
     * @param column the column name
     * @return the fully-qualified column string
     */
    private String c(String column) {
        return alias + "." + column;
    }

    // ── CH expression shortcuts ─────────────────────────────────────────

    /** {@code col(alias.column)} → returns an Expr for fluent chaining like {@code st.col("amount").as("bet")} */
    public CH.Expr col(String column) {
        return new CH.Expr(c(column));
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

    /** {@code any(alias.column)} — picks an arbitrary value from the group. */
    public CH.Expr any(String column) {
        return CH.any(c(column));
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

    /** Fluent: {@code sumIf(alias.column).where("status").eq("COMPLETED")} */
    public AggIfBuilder sumIf(String column) {
        return CH.sumIf(c(column));
    }

    /** Fluent: {@code countIf(alias.column).where("type").eq("VIP")} */
    public AggIfBuilder countIf(String column) {
        return CH.countIf(c(column));
    }

    /** Fluent: {@code minIf(alias.column).where("type").eq("SALE")} */
    public AggIfBuilder minIf(String column) {
        return CH.minIf(c(column));
    }

    /** Fluent: {@code maxIf(alias.column).where("type").eq("SALE")} */
    public AggIfBuilder maxIf(String column) {
        return CH.maxIf(c(column));
    }

    /** Fluent: {@code avgIf(alias.column).where("status").eq("DONE")} */
    public AggIfBuilder avgIf(String column) {
        return CH.avgIf(c(column));
    }

    /** {@code sumIf(alias.column, rawCondition)} */
    public CH.Expr sumIfRaw(String column, String condition) {
        return CH.sumIfRaw(c(column), condition);
    }

    /** {@code countIf(alias.column, rawCondition)} */
    public CH.Expr countIfRaw(String column, String condition) {
        return CH.countIfRaw(c(column), condition);
    }

    /** {@code minIf(alias.column, rawCondition)} */
    public CH.Expr minIfRaw(String column, String condition) {
        return CH.minIfRaw(c(column), condition);
    }

    /** {@code maxIf(alias.column, rawCondition)} */
    public CH.Expr maxIfRaw(String column, String condition) {
        return CH.maxIfRaw(c(column), condition);
    }

    /** {@code avgIf(alias.column, rawCondition)} */
    public CH.Expr avgIfRaw(String column, String condition) {
        return CH.avgIfRaw(c(column), condition);
    }

    // ── CASE WHEN ────────────────────────────────────────────────────────

    /**
     * Start a CASE WHEN with alias-prefixed column.
     * <pre>{@code orders.caseWhen("amount").gt(5000).then("HIGH").orElse("LOW").as("tier")}</pre>
     */
    public CaseConditionBuilder caseWhen(String column) {
        return CH.caseWhen(c(column));
    }

    /** Returns the alias prefix (e.g. {@code "o"}) */
    @Override
    public String toString() {
        return alias;
    }
}
