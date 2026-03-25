package lib.core.clickhouse.query;

import lib.core.clickhouse.expression.CH;

/**
 * Type-safe table alias for avoiding hard-coded prefix strings.
 *
 * <pre>{@code
 * Alias o = Alias.of("o");
 * Alias u = Alias.of("u");
 *
 * ClickHouseQuery.select(
 *         col(u.c("name")),
 *         sum(o.c("amount")).as("total_revenue")
 *     )
 *     .from("orders o")
 *     .join("users u").on(u.c("id"), o.c("user_id"))
 *     .where(o.c("tenant_id")).eq(tenantId)
 *     .where(o.c("created_at")).between(from, to)
 *     .orderBy(o.c("amount"), SortOrder.DESC)
 *     .query(namedJdbc, rowMapper);
 * }</pre>
 */
public final class Alias {

    private final String prefix;

    private Alias(String prefix) {
        this.prefix = prefix;
    }

    /**
     * Create a new Alias.
     *
     * @param name the alias name (e.g. {@code "o"}, {@code "u"})
     * @return a reusable Alias instance
     */
    public static Alias of(String name) {
        return new Alias(name);
    }

    /**
     * Prefix a column name with this alias.
     * {@code o.c("amount")} → {@code "o.amount"}
     *
     * @param column the column name
     * @return the fully-qualified column string
     */
    public String c(String column) {
        return prefix + "." + column;
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
        return prefix;
    }
}
