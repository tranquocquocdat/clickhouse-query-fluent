package lib.core.clickhouse.expression;

import lib.core.query.expression.CaseBuilder;
import lib.core.query.expression.CaseConditionBuilder;
import lib.core.query.expression.CommonFunctions;

/**
 * Fluent column expression builder for ClickHouse queries.
 * Extends {@link CommonFunctions} with ClickHouse-specific functions.
 *
 * <p>Usage with chaining:
 * <pre>{@code
 * import static lib.core.clickhouse.expression.CH.*;
 *
 * ClickHouseQuery.select(
 *     col("product_id"),
 *     countDistinct("order_id").as("total_orders"),
 *     countDistinct("user_id", "session_id").as("unique_sessions"),   // multi-column
 *     sum("amount").as("total_revenue"),
 *     sum("revenue").minus(sum("cost")).as("net_result"),              // arithmetic
 *     sumIf("amount", "type = 'SALE'").as("total_revenue"),
 *     sumIf("amount", in("action", "SALE", "UPSELL")).as("total_sales"),
 *     min("created_at").as("first_ts"),
 *     max("created_at").as("last_ts"),
 *     count().as("total_count")
 * )
 * }</pre>
 */
public final class CH extends CommonFunctions {

    private CH() {}

    // ── ClickHouse-specific aggregates ──────────────────────────────────

    /**
     * {@code any(column)} — picks an arbitrary value from the group.
     */
    public static Expr any(String column) {
        return new Expr("any(" + column + ")");
    }

    // ── Multi-column countDistinct ──────────────────────────────────────

    /** {@code count(DISTINCT (col1, col2, ...))} — multi-column distinct count */
    public static Expr countDistinct(String col1, String col2, String... more) {
        StringBuilder sb = new StringBuilder("count(DISTINCT (")
                .append(col1).append(", ").append(col2);
        for (String c : more) {
            sb.append(", ").append(c);
        }
        sb.append("))");
        return new Expr(sb.toString());
    }

    /** {@code count(DISTINCT (expr1, expr2, ...))} — multi-column Expr overload */
    public static Expr countDistinct(Expr col1, Expr col2, Expr... more) {
        StringBuilder sb = new StringBuilder("count(DISTINCT (")
                .append(col1).append(", ").append(col2);
        for (Expr c : more) {
            sb.append(", ").append(c);
        }
        sb.append("))");
        return new Expr(sb.toString());
    }

    // ── Conditional aggregates (fluent) ────────────────────────────────

    /** Fluent sumIf: {@code sumIf("amount").where("status").eq("COMPLETED")} */
    public static AggIfBuilder sumIf(String column) {
        return new AggIfBuilder("sumIf", column);
    }

    /** Fluent countIf: {@code countIf("user_id").where("type").eq("PREMIUM")} */
    public static AggIfBuilder countIf(String column) {
        return new AggIfBuilder("countIf", column);
    }

    /** Fluent minIf: {@code minIf("amount").where("type").eq("SALE")} */
    public static AggIfBuilder minIf(String column) {
        return new AggIfBuilder("minIf", column);
    }

    /** Fluent maxIf: {@code maxIf("amount").where("type").eq("SALE")} */
    public static AggIfBuilder maxIf(String column) {
        return new AggIfBuilder("maxIf", column);
    }

    /** Fluent avgIf: {@code avgIf("score").where("status").eq("DONE")} */
    public static AggIfBuilder avgIf(String column) {
        return new AggIfBuilder("avgIf", column);
    }

    // ── Conditional aggregates (raw condition) ───────────────────────

    /** {@code sumIf(column, rawCondition)} */
    public static Expr sumIfRaw(String column, String condition) {
        return new Expr("sumIf(" + column + ", " + condition + ")");
    }

    /** {@code countIf(column, rawCondition)} */
    public static Expr countIfRaw(String column, String condition) {
        return new Expr("countIf(" + column + ", " + condition + ")");
    }

    /** {@code minIf(column, rawCondition)} */
    public static Expr minIfRaw(String column, String condition) {
        return new Expr("minIf(" + column + ", " + condition + ")");
    }

    /** {@code maxIf(column, rawCondition)} */
    public static Expr maxIfRaw(String column, String condition) {
        return new Expr("maxIf(" + column + ", " + condition + ")");
    }

    /** {@code avgIf(column, rawCondition)} */
    public static Expr avgIfRaw(String column, String condition) {
        return new Expr("avgIf(" + column + ", " + condition + ")");
    }

    // ── IN expression (for use in sumIf conditions) ─────────────────────

    /**
     * Generates an IN expression for use in conditions.
     * Example: {@code in("action", "SALE", "UPSELL")} → {@code "action IN ('SALE','UPSELL')"}
     */
    public static String in(String column, String... values) {
        if (values.length == 0) return "1=0";  // Empty IN → always false
        StringBuilder sb = new StringBuilder(column).append(" IN (");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) sb.append(",");
            sb.append("'").append(values[i].replace("'", "''")).append("'");
        }
        sb.append(")");
        return sb.toString();
    }

    // ── CASE WHEN (override to use ClickHouse CaseBuilder) ──────────────

    /**
     * Start a fluent CASE WHEN expression.
     *
     * <pre>{@code
     * import static lib.core.clickhouse.expression.CH.*;
     *
     * ClickHouseQuery.select(
     *     col("user_id"),
     *     caseWhen("amount").gt(0).then("PROFIT")
     *         .when("amount").eq(0).then("NEUTRAL")
     *         .orElse("LOSS")
     *         .as("result"),
     *     caseWhen("action").eq("SALE").then(1)
     *         .orElse(0)
     *         .as("is_sale")
     * )
     * }</pre>
     *
     * @param column the column for the first WHEN condition
     * @return a {@link CaseConditionBuilder} for specifying the comparison
     */
    public static CaseConditionBuilder caseWhen(String column) {
        CaseBuilder builder = new CaseBuilder();
        return new CaseConditionBuilder(builder, column);
    }
}
