package lib.core.clickhouse.expression;

/**
 * Fluent column expression builder for ClickHouse queries.
 *
 * <p>Usage with chaining:
 * <pre>{@code
 * import static lib.core.clickhouse.expression.CH.*;
 *
 * ClickHouseQuery.select(
 *     col("product_id"),
 *     countDistinct("order_id").as("total_orders"),
 *     sum("amount").as("total_revenue"),
 *     sumIf("amount", "type = 'SALE'").as("total_revenue"),
 *     sumIf("amount", in("action", "SALE", "UPSELL")).as("total_sales"),
 *     min("created_at").as("first_ts"),
 *     max("created_at").as("last_ts"),
 *     count().as("total_count")
 * )
 * }</pre>
 */
public final class CH {

    private CH() {}

    // ── Expression wrapper ──────────────────────────────────────────────

    /** Intermediate expression that supports {@code .as(alias)} chaining. */
    public static final class Expr {
        private final String expression;

        public Expr(String expression) {
            this.expression = expression;
        }

        /** Add an alias: {@code expression AS alias} */
        public String as(String alias) {
            return expression + " AS " + alias;
        }

        /** Return raw expression without alias. */
        @Override
        public String toString() {
            return expression;
        }
    }

    // ── Aggregation ─────────────────────────────────────────────────────

    /** {@code count(*)} */
    public static Expr count() {
        return new Expr("count(*)");
    }

    /** {@code count(column)} */
    public static Expr count(String column) {
        return new Expr("count(" + column + ")");
    }

    /** {@code countDistinct(column)} */
    public static Expr countDistinct(String column) {
        return new Expr("countDistinct(" + column + ")");
    }

    /** {@code sum(column)} */
    public static Expr sum(String column) {
        return new Expr("sum(" + column + ")");
    }

    /** {@code sumIf(column, condition)} */
    public static Expr sumIf(String column, String condition) {
        return new Expr("sumIf(" + column + ", " + condition + ")");
    }

    /** {@code countIf(column, condition)} */
    public static Expr countIf(String column, String condition) {
        return new Expr("countIf(" + column + ", " + condition + ")");
    }

    /** {@code minIf(column, condition)} */
    public static Expr minIf(String column, String condition) {
        return new Expr("minIf(" + column + ", " + condition + ")");
    }

    /** {@code maxIf(column, condition)} */
    public static Expr maxIf(String column, String condition) {
        return new Expr("maxIf(" + column + ", " + condition + ")");
    }

    /** {@code avgIf(column, condition)} */
    public static Expr avgIf(String column, String condition) {
        return new Expr("avgIf(" + column + ", " + condition + ")");
    }

    /** {@code min(column)} */
    public static Expr min(String column) {
        return new Expr("min(" + column + ")");
    }

    /** {@code max(column)} */
    public static Expr max(String column) {
        return new Expr("max(" + column + ")");
    }

    /** {@code avg(column)} */
    public static Expr avg(String column) {
        return new Expr("avg(" + column + ")");
    }

    // ── Column expressions ──────────────────────────────────────────────

    /** Plain column name (for readability in select). */
    public static String col(String column) {
        return column;
    }

    /** {@code column AS alias} */
    public static String col(String column, String alias) {
        return column + " AS " + alias;
    }

    // ── IN expression (for use in sumIf conditions) ─────────────────────

    /**
     * Generates an IN expression for use in conditions.
     * Example: {@code in("action", "SALE", "UPSELL")} → {@code "action IN ('SALE','UPSELL')"}
     */
    public static String in(String column, String... values) {
        StringBuilder sb = new StringBuilder(column).append(" IN (");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) sb.append(",");
            sb.append("'").append(values[i]).append("'");
        }
        sb.append(")");
        return sb.toString();
    }

    // ── CASE WHEN ───────────────────────────────────────────────────────

    /**
     * Start a fluent CASE WHEN expression.
     *
     * <pre>{@code
     * import static lib.core.clickhouse.expression.CH.*;
     *
     * ClickHouseQuery.select(
     *     col("user_id"),
     *     caseWhen("amount").gt(0).then("WIN")
     *         .when("amount").eq(0).then("DRAW")
     *         .orElse("LOSE")
     *         .as("result"),
     *     caseWhen("action").eq("BET").then(1)
     *         .orElse(0)
     *         .as("is_bet")
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

    // ── Value rendering ─────────────────────────────────────────────────

    /**
     * Render a value for SQL: String → quoted, Number → raw.
     */
    public static String renderValue(Object value) {
        if (value instanceof Number) {
            return value.toString();
        }
        return "'" + value + "'";
    }
}
