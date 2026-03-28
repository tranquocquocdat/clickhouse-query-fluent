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
        public Expr as(String alias) {
            return new Expr(expression + " AS " + alias);
        }

        /** Subtract: {@code expr1 - expr2} */
        public Expr minus(Expr other) {
            return new Expr(expression + " - " + other.expression);
        }

        /** Subtract raw: {@code expr1 - rawExpr} */
        public Expr minus(String raw) {
            return new Expr(expression + " - " + raw);
        }

        /** Add: {@code expr1 + expr2} */
        public Expr plus(Expr other) {
            return new Expr(expression + " + " + other.expression);
        }

        /** Add raw: {@code expr1 + rawExpr} */
        public Expr plus(String raw) {
            return new Expr(expression + " + " + raw);
        }

        /** Multiply: {@code expr1 * expr2} */
        public Expr multiply(Expr other) {
            return new Expr("(" + expression + ") * (" + other.expression + ")");
        }

        /** Multiply raw: {@code expr1 * rawExpr} */
        public Expr multiply(String raw) {
            return new Expr("(" + expression + ") * (" + raw + ")");
        }

        /** Divide: {@code expr1 / expr2} */
        public Expr divide(Expr other) {
            return new Expr("(" + expression + ") / (" + other.expression + ")");
        }

        /** Divide raw: {@code expr1 / rawExpr} */
        public Expr divide(String raw) {
            return new Expr("(" + expression + ") / (" + raw + ")");
        }

        /**
         * Start a window expression: {@code expr OVER(...)}.
         *
         * <pre>{@code
         * CH.sum("amount").over().partitionBy("user_id").orderBy("created_at").as("running_total")
         * CH.rowNumber().over().orderBy("amount", SortOrder.DESC).as("rank")
         * }</pre>
         *
         * @return a {@link WindowBuilder} for specifying PARTITION BY / ORDER BY
         */
        public WindowBuilder over() {
            return new WindowBuilder(expression);
        }

        /** Return raw expression without alias. */
        @Override
        public String toString() {
            return expression;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o instanceof Expr) return expression.equals(((Expr) o).expression);
            if (o instanceof String) return expression.equals(o);
            return false;
        }

        @Override
        public int hashCode() {
            return expression.hashCode();
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

    /** {@code countDistinct(expr)} — single Expr overload */
    public static Expr countDistinct(Expr column) {
        return new Expr("countDistinct(" + column + ")");
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

    /** {@code any(column)} — picks an arbitrary value from the group. */
    public static Expr any(String column) {
        return new Expr("any(" + column + ")");
    }

    /** {@code sum(column)} */
    public static Expr sum(String column) {
        return new Expr("sum(" + column + ")");
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

    // ── Window functions ────────────────────────────────────────────────

    /** {@code row_number()} — assign sequential number to each row within partition. */
    public static Expr rowNumber() {
        return new Expr("row_number()");
    }

    /** {@code rank()} — assign rank with gaps for ties. */
    public static Expr rank() {
        return new Expr("rank()");
    }

    /** {@code dense_rank()} — assign rank without gaps for ties. */
    public static Expr denseRank() {
        return new Expr("dense_rank()");
    }

    /** {@code lag(column, 1)} — access the previous row value. */
    public static Expr lag(String column) {
        return new Expr("lag(" + column + ", 1)");
    }

    /** {@code lag(column, offset)} — access value N rows before. */
    public static Expr lag(String column, int offset) {
        return new Expr("lag(" + column + ", " + offset + ")");
    }

    /** {@code lead(column, 1)} — access the next row value. */
    public static Expr lead(String column) {
        return new Expr("lead(" + column + ", 1)");
    }

    /** {@code lead(column, offset)} — access value N rows after. */
    public static Expr lead(String column, int offset) {
        return new Expr("lead(" + column + ", " + offset + ")");
    }

    /** {@code first_value(column)} — first value in the window frame. */
    public static Expr firstValue(String column) {
        return new Expr("first_value(" + column + ")");
    }

    /** {@code last_value(column)} — last value in the window frame. */
    public static Expr lastValue(String column) {
        return new Expr("last_value(" + column + ")");
    }

    /** {@code ntile(n)} — divide partition into N buckets. */
    public static Expr ntile(int n) {
        return new Expr("ntile(" + n + ")");
    }

    // ── Column expressions ──────────────────────────────────────────────

    /**
     * Wrap a column name as a type-safe {@link Expr}.
     * Use this for column references in SELECT, WHERE, GROUP BY, ORDER BY.
     *
     * <pre>{@code
     * CH.col("user_id")                // → Expr("user_id")
     * CH.col("amount").as("total")     // → Expr("amount AS total")
     * }</pre>
     *
     * @param column the column name
     * @return an Expr wrapping the column name
     */
    public static Expr col(String column) {
        return new Expr(column);
    }

    /**
     * Wrap a column name with an alias.
     * {@code CH.col("user_id", "uid")} → {@code user_id AS uid}
     */
    public static Expr col(String column, String alias) {
        return new Expr(column + " AS " + alias);
    }

    /**
     * Wrap a raw SQL expression as a type-safe {@link Expr}.
     * Use for expressions that don't map to a simple column or aggregate.
     *
     * <pre>{@code
     * CH.raw("toYYYYMM(created_at)")
     * CH.raw("1")
     * CH.raw("*")
     * }</pre>
     *
     * @param expression raw SQL expression
     * @return an Expr wrapping the expression
     */
    public static Expr raw(String expression) {
        return new Expr(expression);
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

    // ── CASE WHEN ───────────────────────────────────────────────────────

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

    // ── Value rendering ─────────────────────────────────────────────────

    /**
     * Render a value for SQL: String → quoted (with escaped quotes), Number → raw.
     */
    public static String renderValue(Object value) {
        if (value instanceof Number) {
            return value.toString();
        }
        return "'" + value.toString().replace("'", "''") + "'";
    }
}
