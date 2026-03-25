package lib.core.clickhouse;

/**
 * Fluent column expression builder for ClickHouse queries.
 *
 * <p>Usage with chaining:
 * <pre>{@code
 * import static lib.core.clickhouse.CH.*;
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

        Expr(String expression) {
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
     * Example: {@code in("action", "SALE", "UPSELL")} → {@code "action IN ('RESULT','JACKPOT_WIN')"}
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
     * import static lib.core.clickhouse.CH.*;
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

    // ── CaseBuilder ─────────────────────────────────────────────────────

    /**
     * Accumulates WHEN/THEN clauses and builds the final CASE expression.
     *
     * <p>Flow: {@code caseWhen("col") → condition → .then(val) → .when("col") → ... → .orElse(val)}
     */
    public static final class CaseBuilder {
        private final StringBuilder sql = new StringBuilder("CASE");

        CaseBuilder() {}

        void addWhenThen(String condition, Object thenValue) {
            sql.append(" WHEN ").append(condition).append(" THEN ").append(renderValue(thenValue));
        }

        /**
         * Add another WHEN condition.
         *
         * @param column the column for this condition
         * @return a {@link CaseConditionBuilder}
         */
        public CaseConditionBuilder when(String column) {
            return new CaseConditionBuilder(this, column);
        }

        /**
         * Set the ELSE value and complete the CASE expression.
         *
         * @param value the default value (String → quoted, Number → raw)
         * @return an {@link Expr} for {@code .as(alias)}
         */
        public Expr orElse(Object value) {
            sql.append(" ELSE ").append(renderValue(value)).append(" END");
            return new Expr(sql.toString());
        }

        /**
         * Set the ELSE value as a raw expression (column or function, not quoted).
         *
         * @param expression raw SQL expression (e.g. {@code "other_column"})
         * @return an {@link Expr}
         */
        public Expr orElseRaw(String expression) {
            sql.append(" ELSE ").append(expression).append(" END");
            return new Expr(sql.toString());
        }

        /**
         * Complete without ELSE (NULL by default).
         *
         * @return an {@link Expr}
         */
        public Expr end() {
            sql.append(" END");
            return new Expr(sql.toString());
        }
    }

    // ── CaseConditionBuilder ────────────────────────────────────────────

    /**
     * Fluent condition builder for CASE WHEN.
     * Supports: eq, ne, gt, gte, lt, lte, in, isNull, isNotNull.
     */
    public static final class CaseConditionBuilder {
        private final CaseBuilder caseBuilder;
        private final String column;

        CaseConditionBuilder(CaseBuilder caseBuilder, String column) {
            this.caseBuilder = caseBuilder;
            this.column = column;
        }

        /** {@code column = value} */
        public CaseThenBuilder eq(Object value) {
            return new CaseThenBuilder(caseBuilder, column + " = " + renderValue(value));
        }

        /** {@code column != value} */
        public CaseThenBuilder ne(Object value) {
            return new CaseThenBuilder(caseBuilder, column + " != " + renderValue(value));
        }

        /** {@code column > value} */
        public CaseThenBuilder gt(Object value) {
            return new CaseThenBuilder(caseBuilder, column + " > " + renderValue(value));
        }

        /** {@code column >= value} */
        public CaseThenBuilder gte(Object value) {
            return new CaseThenBuilder(caseBuilder, column + " >= " + renderValue(value));
        }

        /** {@code column < value} */
        public CaseThenBuilder lt(Object value) {
            return new CaseThenBuilder(caseBuilder, column + " < " + renderValue(value));
        }

        /** {@code column <= value} */
        public CaseThenBuilder lte(Object value) {
            return new CaseThenBuilder(caseBuilder, column + " <= " + renderValue(value));
        }

        /** {@code column IN ('v1','v2',...)} */
        public CaseThenBuilder in(String... values) {
            StringBuilder sb = new StringBuilder(column).append(" IN (");
            for (int i = 0; i < values.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append("'").append(values[i]).append("'");
            }
            sb.append(")");
            return new CaseThenBuilder(caseBuilder, sb.toString());
        }

        /** {@code column IS NULL} */
        public CaseThenBuilder isNull() {
            return new CaseThenBuilder(caseBuilder, column + " IS NULL");
        }

        /** {@code column IS NOT NULL} */
        public CaseThenBuilder isNotNull() {
            return new CaseThenBuilder(caseBuilder, column + " IS NOT NULL");
        }

        /**
         * {@code column BETWEEN from AND to}.
         *
         * <pre>{@code
         * caseWhen("score").between(50, 100).then("MEDIUM")
         * // → CASE WHEN score BETWEEN 50 AND 100 THEN 'MEDIUM'
         * }</pre>
         */
        public CaseThenBuilder between(Object from, Object to) {
            return new CaseThenBuilder(caseBuilder,
                    column + " BETWEEN " + renderValue(from) + " AND " + renderValue(to));
        }

        /** Raw condition: {@code WHEN raw_condition THEN ...} */
        public CaseThenBuilder raw(String condition) {
            return new CaseThenBuilder(caseBuilder, condition);
        }
    }

    // ── CaseThenBuilder ─────────────────────────────────────────────────

    /**
     * After specifying the WHEN condition, specify the THEN value.
     */
    public static final class CaseThenBuilder {
        private final CaseBuilder caseBuilder;
        private final String condition;

        CaseThenBuilder(CaseBuilder caseBuilder, String condition) {
            this.caseBuilder = caseBuilder;
            this.condition = condition;
        }

        /**
         * Set the THEN value (String → quoted, Number → raw).
         *
         * @param value the result value
         * @return the parent {@link CaseBuilder} for chaining more WHEN clauses
         */
        public CaseBuilder then(Object value) {
            caseBuilder.addWhenThen(condition, value);
            return caseBuilder;
        }

        /**
         * Set the THEN value as a raw expression (column or function, not quoted).
         *
         * @param expression raw SQL expression (e.g. {@code "amount * 2"})
         * @return the parent {@link CaseBuilder}
         */
        public CaseBuilder thenRaw(String expression) {
            caseBuilder.sql.append(" WHEN ").append(condition).append(" THEN ").append(expression);
            return caseBuilder;
        }
    }

    // ── Value rendering ─────────────────────────────────────────────────

    /**
     * Render a value for SQL: String → quoted, Number → raw.
     */
    static String renderValue(Object value) {
        if (value instanceof Number) {
            return value.toString();
        }
        return "'" + value + "'";
    }
}
