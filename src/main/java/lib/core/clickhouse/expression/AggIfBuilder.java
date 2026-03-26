package lib.core.clickhouse.expression;

/**
 * Fluent builder for conditional aggregate functions (sumIf, countIf, minIf, maxIf, avgIf).
 *
 * <pre>{@code
 * import static lib.core.clickhouse.expression.CH.*;
 *
 * sumIf("amount").where("status").eq("COMPLETED").as("completed_revenue")
 * countIf("user_id").where("type").in("VIP", "PREMIUM").as("vip_count")
 * avgIf("score").where("status").eq("DONE").as("avg_done_score")
 * }</pre>
 */
public final class AggIfBuilder {
    private final String func;    // e.g. "sumIf"
    private final String column;  // e.g. "amount" or "o.amount"

    AggIfBuilder(String func, String column) {
        this.func = func;
        this.column = column;
    }

    /**
     * Start the condition for this aggregate.
     *
     * @param condColumn the column to compare
     * @return a {@link ConditionBuilder} for specifying the operator
     */
    public ConditionBuilder where(String condColumn) {
        return new ConditionBuilder(this, condColumn);
    }

    private CH.Expr build(String condition) {
        return new CH.Expr(func + "(" + column + ", " + condition + ")");
    }

    /**
     * Intermediate builder for specifying the condition operator.
     */
    public static final class ConditionBuilder {
        private final AggIfBuilder parent;
        private final String condColumn;

        ConditionBuilder(AggIfBuilder parent, String condColumn) {
            this.parent = parent;
            this.condColumn = condColumn;
        }

        /** {@code column = 'value'} */
        public CH.Expr eq(Object value) {
            return parent.build(condColumn + " = " + CH.renderValue(value));
        }

        /** {@code column != 'value'} */
        public CH.Expr ne(Object value) {
            return parent.build(condColumn + " != " + CH.renderValue(value));
        }

        /** {@code column > value} */
        public CH.Expr gt(Object value) {
            return parent.build(condColumn + " > " + CH.renderValue(value));
        }

        /** {@code column >= value} */
        public CH.Expr gte(Object value) {
            return parent.build(condColumn + " >= " + CH.renderValue(value));
        }

        /** {@code column < value} */
        public CH.Expr lt(Object value) {
            return parent.build(condColumn + " < " + CH.renderValue(value));
        }

        /** {@code column <= value} */
        public CH.Expr lte(Object value) {
            return parent.build(condColumn + " <= " + CH.renderValue(value));
        }

        /** {@code column IN ('v1','v2',...)} */
        public CH.Expr in(String... values) {
            return parent.build(CH.in(condColumn, values));
        }

        /** {@code column IS NULL} */
        public CH.Expr isNull() {
            return parent.build(condColumn + " IS NULL");
        }

        /** {@code column IS NOT NULL} */
        public CH.Expr isNotNull() {
            return parent.build(condColumn + " IS NOT NULL");
        }
    }
}
