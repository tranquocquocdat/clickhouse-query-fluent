package lib.core.query.expression;

/**
 * Accumulates WHEN/THEN clauses and builds the final CASE expression.
 *
 * <p>Flow: {@code caseWhen("col") → condition → .then(val) → .when("col") → ... → .orElse(val)}
 *
 * @see CommonFunctions#caseWhen(String)
 * @see CaseConditionBuilder
 * @see CaseThenBuilder
 */
public final class CaseBuilder {
    final StringBuilder sql = new StringBuilder("CASE");

    CaseBuilder() {}

    void addWhenThen(String condition, Object thenValue) {
        sql.append(" WHEN ").append(condition).append(" THEN ").append(CommonFunctions.renderValue(thenValue));
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
     * @return an {@link CommonFunctions.Expr} for {@code .as(alias)}
     */
    public CommonFunctions.Expr orElse(Object value) {
        sql.append(" ELSE ").append(CommonFunctions.renderValue(value)).append(" END");
        return new CommonFunctions.Expr(sql.toString());
    }

    /**
     * Set the ELSE value as a raw expression (column or function, not quoted).
     *
     * @param expression raw SQL expression (e.g. {@code "other_column"})
     * @return an {@link CommonFunctions.Expr}
     */
    public CommonFunctions.Expr orElseRaw(String expression) {
        sql.append(" ELSE ").append(expression).append(" END");
        return new CommonFunctions.Expr(sql.toString());
    }

    /**
     * Complete without ELSE (NULL by default).
     *
     * @return an {@link CommonFunctions.Expr}
     */
    public CommonFunctions.Expr end() {
        sql.append(" END");
        return new CommonFunctions.Expr(sql.toString());
    }
}
