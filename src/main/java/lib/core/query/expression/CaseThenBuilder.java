package lib.core.query.expression;

/**
 * After specifying the WHEN condition, specify the THEN value.
 *
 * @see CaseConditionBuilder
 * @see CaseBuilder
 */
public final class CaseThenBuilder {
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
