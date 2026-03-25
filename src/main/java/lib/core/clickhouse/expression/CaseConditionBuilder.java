package lib.core.clickhouse.expression;

/**
 * Fluent condition builder for CASE WHEN.
 * Supports: eq, ne, gt, gte, lt, lte, in, isNull, isNotNull, between, raw.
 *
 * @see CH#caseWhen(String)
 * @see CaseBuilder
 * @see CaseThenBuilder
 */
public final class CaseConditionBuilder {
    private final CaseBuilder caseBuilder;
    private final String column;

    CaseConditionBuilder(CaseBuilder caseBuilder, String column) {
        this.caseBuilder = caseBuilder;
        this.column = column;
    }

    /** {@code column = value} */
    public CaseThenBuilder eq(Object value) {
        return new CaseThenBuilder(caseBuilder, column + " = " + CH.renderValue(value));
    }

    /** {@code column != value} */
    public CaseThenBuilder ne(Object value) {
        return new CaseThenBuilder(caseBuilder, column + " != " + CH.renderValue(value));
    }

    /** {@code column > value} */
    public CaseThenBuilder gt(Object value) {
        return new CaseThenBuilder(caseBuilder, column + " > " + CH.renderValue(value));
    }

    /** {@code column >= value} */
    public CaseThenBuilder gte(Object value) {
        return new CaseThenBuilder(caseBuilder, column + " >= " + CH.renderValue(value));
    }

    /** {@code column < value} */
    public CaseThenBuilder lt(Object value) {
        return new CaseThenBuilder(caseBuilder, column + " < " + CH.renderValue(value));
    }

    /** {@code column <= value} */
    public CaseThenBuilder lte(Object value) {
        return new CaseThenBuilder(caseBuilder, column + " <= " + CH.renderValue(value));
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
                column + " BETWEEN " + CH.renderValue(from) + " AND " + CH.renderValue(to));
    }

    /** Raw condition: {@code WHEN raw_condition THEN ...} */
    public CaseThenBuilder raw(String condition) {
        return new CaseThenBuilder(caseBuilder, condition);
    }
}
