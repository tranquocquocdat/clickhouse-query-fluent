package lib.core.query.builder;

import lib.core.query.BaseQuery;
import lib.core.query.exception.InvalidRangeException;

/**
 * Fluent builder for HAVING conditions.
 * Created via {@link BaseQuery#having(lib.core.query.expression.CommonFunctions.Expr)}.
 */
public final class HavingBuilder<T extends BaseQuery<T>> {
    private final T query;
    private final String expression;
    private int paramSeq = 0;

    public HavingBuilder(T query, String expression) {
        this.query = query;
        this.expression = expression;
    }

    private String nextParam() {
        return "_having" + (paramSeq++);
    }

    /** {@code expression > value} — skipped when value is null or empty string. */
    public T gt(Object value) {
        if (value == null) return query;
        if (value instanceof String && ((String) value).isEmpty()) return query;
        String p = nextParam();
        query.havingClauses.add(expression + " > :" + p);
        query.params.addValue(p, value);
        return query;
    }

    /** {@code expression >= value} — skipped when value is null or empty string. */
    public T gte(Object value) {
        if (value == null) return query;
        if (value instanceof String && ((String) value).isEmpty()) return query;
        String p = nextParam();
        query.havingClauses.add(expression + " >= :" + p);
        query.params.addValue(p, value);
        return query;
    }

    /** {@code expression < value} — skipped when value is null or empty string. */
    public T lt(Object value) {
        if (value == null) return query;
        if (value instanceof String && ((String) value).isEmpty()) return query;
        String p = nextParam();
        query.havingClauses.add(expression + " < :" + p);
        query.params.addValue(p, value);
        return query;
    }

    /** {@code expression <= value} — skipped when value is null or empty string. */
    public T lte(Object value) {
        if (value == null) return query;
        if (value instanceof String && ((String) value).isEmpty()) return query;
        String p = nextParam();
        query.havingClauses.add(expression + " <= :" + p);
        query.params.addValue(p, value);
        return query;
    }

    /** {@code expression = value} — skipped when value is null or empty string. */
    public T eq(Object value) {
        if (value == null) return query;
        if (value instanceof String && ((String) value).isEmpty()) return query;
        String p = nextParam();
        query.havingClauses.add(expression + " = :" + p);
        query.params.addValue(p, value);
        return query;
    }

    /** {@code expression != value} — skipped when value is null or empty string. */
    public T ne(Object value) {
        if (value == null) return query;
        if (value instanceof String && ((String) value).isEmpty()) return query;
        String p = nextParam();
        query.havingClauses.add(expression + " != :" + p);
        query.params.addValue(p, value);
        return query;
    }

    /** 
     * {@code expression >= from AND expression <= to}.
     * Only non-null and non-empty bounds are applied.
     * 
     * @throws InvalidRangeException if from > to (for Comparable types)
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public T between(Object from, Object to) {
        // Validate range if both bounds are present and comparable
        if (from != null && to != null 
            && !(from instanceof String && ((String) from).isEmpty())
            && !(to instanceof String && ((String) to).isEmpty())) {
            
            // Check if both are Comparable and of the same type
            if (from instanceof Comparable && to instanceof Comparable 
                && from.getClass().equals(to.getClass())) {
                try {
                    Comparable comparableFrom = (Comparable) from;
                    if (comparableFrom.compareTo(to) > 0) {
                        throw new InvalidRangeException(expression, from, to);
                    }
                } catch (ClassCastException e) {
                    // If comparison fails, skip validation (incompatible types)
                }
            }
        }
        
        if (from != null && !(from instanceof String && ((String) from).isEmpty())) {
            String p1 = nextParam();
            query.havingClauses.add(expression + " >= :" + p1);
            query.params.addValue(p1, from);
        }
        if (to != null && !(to instanceof String && ((String) to).isEmpty())) {
            String p2 = nextParam();
            query.havingClauses.add(expression + " <= :" + p2);
            query.params.addValue(p2, to);
        }
        return query;
    }
}
