package lib.core.clickhouse;

/**
 * Fluent builder for HAVING conditions.
 * Created via {@link ClickHouseQuery#having(CH.Expr)} or {@link ClickHouseQuery#having(String)}.
 *
 * <pre>{@code
 * import static lib.core.clickhouse.CH.*;
 *
 * .having(sum("amount")).gt(100)
 * .having(count()).gte(5)
 * .having(avg("score")).between(50, 100)
 * }</pre>
 */
public final class HavingBuilder {
    private final ClickHouseQuery query;
    private final String expression;
    private static int paramSeq = 0;

    HavingBuilder(ClickHouseQuery query, String expression) {
        this.query = query;
        this.expression = expression;
    }

    private String nextParam() {
        return "_having" + (paramSeq++);
    }

    /** {@code expression > value} */
    public ClickHouseQuery gt(Object value) {
        String p = nextParam();
        query.havingClauses.add(expression + " > :" + p);
        query.params.addValue(p, value);
        return query;
    }

    /** {@code expression >= value} */
    public ClickHouseQuery gte(Object value) {
        String p = nextParam();
        query.havingClauses.add(expression + " >= :" + p);
        query.params.addValue(p, value);
        return query;
    }

    /** {@code expression < value} */
    public ClickHouseQuery lt(Object value) {
        String p = nextParam();
        query.havingClauses.add(expression + " < :" + p);
        query.params.addValue(p, value);
        return query;
    }

    /** {@code expression <= value} */
    public ClickHouseQuery lte(Object value) {
        String p = nextParam();
        query.havingClauses.add(expression + " <= :" + p);
        query.params.addValue(p, value);
        return query;
    }

    /** {@code expression = value} */
    public ClickHouseQuery eq(Object value) {
        String p = nextParam();
        query.havingClauses.add(expression + " = :" + p);
        query.params.addValue(p, value);
        return query;
    }

    /** {@code expression != value} */
    public ClickHouseQuery ne(Object value) {
        String p = nextParam();
        query.havingClauses.add(expression + " != :" + p);
        query.params.addValue(p, value);
        return query;
    }

    /** {@code expression >= from AND expression <= to} */
    public ClickHouseQuery between(Object from, Object to) {
        String p1 = nextParam();
        String p2 = nextParam();
        query.havingClauses.add(expression + " >= :" + p1 + " AND " + expression + " <= :" + p2);
        query.params.addValue(p1, from);
        query.params.addValue(p2, to);
        return query;
    }
}
