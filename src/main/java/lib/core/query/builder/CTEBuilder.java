package lib.core.query.builder;

import lib.core.query.Alias;
import lib.core.query.BaseQuery;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder for WITH (Common Table Expressions).
 * Created via static factory methods in query classes.
 *
 * <pre>{@code
 * ClickHouseQuery
 *     .with("active_users",
 *         ClickHouseQuery.select("user_id").from("users").where("status").eq("ACTIVE"))
 *     .with("user_orders",
 *         ClickHouseQuery.select("user_id", "sum(amount) AS total")
 *             .from("orders").groupBy("user_id"))
 *     .select("au.user_id", "uo.total")
 *     .from("active_users au")
 *     .join("user_orders uo").on("uo.user_id", "au.user_id")
 * }</pre>
 */
public final class CTEBuilder<T extends BaseQuery<T>> {
    private final List<String[]> ctes = new ArrayList<>();
    private final List<BaseQuery<?>> cteQueries = new ArrayList<>();
    private final QueryFactory<T> factory;

    /**
     * Interface for creating new query instances.
     * Implemented by query classes to provide their specific factory methods.
     */
    public interface QueryFactory<T extends BaseQuery<T>> {
        T select(String... columns);
        T selectDistinct(String... columns);
    }

    public CTEBuilder(QueryFactory<T> factory) {
        this.factory = factory;
    }

    /**
     * Add a CTE definition.
     *
     * @param name  the CTE name
     * @param query the CTE query
     */
    public void addCTE(String name, BaseQuery<?> query) {
        ctes.add(new String[]{name, query.toSql()});
        cteQueries.add(query);
    }

    /**
     * Chain another CTE definition.
     *
     * @param name  the CTE name
     * @param query the CTE query
     * @return this builder for chaining
     */
    public CTEBuilder<T> with(String name, BaseQuery<?> query) {
        addCTE(name, query);
        return this;
    }

    /**
     * Chain another CTE definition using an Alias.
     *
     * @param alias the Alias whose toString() provides the CTE name
     * @param query the CTE query
     * @return this builder for chaining
     */
    public CTEBuilder<T> with(Alias alias, BaseQuery<?> query) {
        return with(alias.toString(), query);
    }

    /**
     * Start the main SELECT clause after defining CTEs.
     *
     * @param columns columns/expressions to select
     * @return a new query with the CTEs attached
     */
    public T select(String... columns) {
        T q = factory.select(columns);
        applyCTEs(q);
        return q;
    }

    /**
     * Start the main SELECT clause, accepting mixed types (String, Alias.col(), CH.Expr, etc.).
     * Each argument is converted via toString().
     *
     * @param columns columns/expressions (any object with a meaningful toString)
     * @return a new query with the CTEs attached
     */
    public T select(Object... columns) {
        String[] cols = new String[columns.length];
        for (int i = 0; i < columns.length; i++) {
            cols[i] = columns[i].toString();
        }
        T q = factory.select(cols);
        applyCTEs(q);
        return q;
    }

    /**
     * Start a SELECT DISTINCT after defining CTEs.
     *
     * @param columns columns to select
     * @return a new query with the CTEs attached
     */
    public T selectDistinct(String... columns) {
        T q = factory.selectDistinct(columns);
        applyCTEs(q);
        return q;
    }

    /** Internal: attach CTE definitions and merge params into the main query. */
    private void applyCTEs(T q) {
        q.cteList.addAll(ctes);
        for (BaseQuery<?> cteQuery : cteQueries) {
            cteQuery.params.getValues().forEach((k, v) -> q.params.addValue((String) k, v));
        }
    }
}
