package lib.core.clickhouse.query.builder;

import lib.core.clickhouse.query.ClickHouseQuery;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder for WITH (Common Table Expressions).
 * Created via {@link ClickHouseQuery#with(String, ClickHouseQuery)}.
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
public final class CTEBuilder {
    private final List<String[]> ctes = new ArrayList<>();
    private final List<ClickHouseQuery> cteQueries = new ArrayList<>();

    public CTEBuilder() {}

    /**
     * Add a CTE definition.
     *
     * @param name  the CTE name
     * @param query the CTE query
     */
    public void addCTE(String name, ClickHouseQuery query) {
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
    public CTEBuilder with(String name, ClickHouseQuery query) {
        addCTE(name, query);
        return this;
    }

    /**
     * Start the main SELECT clause after defining CTEs.
     *
     * @param columns columns/expressions to select
     * @return a new {@link ClickHouseQuery} with the CTEs attached
     */
    public ClickHouseQuery select(String... columns) {
        ClickHouseQuery q = ClickHouseQuery.select(columns);
        q.cteList.addAll(ctes);
        // Merge CTE params into main query
        for (ClickHouseQuery cteQuery : cteQueries) {
            cteQuery.params.getValues().forEach((k, v) -> q.params.addValue((String) k, v));
        }
        return q;
    }

    /**
     * Start a SELECT DISTINCT after defining CTEs.
     *
     * @param columns columns to select
     * @return a new {@link ClickHouseQuery} with the CTEs attached
     */
    public ClickHouseQuery selectDistinct(String... columns) {
        ClickHouseQuery q = ClickHouseQuery.selectDistinct(columns);
        q.cteList.addAll(ctes);
        for (ClickHouseQuery cteQuery : cteQueries) {
            cteQuery.params.getValues().forEach((k, v) -> q.params.addValue((String) k, v));
        }
        return q;
    }
}
