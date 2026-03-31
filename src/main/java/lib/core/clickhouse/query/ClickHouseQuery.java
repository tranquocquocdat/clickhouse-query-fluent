package lib.core.clickhouse.query;


import lib.core.clickhouse.expression.CH;
import lib.core.query.util.StringUtils;
import lib.core.query.BaseQuery;
import lib.core.query.builder.CTEBuilder;
import lib.core.query.builder.CountQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.List;

/**
 * Fluent SELECT query builder for ClickHouse with <b>clause-order validation</b>.
 *
 * <p>The builder enforces correct SQL clause ordering at runtime:
 * <pre>
 * SELECT → FROM → JOIN → WHERE → GROUP_BY → HAVING → ORDER_BY → LIMIT
 * </pre>
 * Calling a method out of order throws {@link IllegalStateException} with a clear message.
 *
 * <h3>Basic Usage</h3>
 * <pre>{@code
 * import static lib.core.clickhouse.CH.*;
 *
 * List<Item> items = ClickHouseQuery
 *     .select(col("user_id"), sum("amount").as("total"))
 *     .from("orders")
 *     .where("tenant_id").eq(tenantId)
 *     .where("created_at").between(fromPeriod, toPeriod)
 *     .where("status").eqIfNotBlank(status)
 *     .where("product_id").in(productIds)
 *     .whereILike(keyword).on("session_id", "user_id")
 *     .groupBy("user_id")
 *     .having(sum("amount")).gt(100)
 *     .orderBy("total", SortOrder.DESC)
 *     .limit(10).offset(0)
 *     .query(namedJdbc, rowMapper);
 * }</pre>
 *
 * <h3>Subquery Count</h3>
 * <pre>{@code
 * long total = ClickHouseQuery
 *     .count(
 *         ClickHouseQuery.select("user_id", "session_id")
 *             .from("order_items")
 *             .where("created_at").between(from, to)
 *             .groupBy("user_id", "session_id")
 *     )
 *     .execute(namedJdbc);
 * }</pre>
 *
 * @see CH
 * @see ClickHouseInsert
 * @see lib.core.query.builder.WhereBuilder
 * @see lib.core.query.builder.JoinBuilder
 * @see lib.core.query.builder.HavingBuilder
 */
public final class ClickHouseQuery extends BaseQuery<ClickHouseQuery> {

    /** Default LIMIT applied when query() is called without an explicit limit. */
    public static final int DEFAULT_LIMIT = 1000;

    // ClickHouse-specific GROUP BY modifiers
    private String groupByModifier;  // WITH TOTALS / WITH ROLLUP / WITH CUBE

    private ClickHouseQuery() {}

    // ── Factory ──────────────────────────────────────────────────────────

    /**
     * Start a SELECT query with the given columns/expressions.
     *
     * @param columns column names or expressions (e.g. {@code "user_id"}, {@code "sum(amount) AS total"})
     * @return a new query builder in the SELECT phase
     */
    public static ClickHouseQuery select(String... columns) {
        ClickHouseQuery q = new ClickHouseQuery();
        q.selectColumns.addAll(List.of(columns));
        return q;
    }

    /**
     * Start a SELECT query accepting {@link CH.Expr} or String values.
     */
    public static ClickHouseQuery select(Object... columns) {
        String[] strs = new String[columns.length];
        for (int i = 0; i < columns.length; i++) strs[i] = columns[i].toString();
        return select(strs);
    }

    /**
     * Start a SELECT DISTINCT query.
     *
     * @param columns column names or expressions
     * @return a new query builder in the SELECT phase with DISTINCT
     */
    public static ClickHouseQuery selectDistinct(String... columns) {
        ClickHouseQuery q = new ClickHouseQuery();
        q.distinct = true;
        q.selectColumns.addAll(List.of(columns));
        return q;
    }

    /**
     * Start a SELECT DISTINCT query accepting {@link CH.Expr} or String values.
     */
    public static ClickHouseQuery selectDistinct(Object... columns) {
        String[] strs = new String[columns.length];
        for (int i = 0; i < columns.length; i++) strs[i] = columns[i].toString();
        return selectDistinct(strs);
    }

    /** Start a raw SELECT query (e.g., building from scratch). */
    public static ClickHouseQuery raw() {
        return new ClickHouseQuery();
    }

    // ── WITH (CTE) ───────────────────────────────────────────────────────

    /**
     * Start building a query with a Common Table Expression (CTE).
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
     *
     * @param name  the CTE name
     * @param query the CTE query
     * @return a {@link CTEBuilder} for chaining more CTEs or starting SELECT
     */
    public static CTEBuilder<ClickHouseQuery> with(String name, ClickHouseQuery query) {
        CTEBuilder<ClickHouseQuery> builder = new CTEBuilder<>(new CTEBuilder.QueryFactory<ClickHouseQuery>() {
            @Override
            public ClickHouseQuery select(String... columns) {
                return ClickHouseQuery.select(columns);
            }

            @Override
            public ClickHouseQuery selectDistinct(String... columns) {
                return ClickHouseQuery.selectDistinct(columns);
            }
        });
        builder.addCTE(name, query);
        return builder;
    }

    // ── ClickHouse-specific GROUP BY methods ────────────────────────────

    // ── ClickHouse-specific GROUP BY methods ────────────────────────────

    /**
     * GROUP BY with TOTALS modifier — adds a summary row with totals.
     *
     * <pre>{@code
     * ClickHouseQuery.select("product_id", sum("amount").as("total"))
     *     .from("orders")
     *     .groupByWithTotals("product_id")
     *     .query(namedJdbc, Report.class);
     * // → GROUP BY product_id WITH TOTALS
     * }</pre>
     *
     * @param columns columns to group by
     * @return this query builder
     */
    public ClickHouseQuery groupByWithTotals(String... columns) {
        groupBy(columns);
        this.groupByModifier = "WITH TOTALS";
        return this;
    }

    /**
     * GROUP BY with TOTALS modifier (Expr overload).
     */
    public ClickHouseQuery groupByWithTotals(Object... columns) {
        groupBy(columns);
        this.groupByModifier = "WITH TOTALS";
        return this;
    }

    /**
     * GROUP BY with ROLLUP modifier — creates subtotals for hierarchical grouping.
     *
     * <pre>{@code
     * ClickHouseQuery.select("year", "month", sum("amount").as("total"))
     *     .from("orders")
     *     .groupByWithRollup("year", "month")
     *     .query(namedJdbc, Report.class);
     * // → GROUP BY year, month WITH ROLLUP
     * }</pre>
     *
     * @param columns columns to group by (order matters for hierarchy)
     * @return this query builder
     */
    public ClickHouseQuery groupByWithRollup(String... columns) {
        groupBy(columns);
        this.groupByModifier = "WITH ROLLUP";
        return this;
    }

    /**
     * GROUP BY with ROLLUP modifier (Expr overload).
     */
    public ClickHouseQuery groupByWithRollup(Object... columns) {
        groupBy(columns);
        this.groupByModifier = "WITH ROLLUP";
        return this;
    }

    /**
     * GROUP BY with CUBE modifier — creates subtotals for all combinations.
     *
     * <pre>{@code
     * ClickHouseQuery.select("region", "product", sum("amount").as("total"))
     *     .from("orders")
     *     .groupByWithCube("region", "product")
     *     .query(namedJdbc, Report.class);
     * // → GROUP BY region, product WITH CUBE
     * }</pre>
     *
     * @param columns columns to group by
     * @return this query builder
     */
    public ClickHouseQuery groupByWithCube(String... columns) {
        groupBy(columns);
        this.groupByModifier = "WITH CUBE";
        return this;
    }

    /**
     * GROUP BY with CUBE modifier (Expr overload).
     */
    public ClickHouseQuery groupByWithCube(Object... columns) {
        groupBy(columns);
        this.groupByModifier = "WITH CUBE";
        return this;
    }

    /**
     * Override to append ClickHouse-specific GROUP BY modifiers.
     */
    @Override
    protected String generateGroupByClause() {
        if (groupByColumns.isEmpty()) return "";
        String clause = "GROUP BY " + String.join(", ", groupByColumns);
        if (groupByModifier != null) {
            clause += " " + groupByModifier;
        }
        return clause;
    }

    // ── Execute (override to apply DEFAULT_LIMIT) ───────────────────────

    /**
     * Execute query and return list of mapped results.
     * <p>If no LIMIT was set, a default limit of {@value #DEFAULT_LIMIT} is applied
     * to prevent accidental full-table scans on large tables.
     */
    @Override
    public <R> List<R> query(NamedParameterJdbcTemplate jdbc, RowMapper<R> rowMapper) {
        if (limitVal == null && unionQueries.isEmpty()) {
            limit(DEFAULT_LIMIT);
        }
        String sql = toSql();
        logQuery(sql);
        return jdbc.query(sql, params, rowMapper);
    }

    // ── Static count factory ────────────────────────────────────────────

    /**
     * Static subquery-style count. Reads more like natural SQL:
     * {@code SELECT COUNT(*) FROM (subquery)}.
     *
     * <pre>{@code
     * long total = ClickHouseQuery
     *     .count(
     *         ClickHouseQuery.select("user_id", "session_id")
     *             .from("order_items")
     *             .where("created_at").between(from, to)
     *             .groupBy("user_id", "session_id")
     *     )
     *     .execute(namedJdbc);
     * }</pre>
     *
     * @param subQuery the inner query to count
     * @return a {@link CountQuery} that can be executed
     */
    public static CountQuery<ClickHouseQuery> count(ClickHouseQuery subQuery) {
        return new CountQuery<>(subQuery);
    }

    // ── Logging ──────────────────────────────────────────────────────────

    private static final Logger log = LoggerFactory.getLogger(ClickHouseQuery.class);

    /**
     * Log the generated SQL and parameters.
     * <ul>
     *   <li><b>DEBUG</b> — logs the SQL statement</li>
     *   <li><b>TRACE</b> — also logs the bound parameter values</li>
     * </ul>
     *
     * <p>Enable in your {@code application.yml}:
     * <pre>{@code
     * logging:
     *   level:
     *     lib.core.clickhouse.query.ClickHouseQuery: DEBUG   # SQL only
     *     lib.core.clickhouse.query.ClickHouseQuery: TRACE   # SQL + params
     * }</pre>
     */
    private void logQuery(String sql) {
        if (log.isDebugEnabled()) {
            log.debug("\n╔══ ClickHouse Query ══════════════════════════════════════\n{}\n╚═════════════════════════════════════════════════════════", sql);
        }
        if (log.isTraceEnabled()) {
            log.trace("Query params: {}", params.getValues());
        }
    }

    // ── Utilities ────────────────────────────────────────────────────────

    /** Convert snake_case to camelCase for parameter naming. */
    public static String toCamelCase(String snake) {
        return StringUtils.toCamelCase(snake);
    }
}
