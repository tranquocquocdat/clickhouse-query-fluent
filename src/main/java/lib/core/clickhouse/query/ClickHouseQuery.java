package lib.core.clickhouse.query;


import lib.core.clickhouse.expression.CH;
import lib.core.clickhouse.query.builder.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

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
 * @see WhereBuilder
 * @see JoinBuilder
 * @see HavingBuilder
 */
public final class ClickHouseQuery {

    /**
     * Phases enforce correct SQL clause ordering.
     * Each method advances the phase; calling out of order throws {@link IllegalStateException}.
     *
     * <pre>
     * SELECT → FROM → JOIN → WHERE → GROUP_BY → HAVING → ORDER_BY → LIMIT
     * </pre>
     */
    private enum Phase {
        SELECT(0),
        FROM(1),
        JOIN(2),
        WHERE(3),
        GROUP_BY(4),
        HAVING(5),
        ORDER_BY(6),
        LIMIT(7);

        final int order;
        Phase(int order) { this.order = order; }
    }

    private Phase currentPhase = Phase.SELECT;

    /** Default LIMIT applied when query() is called without an explicit limit. */
    public static final int DEFAULT_LIMIT = 1000;

    // Public fields — accessed by builder classes in query.builder sub-package
    public final List<String> selectColumns = new ArrayList<>();
    public boolean distinct;
    public String tableName;
    public final List<String> joinClauses = new ArrayList<>();
    public final List<String> whereClauses = new ArrayList<>();
    public final MapSqlParameterSource params = new MapSqlParameterSource();
    public final List<String> groupByColumns = new ArrayList<>();
    public final List<String> havingClauses = new ArrayList<>();
    public final List<String> orderByClauses = new ArrayList<>();
    public Integer limitVal;
    public Long offsetVal;

    // Subquery FROM
    private ClickHouseQuery fromSubQuery;
    private String fromSubQueryAlias;

    // UNION ALL
    public final List<ClickHouseQuery> unionQueries = new ArrayList<>();

    // WITH (CTE)
    public final List<String[]> cteList = new ArrayList<>();

    private ClickHouseQuery() {}

    /**
     * Advance to the given phase. Same phase is allowed (e.g. multiple {@code .where()} calls).
     * Going backward throws {@link IllegalStateException}.
     */
    void advanceTo(Phase target) {
        if (target.order < currentPhase.order) {
            throw new IllegalStateException(
                    "ClickHouseQuery: cannot call " + target.name()
                    + " after " + currentPhase.name()
                    + ". Expected order: SELECT → FROM → JOIN → WHERE → GROUP_BY → HAVING → ORDER_BY → LIMIT");
        }
        currentPhase = target;
    }

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
    public static CTEBuilder with(String name, ClickHouseQuery query) {
        CTEBuilder builder = new CTEBuilder();
        builder.addCTE(name, query);
        return builder;
    }

    // ── FROM ─────────────────────────────────────────────────────────────

    /**
     * Set the table name for FROM clause.
     *
     * @param table the table name (e.g. {@code "orders"})
     * @return this query builder
     * @throws IllegalStateException if called after FROM phase
     */
    public ClickHouseQuery from(String table) {
        advanceTo(Phase.FROM);
        this.tableName = table;
        return this;
    }

    /**
     * Set the table from an {@link Alias}.
     * {@code .from(o)} → {@code FROM orders o}
     *
     * @param alias the table alias (created with {@code Alias.of("orders", "o")})
     * @return this query builder
     */
    public ClickHouseQuery from(Alias alias) {
        return from(alias.ref());
    }

    /**
     * FROM subquery: {@code FROM (SELECT ...) AS alias}.
     *
     * <pre>{@code
     * ClickHouseQuery.select("user_id", "total")
     *     .from(
     *         ClickHouseQuery.select("user_id", "sum(amount) AS total")
     *             .from("orders").groupBy("user_id")
     *     )
     *     .as("sub")
     *     .where("total").gt(1000)
     * }</pre>
     *
     * @param subQuery the inner query
     * @return a {@link SubQueryFromBuilder} — call {@code .as("alias")} to set the alias
     */
    public SubQueryFromBuilder from(ClickHouseQuery subQuery) {
        advanceTo(Phase.FROM);
        this.fromSubQuery = subQuery;
        return new SubQueryFromBuilder(this);
    }

    /**
     * FROM subquery with alias (shorthand).
     *
     * @param subQuery the inner query
     * @param alias    alias for the subquery table
     * @return this query builder
     */
    public ClickHouseQuery from(ClickHouseQuery subQuery, String alias) {
        return from(subQuery).as(alias);
    }

    /**
     * FROM subquery with type-safe {@link Alias}.
     *
     * <pre>{@code
     * Alias sub = Alias.of("sub");
     * ClickHouseQuery.select(sub.col("user_id"), sub.col("total"))
     *     .from(innerQuery, sub)
     *     .where(sub.col("total")).gt(1000)
     * }</pre>
     *
     * @param subQuery the inner query
     * @param alias    the Alias for the subquery
     * @return this query builder
     */
    public ClickHouseQuery from(ClickHouseQuery subQuery, Alias alias) {
        return from(subQuery).as(alias.toString());
    }

    /**
     * Intermediate builder for setting subquery alias fluently.
     * <p>Usage: {@code .from(subQuery).as("alias")}
     */
    public static final class SubQueryFromBuilder {
        private final ClickHouseQuery query;

        SubQueryFromBuilder(ClickHouseQuery query) {
            this.query = query;
        }

        /**
         * Set the alias for this subquery.
         *
         * @param alias the alias name
         * @return the parent query for further chaining
         */
        public ClickHouseQuery as(String alias) {
            query.fromSubQueryAlias = alias;
            return query;
        }
    }

    // ── JOIN ─────────────────────────────────────────────────────────────

    /**
     * Fluent INNER JOIN.
     * <pre>{@code
     * .join("user_profile u").on("u.id = t.user_id")
     * }</pre>
     *
     * @param table the table to join (e.g. {@code "user_profile u"})
     * @return a {@link JoinBuilder} for specifying the ON condition
     */
    public JoinBuilder join(String table) {
        advanceTo(Phase.JOIN);
        return new JoinBuilder(this, "JOIN", table);
    }

    /** INNER JOIN using an {@link Alias}. {@code .join(u)} → {@code JOIN users u} */
    public JoinBuilder join(Alias alias) {
        return join(alias.ref());
    }

    /**
     * Fluent LEFT JOIN.
     * <pre>{@code
     * .leftJoin("settings s").on("s.user_id = t.user_id")
     * }</pre>
     */
    public JoinBuilder leftJoin(String table) {
        advanceTo(Phase.JOIN);
        return new JoinBuilder(this, "LEFT JOIN", table);
    }

    /** LEFT JOIN using an {@link Alias}. {@code .leftJoin(p)} → {@code LEFT JOIN products p} */
    public JoinBuilder leftJoin(Alias alias) {
        return leftJoin(alias.ref());
    }

    /**
     * Fluent RIGHT JOIN.
     * <pre>{@code
     * .rightJoin("other o").on("o.id = t.other_id")
     * }</pre>
     */
    public JoinBuilder rightJoin(String table) {
        advanceTo(Phase.JOIN);
        return new JoinBuilder(this, "RIGHT JOIN", table);
    }

    /** RIGHT JOIN using an {@link Alias}. {@code .rightJoin(x)} → {@code RIGHT JOIN x_table x} */
    public JoinBuilder rightJoin(Alias alias) {
        return rightJoin(alias.ref());
    }

    /**
     * Fluent FULL OUTER JOIN.
     * <pre>{@code
     * .fullJoin("other o").on("o.id = t.other_id")
     * }</pre>
     */
    public JoinBuilder fullJoin(String table) {
        advanceTo(Phase.JOIN);
        return new JoinBuilder(this, "FULL OUTER JOIN", table);
    }

    /** FULL OUTER JOIN using an {@link Alias}. */
    public JoinBuilder fullJoin(Alias alias) {
        return fullJoin(alias.ref());
    }

    // ── WHERE (fluent column-first) ──────────────────────────────────────

    /**
     * Start a fluent WHERE clause on a column.
     * <pre>{@code
     * .where("tenant_id").eq(tenantId)
     * .where("amount").gt(100)
     * .where("created_at").between(from, to)
     * .where("session_status").eqIfNotBlank(status)
     * .where("product_id").in(productIds)
     * }</pre>
     *
     * @param column the column name
     * @return a {@link WhereBuilder} for chaining the comparison operator
     */
    public WhereBuilder where(String column) {
        advanceTo(Phase.WHERE);
        return new WhereBuilder(this, column);
    }

    /**
     * Start a fluent WHERE clause using a type-safe {@link CH.Expr}.
     * <pre>{@code
     * .where(alias.col("tenant_id")).eq(tenantId)
     * }</pre>
     */
    public WhereBuilder where(CH.Expr column) {
        return where(column.toString());
    }

    /**
     * Start a fluent ILIKE search across multiple columns.
     * <pre>{@code
     * .whereILike(keyword).on("session_id", "user_id")
     * }</pre>
     * Skipped when keyword is null or blank.
     *
     * @param keyword the search keyword (will be wrapped with {@code %keyword%})
     * @return a {@link WhereILikeBuilder} for specifying target columns
     */
    public WhereILikeBuilder whereILike(String keyword) {
        advanceTo(Phase.WHERE);
        return new WhereILikeBuilder(this, keyword, false);
    }

    /**
     * Start a fluent case-sensitive LIKE search across multiple columns.
     * <pre>{@code
     * .whereLike(keyword).on("session_id", "user_id")
     * }</pre>
     * Skipped when keyword is null or blank.
     *
     * @param keyword the search keyword (will be wrapped with {@code %keyword%})
     * @return a {@link WhereILikeBuilder} for specifying target columns
     */
    public WhereILikeBuilder whereLike(String keyword) {
        advanceTo(Phase.WHERE);
        return new WhereILikeBuilder(this, keyword, true);
    }

    /** Always-applied WHERE clause with raw condition and parameter. */
    public ClickHouseQuery whereRaw(String condition, String paramName, Object value) {
        advanceTo(Phase.WHERE);
        whereClauses.add(condition);
        params.addValue(paramName, value);
        return this;
    }

    /** Always-applied WHERE clause with no parameter (e.g. {@code "1=1"}). */
    public ClickHouseQuery whereRaw(String condition) {
        advanceTo(Phase.WHERE);
        whereClauses.add(condition);
        return this;
    }

    // ── WHERE OR ─────────────────────────────────────────────────────────

    /**
     * Add an OR group to WHERE.
     * <pre>{@code
     * .whereOr(or -> or
     *     .add("status", "ACTIVE")
     *     .add("status", "PENDING")
     * )
     * // → (status = :_or0 OR status = :_or1)
     * }</pre>
     */
    public ClickHouseQuery whereOr(Consumer<OrBuilder> consumer) {
        advanceTo(Phase.WHERE);
        OrBuilder or = new OrBuilder(this);
        consumer.accept(or);
        or.apply();
        return this;
    }

    // ── GROUP BY ─────────────────────────────────────────────────────────

    /**
     * GROUP BY the given columns.
     *
     * @param columns columns to group by
     * @return this query builder
     * @throws IllegalStateException if called after GROUP_BY phase
     */
    public ClickHouseQuery groupBy(String... columns) {
        advanceTo(Phase.GROUP_BY);
        this.groupByColumns.addAll(List.of(columns));
        return this;
    }

    /**
     * GROUP BY accepting {@link CH.Expr} or String values.
     */
    public ClickHouseQuery groupBy(Object... columns) {
        String[] strs = new String[columns.length];
        for (int i = 0; i < columns.length; i++) strs[i] = columns[i].toString();
        return groupBy(strs);
    }

    // ── HAVING (fluent) ──────────────────────────────────────────────────

    /**
     * Start a fluent HAVING clause on an expression.
     * <pre>{@code
     * import static lib.core.clickhouse.CH.*;
     *
     * .having(sum("amount")).gt(100)
     * .having(count()).gte(5)
     * .having(avg("score")).between(50, 100)
     * }</pre>
     *
     * @param expression a {@link CH.Expr} (e.g. {@code sum("amount")})
     * @return a {@link HavingBuilder} for chaining the comparison
     */
    public HavingBuilder having(CH.Expr expression) {
        advanceTo(Phase.HAVING);
        return new HavingBuilder(this, expression.toString());
    }

    /** Start a fluent HAVING clause on a raw expression string. */
    public HavingBuilder havingRaw(String expression) {
        advanceTo(Phase.HAVING);
        return new HavingBuilder(this, expression);
    }

    // ── ORDER BY ────────────────────────────────────────────────────────

    /**
     * ORDER BY with type-safe enum direction.
     * Supports multiple calls for multiple columns.
     *
     * <pre>{@code
     * .orderBy("total", SortOrder.DESC)
     * .orderBy("user_id", SortOrder.ASC)
     * // → ORDER BY total DESC, user_id ASC
     * }</pre>
     *
     * @param column    the column to order by
     * @param direction {@link SortOrder#ASC} or {@link SortOrder#DESC}
     * @return this query builder
     */
    public ClickHouseQuery orderBy(String column, SortOrder direction) {
        advanceTo(Phase.ORDER_BY);
        this.orderByClauses.add(column + " " + direction.name());
        return this;
    }

    /** ORDER BY column ASC. */
    public ClickHouseQuery orderBy(String column) {
        return orderBy(column, SortOrder.ASC);
    }

    /** ORDER BY column with direction (Expr overload). */
    public ClickHouseQuery orderBy(Object column, SortOrder direction) {
        return orderBy(column.toString(), direction);
    }

    /** ORDER BY column ASC (Expr overload). */
    public ClickHouseQuery orderBy(Object column) {
        return orderBy(column.toString(), SortOrder.ASC);
    }

    // ── UNION ALL ────────────────────────────────────────────────────────

    /**
     * Append a UNION ALL query.
     * Can be chained multiple times for 3+ unions.
     * ORDER BY and LIMIT after UNION ALL apply to the combined result.
     *
     * <pre>{@code
     * ClickHouseQuery.select("user_id", "amount").from("orders_2024")
     *     .unionAll(
     *         ClickHouseQuery.select("user_id", "amount").from("orders_2025")
     *     )
     *     .orderBy("amount", SortOrder.DESC)
     *     .limit(10)
     * }</pre>
     *
     * @param other the query to UNION ALL with
     * @return this query builder
     */
    public ClickHouseQuery unionAll(ClickHouseQuery other) {
        this.unionQueries.add(other);
        return this;
    }

    // ── LIMIT / OFFSET ──────────────────────────────────────────────────

    /**
     * Set LIMIT for the query.
     *
     * @param limit number of rows to return
     * @return this query builder
     */
    public ClickHouseQuery limit(int limit) {
        advanceTo(Phase.LIMIT);
        this.limitVal = limit;
        return this;
    }

    /**
     * Set OFFSET for the query (typically used with {@link #limit(int)}).
     *
     * @param offset number of rows to skip
     * @return this query builder
     */
    public ClickHouseQuery offset(long offset) {
        advanceTo(Phase.LIMIT);
        this.offsetVal = offset;
        return this;
    }

    // ── Extra params ────────────────────────────────────────────────────

    /** Add an extra parameter (for custom SQL fragments). */
    public ClickHouseQuery param(String name, Object value) {
        params.addValue(name, value);
        return this;
    }

    // ── Build ────────────────────────────────────────────────────────────

    /** Build the SQL string (formatted with newlines for readability). */
    public String toSql() {
        StringBuilder sql = new StringBuilder();

        // WITH (CTE)
        if (!cteList.isEmpty()) {
            sql.append("WITH ");
            for (int i = 0; i < cteList.size(); i++) {
                if (i > 0) sql.append(",\n     ");
                String[] cte = cteList.get(i);
                sql.append(cte[0]).append(" AS (\n  ").append(cte[1]).append("\n)");
            }
            sql.append("\n");
        }

        // SELECT
        if (!selectColumns.isEmpty()) {
            sql.append(distinct ? "SELECT DISTINCT " : "SELECT ");
            sql.append(String.join(",\n       ", selectColumns));
        }

        // FROM
        if (fromSubQuery != null) {
            // Subquery FROM
            sql.append("\nFROM (\n  ").append(fromSubQuery.toSql()).append("\n) AS ").append(fromSubQueryAlias);
            // Merge subquery params
            fromSubQuery.params.getValues().forEach((k, v) -> params.addValue((String) k, v));
        } else if (tableName != null) {
            sql.append("\nFROM ").append(tableName);
        }

        // JOIN
        for (String join : joinClauses) {
            sql.append("\n").append(join);
        }

        // WHERE
        if (!whereClauses.isEmpty()) {
            sql.append("\nWHERE ").append(whereClauses.get(0));
            for (int i = 1; i < whereClauses.size(); i++) {
                sql.append("\n  AND ").append(whereClauses.get(i));
            }
        }

        // GROUP BY
        if (!groupByColumns.isEmpty()) {
            sql.append("\nGROUP BY ").append(String.join(", ", groupByColumns));
        }

        // HAVING
        if (!havingClauses.isEmpty()) {
            sql.append("\nHAVING ").append(havingClauses.get(0));
            for (int i = 1; i < havingClauses.size(); i++) {
                sql.append("\n  AND ").append(havingClauses.get(i));
            }
        }

        // UNION ALL
        for (ClickHouseQuery union : unionQueries) {
            sql.append("\nUNION ALL\n").append(union.toSql());
            // Merge union params
            union.params.getValues().forEach((k, v) -> params.addValue((String) k, v));
        }

        // ORDER BY
        if (!orderByClauses.isEmpty()) {
            sql.append("\nORDER BY ").append(String.join(", ", orderByClauses));
        }

        // LIMIT / OFFSET
        if (limitVal != null) {
            sql.append("\nLIMIT :_limit");
            params.addValue("_limit", limitVal);
        }
        if (offsetVal != null) {
            sql.append(" OFFSET :_offset");
            params.addValue("_offset", offsetVal);
        }

        return sql.toString();
    }

    /** Get the parameter source for this query. */
    public MapSqlParameterSource toParams() {
        // Trigger toSql() to ensure limit/offset params are registered
        toSql();
        return params;
    }

    // ── Execute ──────────────────────────────────────────────────────────

    /**
     * Execute query and return list of mapped results.
     * <p>If no LIMIT was set, a default limit of {@value #DEFAULT_LIMIT} is applied
     * to prevent accidental full-table scans on large tables.
     */
    public <T> List<T> query(NamedParameterJdbcTemplate jdbc, RowMapper<T> rowMapper) {
        if (limitVal == null && unionQueries.isEmpty()) {
            this.limitVal = DEFAULT_LIMIT;
        }
        String sql = toSql();
        logQuery(sql);
        return jdbc.query(sql, params, rowMapper);
    }

    /**
     * Execute query and auto-map results to a DTO class or Java record.
     * <ul>
     *   <li>For <b>records</b>: uses {@link RecordRowMapper} — maps {@code snake_case} columns
     *       to record component names (camelCase). No setters needed.</li>
     *   <li>For <b>classes</b>: uses {@link BeanPropertyRowMapper} — requires default constructor + setters.</li>
     * </ul>
     *
     * <pre>{@code
     * // Works with records:
     * public record OrderHeader(String userId, String orderId) {}
     * List<SessionHeader> list = query.query(namedJdbc, SessionHeader.class);
     *
     * // Also works with regular classes:
     * public class OrderReport {
     *     private String userId;
     *     private BigDecimal totalAmount;
     *     // getters + setters
     * }
     * List<OrderReport> list = query.query(namedJdbc, OrderReport.class);
     * }</pre>
     *
     * @param jdbc the JDBC template
     * @param type the DTO class or record
     * @param <T>  the DTO type
     * @return list of mapped DTOs
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> query(NamedParameterJdbcTemplate jdbc, Class<T> type) {
        return query(jdbc, smartMapper(type));
    }

    /**
     * Execute query and return a single DTO result (or null).
     *
     * <pre>{@code
     * OrderSummary summary = ClickHouseQuery.select("count(*) AS total_orders", "sum(amount) AS total_amount")
     *     .from("orders")
     *     .where("tenant_id").eq(tenantId)
     *     .queryOne(namedJdbc, OrderSummary.class);
     * }</pre>
     *
     * @param jdbc the JDBC template
     * @param type the DTO class
     * @param <T>  the DTO type
     * @return the mapped DTO, or null if no result
     */
    public <T> T queryOne(NamedParameterJdbcTemplate jdbc, Class<T> type) {
        List<T> results = query(jdbc, type);
        return results.isEmpty() ? null : results.get(0);
    }

    /**
     * Execute a <b>single query</b> that returns both paginated data and total count.
     * Internally appends {@code count(*) OVER() AS _total} to the SELECT, so no second query is needed.
     *
     * <pre>{@code
     * Page<Report> page = ClickHouseQuery.select("user_id", "amount")
     *     .from("orders")
     *     .where("tenant_id").eq(tenantId)
     *     .orderBy("created_at", SortOrder.DESC)
     *     .queryPage(0, 10, namedJdbc, rowMapper);
     *
     * page.getData();       // List<Report>  (max 10 items)
     * page.getTotal();      // 1234          (total matching rows)
     * page.getTotalPages(); // 124
     * page.hasNext();       // true
     * }</pre>
     *
     * @param page      the page index (0-based)
     * @param pageSize  number of rows per page
     * @param jdbc      the JDBC template
     * @param rowMapper the row mapper for data rows
     * @param <T>       the row type
     * @return a {@link Page} containing data + total count
     */
    public <T> Page<T> queryPage(int page, int pageSize, NamedParameterJdbcTemplate jdbc, RowMapper<T> rowMapper) {
        // Inject count(*) OVER() into SELECT columns
        this.selectColumns.add("count(*) OVER() AS _total");

        // Apply pagination
        this.limitVal = pageSize;
        this.offsetVal = (long) page * pageSize;

        String sql = toSql();
        logQuery(sql);

        // Track total from first row
        final long[] totalHolder = {0};

        List<T> data = jdbc.query(sql, params, (rs, rowNum) -> {
            if (rowNum == 0) {
                totalHolder[0] = rs.getLong("_total");
            }
            return rowMapper.mapRow(rs, rowNum);
        });

        // Remove the injected _total column so the builder can be reused
        this.selectColumns.remove(this.selectColumns.size() - 1);

        return new Page<>(data, totalHolder[0], page, pageSize);
    }

    /**
     * Single-query pagination with auto DTO mapping.
     *
     * <pre>{@code
     * Page<OrderReport> page = ClickHouseQuery.select("user_id", "sum(amount) AS total_amount")
     *     .from("orders")
     *     .where("tenant_id").eq(tenantId)
     *     .groupBy("user_id")
     *     .queryPage(0, 10, namedJdbc, OrderReport.class);
     * }</pre>
     *
     * @param page     the page index (0-based)
     * @param pageSize number of rows per page
     * @param jdbc     the JDBC template
     * @param type     the DTO class
     * @param <T>      the DTO type
     * @return a {@link Page} containing auto-mapped DTOs + total count
     */
    @SuppressWarnings("unchecked")
    public <T> Page<T> queryPage(int page, int pageSize, NamedParameterJdbcTemplate jdbc, Class<T> type) {
        return queryPage(page, pageSize, jdbc, smartMapper(type));
    }

    /** Execute query and return a single typed value. */
    public <T> T queryForObject(NamedParameterJdbcTemplate jdbc, Class<T> type) {
        String sql = toSql();
        logQuery(sql);
        return jdbc.queryForObject(sql, params, type);
    }

    /**
     * Wrap the current query as a subquery for counting (terminal style).
     * Generates: {@code SELECT count(*) FROM (current_query)}
     *
     * @param jdbc the JDBC template to execute with
     * @return the count result
     */
    public long count(NamedParameterJdbcTemplate jdbc) {
        String countSql = "SELECT count(*) FROM (" + toSql() + ")";
        logQuery(countSql);
        Long result = jdbc.queryForObject(countSql, params, Long.class);
        return result != null ? result : 0;
    }

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
    public static CountQuery count(ClickHouseQuery subQuery) {
        return new CountQuery(subQuery);
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

    /**
     * Smart mapper: returns {@link RecordRowMapper} for records,
     * {@link BeanPropertyRowMapper} for regular classes.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T> RowMapper<T> smartMapper(Class<T> type) {
        if (type.isRecord()) {
            return (RowMapper<T>) RecordRowMapper.of((Class) type);
        }
        return BeanPropertyRowMapper.newInstance(type);
    }

    /** Convert snake_case to camelCase for parameter naming. */
    public static String toCamelCase(String snake) {
        String[] parts = snake.split("_");
        StringBuilder sb = new StringBuilder(parts[0]);
        for (int i = 1; i < parts.length; i++) {
            sb.append(Character.toUpperCase(parts[i].charAt(0)));
            sb.append(parts[i].substring(1));
        }
        return sb.toString();
    }
}
