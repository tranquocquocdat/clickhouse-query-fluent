package lib.core.clickhouse.query;


import lib.core.clickhouse.expression.CH;
import lib.core.clickhouse.query.builder.*;
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

    // Package-private fields — accessed by builder classes in the same package
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

    /** Start a raw SELECT query (e.g., building from scratch). */
    public static ClickHouseQuery raw() {
        return new ClickHouseQuery();
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
    public ClickHouseQuery where(String condition, String paramName, Object value) {
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
    public HavingBuilder having(String expression) {
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

        // SELECT
        if (!selectColumns.isEmpty()) {
            sql.append(distinct ? "SELECT DISTINCT " : "SELECT ");
            sql.append(String.join(",\n       ", selectColumns));
        }

        // FROM
        if (tableName != null) {
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

    /** Execute query and return list of mapped results. */
    public <T> List<T> query(NamedParameterJdbcTemplate jdbc, RowMapper<T> rowMapper) {
        return jdbc.query(toSql(), params, rowMapper);
    }

    /** Execute query and return a single typed value. */
    public <T> T queryForObject(NamedParameterJdbcTemplate jdbc, Class<T> type) {
        return jdbc.queryForObject(toSql(), params, type);
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

    // ── Utilities ────────────────────────────────────────────────────────

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
