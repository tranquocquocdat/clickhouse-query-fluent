package lib.core.query;

import lib.core.query.builder.*;
import lib.core.query.expression.CommonFunctions;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Abstract base class for SQL query builders.
 * Provides common SQL query building functionality that works across all
 * relational databases.
 * 
 * <p>
 * Subclasses (e.g., ClickHouseQuery, PostgreSQLQuery) extend this to add
 * database-specific features.
 * 
 * <p>
 * The builder enforces correct SQL clause ordering at runtime:
 * 
 * <pre>
 * SELECT → FROM → JOIN → WHERE → GROUP_BY → HAVING → ORDER_BY → LIMIT
 * </pre>
 * 
 * Calling a method out of order throws {@link IllegalStateException} with a
 * clear message.
 * 
 * @param <T> the concrete subclass type (self-referential generic for fluent
 *            API)
 */
public abstract class BaseQuery<T extends BaseQuery<T>> {

    /**
     * Phases enforce correct SQL clause ordering.
     * Each method advances the phase; calling out of order throws
     * {@link IllegalStateException}.
     *
     * <pre>
     * SELECT → FROM → JOIN → WHERE → GROUP_BY → HAVING → ORDER_BY → LIMIT
     * </pre>
     */
    protected enum Phase {
        SELECT(0),
        FROM(1),
        JOIN(2),
        WHERE(3),
        GROUP_BY(4),
        HAVING(5),
        ORDER_BY(6),
        LIMIT(7);

        final int order;

        Phase(int order) {
            this.order = order;
        }
    }

    protected Phase currentPhase = Phase.SELECT;

    // Protected fields — accessible to subclasses
    protected final List<String> selectColumns = new ArrayList<>();
    protected boolean distinct;
    protected String tableName;
    protected final List<String> joinClauses = new ArrayList<>();
    protected final List<String> whereClauses = new ArrayList<>();
    protected final MapSqlParameterSource params = new MapSqlParameterSource();
    protected final List<String> groupByColumns = new ArrayList<>();
    protected final List<String> havingClauses = new ArrayList<>();
    protected final List<String> orderByClauses = new ArrayList<>();
    protected Integer limitVal;
    protected Long offsetVal;

    // Subquery FROM
    protected BaseQuery<?> fromSubQuery;
    String fromSubQueryAlias; // Package-private for SubQueryFromBuilder access

    // UNION ALL
    protected final List<BaseQuery<?>> unionQueries = new ArrayList<>();

    // WITH (CTE)
    protected final List<String[]> cteList = new ArrayList<>();

    /**
     * Package-private constructor prevents external instantiation.
     * Subclasses should provide their own factory methods.
     */
    BaseQuery() {
    }

    /**
     * Advance to the given phase. Same phase is allowed (e.g. multiple
     * {@code .where()} calls).
     * Going backward throws {@link IllegalStateException}.
     */
    protected void advanceTo(Phase target) {
        if (target.order < currentPhase.order) {
            throw new IllegalStateException(
                    "Cannot call " + target.name()
                            + " after " + currentPhase.name()
                            + ". Expected order: SELECT → FROM → JOIN → WHERE → GROUP_BY → HAVING → ORDER_BY → LIMIT");
        }
        currentPhase = target;
    }

    /**
     * Returns this instance cast to the concrete subclass type.
     * Used internally for fluent method chaining.
     */
    @SuppressWarnings("unchecked")
    protected T self() {
        return (T) this;
    }

    /**
     * Generate the GROUP BY clause for SQL generation.
     * Subclasses can override to add database-specific modifiers (e.g., WITH TOTALS
     * in ClickHouse).
     * 
     * @return the GROUP BY clause string, or empty string if no GROUP BY
     */
    protected String generateGroupByClause() {
        if (groupByColumns.isEmpty())
            return "";
        return "GROUP BY " + String.join(", ", groupByColumns);
    }

    // ── FROM ─────────────────────────────────────────────────────────────

    /**
     * Set the table name for FROM clause.
     *
     * @param table the table name (e.g. {@code "orders"})
     * @return this query builder
     * @throws IllegalStateException if called after FROM phase
     */
    public T from(String table) {
        advanceTo(Phase.FROM);
        this.tableName = table;
        return self();
    }

    /**
     * Set the table from an {@link Alias}.
     * {@code .from(o)} → {@code FROM orders o}
     *
     * @param alias the table alias (created with {@code Alias.of("orders", "o")})
     * @return this query builder
     */
    public T from(Alias alias) {
        return from(alias.ref());
    }

    /**
     * FROM subquery: {@code FROM (SELECT ...) AS alias}.
     *
     * <pre>{@code
     * query.from(
     *         subQuery.select("user_id", "sum(amount) AS total")
     *                 .from("orders").groupBy("user_id"))
     *         .as("sub")
     *         .where("total").gt(1000)
     * }</pre>
     *
     * @param subQuery the inner query
     * @return a {@link SubQueryFromBuilder} — call {@code .as("alias")} to set the
     *         alias
     */
    public SubQueryFromBuilder<T> from(BaseQuery<?> subQuery) {
        advanceTo(Phase.FROM);
        this.fromSubQuery = subQuery;
        return new SubQueryFromBuilder<>(self());
    }

    /**
     * FROM subquery with alias (shorthand).
     *
     * @param subQuery the inner query
     * @param alias    alias for the subquery table
     * @return this query builder
     */
    public T from(BaseQuery<?> subQuery, String alias) {
        return from(subQuery).as(alias);
    }

    /**
     * FROM subquery with type-safe {@link Alias}.
     *
     * @param subQuery the inner query
     * @param alias    the Alias for the subquery
     * @return this query builder
     */
    public T from(BaseQuery<?> subQuery, Alias alias) {
        return from(subQuery).as(alias.toString());
    }

    // ── JOIN ─────────────────────────────────────────────────────────────

    /**
     * Fluent INNER JOIN.
     * 
     * <pre>{@code
     * .join("user_profile u").on("u.id = t.user_id")
     * }</pre>
     *
     * @param table the table to join (e.g. {@code "user_profile u"})
     * @return a {@link JoinBuilder} for specifying the ON condition
     */
    public JoinBuilder<T> join(String table) {
        advanceTo(Phase.JOIN);
        return new JoinBuilder<>(self(), "JOIN", table);
    }

    /**
     * INNER JOIN using an {@link Alias}. {@code .join(u)} → {@code JOIN users u}
     */
    public JoinBuilder<T> join(Alias alias) {
        return join(alias.ref());
    }

    /**
     * Fluent LEFT JOIN.
     * 
     * <pre>{@code
     * .leftJoin("settings s").on("s.user_id = t.user_id")
     * }</pre>
     */
    public JoinBuilder<T> leftJoin(String table) {
        advanceTo(Phase.JOIN);
        return new JoinBuilder<>(self(), "LEFT JOIN", table);
    }

    /**
     * LEFT JOIN using an {@link Alias}. {@code .leftJoin(p)} →
     * {@code LEFT JOIN products p}
     */
    public JoinBuilder<T> leftJoin(Alias alias) {
        return leftJoin(alias.ref());
    }

    /**
     * Fluent RIGHT JOIN.
     * 
     * <pre>{@code
     * .rightJoin("other o").on("o.id = t.other_id")
     * }</pre>
     */
    public JoinBuilder<T> rightJoin(String table) {
        advanceTo(Phase.JOIN);
        return new JoinBuilder<>(self(), "RIGHT JOIN", table);
    }

    /**
     * RIGHT JOIN using an {@link Alias}. {@code .rightJoin(x)} →
     * {@code RIGHT JOIN x_table x}
     */
    public JoinBuilder<T> rightJoin(Alias alias) {
        return rightJoin(alias.ref());
    }

    /**
     * Fluent FULL OUTER JOIN.
     * 
     * <pre>{@code
     * .fullJoin("other o").on("o.id = t.other_id")
     * }</pre>
     */
    public JoinBuilder<T> fullJoin(String table) {
        advanceTo(Phase.JOIN);
        return new JoinBuilder<>(self(), "FULL OUTER JOIN", table);
    }

    /** FULL OUTER JOIN using an {@link Alias}. */
    public JoinBuilder<T> fullJoin(Alias alias) {
        return fullJoin(alias.ref());
    }

    // ── WHERE (fluent column-first) ──────────────────────────────────────

    /**
     * Start a fluent WHERE clause on a column.
     * 
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
    public WhereBuilder<T> where(String column) {
        advanceTo(Phase.WHERE);
        return new WhereBuilder<>(self(), column);
    }

    /**
     * Start a fluent WHERE clause using a type-safe {@link CommonFunctions.Expr}.
     * 
     * <pre>{@code
     * .where(alias.col("tenant_id")).eq(tenantId)
     * }</pre>
     */
    public WhereBuilder<T> where(CommonFunctions.Expr column) {
        return where(column.toString());
    }

    /**
     * Start a fluent ILIKE search across multiple columns.
     * 
     * <pre>{@code
     * .whereILike(keyword).on("session_id", "user_id")
     * }</pre>
     * 
     * Skipped when keyword is null or blank.
     *
     * @param keyword the search keyword (will be wrapped with {@code %keyword%})
     * @return a {@link WhereILikeBuilder} for specifying target columns
     */
    public WhereILikeBuilder<T> whereILike(String keyword) {
        advanceTo(Phase.WHERE);
        return new WhereILikeBuilder<>(self(), keyword, false);
    }

    /**
     * Start a fluent case-sensitive LIKE search across multiple columns.
     * 
     * <pre>{@code
     * .whereLike(keyword).on("session_id", "user_id")
     * }</pre>
     * 
     * Skipped when keyword is null or blank.
     *
     * @param keyword the search keyword (will be wrapped with {@code %keyword%})
     * @return a {@link WhereILikeBuilder} for specifying target columns
     */
    public WhereILikeBuilder<T> whereLike(String keyword) {
        advanceTo(Phase.WHERE);
        return new WhereILikeBuilder<>(self(), keyword, true);
    }

    /** Always-applied WHERE clause with raw condition and parameter. */
    public T whereRaw(String condition, String paramName, Object value) {
        advanceTo(Phase.WHERE);
        whereClauses.add(condition);
        params.addValue(paramName, value);
        return self();
    }

    /** Always-applied WHERE clause with no parameter (e.g. {@code "1=1"}). */
    public T whereRaw(String condition) {
        advanceTo(Phase.WHERE);
        whereClauses.add(condition);
        return self();
    }

    // ── WHERE OR ─────────────────────────────────────────────────────────

    /**
     * Add an OR group to WHERE.
     * 
     * <pre>{@code
     * .whereOr(or -> or
     *     .add("status", "ACTIVE")
     *     .add("status", "PENDING")
     * )
     * // → (status = :_or0 OR status = :_or1)
     * }</pre>
     */
    public T whereOr(Consumer<OrBuilder<T>> consumer) {
        advanceTo(Phase.WHERE);
        OrBuilder<T> or = new OrBuilder<>(self());
        consumer.accept(or);
        or.apply();
        return self();
    }

    // ── GROUP BY ─────────────────────────────────────────────────────────

    /**
     * GROUP BY the given columns.
     *
     * @param columns columns to group by
     * @return this query builder
     * @throws IllegalStateException if called after GROUP_BY phase
     */
    public T groupBy(String... columns) {
        advanceTo(Phase.GROUP_BY);
        this.groupByColumns.addAll(List.of(columns));
        return self();
    }

    /**
     * GROUP BY accepting {@link CommonFunctions.Expr} or String values.
     */
    public T groupBy(Object... columns) {
        String[] strs = new String[columns.length];
        for (int i = 0; i < columns.length; i++)
            strs[i] = columns[i].toString();
        return groupBy(strs);
    }

    // ── HAVING (fluent) ──────────────────────────────────────────────────

    /**
     * Start a fluent HAVING clause on an expression.
     * 
     * <pre>{@code
     * import static lib.core.query.expression.CommonFunctions.*;
     *
     * .having(sum("amount")).gt(100)
     * .having(count()).gte(5)
     * .having(avg("score")).between(50, 100)
     * }</pre>
     *
     * @param expression a {@link CommonFunctions.Expr} (e.g. {@code sum("amount")})
     * @return a {@link HavingBuilder} for chaining the comparison
     */
    public HavingBuilder<T> having(CommonFunctions.Expr expression) {
        advanceTo(Phase.HAVING);
        return new HavingBuilder<>(self(), expression.toString());
    }

    /** Start a fluent HAVING clause on a raw expression string. */
    public HavingBuilder<T> havingRaw(String expression) {
        advanceTo(Phase.HAVING);
        return new HavingBuilder<>(self(), expression);
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
    public T orderBy(String column, SortOrder direction) {
        advanceTo(Phase.ORDER_BY);
        this.orderByClauses.add(column + " " + direction.name());
        return self();
    }

    /** ORDER BY column ASC. */
    public T orderBy(String column) {
        return orderBy(column, SortOrder.ASC);
    }

    /** ORDER BY column with direction (Expr overload). */
    public T orderBy(Object column, SortOrder direction) {
        return orderBy(column.toString(), direction);
    }

    /** ORDER BY column ASC (Expr overload). */
    public T orderBy(Object column) {
        return orderBy(column.toString(), SortOrder.ASC);
    }

    // ── UNION ALL ────────────────────────────────────────────────────────

    /**
     * Append a UNION ALL query.
     * Can be chained multiple times for 3+ unions.
     * ORDER BY and LIMIT after UNION ALL apply to the combined result.
     *
     * <pre>{@code
     * query.select("user_id", "amount").from("orders_2024")
     *         .unionAll(
     *                 query.select("user_id", "amount").from("orders_2025"))
     *         .orderBy("amount", SortOrder.DESC)
     *         .limit(10)
     * }</pre>
     *
     * @param other the query to UNION ALL with
     * @return this query builder
     */
    public T unionAll(T other) {
        this.unionQueries.add(other);
        return self();
    }

    // ── LIMIT / OFFSET ──────────────────────────────────────────────────

    /**
     * Set LIMIT for the query.
     *
     * @param limit number of rows to return
     * @return this query builder
     */
    public T limit(int limit) {
        advanceTo(Phase.LIMIT);
        this.limitVal = limit;
        params.addValue("_limit", limit);
        return self();
    }

    /**
     * Set OFFSET for the query (typically used with {@link #limit(int)}).
     *
     * @param offset number of rows to skip
     * @return this query builder
     */
    public T offset(long offset) {
        advanceTo(Phase.LIMIT);
        this.offsetVal = offset;
        params.addValue("_offset", offset);
        return self();
    }

    // ── Extra params ────────────────────────────────────────────────────

    /** Add an extra parameter (for custom SQL fragments). */
    public T param(String name, Object value) {
        params.addValue(name, value);
        return self();
    }

    // ── Build ────────────────────────────────────────────────────────────

    /** Build the SQL string (formatted with newlines for readability). */
    public String toSql() {
        StringBuilder sql = new StringBuilder();

        // WITH (CTE)
        if (!cteList.isEmpty()) {
            sql.append("WITH ");
            for (int i = 0; i < cteList.size(); i++) {
                if (i > 0)
                    sql.append(",\n     ");
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

        // GROUP BY (via abstract method)
        String groupByClause = generateGroupByClause();
        if (!groupByClause.isEmpty()) {
            sql.append("\n").append(groupByClause);
        }

        // HAVING
        if (!havingClauses.isEmpty()) {
            sql.append("\nHAVING ").append(havingClauses.get(0));
            for (int i = 1; i < havingClauses.size(); i++) {
                sql.append("\n  AND ").append(havingClauses.get(i));
            }
        }

        // UNION ALL
        for (BaseQuery<?> union : unionQueries) {
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
        }
        if (offsetVal != null) {
            sql.append(" OFFSET :_offset");
        }

        return sql.toString();
    }

    /** Get the parameter source for this query. */
    public MapSqlParameterSource toParams() {
        return params;
    }

    // ── Execute ──────────────────────────────────────────────────────────

    /**
     * Execute query and return list of mapped results.
     */
    public <R> List<R> query(NamedParameterJdbcTemplate jdbc, RowMapper<R> rowMapper) {
        String sql = toSql();
        return jdbc.query(sql, params, rowMapper);
    }

    /**
     * Execute query and auto-map results to a DTO class or Java record.
     * <ul>
     * <li>For <b>records</b>: uses {@link RecordRowMapper} — maps
     * {@code snake_case} columns
     * to record component names (camelCase). No setters needed.</li>
     * <li>For <b>classes</b>: uses {@link BeanPropertyRowMapper} — requires default
     * constructor + setters.</li>
     * </ul>
     *
     * @param jdbc the JDBC template
     * @param type the DTO class or record
     * @param <R>  the DTO type
     * @return list of mapped DTOs
     */
    @SuppressWarnings("unchecked")
    public <R> List<R> query(NamedParameterJdbcTemplate jdbc, Class<R> type) {
        return query(jdbc, smartMapper(type));
    }

    /**
     * Execute query and return a single DTO result (or null).
     *
     * @param jdbc the JDBC template
     * @param type the DTO class
     * @param <R>  the DTO type
     * @return the mapped DTO, or null if no result
     */
    public <R> R queryOne(NamedParameterJdbcTemplate jdbc, Class<R> type) {
        List<R> results = query(jdbc, type);
        return results.isEmpty() ? null : results.get(0);
    }

    /**
     * Execute a <b>single query</b> that returns both paginated data and total
     * count.
     * Internally appends {@code count(*) OVER() AS _total} to the SELECT, so no
     * second query is needed.
     *
     * @param page      the page index (0-based)
     * @param pageSize  number of rows per page
     * @param jdbc      the JDBC template
     * @param rowMapper the row mapper for data rows
     * @param <R>       the row type
     * @return a {@link Page} containing data + total count
     */
    public <R> Page<R> queryPage(int page, int pageSize, NamedParameterJdbcTemplate jdbc, RowMapper<R> rowMapper) {
        // Apply pagination params first (mutates limitVal / offsetVal + params)
        limit(pageSize);
        offset((long) page * pageSize);

        // Build paginated SQL with count(*) OVER() injected safely via the normal
        // pipeline
        String sql = buildPaginatedSql();

        // Track total from first row
        final long[] totalHolder = { 0 };

        List<R> data = jdbc.query(sql, params, (rs, rowNum) -> {
            if (rowNum == 0) {
                totalHolder[0] = rs.getLong("_total");
            }
            return rowMapper.mapRow(rs, rowNum);
        });

        return new Page<>(data, totalHolder[0], page, pageSize);
    }

    /**
     * Build the paginated SQL by temporarily adding
     * {@code count(*) OVER() AS _total}
     * to the SELECT list, generating the full SQL, then restoring the original
     * list.
     *
     * <p>
     * This avoids the fragile {@link String#replace(CharSequence, CharSequence)}
     * approach
     * that could corrupt SQL when column expressions appear elsewhere (e.g., in
     * subqueries).
     */
    private String buildPaginatedSql() {
        selectColumns.add("count(*) OVER() AS _total");
        try {
            return toSql();
        } finally {
            // Always remove the injected column, even if toSql() throws
            selectColumns.remove(selectColumns.size() - 1);
        }
    }

    /**
     * Single-query pagination with auto DTO mapping.
     *
     * @param page     the page index (0-based)
     * @param pageSize number of rows per page
     * @param jdbc     the JDBC template
     * @param type     the DTO class
     * @param <R>      the DTO type
     * @return a {@link Page} containing auto-mapped DTOs + total count
     */
    @SuppressWarnings("unchecked")
    public <R> Page<R> queryPage(int page, int pageSize, NamedParameterJdbcTemplate jdbc, Class<R> type) {
        return queryPage(page, pageSize, jdbc, smartMapper(type));
    }

    /** Execute query and return a single typed value. */
    public <R> R queryForObject(NamedParameterJdbcTemplate jdbc, Class<R> type) {
        String sql = toSql();
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
        Long result = jdbc.queryForObject(countSql, params, Long.class);
        return result != null ? result : 0;
    }

    // ── Utilities ────────────────────────────────────────────────────────

    /**
     * Smart mapper: returns {@link RecordRowMapper} for records,
     * {@link BeanPropertyRowMapper} for regular classes.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected static <R> RowMapper<R> smartMapper(Class<R> type) {
        if (type.isRecord()) {
            return (RowMapper<R>) RecordRowMapper.of((Class) type);
        }
        return BeanPropertyRowMapper.newInstance(type);
    }
}
