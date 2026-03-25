package lib.core.clickhouse;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringJoiner;
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
 *     .orderBy("total", "DESC")
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

    private final List<String> selectColumns = new ArrayList<>();
    private boolean distinct;
    private String tableName;
    private final List<String> joinClauses = new ArrayList<>();
    private final List<String> whereClauses = new ArrayList<>();
    private final MapSqlParameterSource params = new MapSqlParameterSource();
    private final List<String> groupByColumns = new ArrayList<>();
    private final List<String> havingClauses = new ArrayList<>();
    private final List<String> orderByClauses = new ArrayList<>();
    private Integer limitVal;
    private Long offsetVal;

    private ClickHouseQuery() {}

    /**
     * Advance to the given phase. Same phase is allowed (e.g. multiple {@code .where()} calls).
     * Going backward throws {@link IllegalStateException}.
     */
    private void advanceTo(Phase target) {
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

    // ── JoinBuilder ──────────────────────────────────────────────────────

    /**
     * Fluent builder for JOIN conditions.
     * Created via {@link ClickHouseQuery#join(String)}, {@link #leftJoin(String)}, or {@link #rightJoin(String)}.
     *
     * <h3>Usage</h3>
     * <pre>{@code
     * // Fluent equality (most common)
     * .join("user_profile u").on("u.id", "t.user_id")
     *
     * // Raw condition (complex cases)
     * .join("user_profile u").on("u.id = t.user_id AND u.active = 1")
     * }</pre>
     */
    public static final class JoinBuilder {
        private final ClickHouseQuery query;
        private final String joinType;
        private final String table;

        JoinBuilder(ClickHouseQuery query, String joinType, String table) {
            this.query = query;
            this.joinType = joinType;
            this.table = table;
        }

        /**
         * Fluent ON with equality: {@code ON leftCol = rightCol}.
         *
         * <pre>{@code
         * .join("user_profile u").on("u.id", "t.user_id")
         * // → JOIN user_profile u ON u.id = t.user_id
         * }</pre>
         *
         * @param leftColumn  the left column (e.g. {@code "u.id"})
         * @param rightColumn the right column (e.g. {@code "t.user_id"})
         * @return the parent query builder
         */
        public ClickHouseQuery on(String leftColumn, String rightColumn) {
            query.joinClauses.add(joinType + " " + table + " ON " + leftColumn + " = " + rightColumn);
            return query;
        }

        /**
         * Raw ON condition for complex cases.
         *
         * <pre>{@code
         * .join("user_profile u").on("u.id = t.user_id AND u.active = 1")
         * }</pre>
         *
         * @param condition the raw join condition
         * @return the parent query builder
         */
        public ClickHouseQuery on(String condition) {
            query.joinClauses.add(joinType + " " + table + " ON " + condition);
            return query;
        }
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

    // ── WhereBuilder (column-first chaining) ────────────────────────────

    /**
     * Fluent builder for WHERE conditions on a specific column.
     * Created via {@link ClickHouseQuery#where(String)}.
     */
    public static final class WhereBuilder {
        private final ClickHouseQuery query;
        private final String column;

        WhereBuilder(ClickHouseQuery query, String column) {
            this.query = query;
            this.column = column;
        }

        /** {@code column = :param} — always applied. */
        public ClickHouseQuery eq(Object value) {
            String paramName = toCamelCase(column);
            query.whereClauses.add(column + " = :" + paramName);
            query.params.addValue(paramName, value);
            return query;
        }

        /** {@code column = :param} — applied only when value is not null and not blank. */
        public ClickHouseQuery eqIfNotBlank(String value) {
            if (value != null && !value.isBlank()) {
                String paramName = toCamelCase(column);
                query.whereClauses.add(column + " = :" + paramName);
                query.params.addValue(paramName, value);
            }
            return query;
        }

        /** {@code column = :param} — applied only when condition is true. */
        public ClickHouseQuery eqIf(boolean condition, Object value) {
            if (condition) {
                String paramName = toCamelCase(column);
                query.whereClauses.add(column + " = :" + paramName);
                query.params.addValue(paramName, value);
            }
            return query;
        }

        /** {@code column != :param} */
        public ClickHouseQuery ne(Object value) {
            String paramName = toCamelCase(column) + "Ne";
            query.whereClauses.add(column + " != :" + paramName);
            query.params.addValue(paramName, value);
            return query;
        }

        /** {@code column > :param} */
        public ClickHouseQuery gt(Object value) {
            String paramName = toCamelCase(column) + "Gt";
            query.whereClauses.add(column + " > :" + paramName);
            query.params.addValue(paramName, value);
            return query;
        }

        /** {@code column >= :param} */
        public ClickHouseQuery gte(Object value) {
            String paramName = toCamelCase(column) + "Gte";
            query.whereClauses.add(column + " >= :" + paramName);
            query.params.addValue(paramName, value);
            return query;
        }

        /** {@code column < :param} */
        public ClickHouseQuery lt(Object value) {
            String paramName = toCamelCase(column) + "Lt";
            query.whereClauses.add(column + " < :" + paramName);
            query.params.addValue(paramName, value);
            return query;
        }

        /** {@code column <= :param} */
        public ClickHouseQuery lte(Object value) {
            String paramName = toCamelCase(column) + "Lte";
            query.whereClauses.add(column + " <= :" + paramName);
            query.params.addValue(paramName, value);
            return query;
        }

        /**
         * Date range: {@code column >= :colFrom AND column <= :colTo}.
         * Only non-null bounds are applied.
         */
        public ClickHouseQuery between(Instant from, Instant to) {
            String base = toCamelCase(column);
            if (from != null) {
                String p = base + "From";
                query.whereClauses.add(column + " >= :" + p);
                query.params.addValue(p, ClickHouseDateUtil.format(from));
            }
            if (to != null) {
                String p = base + "To";
                query.whereClauses.add(column + " <= :" + p);
                query.params.addValue(p, ClickHouseDateUtil.format(to));
            }
            return query;
        }

        /**
         * IN clause with auto-expansion.
         * <p>Generates individual params: {@code :col0, :col1, :col2...}
         * <p>Skipped when values is null or empty.
         */
        public <T> ClickHouseQuery in(Collection<T> values) {
            if (values == null || values.isEmpty()) return query;
            String prefix = toCamelCase(column);
            StringJoiner joiner = new StringJoiner(", ");
            int i = 0;
            for (T val : values) {
                String pName = prefix + i;
                joiner.add(":" + pName);
                query.params.addValue(pName, val);
                i++;
            }
            query.whereClauses.add(column + " IN (" + joiner + ")");
            return query;
        }

        /**
         * NOT IN clause with auto-expansion.
         * <p>Generates individual params: {@code :colNot0, :colNot1...}
         * <p>Skipped when values is null or empty.
         */
        public <T> ClickHouseQuery notIn(Collection<T> values) {
            if (values == null || values.isEmpty()) return query;
            String prefix = toCamelCase(column) + "Not";
            StringJoiner joiner = new StringJoiner(", ");
            int i = 0;
            for (T val : values) {
                String pName = prefix + i;
                joiner.add(":" + pName);
                query.params.addValue(pName, val);
                i++;
            }
            query.whereClauses.add(column + " NOT IN (" + joiner + ")");
            return query;
        }

        /** {@code column IS NULL} */
        public ClickHouseQuery isNull() {
            query.whereClauses.add(column + " IS NULL");
            return query;
        }

        /** {@code column IS NOT NULL} */
        public ClickHouseQuery isNotNull() {
            query.whereClauses.add(column + " IS NOT NULL");
            return query;
        }

        /** {@code column IN (subquery)} — raw SQL string. */
        public ClickHouseQuery inSubQuery(String subQuery) {
            query.whereClauses.add(column + " IN (" + subQuery + ")");
            return query;
        }

        /** {@code column NOT IN (subquery)} — raw SQL string. */
        public ClickHouseQuery notInSubQuery(String subQuery) {
            query.whereClauses.add(column + " NOT IN (" + subQuery + ")");
            return query;
        }

        /**
         * Fluent {@code column IN (subquery)} using a ClickHouseQuery.
         * <pre>{@code
         * .where("product_id").in(
         *     ClickHouseQuery.select("id").from("games").where("active").eq(1)
         * )
         * }</pre>
         *
         * @param subQuery the inner query builder
         * @return the parent query builder
         */
        public ClickHouseQuery in(ClickHouseQuery subQuery) {
            query.whereClauses.add(column + " IN (" + subQuery.toSql() + ")");
            subQuery.params.getValues().forEach((k, v) -> query.params.addValue((String) k, v));
            return query;
        }

        /**
         * Fluent {@code column NOT IN (subquery)} using a ClickHouseQuery.
         * <pre>{@code
         * .where("user_id").notIn(
         *     ClickHouseQuery.select("id").from("banned_users").where("active").eq(1)
         * )
         * }</pre>
         *
         * @param subQuery the inner query builder
         * @return the parent query builder
         */
        public ClickHouseQuery notIn(ClickHouseQuery subQuery) {
            query.whereClauses.add(column + " NOT IN (" + subQuery.toSql() + ")");
            subQuery.params.getValues().forEach((k, v) -> query.params.addValue((String) k, v));
            return query;
        }
    }

    // ── WhereILikeBuilder ───────────────────────────────────────────────

    /**
     * Fluent builder for ILIKE search across multiple columns.
     * Created via {@link ClickHouseQuery#whereILike(String)}.
     */
    public static final class WhereILikeBuilder {
        private final ClickHouseQuery query;
        private final String keyword;
        private final boolean caseSensitive;

        WhereILikeBuilder(ClickHouseQuery query, String keyword, boolean caseSensitive) {
            this.query = query;
            this.keyword = keyword;
            this.caseSensitive = caseSensitive;
        }

        /**
         * Apply LIKE/ILIKE search across the given columns (combined with OR).
         * Skipped when keyword is null or blank.
         *
         * @param columns the columns to search on
         * @return the parent query builder
         */
        public ClickHouseQuery on(String... columns) {
            if (keyword == null || keyword.isBlank()) return query;
            String operator = caseSensitive ? "LIKE" : "ILIKE";
            String paramName = caseSensitive ? "_likeKeyword" : "_keyword";
            StringJoiner or = new StringJoiner(" OR ", "(", ")");
            for (String col : columns) {
                or.add(col + " " + operator + " :" + paramName);
            }
            query.whereClauses.add(or.toString());
            query.params.addValue(paramName, "%" + keyword.trim() + "%");
            return query;
        }
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

    // ── OrBuilder ────────────────────────────────────────────────────────

    /** Builder for OR-grouped WHERE conditions. */
    public static final class OrBuilder {
        private final ClickHouseQuery query;
        private final List<String> conditions = new ArrayList<>();
        private static int orSeq = 0;

        OrBuilder(ClickHouseQuery query) {
            this.query = query;
        }

        /** {@code column = value} */
        public OrBuilder add(String column, Object value) {
            String p = "_or" + (orSeq++);
            conditions.add(column + " = :" + p);
            query.params.addValue(p, value);
            return this;
        }

        /** Raw condition with param: {@code or.addRaw("amount > :minAmt", "minAmt", 100)} */
        public OrBuilder addRaw(String condition, String paramName, Object value) {
            conditions.add(condition);
            query.params.addValue(paramName, value);
            return this;
        }

        /** Raw condition without param. */
        public OrBuilder addRaw(String condition) {
            conditions.add(condition);
            return this;
        }

        void apply() {
            if (!conditions.isEmpty()) {
                query.whereClauses.add("(" + String.join(" OR ", conditions) + ")");
            }
        }
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

    // ── HavingBuilder ───────────────────────────────────────────────────

    /** Fluent builder for HAVING conditions. Created via {@link ClickHouseQuery#having}. */
    public static final class HavingBuilder {
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

    /**
     * ORDER BY with safe direction validation (defaults to DESC for invalid values).
     * Supports multiple calls for multiple columns.
     *
     * <pre>{@code
     * .orderBy("total", "DESC")
     * .orderBy("user_id", "ASC")
     * // → ORDER BY total DESC, user_id ASC
     * }</pre>
     *
     * @param column    the column to order by
     * @param direction "ASC" or "DESC" (case-insensitive, defaults to DESC)
     * @return this query builder
     */
    public ClickHouseQuery orderBy(String column, String direction) {
        advanceTo(Phase.ORDER_BY);
        String safeDir = "ASC".equalsIgnoreCase(direction) ? "ASC" : "DESC";
        this.orderByClauses.add(column + " " + safeDir);
        return this;
    }

    /** ORDER BY column ASC. */
    public ClickHouseQuery orderBy(String column) {
        return orderBy(column, "ASC");
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

    /**
     * Holder for a {@code SELECT COUNT(*) FROM (subquery)} pattern.
     * Created via {@link ClickHouseQuery#count(ClickHouseQuery)}.
     */
    public static final class CountQuery {
        private final ClickHouseQuery subQuery;

        CountQuery(ClickHouseQuery subQuery) {
            this.subQuery = subQuery;
        }

        /**
         * Execute the count query.
         *
         * @param jdbc the JDBC template
         * @return the total count
         */
        public long execute(NamedParameterJdbcTemplate jdbc) {
            String sql = "SELECT count(*) FROM (" + subQuery.toSql() + ")";
            Long result = jdbc.queryForObject(sql, subQuery.params, Long.class);
            return result != null ? result : 0;
        }

        /** Get the generated SQL (useful for debugging/testing). */
        public String toSql() {
            return "SELECT count(*) FROM (" + subQuery.toSql() + ")";
        }
    }

    // ── Utilities ────────────────────────────────────────────────────────

    /** Convert snake_case to camelCase for parameter naming. */
    static String toCamelCase(String snake) {
        String[] parts = snake.split("_");
        StringBuilder sb = new StringBuilder(parts[0]);
        for (int i = 1; i < parts.length; i++) {
            sb.append(Character.toUpperCase(parts[i].charAt(0)));
            sb.append(parts[i].substring(1));
        }
        return sb.toString();
    }
}
