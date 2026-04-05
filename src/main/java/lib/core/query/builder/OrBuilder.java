package lib.core.query.builder;

import lib.core.query.BaseQuery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringJoiner;

/**
 * Fluent builder for OR-grouped WHERE conditions.
 * Created via {@link BaseQuery#whereOr(java.util.function.Consumer)}.
 *
 * <pre>{@code
 * .whereOr(or -> or
 *     .where("status").eq("ACTIVE")
 *     .where("status").eq("PENDING")
 *     .where("amount").gt(1000)
 *     .where("type").in(List.of("SALE", "UPSELL"))
 * )
 * // → (status = :_or0 OR status = :_or1 OR amount > :_or2 OR type IN (:_or3, :_or4))
 * }</pre>
 */
public final class OrBuilder<T extends BaseQuery<T>> {
    private final T query;
    private final List<String> conditions = new ArrayList<>();
    private int seq = 0;

    public OrBuilder(T query) {
        this.query = query;
    }

    private String nextParam() {
        return "_or" + (seq++);
    }

    // ── Fluent column-first API ─────────────────────────────────────────

    /**
     * Start a fluent OR condition on a column.
     * <pre>{@code
     * .whereOr(or -> or
     *     .where("status").eq("ACTIVE")
     *     .where("amount").gt(1000)
     * )
     * }</pre>
     *
     * @param column the column name
     * @return an {@link OrCondition} for specifying the operator
     */
    public OrCondition<T> where(String column) {
        return new OrCondition<>(this, column);
    }

    /** Start a fluent OR condition on a column (Expr overload). */
    public OrCondition<T> where(lib.core.query.expression.CommonFunctions.Expr column) {
        return new OrCondition<>(this, column.toString());
    }

    // ── Legacy API (still works) ────────────────────────────────────────

    /** {@code column = value} (shorthand, legacy) */
    public OrBuilder<T> addEq(String column, Object value) {
        String p = nextParam();
        conditions.add(column + " = :" + p);
        query.params.addValue(p, value);
        return this;
    }

    /** Raw condition with param. */
    public OrBuilder<T> addRaw(String condition, String paramName, Object value) {
        conditions.add(condition);
        query.params.addValue(paramName, value);
        return this;
    }

    /** Raw condition without param. */
    public OrBuilder<T> addRaw(String condition) {
        conditions.add(condition);
        return this;
    }

    /**
     * Fluent ILIKE search on multiple columns inside an OR group.
     * <pre>{@code
     * .whereOr(or -> or
     *     .where("status").eq("ACTIVE")
     *     .whereILike("keyword").on("session_id", "user_id", "order_id")
     * )
     * // → (status = :_or0 OR session_id ILIKE :_or1 OR user_id ILIKE :_or2 OR order_id ILIKE :_or3)
     * }</pre>
     */
    public OrILikeBuilder<T> whereILike(String keyword) {
        return new OrILikeBuilder<>(this, keyword);
    }

    /** Apply all conditions as a single OR group to the parent query's WHERE. */
    public void apply() {
        if (!conditions.isEmpty()) {
            query.whereClauses.add("(" + String.join(" OR ", conditions) + ")");
        }
    }

    /** Inner builder for ILIKE on multiple columns inside OR group. */
    public static final class OrILikeBuilder<T extends BaseQuery<T>> {
        private final OrBuilder<T> or;
        private final String keyword;

        OrILikeBuilder(OrBuilder<T> or, String keyword) {
            this.or = or;
            this.keyword = keyword;
        }

        /** Specify columns to search with ILIKE. Skipped when keyword is null/blank. */
        public OrBuilder<T> on(String... columns) {
            if (keyword == null || keyword.isBlank()) return or;
            String trimmed = keyword.trim();
            for (String col : columns) {
                String p = or.nextParam();
                or.conditions.add(col + " ILIKE :" + p);
                or.query.params.addValue(p, "%" + trimmed + "%");
            }
            return or;
        }

        /** Specify columns to search with ILIKE (Expr/Object overload). */
        public OrBuilder<T> on(Object... columns) {
            if (keyword == null || keyword.isBlank()) return or;
            String trimmed = keyword.trim();
            for (Object col : columns) {
                String p = or.nextParam();
                or.conditions.add(col.toString() + " ILIKE :" + p);
                or.query.params.addValue(p, "%" + trimmed + "%");
            }
            return or;
        }
    }

    // ── Fluent condition builder ────────────────────────────────────────

    /**
     * Intermediate builder for specifying the operator on a column inside OR.
     * Supports all operators from {@link WhereBuilder}: eq, ne, gt, gte, lt, lte, in, isNull, etc.
     */
    public static final class OrCondition<T extends BaseQuery<T>> {
        private final OrBuilder<T> or;
        private final String column;

        OrCondition(OrBuilder<T> or, String column) {
            this.or = or;
            this.column = column;
        }

        /** {@code column = value} — skipped when value is null or empty string. */
        public OrBuilder<T> eq(Object value) {
            if (value == null) return or;
            if (value instanceof String && ((String) value).isEmpty()) return or;
            String p = or.nextParam();
            or.conditions.add(column + " = :" + p);
            or.query.params.addValue(p, value);
            return or;
        }

        /** {@code column != value} — skipped when value is null or empty string. */
        public OrBuilder<T> ne(Object value) {
            if (value == null) return or;
            if (value instanceof String && ((String) value).isEmpty()) return or;
            String p = or.nextParam();
            or.conditions.add(column + " != :" + p);
            or.query.params.addValue(p, value);
            return or;
        }

        /** {@code column > value} — skipped when value is null or empty string. */
        public OrBuilder<T> gt(Object value) {
            if (value == null) return or;
            if (value instanceof String && ((String) value).isEmpty()) return or;
            String p = or.nextParam();
            or.conditions.add(column + " > :" + p);
            or.query.params.addValue(p, value);
            return or;
        }

        /** {@code column >= value} — skipped when value is null or empty string. */
        public OrBuilder<T> gte(Object value) {
            if (value == null) return or;
            if (value instanceof String && ((String) value).isEmpty()) return or;
            String p = or.nextParam();
            or.conditions.add(column + " >= :" + p);
            or.query.params.addValue(p, value);
            return or;
        }

        /** {@code column < value} — skipped when value is null or empty string. */
        public OrBuilder<T> lt(Object value) {
            if (value == null) return or;
            if (value instanceof String && ((String) value).isEmpty()) return or;
            String p = or.nextParam();
            or.conditions.add(column + " < :" + p);
            or.query.params.addValue(p, value);
            return or;
        }

        /** {@code column <= value} — skipped when value is null or empty string. */
        public OrBuilder<T> lte(Object value) {
            if (value == null) return or;
            if (value instanceof String && ((String) value).isEmpty()) return or;
            String p = or.nextParam();
            or.conditions.add(column + " <= :" + p);
            or.query.params.addValue(p, value);
            return or;
        }

        /** {@code column IN (v1, v2, ...)} — skipped when values is null or empty. */
        public <V> OrBuilder<T> in(Collection<V> values) {
            if (values == null || values.isEmpty()) return or;
            StringJoiner joiner = new StringJoiner(", ");
            for (V val : values) {
                String p = or.nextParam();
                joiner.add(":" + p);
                or.query.params.addValue(p, val);
            }
            or.conditions.add(column + " IN (" + joiner + ")");
            return or;
        }

        /** {@code column NOT IN (v1, v2, ...)} — skipped when values is null or empty. */
        public <V> OrBuilder<T> notIn(Collection<V> values) {
            if (values == null || values.isEmpty()) return or;
            StringJoiner joiner = new StringJoiner(", ");
            for (V val : values) {
                String p = or.nextParam();
                joiner.add(":" + p);
                or.query.params.addValue(p, val);
            }
            or.conditions.add(column + " NOT IN (" + joiner + ")");
            return or;
        }

        /** {@code column IS NULL} */
        public OrBuilder<T> isNull() {
            or.conditions.add(column + " IS NULL");
            return or;
        }

        /** {@code column IS NOT NULL} */
        public OrBuilder<T> isNotNull() {
            or.conditions.add(column + " IS NOT NULL");
            return or;
        }

        /**
         * {@code column >= from AND column <= to} — skipped when both are null.
         * If only one bound is null, generates a one-sided condition.
         * 
         * @throws lib.core.query.exception.InvalidRangeException if from > to
         */
        public OrBuilder<T> between(java.time.Instant from, java.time.Instant to) {
            if (from == null && to == null) return or;
            
            // Validate range if both bounds are present
            if (from != null && to != null && from.isAfter(to)) {
                throw new lib.core.query.exception.InvalidRangeException(column, from, to);
            }
            
            if (from != null && to != null) {
                String pFrom = or.nextParam();
                String pTo = or.nextParam();
                or.conditions.add("(" + column + " >= :" + pFrom + " AND " + column + " <= :" + pTo + ")");
                or.query.params.addValue(pFrom, lib.core.clickhouse.util.ClickHouseDateUtil.format(from));
                or.query.params.addValue(pTo, lib.core.clickhouse.util.ClickHouseDateUtil.format(to));
            } else if (from != null) {
                String p = or.nextParam();
                or.conditions.add(column + " >= :" + p);
                or.query.params.addValue(p, lib.core.clickhouse.util.ClickHouseDateUtil.format(from));
            } else {
                String p = or.nextParam();
                or.conditions.add(column + " <= :" + p);
                or.query.params.addValue(p, lib.core.clickhouse.util.ClickHouseDateUtil.format(to));
            }
            return or;
        }

        /** {@code column ILIKE :param} — skipped when value is null or blank. */
        public OrBuilder<T> ilike(String value) {
            if (value == null || value.isBlank()) return or;
            String p = or.nextParam();
            or.conditions.add(column + " ILIKE :" + p);
            or.query.params.addValue(p, "%" + value.trim() + "%");
            return or;
        }

        /** {@code column LIKE :param} — skipped when value is null or blank. */
        public OrBuilder<T> like(String value) {
            if (value == null || value.isBlank()) return or;
            String p = or.nextParam();
            or.conditions.add(column + " LIKE :" + p);
            or.query.params.addValue(p, "%" + value.trim() + "%");
            return or;
        }

        /** {@code column IN (subquery)} */
        public OrBuilder<T> in(T subQuery) {
            or.conditions.add(column + " IN (" + subQuery.toSql() + ")");
            subQuery.params.getValues().forEach((k, v) -> or.query.params.addValue((String) k, v));
            return or;
        }
    }
}
