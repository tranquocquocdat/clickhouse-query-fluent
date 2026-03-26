package lib.core.clickhouse.query.builder;

import lib.core.clickhouse.query.ClickHouseQuery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.StringJoiner;

/**
 * Fluent builder for OR-grouped WHERE conditions.
 * Created via {@link ClickHouseQuery#whereOr(java.util.function.Consumer)}.
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
public final class OrBuilder {
    private final ClickHouseQuery query;
    private final List<String> conditions = new ArrayList<>();
    private int seq = 0;

    public OrBuilder(ClickHouseQuery query) {
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
    public OrCondition where(String column) {
        return new OrCondition(this, column);
    }

    // ── Legacy API (still works) ────────────────────────────────────────

    /** {@code column = value} (shorthand, legacy) */
    public OrBuilder addEq(String column, Object value) {
        String p = nextParam();
        conditions.add(column + " = :" + p);
        query.params.addValue(p, value);
        return this;
    }

    /** Raw condition with param. */
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

    /** Apply all conditions as a single OR group to the parent query's WHERE. */
    public void apply() {
        if (!conditions.isEmpty()) {
            query.whereClauses.add("(" + String.join(" OR ", conditions) + ")");
        }
    }

    // ── Fluent condition builder ────────────────────────────────────────

    /**
     * Intermediate builder for specifying the operator on a column inside OR.
     * Supports all operators from {@link WhereBuilder}: eq, ne, gt, gte, lt, lte, in, isNull, etc.
     */
    public static final class OrCondition {
        private final OrBuilder or;
        private final String column;

        OrCondition(OrBuilder or, String column) {
            this.or = or;
            this.column = column;
        }

        /** {@code column = value} — skipped when value is null. */
        public OrBuilder eq(Object value) {
            if (value == null) return or;
            String p = or.nextParam();
            or.conditions.add(column + " = :" + p);
            or.query.params.addValue(p, value);
            return or;
        }

        /** {@code column != value} — skipped when value is null. */
        public OrBuilder ne(Object value) {
            if (value == null) return or;
            String p = or.nextParam();
            or.conditions.add(column + " != :" + p);
            or.query.params.addValue(p, value);
            return or;
        }

        /** {@code column > value} — skipped when value is null. */
        public OrBuilder gt(Object value) {
            if (value == null) return or;
            String p = or.nextParam();
            or.conditions.add(column + " > :" + p);
            or.query.params.addValue(p, value);
            return or;
        }

        /** {@code column >= value} — skipped when value is null. */
        public OrBuilder gte(Object value) {
            if (value == null) return or;
            String p = or.nextParam();
            or.conditions.add(column + " >= :" + p);
            or.query.params.addValue(p, value);
            return or;
        }

        /** {@code column < value} — skipped when value is null. */
        public OrBuilder lt(Object value) {
            if (value == null) return or;
            String p = or.nextParam();
            or.conditions.add(column + " < :" + p);
            or.query.params.addValue(p, value);
            return or;
        }

        /** {@code column <= value} — skipped when value is null. */
        public OrBuilder lte(Object value) {
            if (value == null) return or;
            String p = or.nextParam();
            or.conditions.add(column + " <= :" + p);
            or.query.params.addValue(p, value);
            return or;
        }

        /** {@code column IN (v1, v2, ...)} — skipped when values is null or empty. */
        public <T> OrBuilder in(Collection<T> values) {
            if (values == null || values.isEmpty()) return or;
            StringJoiner joiner = new StringJoiner(", ");
            for (T val : values) {
                String p = or.nextParam();
                joiner.add(":" + p);
                or.query.params.addValue(p, val);
            }
            or.conditions.add(column + " IN (" + joiner + ")");
            return or;
        }

        /** {@code column NOT IN (v1, v2, ...)} — skipped when values is null or empty. */
        public <T> OrBuilder notIn(Collection<T> values) {
            if (values == null || values.isEmpty()) return or;
            StringJoiner joiner = new StringJoiner(", ");
            for (T val : values) {
                String p = or.nextParam();
                joiner.add(":" + p);
                or.query.params.addValue(p, val);
            }
            or.conditions.add(column + " NOT IN (" + joiner + ")");
            return or;
        }

        /** {@code column IS NULL} */
        public OrBuilder isNull() {
            or.conditions.add(column + " IS NULL");
            return or;
        }

        /** {@code column IS NOT NULL} */
        public OrBuilder isNotNull() {
            or.conditions.add(column + " IS NOT NULL");
            return or;
        }

        /** {@code column ILIKE :param} — skipped when value is null or blank. */
        public OrBuilder ilike(String value) {
            if (value == null || value.isBlank()) return or;
            String p = or.nextParam();
            or.conditions.add(column + " ILIKE :" + p);
            or.query.params.addValue(p, "%" + value.trim() + "%");
            return or;
        }

        /** {@code column LIKE :param} — skipped when value is null or blank. */
        public OrBuilder like(String value) {
            if (value == null || value.isBlank()) return or;
            String p = or.nextParam();
            or.conditions.add(column + " LIKE :" + p);
            or.query.params.addValue(p, "%" + value.trim() + "%");
            return or;
        }

        /** {@code column IN (subquery)} */
        public OrBuilder in(ClickHouseQuery subQuery) {
            or.conditions.add(column + " IN (" + subQuery.toSql() + ")");
            subQuery.params.getValues().forEach((k, v) -> or.query.params.addValue((String) k, v));
            return or;
        }
    }
}
