package lib.core.clickhouse;

import java.time.Instant;
import java.util.Collection;
import java.util.StringJoiner;

/**
 * Fluent builder for WHERE conditions on a specific column.
 * Created via {@link ClickHouseQuery#where(String)}.
 *
 * <pre>{@code
 * .where("tenant_id").eq(tenantId)
 * .where("amount").gt(100)
 * .where("created_at").between(from, to)
 * .where("session_status").eqIfNotBlank(status)
 * .where("product_id").in(productIds)
 * }</pre>
 */
public final class WhereBuilder {
    private final ClickHouseQuery query;
    private final String column;

    WhereBuilder(ClickHouseQuery query, String column) {
        this.query = query;
        this.column = column;
    }

    /** {@code column = :param} — always applied. */
    public ClickHouseQuery eq(Object value) {
        String paramName = ClickHouseQuery.toCamelCase(column);
        query.whereClauses.add(column + " = :" + paramName);
        query.params.addValue(paramName, value);
        return query;
    }

    /** {@code column = :param} — applied only when value is not null and not blank. */
    public ClickHouseQuery eqIfNotBlank(String value) {
        if (value != null && !value.isBlank()) {
            String paramName = ClickHouseQuery.toCamelCase(column);
            query.whereClauses.add(column + " = :" + paramName);
            query.params.addValue(paramName, value);
        }
        return query;
    }

    /** {@code column = :param} — applied only when condition is true. */
    public ClickHouseQuery eqIf(boolean condition, Object value) {
        if (condition) {
            String paramName = ClickHouseQuery.toCamelCase(column);
            query.whereClauses.add(column + " = :" + paramName);
            query.params.addValue(paramName, value);
        }
        return query;
    }

    /** {@code column != :param} */
    public ClickHouseQuery ne(Object value) {
        String paramName = ClickHouseQuery.toCamelCase(column) + "Ne";
        query.whereClauses.add(column + " != :" + paramName);
        query.params.addValue(paramName, value);
        return query;
    }

    /** {@code column > :param} */
    public ClickHouseQuery gt(Object value) {
        String paramName = ClickHouseQuery.toCamelCase(column) + "Gt";
        query.whereClauses.add(column + " > :" + paramName);
        query.params.addValue(paramName, value);
        return query;
    }

    /** {@code column >= :param} */
    public ClickHouseQuery gte(Object value) {
        String paramName = ClickHouseQuery.toCamelCase(column) + "Gte";
        query.whereClauses.add(column + " >= :" + paramName);
        query.params.addValue(paramName, value);
        return query;
    }

    /** {@code column < :param} */
    public ClickHouseQuery lt(Object value) {
        String paramName = ClickHouseQuery.toCamelCase(column) + "Lt";
        query.whereClauses.add(column + " < :" + paramName);
        query.params.addValue(paramName, value);
        return query;
    }

    /** {@code column <= :param} */
    public ClickHouseQuery lte(Object value) {
        String paramName = ClickHouseQuery.toCamelCase(column) + "Lte";
        query.whereClauses.add(column + " <= :" + paramName);
        query.params.addValue(paramName, value);
        return query;
    }

    /**
     * Date range: {@code column >= :colFrom AND column <= :colTo}.
     * Only non-null bounds are applied.
     */
    public ClickHouseQuery between(Instant from, Instant to) {
        String base = ClickHouseQuery.toCamelCase(column);
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
        String prefix = ClickHouseQuery.toCamelCase(column);
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
        String prefix = ClickHouseQuery.toCamelCase(column) + "Not";
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
