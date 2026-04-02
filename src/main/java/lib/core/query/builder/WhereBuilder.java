package lib.core.query.builder;

import java.time.Instant;
import java.util.Collection;
import java.util.StringJoiner;

import lib.core.clickhouse.util.ClickHouseDateUtil;
import lib.core.query.BaseQuery;
import lib.core.query.exception.InvalidRangeException;

/**
 * Fluent builder for WHERE conditions on a specific column.
 * Created via {@link BaseQuery#where(String)}.
 * 
 * @param <T> the concrete query type
 */
public final class WhereBuilder<T extends BaseQuery<T>> {
    private final T query;
    private final String column;

    public WhereBuilder(T query, String column) {
        this.query = query;
        this.column = column;
    }

    /** {@code column = :param} — skipped when value is null or empty string. */
    public T eq(Object value) {
        if (value == null) return query;
        if (value instanceof String && ((String) value).isEmpty()) return query;
        String paramName = toCamelCase(stripTablePrefix(column));
        query.whereClauses.add(column + " = :" + paramName);
        query.params.addValue(paramName, value);
        return query;
    }

    /** {@code column = :param} — applied only when value is not null and not blank. */
    public T eqIfNotBlank(String value) {
        if (value != null && !value.isBlank()) {
            String paramName = toCamelCase(stripTablePrefix(column));
            query.whereClauses.add(column + " = :" + paramName);
            query.params.addValue(paramName, value);
        }
        return query;
    }

    /** {@code column = :param} — applied only when condition is true. */
    public T eqIf(boolean condition, Object value) {
        if (condition) {
            String paramName = toCamelCase(stripTablePrefix(column));
            query.whereClauses.add(column + " = :" + paramName);
            query.params.addValue(paramName, value);
        }
        return query;
    }

    /** {@code column != :param} — skipped when value is null or empty string. */
    public T ne(Object value) {
        if (value == null) return query;
        if (value instanceof String && ((String) value).isEmpty()) return query;
        String paramName = toCamelCase(stripTablePrefix(column)) + "Ne";
        query.whereClauses.add(column + " != :" + paramName);
        query.params.addValue(paramName, value);
        return query;
    }

    /** {@code column > :param} — skipped when value is null or empty string. */
    public T gt(Object value) {
        if (value == null) return query;
        if (value instanceof String && ((String) value).isEmpty()) return query;
        String paramName = toCamelCase(stripTablePrefix(column)) + "Gt";
        query.whereClauses.add(column + " > :" + paramName);
        query.params.addValue(paramName, value);
        return query;
    }

    /** {@code column >= :param} — skipped when value is null or empty string. */
    public T gte(Object value) {
        if (value == null) return query;
        if (value instanceof String && ((String) value).isEmpty()) return query;
        String paramName = toCamelCase(stripTablePrefix(column)) + "Gte";
        query.whereClauses.add(column + " >= :" + paramName);
        query.params.addValue(paramName, value);
        return query;
    }

    /** {@code column < :param} — skipped when value is null or empty string. */
    public T lt(Object value) {
        if (value == null) return query;
        if (value instanceof String && ((String) value).isEmpty()) return query;
        String paramName = toCamelCase(stripTablePrefix(column)) + "Lt";
        query.whereClauses.add(column + " < :" + paramName);
        query.params.addValue(paramName, value);
        return query;
    }

    /** {@code column <= :param} — skipped when value is null or empty string. */
    public T lte(Object value) {
        if (value == null) return query;
        if (value instanceof String && ((String) value).isEmpty()) return query;
        String paramName = toCamelCase(stripTablePrefix(column)) + "Lte";
        query.whereClauses.add(column + " <= :" + paramName);
        query.params.addValue(paramName, value);
        return query;
    }

    /**
     * Date range: {@code column >= :colFrom AND column <= :colTo}.
     * Only non-null bounds are applied.
     * 
     * @throws InvalidRangeException if from > to
     */
    public T between(Instant from, Instant to) {
        // Validate range if both bounds are present
        if (from != null && to != null && from.isAfter(to)) {
            throw new InvalidRangeException(column, from, to);
        }
        
        String base = toCamelCase(stripTablePrefix(column));
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
     * Generic range: {@code column >= :colFrom AND column <= :colTo}.
     * Works with numbers, strings, or any comparable values.
     * Only non-null and non-empty bounds are applied.
     * 
     * @throws InvalidRangeException if from > to (for Comparable types)
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public T between(Object from, Object to) {
        // Validate range if both bounds are present and comparable
        if (from != null && to != null 
            && !(from instanceof String && ((String) from).isEmpty())
            && !(to instanceof String && ((String) to).isEmpty())) {
            
            // Check if both are Comparable and of the same type
            if (from instanceof Comparable && to instanceof Comparable 
                && from.getClass().equals(to.getClass())) {
                try {
                    Comparable comparableFrom = (Comparable) from;
                    if (comparableFrom.compareTo(to) > 0) {
                        throw new InvalidRangeException(column, from, to);
                    }
                } catch (ClassCastException e) {
                    // If comparison fails, skip validation (incompatible types)
                }
            }
        }
        
        String base = toCamelCase(stripTablePrefix(column));
        if (from != null && !(from instanceof String && ((String) from).isEmpty())) {
            String p = base + "From";
            query.whereClauses.add(column + " >= :" + p);
            query.params.addValue(p, from);
        }
        if (to != null && !(to instanceof String && ((String) to).isEmpty())) {
            String p = base + "To";
            query.whereClauses.add(column + " <= :" + p);
            query.params.addValue(p, to);
        }
        return query;
    }

    /**
     * IN clause with auto-expansion.
     * <p>Skipped when values is null or empty.
     */
    public <V> T in(Collection<V> values) {
        if (values == null || values.isEmpty()) return query;
        String prefix = toCamelCase(stripTablePrefix(column));
        StringJoiner joiner = new StringJoiner(", ");
        int i = 0;
        for (V val : values) {
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
     * <p>Skipped when values is null or empty.
     */
    public <V> T notIn(Collection<V> values) {
        if (values == null || values.isEmpty()) return query;
        String prefix = toCamelCase(stripTablePrefix(column)) + "Not";
        StringJoiner joiner = new StringJoiner(", ");
        int i = 0;
        for (V val : values) {
            String pName = prefix + i;
            joiner.add(":" + pName);
            query.params.addValue(pName, val);
            i++;
        }
        query.whereClauses.add(column + " NOT IN (" + joiner + ")");
        return query;
    }

    /** {@code column IS NULL} */
    public T isNull() {
        query.whereClauses.add(column + " IS NULL");
        return query;
    }

    /** {@code column IS NOT NULL} */
    public T isNotNull() {
        query.whereClauses.add(column + " IS NOT NULL");
        return query;
    }

    /**
     * ClickHouse String empty check: {@code column = ''}.
     * Use instead of {@link #isNull()} for non-Nullable String columns,
     * where LEFT JOIN misses produce '' instead of NULL.
     */
    public T isEmpty() {
        query.whereClauses.add(column + " = ''");
        return query;
    }

    /**
     * ClickHouse String non-empty check: {@code column != ''}.
     * Use instead of {@link #isNotNull()} for non-Nullable String columns,
     * where LEFT JOIN misses produce '' instead of NULL.
     */
    public T isNotEmpty() {
        query.whereClauses.add(column + " != ''");
        return query;
    }

    /** {@code column IN (subquery)} — raw SQL string. */
    public T inSubQuery(String subQuery) {
        query.whereClauses.add(column + " IN (" + subQuery + ")");
        return query;
    }

    /** {@code column NOT IN (subquery)} — raw SQL string. */
    public T notInSubQuery(String subQuery) {
        query.whereClauses.add(column + " NOT IN (" + subQuery + ")");
        return query;
    }

    /** Fluent {@code column IN (subquery)} using a BaseQuery. */
    public T in(BaseQuery<?> subQuery) {
        query.whereClauses.add(column + " IN (" + subQuery.toSql() + ")");
        subQuery.params.getValues().forEach((k, v) -> query.params.addValue((String) k, v));
        return query;
    }

    /** Fluent {@code column NOT IN (subquery)} using a BaseQuery. */
    public T notIn(BaseQuery<?> subQuery) {
        query.whereClauses.add(column + " NOT IN (" + subQuery.toSql() + ")");
        subQuery.params.getValues().forEach((k, v) -> query.params.addValue((String) k, v));
        return query;
    }

    /**
     * Strip table prefix from column name for param naming.
     * Example: "o.user_id" → "user_id", "user_id" → "user_id"
     */
    private String stripTablePrefix(String column) {
        return column.contains(".") 
            ? column.substring(column.lastIndexOf('.') + 1) 
            : column;
    }

    /** Convert snake_case to camelCase for parameter naming. */
    private static String toCamelCase(String snake) {
        if (snake == null || snake.isEmpty()) return snake;
        StringBuilder result = new StringBuilder();
        boolean capitalizeNext = false;
        for (char c : snake.toCharArray()) {
            if (c == '_') {
                capitalizeNext = true;
            } else {
                result.append(capitalizeNext ? Character.toUpperCase(c) : c);
                capitalizeNext = false;
            }
        }
        return result.toString();
    }
}
