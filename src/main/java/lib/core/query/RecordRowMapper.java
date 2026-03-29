package lib.core.query;

import lib.core.clickhouse.util.CHStringUtils;
import org.springframework.jdbc.core.RowMapper;

import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link RowMapper} implementation for Java {@code record} types.
 * <p>
 * Automatically maps {@code snake_case} column names from the ResultSet
 * to the record's component names (camelCase).
 *
 * <pre>{@code
 * // Given this record:
 * public record OrderHeader(String userId, String orderId, String tenantId) {
 * }
 *
 * // And a query returning columns: user_id, order_id, tenant_id
 * List<SessionHeader> list = query.query(namedJdbc, SessionHeader.class);
 * }</pre>
 *
 * @param <T> the record type
 */
public final class RecordRowMapper<T extends Record> implements RowMapper<T> {

    private final Constructor<T> constructor;
    private final RecordComponent[] components;
    /**
     * Immutable map: camelCase component name → component index (0-based).
     * Built eagerly from record metadata — thread-safe without locking.
     */
    private final Map<String, Integer> componentIndex;

    private RecordRowMapper(Class<T> recordType) {
        this.components = recordType.getRecordComponents();

        // Build component name → index map eagerly (immutable after construction)
        Map<String, Integer> index = new HashMap<>(components.length * 2);
        for (int i = 0; i < components.length; i++) {
            index.put(components[i].getName(), i);
        }
        this.componentIndex = Map.copyOf(index);

        // Build the canonical constructor parameter types
        Class<?>[] paramTypes = new Class<?>[components.length];
        for (int i = 0; i < components.length; i++) {
            paramTypes[i] = components[i].getType();
        }
        try {
            this.constructor = recordType.getDeclaredConstructor(paramTypes);
            this.constructor.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(
                    "No canonical constructor found for record " + recordType.getName(), e);
        }
    }

    /**
     * Create a RecordRowMapper for the given record class.
     *
     * @param recordType the Java record class
     * @param <T>        the record type
     * @return a new RecordRowMapper
     */
    public static <T extends Record> RecordRowMapper<T> of(Class<T> recordType) {
        return new RecordRowMapper<>(recordType);
    }

    @Override
    public T mapRow(ResultSet rs, int rowNum) throws SQLException {
        // Build a per-query mapping: component name → ResultSet column index (1-based)
        // This is done once per query execution (rowNum == 0) and reused for remaining
        // rows.
        // columnToResultSetIndex is local — no shared mutable state.
        Map<String, Integer> colMap = buildResultSetMapping(rs);

        Object[] args = new Object[components.length];
        for (int i = 0; i < components.length; i++) {
            String componentName = components[i].getName();
            Integer rsColIndex = colMap.get(componentName);
            if (rsColIndex != null) {
                args[i] = getValue(rs, rsColIndex, components[i].getType());
            }
            // null/primitive-default if column not present in result set
        }

        try {
            return constructor.newInstance(args);
        } catch (Exception e) {
            throw new SQLException("Failed to instantiate record: " + e.getMessage(), e);
        }
    }

    /**
     * Build a mapping from record component name → ResultSet column index
     * (1-based).
     * Converts ResultSet column labels from snake_case to camelCase, then matches
     * against the pre-built {@link #componentIndex} map.
     * <p>
     * This method is called once per {@link #mapRow} invocation but is stateless
     * (no shared mutable field) — fully thread-safe.
     */
    private Map<String, Integer> buildResultSetMapping(ResultSet rs) throws SQLException {
        var meta = rs.getMetaData();
        Map<String, Integer> map = new HashMap<>(meta.getColumnCount() * 2);
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            String label = meta.getColumnLabel(i);
            String camelCase = toCamelCase(label);
            if (componentIndex.containsKey(camelCase)) {
                map.put(camelCase, i);
            }
        }
        return map;
    }

    /**
     * Extract a typed value from the ResultSet based on the target type.
     */
    private Object getValue(ResultSet rs, int colIndex, Class<?> type) throws SQLException {
        if (rs.getObject(colIndex) == null) {
            return getDefault(type);
        }

        if (type == String.class)
            return rs.getString(colIndex);
        if (type == int.class || type == Integer.class)
            return rs.getInt(colIndex);
        if (type == long.class || type == Long.class)
            return rs.getLong(colIndex);
        if (type == double.class || type == Double.class)
            return rs.getDouble(colIndex);
        if (type == float.class || type == Float.class)
            return rs.getFloat(colIndex);
        if (type == boolean.class || type == Boolean.class)
            return rs.getBoolean(colIndex);
        if (type == BigDecimal.class)
            return rs.getBigDecimal(colIndex);
        if (type == Instant.class) {
            Timestamp ts = rs.getTimestamp(colIndex);
            return ts != null ? ts.toInstant() : null;
        }
        if (type == Timestamp.class)
            return rs.getTimestamp(colIndex);

        // Fallback
        return rs.getObject(colIndex, type);
    }

    /**
     * Return default values for primitive types so records don't fail on null
     * columns.
     */
    private Object getDefault(Class<?> type) {
        if (type == int.class)
            return 0;
        if (type == long.class)
            return 0L;
        if (type == double.class)
            return 0.0;
        if (type == float.class)
            return 0.0f;
        if (type == boolean.class)
            return false;
        return null;
    }

    /** Convert snake_case to camelCase. */
    private static String toCamelCase(String snake) {
        return CHStringUtils.toCamelCase(snake);
    }
}
