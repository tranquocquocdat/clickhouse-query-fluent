package lib.core.clickhouse.query;

import lib.core.clickhouse.util.CHStringUtils;
import org.springframework.jdbc.core.RowMapper;

import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
 * public record OrderHeader(String userId, String orderId, String tenantId) {}
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
    /** Lazy map: snake_case column alias → record component index */
    private volatile Map<String, Integer> columnMapping;

    private RecordRowMapper(Class<T> recordType) {
        this.components = recordType.getRecordComponents();

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
        if (columnMapping == null) {
            columnMapping = buildColumnMapping(rs.getMetaData());
        }

        Object[] args = new Object[components.length];
        for (int i = 0; i < components.length; i++) {
            String componentName = components[i].getName();
            Integer colIndex = columnMapping.get(componentName);
            if (colIndex != null) {
                args[i] = getValue(rs, colIndex, components[i].getType());
            }
            // null if column not found (stays null)
        }

        try {
            return constructor.newInstance(args);
        } catch (Exception e) {
            throw new SQLException("Failed to instantiate record: " + e.getMessage(), e);
        }
    }

    /**
     * Build a mapping from record component name → ResultSet column index.
     * Converts column labels from snake_case to camelCase for matching.
     */
    private Map<String, Integer> buildColumnMapping(ResultSetMetaData meta) throws SQLException {
        Map<String, Integer> map = new HashMap<>();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            String label = meta.getColumnLabel(i);
            String camelCase = toCamelCase(label);
            map.put(camelCase, i);
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

        if (type == String.class) return rs.getString(colIndex);
        if (type == int.class || type == Integer.class) return rs.getInt(colIndex);
        if (type == long.class || type == Long.class) return rs.getLong(colIndex);
        if (type == double.class || type == Double.class) return rs.getDouble(colIndex);
        if (type == float.class || type == Float.class) return rs.getFloat(colIndex);
        if (type == boolean.class || type == Boolean.class) return rs.getBoolean(colIndex);
        if (type == BigDecimal.class) return rs.getBigDecimal(colIndex);
        if (type == Instant.class) {
            Timestamp ts = rs.getTimestamp(colIndex);
            return ts != null ? ts.toInstant() : null;
        }
        if (type == Timestamp.class) return rs.getTimestamp(colIndex);

        // Fallback
        return rs.getObject(colIndex, type);
    }

    /**
     * Return default values for primitive types so records don't fail on null columns.
     */
    private Object getDefault(Class<?> type) {
        if (type == int.class) return 0;
        if (type == long.class) return 0L;
        if (type == double.class) return 0.0;
        if (type == float.class) return 0.0f;
        if (type == boolean.class) return false;
        return null;
    }

    /** Convert snake_case to camelCase. */
    private static String toCamelCase(String snake) {
        return CHStringUtils.toCamelCase(snake);
    }
}
