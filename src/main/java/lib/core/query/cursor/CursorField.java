package lib.core.query.cursor;

import java.util.Objects;

/**
 * A single cursor key field — a pair of {@code (column, value)}.
 *
 * <p>
 * Users supply a list of {@code CursorField}s via a {@code keyExtractor}
 * lambda to tell the library which columns and values to use when encoding
 * the next-page cursor token.
 *
 * <pre>{@code
 * row -> List.of(
 *         CursorField.of("event_date", row.getEventDate()),
 *         CursorField.of("id", row.getId()))
 * }</pre>
 */
public final class CursorField {

    private final String column;
    private final Object value;

    private CursorField(String column, Object value) {
        this.column = Objects.requireNonNull(column, "column must not be null");
        this.value = value;
    }

    /** Factory method. */
    public static CursorField of(String column, Object value) {
        return new CursorField(column, value);
    }

    /** The DB column name (e.g. {@code "event_date"}). */
    public String getColumn() {
        return column;
    }

    /** The value of the column in the last row of the current page. */
    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "CursorField{" + column + "=" + value + "}";
    }
}
