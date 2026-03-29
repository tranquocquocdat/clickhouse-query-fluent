package lib.core.query.exception;

/**
 * Exception thrown when a range query has invalid bounds (from > to).
 * 
 * <p>Example scenarios:
 * <ul>
 *   <li>Date range: from = "2024-12-31", to = "2024-01-01" (from > to)</li>
 *   <li>Number range: from = 100, to = 50 (from > to)</li>
 *   <li>String range: from = "Z", to = "A" (from > to)</li>
 * </ul>
 */
public class InvalidRangeException extends RuntimeException {
    
    private final Object from;
    private final Object to;
    private final String column;
    
    public InvalidRangeException(String column, Object from, Object to) {
        super(String.format(
            "Invalid range for column '%s': from (%s) must be less than or equal to to (%s)",
            column, from, to
        ));
        this.column = column;
        this.from = from;
        this.to = to;
    }
    
    public InvalidRangeException(String column, Object from, Object to, String message) {
        super(message);
        this.column = column;
        this.from = from;
        this.to = to;
    }
    
    public Object getFrom() {
        return from;
    }
    
    public Object getTo() {
        return to;
    }
    
    public String getColumn() {
        return column;
    }
}
