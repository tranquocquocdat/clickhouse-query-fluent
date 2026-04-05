package lib.core.query.util;

import java.util.regex.Pattern;

/**
 * Validates SQL column names to prevent SQL injection.
 * 
 * <p>Valid column names must match one of these patterns:
 * <ul>
 *   <li>Simple: {@code user_id}, {@code amount}, {@code created_at}</li>
 *   <li>Qualified: {@code orders.user_id}, {@code t.amount}</li>
 *   <li>Function calls: {@code sum(amount)}, {@code count(*)}</li>
 *   <li>Expressions: {@code amount * 1.1}, {@code (revenue - cost)}</li>
 * </ul>
 * 
 * <p>Rejects dangerous patterns like:
 * <ul>
 *   <li>SQL keywords: {@code DROP}, {@code DELETE}, {@code UPDATE}</li>
 *   <li>Comments: {@code --}, {@code /*}, {@code */}</li>
 *   <li>Semicolons: {@code ;}</li>
 * </ul>
 */
public final class ColumnValidator {
    
    private ColumnValidator() {}
    
    // Valid column name patterns
    private static final Pattern VALID_COLUMN = Pattern.compile(
        "^[a-zA-Z_][a-zA-Z0-9_]*(\\.[a-zA-Z_][a-zA-Z0-9_]*)?$"
    );
    
    // Valid expression patterns (allows functions, operators, parentheses)
    private static final Pattern VALID_EXPRESSION = Pattern.compile(
        "^[a-zA-Z0-9_.*+\\-/()\\s,]+$"
    );
    
    // Dangerous SQL keywords that should never appear in column names
    private static final Pattern DANGEROUS_KEYWORDS = Pattern.compile(
        "(?i)\\b(DROP|DELETE|UPDATE|INSERT|TRUNCATE|ALTER|CREATE|EXEC|EXECUTE)\\b"
    );
    
    // SQL comment patterns
    private static final Pattern SQL_COMMENTS = Pattern.compile(
        "--|\\/\\*|\\*\\/"
    );
    
    /**
     * Validates a column name or expression.
     * 
     * @param column the column name or expression to validate
     * @throws IllegalArgumentException if the column name is invalid or potentially dangerous
     */
    public static void validate(String column) {
        if (column == null || column.trim().isEmpty()) {
            throw new IllegalArgumentException("Column name cannot be null or empty");
        }
        
        String trimmed = column.trim();
        
        // Check for SQL comments
        if (SQL_COMMENTS.matcher(trimmed).find()) {
            throw new IllegalArgumentException(
                "Invalid column name: contains SQL comment pattern: " + column
            );
        }
        
        // Check for dangerous keywords
        if (DANGEROUS_KEYWORDS.matcher(trimmed).find()) {
            throw new IllegalArgumentException(
                "Invalid column name: contains dangerous SQL keyword: " + column
            );
        }
        
        // Check for semicolons (statement separator)
        if (trimmed.contains(";")) {
            throw new IllegalArgumentException(
                "Invalid column name: contains semicolon: " + column
            );
        }
        
        // Allow either simple column names or valid expressions
        if (!VALID_COLUMN.matcher(trimmed).matches() && 
            !VALID_EXPRESSION.matcher(trimmed).matches()) {
            throw new IllegalArgumentException(
                "Invalid column name or expression: " + column
            );
        }
    }
    
    /**
     * Validates a column name and returns it if valid.
     * Convenience method for inline validation.
     * 
     * @param column the column name to validate
     * @return the validated column name
     * @throws IllegalArgumentException if invalid
     */
    public static String validated(String column) {
        validate(column);
        return column;
    }
}
