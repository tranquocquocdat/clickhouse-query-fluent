package lib.core.query.expression;

/**
 * Base class providing common SQL functions and expression building
 * that work across all relational databases.
 * 
 * Subclasses can extend this to add database-specific functions.
 */
public class CommonFunctions {
    
    protected CommonFunctions() {}
    
    /**
     * Expression wrapper class that supports method chaining and aliasing.
     */
    public static class Expr {
        protected final String expression;
        
        public Expr(String expression) {
            this.expression = expression;
        }
        
        /**
         * Add an alias to this expression.
         */
        public Expr as(String alias) {
            return new Expr(expression + " AS " + alias);
        }
        
        /**
         * Subtract another expression from this one.
         */
        public Expr minus(Expr other) {
            return new Expr("(" + expression + " - " + other.expression + ")");
        }
        
        public Expr minus(String raw) {
            return new Expr("(" + expression + " - " + raw + ")");
        }
        
        /**
         * Add another expression to this one.
         */
        public Expr plus(Expr other) {
            return new Expr("(" + expression + " + " + other.expression + ")");
        }
        
        public Expr plus(String raw) {
            return new Expr("(" + expression + " + " + raw + ")");
        }
        
        /**
         * Multiply this expression by another.
         */
        public Expr multiply(Expr other) {
            return new Expr("(" + expression + " * " + other.expression + ")");
        }
        
        public Expr multiply(String raw) {
            return new Expr("(" + expression + " * " + raw + ")");
        }
        
        /**
         * Divide this expression by another.
         */
        public Expr divide(Expr other) {
            return new Expr("(" + expression + " / " + other.expression + ")");
        }
        
        public Expr divide(String raw) {
            return new Expr("(" + expression + " / " + raw + ")");
        }
        
        /**
         * Start building a window function with OVER clause.
         */
        public WindowBuilder over() {
            return new WindowBuilder(expression);
        }
        
        @Override
        public String toString() {
            return expression;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof Expr)) return false;
            return expression.equals(((Expr) obj).expression);
        }
        
        @Override
        public int hashCode() {
            return expression.hashCode();
        }
    }
    
    // Standard aggregates
    
    /**
     * COUNT(*) aggregate function.
     */
    public static Expr count() {
        return new Expr("COUNT(*)");
    }
    
    /**
     * COUNT(column) aggregate function.
     */
    public static Expr count(String column) {
        return new Expr("COUNT(" + column + ")");
    }
    
    /**
     * COUNT(DISTINCT column) aggregate function (single column).
     */
    public static Expr countDistinct(String column) {
        return new Expr("COUNT(DISTINCT " + column + ")");
    }
    
    public static Expr countDistinct(Expr column) {
        return new Expr("COUNT(DISTINCT " + column.expression + ")");
    }
    
    /**
     * SUM(column) aggregate function.
     */
    public static Expr sum(String column) {
        return new Expr("SUM(" + column + ")");
    }
    
    /**
     * MIN(column) aggregate function.
     */
    public static Expr min(String column) {
        return new Expr("MIN(" + column + ")");
    }
    
    /**
     * MAX(column) aggregate function.
     */
    public static Expr max(String column) {
        return new Expr("MAX(" + column + ")");
    }
    
    /**
     * AVG(column) aggregate function.
     */
    public static Expr avg(String column) {
        return new Expr("AVG(" + column + ")");
    }
    
    // Window functions
    
    /**
     * ROW_NUMBER() window function.
     */
    public static Expr rowNumber() {
        return new Expr("ROW_NUMBER()");
    }
    
    /**
     * RANK() window function.
     */
    public static Expr rank() {
        return new Expr("RANK()");
    }
    
    /**
     * DENSE_RANK() window function.
     */
    public static Expr denseRank() {
        return new Expr("DENSE_RANK()");
    }
    
    /**
     * LAG(column) window function with default offset 1.
     */
    public static Expr lag(String column) {
        return new Expr("LAG(" + column + ")");
    }
    
    /**
     * LAG(column, offset) window function.
     */
    public static Expr lag(String column, int offset) {
        return new Expr("LAG(" + column + ", " + offset + ")");
    }
    
    /**
     * LEAD(column) window function with default offset 1.
     */
    public static Expr lead(String column) {
        return new Expr("LEAD(" + column + ")");
    }
    
    /**
     * LEAD(column, offset) window function.
     */
    public static Expr lead(String column, int offset) {
        return new Expr("LEAD(" + column + ", " + offset + ")");
    }
    
    /**
     * FIRST_VALUE(column) window function.
     */
    public static Expr firstValue(String column) {
        return new Expr("FIRST_VALUE(" + column + ")");
    }
    
    /**
     * LAST_VALUE(column) window function.
     */
    public static Expr lastValue(String column) {
        return new Expr("LAST_VALUE(" + column + ")");
    }
    
    /**
     * NTILE(n) window function.
     */
    public static Expr ntile(int n) {
        return new Expr("NTILE(" + n + ")");
    }
    
    // Column expressions
    
    /**
     * Reference a column by name.
     */
    public static Expr col(String column) {
        return new Expr(column);
    }
    
    /**
     * Reference a column with an alias.
     */
    public static Expr col(String column, String alias) {
        return new Expr(column + " AS " + alias);
    }
    
    /**
     * Raw SQL expression (use with caution).
     */
    public static Expr raw(String expression) {
        return new Expr(expression);
    }
    
    /**
     * Start building a CASE WHEN expression.
     */
    public static CaseConditionBuilder caseWhen(String column) {
        return new CaseConditionBuilder(column);
    }
    
    /**
     * Render a value for SQL (numbers unquoted, strings quoted with escaped single quotes).
     */
    public static String renderValue(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof Number) {
            return value.toString();
        }
        // String - quote and escape single quotes
        String str = value.toString();
        return "'" + str.replace("'", "''") + "'";
    }
}
