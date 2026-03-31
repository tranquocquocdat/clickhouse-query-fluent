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
            return new Expr("(" + expression + ") * (" + other.expression + ")");
        }
        
        public Expr multiply(String raw) {
            return new Expr("(" + expression + ") * (" + raw + ")");
        }
        
        /**
         * Divide this expression by another.
         */
        public Expr divide(Expr other) {
            return new Expr("(" + expression + ") / (" + other.expression + ")");
        }
        
        public Expr divide(String raw) {
            return new Expr("(" + expression + ") / (" + raw + ")");
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
            if (obj instanceof Expr) return expression.equals(((Expr) obj).expression);
            if (obj instanceof String) return expression.equals(obj);
            return false;
        }
        
        @Override
        public int hashCode() {
            return expression.hashCode();
        }
    }
    
    // Standard aggregates
    
    /**
     * count(*) aggregate function.
     */
    public static Expr count() {
        return new Expr("count(*)");
    }
    
    /**
     * count(column) aggregate function.
     */
    public static Expr count(String column) {
        return new Expr("count(" + column + ")");
    }
    
    /**
     * countDistinct(column) aggregate function (single column).
     */
    public static Expr countDistinct(String column) {
        return new Expr("countDistinct(" + column + ")");
    }
    
    public static Expr countDistinct(Expr column) {
        return new Expr("countDistinct(" + column.expression + ")");
    }
    
    /**
     * sum(column) aggregate function.
     */
    public static Expr sum(String column) {
        return new Expr("sum(" + column + ")");
    }
    
    /**
     * min(column) aggregate function.
     */
    public static Expr min(String column) {
        return new Expr("min(" + column + ")");
    }
    
    /**
     * max(column) aggregate function.
     */
    public static Expr max(String column) {
        return new Expr("max(" + column + ")");
    }
    
    /**
     * avg(column) aggregate function.
     */
    public static Expr avg(String column) {
        return new Expr("avg(" + column + ")");
    }
    
    // Window functions
    
    /**
     * row_number() window function.
     */
    public static Expr rowNumber() {
        return new Expr("row_number()");
    }
    
    /**
     * rank() window function.
     */
    public static Expr rank() {
        return new Expr("rank()");
    }
    
    /**
     * dense_rank() window function.
     */
    public static Expr denseRank() {
        return new Expr("dense_rank()");
    }
    
    /**
     * lag(column, 1) window function with default offset 1.
     */
    public static Expr lag(String column) {
        return new Expr("lag(" + column + ", 1)");
    }
    
    /**
     * lag(column, offset) window function.
     */
    public static Expr lag(String column, int offset) {
        return new Expr("lag(" + column + ", " + offset + ")");
    }
    
    /**
     * lead(column, 1) window function with default offset 1.
     */
    public static Expr lead(String column) {
        return new Expr("lead(" + column + ", 1)");
    }
    
    /**
     * lead(column, offset) window function.
     */
    public static Expr lead(String column, int offset) {
        return new Expr("lead(" + column + ", " + offset + ")");
    }
    
    /**
     * first_value(column) window function.
     */
    public static Expr firstValue(String column) {
        return new Expr("first_value(" + column + ")");
    }
    
    /**
     * last_value(column) window function.
     */
    public static Expr lastValue(String column) {
        return new Expr("last_value(" + column + ")");
    }
    
    /**
     * ntile(n) window function.
     */
    public static Expr ntile(int n) {
        return new Expr("ntile(" + n + ")");
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
        return new CaseConditionBuilder(new CaseBuilder(), column);
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
