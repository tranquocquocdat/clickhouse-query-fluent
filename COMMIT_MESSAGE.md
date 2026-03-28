# Commit Message

## Version 1.1.0 - Add Window Functions and GROUP BY Modifiers

### New Features

#### 1. Window Functions Support
Added comprehensive window function support with fluent API:

**Window Functions:**
- `rowNumber()` - Sequential row numbering within partition
- `rank()` - Ranking with gaps for ties
- `denseRank()` - Ranking without gaps
- `lag(col)` / `lag(col, offset)` - Access previous row values
- `lead(col)` / `lead(col, offset)` - Access next row values
- `firstValue(col)` - First value in window frame
- `lastValue(col)` - Last value in window frame
- `ntile(n)` - Divide partition into N buckets

**Window Builder:**
- `.over()` - Start window specification
- `.partitionBy(...)` - Define partition columns
- `.orderBy(col, direction)` - Define ordering within partition
- Supports aggregate functions with OVER: `sum().over()`, `avg().over()`, `count().over()`

**Example:**
```java
CH.rowNumber().over()
    .partitionBy("user_id")
    .orderBy("created_at", SortOrder.DESC)
    .as("rank")
```

#### 2. GROUP BY Modifiers
Added ClickHouse-specific GROUP BY modifiers:

- `groupByWithTotals(...)` - Adds summary row with totals
- `groupByWithRollup(...)` - Hierarchical subtotals
- `groupByWithCube(...)` - All combination subtotals

**Example:**
```java
ClickHouseQuery.select("product_id", sum("amount").as("total"))
    .from("orders")
    .groupByWithTotals("product_id")
```

#### 3. Helper Methods
- `CH.raw(expr)` - Wrap raw SQL expressions as type-safe Expr

### Files Changed

**New Files:**
- `src/main/java/lib/core/clickhouse/expression/WindowBuilder.java` - Window function builder

**Modified Files:**
- `src/main/java/lib/core/clickhouse/expression/CH.java`
  - Added window function methods: `rowNumber()`, `rank()`, `denseRank()`, `lag()`, `lead()`, `firstValue()`, `lastValue()`, `ntile()`
  - Added `.over()` method to `Expr` class
  - Added `raw()` helper method

- `src/main/java/lib/core/clickhouse/query/ClickHouseQuery.java`
  - Added `groupByModifier` field
  - Added `groupByWithTotals()`, `groupByWithRollup()`, `groupByWithCube()` methods
  - Updated `toSql()` to include GROUP BY modifiers

- `README.md`
  - Added window functions section with examples
  - Added GROUP BY modifiers section with examples
  - Updated API reference with new methods
  - Updated version to 1.1.0

- `build.gradle`
  - Updated version from 1.0.0 to 1.1.0

**Test Files:**
- `src/test/java/lib/core/clickhouse/expression/CHTest.java`
  - Added 15 window function tests
  - Added `raw()` method tests

- `src/test/java/lib/core/clickhouse/query/ClickHouseQueryTest.java`
  - Added 4 GROUP BY modifier tests

**Deleted Files:**
- `ALIAS_COMPARISON.md` - Temporary analysis file
- `FIXES_SUMMARY.md` - Temporary analysis file
- `REVIEW_SUMMARY.md` - Temporary analysis file

### Breaking Changes
None - all changes are backward compatible additions.

### Migration Guide
No migration needed. All existing code continues to work. New features are opt-in.

---

## Git Commands

```bash
# Stage all changes
git add .

# Commit with message
git commit -m "feat: add window functions and GROUP BY modifiers (v1.1.0)

- Add window functions: rowNumber, rank, denseRank, lag, lead, firstValue, lastValue, ntile
- Add WindowBuilder for fluent OVER(PARTITION BY ... ORDER BY ...) syntax
- Add GROUP BY modifiers: WITH TOTALS, WITH ROLLUP, WITH CUBE
- Add CH.raw() helper for raw SQL expressions
- Add comprehensive tests for new features
- Update documentation with examples and API reference
- Bump version to 1.1.0"

# Tag the release
git tag -a v1.1.0 -m "Release v1.1.0 - Window Functions and GROUP BY Modifiers"

# Push to remote
git push origin master
git push origin v1.1.0
```
