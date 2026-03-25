# ClickHouse Query Builder

Fluent Java DSL for building ClickHouse SELECT & INSERT queries with Spring `NamedParameterJdbcTemplate`.

## Features

- ✅ **Fluent API** — chainable methods that read like SQL
- ✅ **Clause-order validation** — enforces `SELECT → FROM → JOIN → WHERE → GROUP_BY → HAVING → ORDER_BY → LIMIT`
- ✅ **Fluent JOIN** — `.join("table").on("left", "right")` / `.leftJoin()` / `.rightJoin()`
- ✅ **Fluent Subquery** — `.where("col").in(ClickHouseQuery.select(...))`
- ✅ **CASE WHEN** — `caseWhen("col").gt(0).then("HIGH").orElse("LOW").as("level")`
- ✅ **Conditional filters** — `.eqIfNotBlank()`, `.eqIf()` skip when value is empty
- ✅ **LIKE / ILIKE search** — `.whereLike(keyword).on(...)` / `.whereILike(keyword).on(...)`
- ✅ **Subquery count** — `ClickHouseQuery.count(subQuery).execute(jdbc)`
- ✅ **Multiple ORDER BY** — `.orderBy("col1", SortOrder.DESC).orderBy("col2", SortOrder.ASC)`
- ✅ **Expression builder** — `CH.sum()`, `CH.count()`, `CH.avg()`, `CH.caseWhen()`

## Installation

### Gradle (local JAR)

```bash
./gradlew clean jar
# → build/libs/clickhouse-query-builder-1.0.0.jar
```

Copy JAR to your project's `app/libs/` folder:

```groovy
repositories {
    mavenCentral()
    flatDir { dirs 'libs' }
}

dependencies {
    implementation name: 'clickhouse-query-builder-1.0.0'
}
```

### Publish to local Maven

```bash
./gradlew publishToMavenLocal
```

```groovy
repositories { mavenLocal() }
dependencies { implementation 'lib.core:clickhouse-query-builder:1.0.0' }
```

## Query Examples

### 1. Basic SELECT

```java
import static lib.core.clickhouse.expression.CH.*;

List<Item> items = ClickHouseQuery
    .select(col("user_id"), sum("amount").as("total"))
    .from("orders")
    .where("tenant_id").eq(tenantId)
    .where("created_at").between(fromDate, toDate)
    .where("status").eqIfNotBlank(status)       // skipped if blank
    .where("category_id").in(categoryIds)       // IN (list)
    .where("deleted_at").isNull()
    .groupBy("user_id")
    .orderBy("total", SortOrder.DESC)
    .limit(10).offset(0)
    .query(namedJdbc, rowMapper);
```

### 2. Fluent JOIN

```java
List<Result> results = ClickHouseQuery
    .select("u.name", sum("o.amount").as("total"))
    .from("orders o")
    .join("users u").on("u.id", "o.user_id")
    .leftJoin("products p").on("p.id", "o.product_id")
    .where("o.tenant_id").eq(tenantId)
    .groupBy("u.name")
    .having(sum("o.amount")).gt(1000)
    .orderBy("total", SortOrder.DESC)
    .limit(20)
    .query(namedJdbc, rowMapper);

// Raw ON condition for complex cases:
.join("users u").on("u.id = o.user_id AND u.active = 1")
```

### 3. Fluent Subquery (IN / NOT IN)

```java
List<Item> items = ClickHouseQuery
    .select("*")
    .from("orders")
    .where("product_id").in(
        ClickHouseQuery.select("id")
            .from("products")
            .where("active").eq(1)
    )
    .where("user_id").notIn(
        ClickHouseQuery.select("id")
            .from("blocked_users")
            .where("status").eq("BLOCKED")
    )
    .query(namedJdbc, rowMapper);
```

### 4. CASE WHEN

```java
import static lib.core.clickhouse.expression.CH.*;

// String result values (auto-quoted)
ClickHouseQuery.select(
    col("user_id"),
    caseWhen("amount").gt(0).then("INCOME")
        .when("amount").eq(0).then("NEUTRAL")
        .orElse("EXPENSE")
        .as("type")
).from("transactions");

// Number result values (not quoted)
ClickHouseQuery.select(
    col("user_id"),
    caseWhen("status").eq("COMPLETED").then(1)
        .orElse(0)
        .as("is_completed")
).from("orders");

// IN operator in CASE WHEN
caseWhen("category").in("FOOD", "DRINK").then("F&B")
    .orElse("OTHER").as("group")

// isNull / isNotNull
caseWhen("deleted_at").isNull().then("ACTIVE")
    .orElse("DELETED").as("status")

// Raw column/expression in THEN (thenRaw / orElseRaw — not quoted)
caseWhen("type").eq("REFUND").thenRaw("amount")
    .orElseRaw("amount * -1")
    .as("adjusted_amount")

// BETWEEN in CASE WHEN
caseWhen("score").between(0, 49).then("LOW")
    .when("score").between(50, 79).then("MEDIUM")
    .when("score").between(80, 100).then("HIGH")
    .orElse("UNKNOWN")
    .as("grade")

// No ELSE (.end())
caseWhen("role").eq("ADMIN").then("YES")
    .end().as("is_admin")
```

### 5. WHERE Operators

```java
.where("amount").gt(100)              // amount > 100
.where("amount").gte(100)             // amount >= 100
.where("amount").lt(50)               // amount < 50
.where("amount").lte(50)              // amount <= 50
.where("status").eq("ACTIVE")         // status = 'ACTIVE'
.where("status").ne("DELETED")        // status != 'DELETED'
.where("category_id").in(ids)         // category_id IN (:id0, :id1, ...)
.where("category_id").notIn(excluded) // category_id NOT IN (...)
.where("deleted_at").isNull()         // deleted_at IS NULL
.where("error").isNotNull()           // error IS NOT NULL
.where("created_at").between(from, to) // created_at >= :from AND created_at <= :to
.where("status").eqIfNotBlank(status) // skipped if null/blank
.where("role").eqIf(hasRole, role)    // skipped if condition false
```

### 6. LIKE / ILIKE Search

```java
// Case-insensitive (ILIKE)
ClickHouseQuery.select("*")
    .from("users")
    .whereILike(keyword).on("name", "email")
    // → (name ILIKE '%keyword%' OR email ILIKE '%keyword%')
    .query(namedJdbc, rowMapper);

// Case-sensitive (LIKE)
ClickHouseQuery.select("*")
    .from("users")
    .whereLike(keyword).on("name", "email")
    // → (name LIKE '%keyword%' OR email LIKE '%keyword%')
    .query(namedJdbc, rowMapper);
```

### 7. WHERE OR Group

```java
ClickHouseQuery.select("*")
    .from("orders")
    .whereOr(or -> or
        .add("status", "PENDING")
        .add("status", "PROCESSING")
    )
    // → (status = :_or0 OR status = :_or1)
    .query(namedJdbc, rowMapper);
```

### 8. HAVING with Aggregates

```java
import static lib.core.clickhouse.expression.CH.*;

ClickHouseQuery.select("user_id", sum("amount").as("total"))
    .from("orders")
    .groupBy("user_id")
    .having(sum("amount")).gt(1000)
    .having(count()).gte(5)
    .having(avg("score")).between(50, 100)
    .query(namedJdbc, rowMapper);
```

### 9. Subquery Count

```java
// Style 1 — Static count (reads like SQL)
long total = ClickHouseQuery
    .count(
        ClickHouseQuery.select("user_id", "order_id")
            .from("order_items")
            .where("created_at").between(from, to)
            .groupBy("user_id", "order_id")
    )
    .execute(namedJdbc);

// Style 2 — Terminal count (end of chain)
long total = ClickHouseQuery
    .select("user_id", "order_id")
    .from("order_items")
    .where("created_at").between(from, to)
    .groupBy("user_id", "order_id")
    .count(namedJdbc);
```

### 10. Multiple ORDER BY

```java
ClickHouseQuery.select("*")
    .from("orders")
    .orderBy("created_at", SortOrder.DESC)
    .orderBy("amount", SortOrder.DESC)
    .orderBy("user_id", SortOrder.ASC)
    // → ORDER BY created_at DESC, amount DESC, user_id ASC
    .query(namedJdbc, rowMapper);
```

### 11. Complex Query (Full Example)

```java
import static lib.core.clickhouse.expression.CH.*;

List<Report> report = ClickHouseQuery.select(
        col("u.name"),
        sum("o.amount").as("total_revenue"),
        sumIf("o.amount", in("o.type", "SALE", "UPSELL")).as("total_sales"),
        countDistinct("o.order_id").as("order_count"),
        caseWhen("o.amount").gt(0).then("REVENUE")
            .when("o.amount").eq(0).then("FREE")
            .orElse("REFUND")
            .as("classification"),
        min("o.created_at").as("first_order"),
        max("o.created_at").as("last_order")
    )
    .from("orders o")
    .join("users u").on("u.id", "o.user_id")
    .leftJoin("products p").on("p.id", "o.product_id")
    .where("o.tenant_id").eq(tenantId)
    .where("o.created_at").between(fromDate, toDate)
    .where("o.status").eqIfNotBlank(status)
    .where("o.product_id").in(
        ClickHouseQuery.select("id")
            .from("products")
            .where("active").eq(1)
    )
    .whereILike(keyword).on("u.name", "o.order_id")
    .groupBy("u.name")
    .having(sum("o.amount")).gt(1000)
    .orderBy("total_revenue", SortOrder.DESC)
    .orderBy("order_count", SortOrder.DESC)
    .limit(50).offset(0)
    .query(namedJdbc, rowMapper);
```

### 12. INSERT

```java
ClickHouseInsert.into("orders")
    .columns("id", "user_id", "amount", "created_at")
    .executeBatch(namedJdbc, orders, o -> CHParams.of()
        .set("id", o.getId())
        .set("userId", o.getUserId())
        .setOrDefault("amount", o.getAmount(), BigDecimal.ZERO)
        .setTimestamp("createdAt", o.getCreatedAt())
        .build()
    );
```

## Clause-Order Validation

The builder enforces SQL clause ordering at runtime (including in subqueries):

```java
// ❌ Throws IllegalStateException
ClickHouseQuery.select("user_id")
    .from("t")
    .groupBy("user_id")
    .where("status").eq("ACTIVE");   // ERROR: cannot call WHERE after GROUP_BY

// ❌ Subquery also validates
.where("product_id").in(
    ClickHouseQuery.select("id")
        .from("products")
        .groupBy("id")
        .where("active").eq(1)       // ERROR: WHERE after GROUP_BY in subquery!
)

// ✅ Correct order
ClickHouseQuery.select("user_id")
    .from("t")
    .where("status").eq("ACTIVE")
    .groupBy("user_id");
```

**Allowed order:**
```
SELECT → FROM → JOIN → WHERE → GROUP_BY → HAVING → ORDER_BY → LIMIT
```

- Same phase is allowed (e.g. multiple `.where()` calls)
- Skipping phases is allowed (e.g. `SELECT → FROM → ORDER_BY`)
- Going backward throws `IllegalStateException`

## API Reference

### CH (Expressions & CASE WHEN)

| Method | Output |
|---|---|
| `col("name")` | `name` |
| `col("name", "alias")` | `name AS alias` |
| `count()` | `count(*)` |
| `count("col")` | `count(col)` |
| `countDistinct("col")` | `countDistinct(col)` |
| `sum("col")` | `sum(col)` |
| `sumIf("col", "cond")` | `sumIf(col, cond)` |
| `min("col")` | `min(col)` |
| `max("col")` | `max(col)` |
| `avg("col")` | `avg(col)` |
| `in("col", "v1", "v2")` | `col IN ('v1','v2')` |
| `.as("alias")` | `... AS alias` |

### CASE WHEN Operators

| Method | Generated SQL |
|---|---|
| `caseWhen("col").eq(val)` | `CASE WHEN col = val` |
| `.ne(val)` | `WHEN col != val` |
| `.gt(val)` | `WHEN col > val` |
| `.gte(val)` | `WHEN col >= val` |
| `.lt(val)` | `WHEN col < val` |
| `.lte(val)` | `WHEN col <= val` |
| `.between(from, to)` | `WHEN col BETWEEN from AND to` |
| `.in("v1", "v2")` | `WHEN col IN ('v1', 'v2')` |
| `.isNull()` | `WHEN col IS NULL` |
| `.isNotNull()` | `WHEN col IS NOT NULL` |
| `.then(val)` | `THEN 'val'` (String) / `THEN val` (Number) |
| `.thenRaw("expr")` | `THEN expr` (unquoted) |
| `.orElse(val)` | `ELSE 'val' END` |
| `.orElseRaw("expr")` | `ELSE expr END` |
| `.end()` | `END` (no ELSE) |

### ClickHouseQuery (SELECT Builder)

| Category | Method | Description |
|---|---|---|
| **SELECT** | `.select(...)` | Start SELECT query |
| | `.selectDistinct(...)` | Start SELECT DISTINCT |
| **FROM** | `.from("table")` | Set table name |
| **JOIN** | `.join("table").on("left", "right")` | INNER JOIN (fluent) |
| | `.leftJoin("table").on(...)` | LEFT JOIN |
| | `.rightJoin("table").on(...)` | RIGHT JOIN |
| | `.join("table").on("raw condition")` | Raw ON condition |
| **WHERE** | `.where("col").eq(val)` | Equal |
| | `.where("col").ne(val)` | Not equal |
| | `.where("col").gt(val)` | Greater than |
| | `.where("col").gte(val)` | Greater than or equal |
| | `.where("col").lt(val)` | Less than |
| | `.where("col").lte(val)` | Less than or equal |
| | `.where("col").eqIfNotBlank(val)` | Equal (skipped if blank) |
| | `.where("col").eqIf(cond, val)` | Equal (conditional) |
| | `.where("col").between(from, to)` | Date range |
| | `.where("col").in(list)` | IN (collection) |
| | `.where("col").notIn(list)` | NOT IN (collection) |
| | `.where("col").in(subQuery)` | IN (fluent subquery) |
| | `.where("col").notIn(subQuery)` | NOT IN (fluent subquery) |
| | `.where("col").inSubQuery(sql)` | IN (raw SQL string) |
| | `.where("col").isNull()` | IS NULL |
| | `.where("col").isNotNull()` | IS NOT NULL |
| | `.whereILike(kw).on("c1", "c2")` | ILIKE search, case-insensitive |
| | `.whereLike(kw).on("c1", "c2")` | LIKE search, case-sensitive |
| | `.whereOr(or -> or.add(...))` | OR group |
| | `.whereRaw("condition")` | Raw WHERE clause |
| **GROUP BY** | `.groupBy("col1", "col2")` | Group by columns |
| **HAVING** | `.having(sum("col")).gt(100)` | Aggregate filter |
| **ORDER BY** | `.orderBy("col", SortOrder.DESC)` | Sort (supports multiple calls) |
| **LIMIT** | `.limit(10).offset(0)` | Pagination |
| **Execute** | `.query(jdbc, mapper)` | Run and map results |
| | `.queryForObject(jdbc, type)` | Single value |
| | `.count(jdbc)` | Terminal count |
| **Static** | `ClickHouseQuery.count(subQuery)` | Subquery count builder |

### CHParams (Insert Parameter Builder)

| Method | Description |
|---|---|
| `.set("name", value)` | Set value |
| `.setOrDefault("name", val, default)` | Set with fallback |
| `.setEnum("name", enumVal)` | Enum → String |
| `.setTimestamp("name", instant)` | Instant → Timestamp |
| `.setArray("name", list, type)` | List → Array |

## Requirements

- Java 21+
- Spring JDBC 6.x
