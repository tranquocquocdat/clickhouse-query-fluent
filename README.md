# ClickHouse Query Builder

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Fluent Java DSL for building type-safe ClickHouse queries with Spring `NamedParameterJdbcTemplate`.
Zero code-gen · Zero config · Null-safe · Auto DTO mapping.

---

## ⚡ Best Practice — Real-World Example

```java
import static lib.core.clickhouse.expression.CH.*;
import lib.core.clickhouse.query.*;

// 1) Define table aliases
Alias orders = Alias.of("orders");
Alias users  = Alias.of("users");

// 2) Build a complex report query — fluent, type-safe, readable
Page<OrderReport> page = ClickHouseQuery.select(
        users.col("name"),
        orders.sum("amount").as("total_revenue"),
        orders.countDistinct("order_id").as("order_count"),
        orders.sumIf("amount").where("status").eq("COMPLETED").as("completed_revenue"),
        orders.caseWhen("amount").gt(5000).then("HIGH")
            .when("amount").gt(1000).then("MEDIUM")
            .orElse("LOW").as("tier")
    )
    .from(orders)
    .join(users).on(users.c("id"), orders.c("user_id"))

    // Null-safe filters — null values are automatically skipped
    .where(orders.c("tenant_id")).eq(tenantId)
    .where(orders.c("created_at")).between(startDate, endDate)
    .where(orders.c("status")).eqIfNotBlank(statusFilter)

    // Multi-column search
    .whereILike(keyword).on(users.c("name"), orders.c("order_id"))

    // Complex OR groups
    .whereOr(or -> or
        .where(users.c("type")).in(List.of("VIP", "PREMIUM"))
        .where(orders.c("amount")).gt(10000)
    )

    .groupBy(users.c("name"))
    .having(sum("amount")).gt(1000)
    .orderBy("total_revenue", SortOrder.DESC)
    .queryPage(0, 20, namedJdbc, OrderReport.class);  // single-query pagination + auto DTO

// Result
page.getData();       // List<OrderReport> — max 20 items
page.getTotal();      // 4567 — total matching rows (no extra query!)
page.getTotalPages(); // 229
page.hasNext();       // true
```

> **19 features** working together: Alias, JOIN, null-safe WHERE, ILIKE search, fluent OR, CASE WHEN, conditional aggregates (`sumIf`), HAVING, single-query pagination, auto DTO mapping — all in one fluent chain.

---

## Features

| # | Feature | Highlight |
|---|---|---|
| 1 | **Fluent API** | Chainable methods that read like SQL |
| 2 | **Clause-order validation** | Runtime enforcement: `SELECT → FROM → JOIN → WHERE → GROUP_BY → HAVING → ORDER_BY → LIMIT` |
| 3 | **Null-safe WHERE** | All operators skip clause when value is `null` — no manual null checks |
| 4 | **Auto DTO mapping** | `.query(jdbc, MyDto.class)` maps `snake_case` → `camelCase` automatically |
| 5 | **Single-query pagination** | `queryPage()` → `Page<T>` with data + total count in **one query** |
| 6 | **Default LIMIT** | Auto `LIMIT 1000` safety guard when no explicit limit is set |
| 7 | **Fluent JOIN** | `.join("t").on("left", "right")` / `.leftJoin()` / `.rightJoin()` |
| 8 | **Fluent OR** | `.whereOr(or -> or.where("col").eq(v).where("col2").gt(n))` — 13 operators |
| 9 | **LIKE / ILIKE** | `.whereILike(kw).on("col1", "col2")` / `.onPrefix("col")` |
| 10 | **Conditional filters** | `.eqIfNotBlank()`, `.eqIf()` skip when value is empty |
| 11 | **CASE WHEN** | `caseWhen("col").gt(0).then("HIGH").orElse("LOW").as("level")` |
| 12 | **Expression builder** | `CH.sum()`, `CH.count()`, `CH.avg()`, `CH.min()`, `CH.max()` |
| 13 | **Conditional aggregates** | `CH.sumIf()`, `CH.countIf()`, `CH.avgIf()`, `CH.minIf()`, `CH.maxIf()` |
| 14 | **Fluent Subquery** | `.where("col").in(ClickHouseQuery.select(...))` |
| 15 | **Subquery FROM** | `.from(ClickHouseQuery.select(...)).as("alias")` |
| 16 | **UNION ALL** | `.unionAll(ClickHouseQuery.select(...))` |
| 17 | **WITH (CTE)** | `ClickHouseQuery.with("name", subQuery).select(...)` |
| 18 | **Type-safe Alias** | `Alias.of("orders")` → `.from(orders)` / `orders.c("amount")` |
| 19 | **INSERT batch** | `ClickHouseInsert.into("t").columns(...).executeBatch(...)` |

---

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

**Requirements:** Java 21+ · Spring JDBC 6.x

---

## Query Examples

### 1. Basic SELECT

```java
import static lib.core.clickhouse.expression.CH.*;

List<Order> orders = ClickHouseQuery
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
    .query(namedJdbc, Order.class);              // auto DTO mapping
```

### 2. Type-Safe Alias

Avoid hard-coded `"o."`, `"u."` prefix strings:

```java
Alias orders = Alias.of("orders");     // orders.c("amount") → "orders.amount"
Alias users  = Alias.of("users");      // users.c("name")    → "users.name"

// With short alias:
Alias o = Alias.of("orders").as("o");  // o.c("amount") → "o.amount"

ClickHouseQuery.select(
        users.col("name"),
        orders.sum("amount").as("total_revenue"),
        orders.countDistinct("order_id").as("order_count")
    )
    .from(orders)                                        // FROM orders
    .join(users).on(users.c("id"), orders.c("user_id"))  // JOIN users ON ...
    .where(orders.c("tenant_id")).eq(tenantId)
    .groupBy(users.c("name"))
    .having(orders.sum("amount")).gt(1000)
    .query(namedJdbc, Report.class);
```

**Alias methods:**

| Method | Output |
|---|---|
| `orders.c("amount")` | `"orders.amount"` |
| `orders.col("amount")` | `"orders.amount"` |
| `orders.sum("amount")` | `sum(orders.amount)` |
| `orders.count("id")` | `count(orders.id)` |
| `orders.countDistinct("id")` | `countDistinct(orders.id)` |
| `orders.min("created_at")` | `min(orders.created_at)` |
| `orders.max("created_at")` | `max(orders.created_at)` |
| `orders.avg("score")` | `avg(orders.score)` |
| `orders.sumIf("amount").where("status").eq("ACTIVE")` | `sumIf(orders.amount, status = 'ACTIVE')` |
| `orders.sumIfRaw("amount", "cond")` | `sumIf(orders.amount, cond)` |
| `orders.caseWhen("amount").gt(5000).then("HIGH")` | `CASE WHEN orders.amount > 5000 ...` |

> [!IMPORTANT]
> **Khi có JOIN (≥ 2 bảng), luôn dùng `Alias.c()` cho TẤT CẢ column references:**
>
> ```java
> // ✅ Đúng — mọi column đều rõ bảng
> .where(orders.c("status")).eq("ACTIVE")
> .groupBy(users.c("name"))
> .orderBy(orders.c("amount"), SortOrder.DESC)
>
> // ❌ Sai — ClickHouse báo "ambiguous column"
> .where("status").eq("ACTIVE")      // status từ bảng nào?
> .groupBy("name")                   // name từ bảng nào?
> ```
>
> **Quy tắc:** Đơn bảng → tùy chọn. JOIN → **bắt buộc** dùng `alias.c("col")`, `alias.sum("col")`, `alias.caseWhen("col")`, v.v.

### 3. Fluent JOIN

```java
ClickHouseQuery
    .select("u.name", sum("o.amount").as("total"))
    .from("orders o")
    .join("users u").on("u.id", "o.user_id")
    .leftJoin("products p").on("p.id", "o.product_id")
    .where("o.tenant_id").eq(tenantId)
    .groupBy("u.name")
    .having(sum("o.amount")).gt(1000)
    .orderBy("total", SortOrder.DESC)
    .query(namedJdbc, Report.class);

// Raw ON condition for complex cases:
.join("users u").on("u.id = o.user_id AND u.active = 1")
```

### 4. WHERE Operators

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

> **Null-safe**: All operators (`eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `notIn`, `between`) **silently skip** the clause when value is `null`. No manual null checks needed.

```java
String status = request.getStatus();     // may be null
Integer minAmount = request.getMin();    // may be null

ClickHouseQuery.select("*").from("orders")
    .where("status").eq(status)          // skipped if null
    .where("amount").gt(minAmount)       // skipped if null
    .where("tenant_id").eq("op-1")       // always applied
    .query(namedJdbc, Order.class);
// → SELECT * FROM orders WHERE tenant_id = :tenantId
```

### 5. Fluent OR Conditions (`whereOr`)

All conditions inside `whereOr` are joined by **OR**. Groups themselves join with the outer query by **AND**.

```
WHERE  ①  AND  ②  AND  (③a OR ③b)  AND  (④a OR ④b)
       ↑        ↑       └ whereOr ┘      └ whereOr ┘
     where    where     inner = OR      inner = OR

└──────────── outer: always AND ────────────────────┘
```

```java
ClickHouseQuery.select("*")
    .from("orders")
    .where("tenant_id").eq(tenantId)            // AND
    .whereOr(or -> or                            // AND (
        .where("status").eq("ACTIVE")            //   status = 'ACTIVE'
        .where("status").eq("PENDING")           //   OR status = 'PENDING'
    )                                            // )
    .whereOr(or -> or                            // AND (
        .where("type").in(List.of("VIP"))        //   type IN ('VIP')
        .where("amount").gt(5000)                //   OR amount > 5000
        .where("name").ilike("john")             //   OR name ILIKE '%john%'
        .where("deleted_at").isNull()            //   OR deleted_at IS NULL
        .where("user_id").in(                    //   OR user_id IN (subquery)
            ClickHouseQuery.select("id").from("vip_users")
        )
    )                                            // )
    .query(namedJdbc, Order.class);
```

**Supported operators inside `whereOr`:**

| Operator | Example | SQL |
|---|---|---|
| `eq(v)` | `.where("status").eq("ACTIVE")` | `status = :_or0` |
| `ne(v)` | `.where("status").ne("DELETED")` | `status != :_or0` |
| `gt(v)` | `.where("amount").gt(100)` | `amount > :_or0` |
| `gte(v)` | `.where("amount").gte(100)` | `amount >= :_or0` |
| `lt(v)` | `.where("score").lt(10)` | `score < :_or0` |
| `lte(v)` | `.where("score").lte(5)` | `score <= :_or0` |
| `in(list)` | `.where("type").in(List.of(...))` | `type IN (:_or0, :_or1)` |
| `notIn(list)` | `.where("type").notIn(List.of(...))` | `type NOT IN (...)` |
| `isNull()` | `.where("col").isNull()` | `col IS NULL` |
| `isNotNull()` | `.where("col").isNotNull()` | `col IS NOT NULL` |
| `ilike(v)` | `.where("name").ilike("john")` | `name ILIKE '%john%'` |
| `like(v)` | `.where("name").like("john")` | `name LIKE '%john%'` |
| `in(subQuery)` | `.where("id").in(subQuery)` | `id IN (SELECT ...)` |

> **Null-safe:** All operators inside `whereOr` skip when value is `null`. Legacy `add()` / `addRaw()` still work.

### 6. LIKE / ILIKE Search

```java
// Case-insensitive (ILIKE) — contains %keyword%
.whereILike(keyword).on("name", "email")
// → (name ILIKE '%keyword%' OR email ILIKE '%keyword%')

// Case-sensitive (LIKE) — contains %keyword%
.whereLike(keyword).on("name", "email")

// Prefix search (keyword%) — index-friendly 🔥
.whereILike(keyword).onPrefix("name", "email")
// → (name ILIKE 'keyword%' OR email ILIKE 'keyword%')
```

### 7. Subquery (IN / NOT IN)

```java
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
```

### 8. CASE WHEN

```java
import static lib.core.clickhouse.expression.CH.*;

// String result values (auto-quoted)
caseWhen("amount").gt(0).then("INCOME")
    .when("amount").eq(0).then("NEUTRAL")
    .orElse("EXPENSE")
    .as("type")

// Number result values (not quoted)
caseWhen("status").eq("COMPLETED").then(1)
    .orElse(0).as("is_completed")

// IN operator
caseWhen("category").in("FOOD", "DRINK").then("F&B")
    .orElse("OTHER").as("group")

// BETWEEN
caseWhen("score").between(0, 49).then("LOW")
    .when("score").between(50, 79).then("MEDIUM")
    .when("score").between(80, 100).then("HIGH")
    .orElse("UNKNOWN").as("grade")

// Raw expressions
caseWhen("type").eq("REFUND").thenRaw("amount")
    .orElseRaw("amount * -1").as("adjusted_amount")

// No ELSE
caseWhen("role").eq("ADMIN").then("YES")
    .end().as("is_admin")
```

### 9. Expression Builder & Conditional Aggregates

**Fluent (recommended):**

```java
import static lib.core.clickhouse.expression.CH.*;

ClickHouseQuery.select(
    col("product_id"),
    sumIf("amount").where("action").eq("BET").as("total_bet"),
    countIf("user_id").where("status").eq("ACTIVE").as("active_count"),
    minIf("amount").where("type").eq("SALE").as("min_sale"),
    maxIf("amount").where("score").gt(100).as("max_sale"),
    avgIf("score").where("status").isNotNull().as("avg_score"),
    countIf("user_id").where("status").in("VIP", "PREMIUM").as("premium_count")
).from("orders").groupBy("product_id");
```

**Raw (explicit):**

```java
sumIfRaw("amount", "action = 'BET'").as("total_bet")
countIfRaw("user_id", in("status", "VIP", "PREMIUM")).as("premium_count")
```

### 10. HAVING with Aggregates

```java
ClickHouseQuery.select("user_id", sum("amount").as("total"))
    .from("orders")
    .groupBy("user_id")
    .having(sum("amount")).gt(1000)
    .having(count()).gte(5)
    .having(avg("score")).between(50, 100)
    .query(namedJdbc, Report.class);
```

### 11. Subquery FROM

```java
ClickHouseQuery.select("user_id", "total")
    .from(
        ClickHouseQuery.select("user_id", "sum(amount) AS total")
            .from("orders")
            .where("tenant_id").eq(tenantId)
            .groupBy("user_id")
    )
    .as("sub")
    .where("total").gt(1000)
    .orderBy("total", SortOrder.DESC)
    .limit(10)
    .query(namedJdbc, Report.class);
// → SELECT ... FROM (SELECT ... GROUP BY user_id) AS sub WHERE total > ...
```

### 12. UNION ALL

```java
// Combine results from multiple tables
ClickHouseQuery.select("user_id", "amount").from("orders_2024")
    .unionAll(ClickHouseQuery.select("user_id", "amount").from("orders_2025"))
    .orderBy("amount", SortOrder.DESC)
    .limit(10)
    .query(namedJdbc, Report.class);

// Chain 3+ unions
ClickHouseQuery.select("user_id", "amount").from("orders_2023")
    .unionAll(ClickHouseQuery.select("user_id", "amount").from("orders_2024"))
    .unionAll(ClickHouseQuery.select("user_id", "amount").from("orders_2025"))
    .orderBy("amount", SortOrder.DESC)
    .query(namedJdbc, Report.class);
```

### 13. WITH (CTE — Common Table Expressions)

```java
// Single CTE
ClickHouseQuery
    .with("active_users",
        ClickHouseQuery.select("user_id").from("users").where("status").eq("ACTIVE"))
    .select("au.user_id", "count(*) AS order_count")
    .from("orders o")
    .join("active_users au").on("au.user_id", "o.user_id")
    .groupBy("au.user_id")
    .query(namedJdbc, Report.class);

// Multiple CTEs
ClickHouseQuery
    .with("cte_users",
        ClickHouseQuery.select("user_id").from("users").where("status").eq("ACTIVE"))
    .with("cte_orders",
        ClickHouseQuery.select("user_id", "sum(amount) AS total")
            .from("orders").groupBy("user_id"))
    .select("u.user_id", "o.total")
    .from("cte_users u")
    .join("cte_orders o").on("o.user_id", "u.user_id")
    .orderBy("o.total", SortOrder.DESC)
    .query(namedJdbc, Report.class);
```

### 14. Subquery Count

```java
// Style 1 — Static count
long total = ClickHouseQuery
    .count(
        ClickHouseQuery.select("user_id", "order_id")
            .from("order_items")
            .where("created_at").between(from, to)
            .groupBy("user_id", "order_id")
    )
    .execute(namedJdbc);

// Style 2 — Terminal count
long total = ClickHouseQuery
    .select("user_id", "order_id")
    .from("order_items")
    .where("created_at").between(from, to)
    .groupBy("user_id", "order_id")
    .count(namedJdbc);
```

### 15. Single-Query Pagination (`queryPage`)

Get paginated data **and total count in one query** — no extra `COUNT(*)` query:

```java
Page<Report> page = ClickHouseQuery.select("user_id", "amount")
    .from("orders")
    .where("tenant_id").eq(tenantId)
    .orderBy("amount", SortOrder.DESC)
    .queryPage(0, 10, namedJdbc, Report.class);   // page 0, size 10

page.getData();       // List<Report> — max 10 items
page.getTotal();      // 1234 — total matching rows
page.getTotalPages(); // 124
page.hasNext();       // true
page.hasPrevious();   // false (page 0)
```

Internally uses `count(*) OVER()` window function — total count computed **before** LIMIT.

### 16. Auto DTO Mapping

No `RowMapper` needed — just pass your DTO class:

```java
public class OrderReport {
    private String userId;            // ← auto mapped from user_id
    private BigDecimal totalAmount;    // ← auto mapped from total_amount
    private Long orderCount;           // ← auto mapped from order_count
    // getters + setters (required)
}

// List query
List<OrderReport> reports = ClickHouseQuery.select(
        col("user_id"), sum("amount").as("total_amount"), count().as("order_count")
    )
    .from("orders")
    .groupBy("user_id")
    .query(namedJdbc, OrderReport.class);

// Single result
OrderSummary summary = ClickHouseQuery.select("count(*) AS total_orders")
    .from("orders")
    .queryOne(namedJdbc, OrderSummary.class);     // single DTO or null

// Page with auto mapping
Page<OrderReport> page = ClickHouseQuery.select(col("user_id"), sum("amount").as("total"))
    .from("orders")
    .groupBy("user_id")
    .queryPage(0, 10, namedJdbc, OrderReport.class);
```

Uses Spring's `BeanPropertyRowMapper` — maps `snake_case` → `camelCase`. DTO needs **default constructor** + **setters**.

**Manual RowMapper** — for complex mapping, transformation, or when column names don't match:

```java
List<OrderReport> reports = ClickHouseQuery.select(
        col("user_id"), sum("amount").as("total"), count().as("cnt")
    )
    .from("orders")
    .groupBy("user_id")
    .query(namedJdbc, (rs, rowNum) -> {
        OrderReport r = new OrderReport();
        r.setUserId(rs.getString("user_id"));
        r.setTotal(rs.getBigDecimal("total"));
        r.setOrderCount(rs.getLong("cnt"));
        r.setFormatted("$" + rs.getBigDecimal("total").toPlainString());
        return r;
    });
```

| Cách | Khi nào dùng |
|---|---|
| **Auto** `.query(jdbc, Class)` | Column name match field name (`snake_case` → `camelCase`) |
| **Manual** `.query(jdbc, RowMapper)` | Cần transform, combine fields, hoặc tên không match |

### 17. Default LIMIT (Safety Guard)

Auto `LIMIT 1000` when `.query()` is called without an explicit `.limit()`:

```java
// No .limit() → auto LIMIT 1000
ClickHouseQuery.select("*").from("orders")
    .query(namedJdbc, Order.class);
// → SQL: ... LIMIT 1000

// Explicit .limit() → your value
ClickHouseQuery.select("*").from("orders")
    .limit(50)
    .query(namedJdbc, Order.class);
// → SQL: ... LIMIT 50
```

> **Note:** `UNION ALL` queries are excluded. Default value: `ClickHouseQuery.DEFAULT_LIMIT = 1000`.

### 18. INSERT

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

---

## Clause-Order Validation

The builder enforces SQL clause ordering at runtime (including subqueries):

```
SELECT → FROM → JOIN → WHERE → GROUP_BY → HAVING → ORDER_BY → LIMIT
```

```java
// ❌ Throws IllegalStateException
ClickHouseQuery.select("user_id")
    .from("t")
    .groupBy("user_id")
    .where("status").eq("ACTIVE");   // ERROR: cannot call WHERE after GROUP_BY

// ✅ Correct order
ClickHouseQuery.select("user_id")
    .from("t")
    .where("status").eq("ACTIVE")
    .groupBy("user_id");
```

- Same phase is allowed (e.g. multiple `.where()` calls)
- Skipping phases is allowed (e.g. `SELECT → FROM → ORDER_BY`)
- Going backward throws `IllegalStateException`

---

## API Reference

### CH (Expressions)

| Method | Output |
|---|---|
| `col("name")` | `name` |
| `col("name", "alias")` | `name AS alias` |
| `count()` | `count(*)` |
| `count("col")` | `count(col)` |
| `countDistinct("col")` | `countDistinct(col)` |
| `sum("col")` | `sum(col)` |
| `min("col")` | `min(col)` |
| `max("col")` | `max(col)` |
| `avg("col")` | `avg(col)` |
| `sumIf("col").where("c").eq(v)` | `sumIf(col, c = 'v')` — **fluent** |
| `countIf("col").where("c").in(...)` | `countIf(col, c IN (...))` — **fluent** |
| `minIf("col").where("c").gt(v)` | `minIf(col, c > v)` — **fluent** |
| `maxIf("col").where("c").lt(v)` | `maxIf(col, c < v)` — **fluent** |
| `avgIf("col").where("c").isNotNull()` | `avgIf(col, c IS NOT NULL)` — **fluent** |
| `sumIfRaw("col", "cond")` | `sumIf(col, cond)` — **raw** |
| `countIfRaw("col", "cond")` | `countIf(col, cond)` — **raw** |
| `minIfRaw("col", "cond")` | `minIf(col, cond)` — **raw** |
| `maxIfRaw("col", "cond")` | `maxIf(col, cond)` — **raw** |
| `avgIfRaw("col", "cond")` | `avgIf(col, cond)` — **raw** |
| `in("col", "v1", "v2")` | `col IN ('v1','v2')` |
| `.as("alias")` | `... AS alias` |

### CASE WHEN Operators

| Method | SQL |
|---|---|
| `caseWhen("col").eq(val)` | `CASE WHEN col = val` |
| `.ne(val)` | `WHEN col != val` |
| `.gt(val)` / `.gte(val)` | `WHEN col > val` / `>=` |
| `.lt(val)` / `.lte(val)` | `WHEN col < val` / `<=` |
| `.between(from, to)` | `WHEN col BETWEEN from AND to` |
| `.in("v1", "v2")` | `WHEN col IN ('v1', 'v2')` |
| `.isNull()` / `.isNotNull()` | `WHEN col IS NULL` / `IS NOT NULL` |
| `.then(val)` | `THEN 'val'` (String) / `THEN val` (Number) |
| `.thenRaw("expr")` | `THEN expr` (unquoted) |
| `.orElse(val)` / `.orElseRaw("expr")` | `ELSE ... END` |
| `.end()` | `END` (no ELSE) |

### ClickHouseQuery (SELECT Builder)

| Category | Method | Description |
|---|---|---|
| **SELECT** | `.select(...)` | Start SELECT query |
| | `.selectDistinct(...)` | Start SELECT DISTINCT |
| **FROM** | `.from("table")` / `.from(alias)` | Set table |
| | `.from(subQuery).as("alias")` | Subquery as table source |
| **JOIN** | `.join("t").on("left", "right")` | INNER JOIN |
| | `.leftJoin("t").on(...)` | LEFT JOIN |
| | `.rightJoin("t").on(...)` | RIGHT JOIN |
| **WHERE** | `.where("col").eq/ne/gt/gte/lt/lte(val)` | Comparison operators |
| | `.where("col").in(list)` / `.notIn(list)` | IN / NOT IN |
| | `.where("col").in(subQuery)` / `.notIn(subQuery)` | Subquery IN |
| | `.where("col").isNull()` / `.isNotNull()` | NULL checks |
| | `.where("col").between(from, to)` | Range |
| | `.where("col").eqIfNotBlank(val)` | Conditional equality |
| | `.where("col").eqIf(cond, val)` | Boolean-conditional |
| | `.whereILike(kw).on(...)` / `.onPrefix(...)` | ILIKE search |
| | `.whereLike(kw).on(...)` | LIKE search |
| | `.whereOr(or -> or.where(...).eq(...))` | Fluent OR group |
| | `.whereRaw("condition")` | Raw WHERE |
| **GROUP BY** | `.groupBy("col1", "col2")` | Group by columns |
| **HAVING** | `.having(sum("col")).gt(100)` | Aggregate filter |
| **ORDER BY** | `.orderBy("col", SortOrder.DESC)` | Sort (multiple calls OK) |
| **LIMIT** | `.limit(10).offset(0)` | Pagination |
| **Execute** | `.query(jdbc, mapper/class)` | Run and map results |
| | `.queryOne(jdbc, class)` | Single DTO or null |
| | `.queryPage(page, size, jdbc, class)` | Paginated results |
| | `.count(jdbc)` | Terminal count |
| **UNION** | `.unionAll(subQuery)` | Append UNION ALL |
| **CTE** | `.with("name", subQuery).select(...)` | Common Table Expression |
| **Static** | `ClickHouseQuery.count(subQuery)` | Subquery count |

### CHParams (Insert Parameter Builder)

| Method | Description |
|---|---|
| `.set("name", value)` | Set value |
| `.setOrDefault("name", val, default)` | Set with fallback |
| `.setEnum("name", enumVal)` | Enum → String |
| `.setTimestamp("name", instant)` | Instant → Timestamp |
| `.setArray("name", list, type)` | List → Array |
