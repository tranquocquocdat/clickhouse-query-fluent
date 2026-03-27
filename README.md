# ClickHouse Query Builder

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Fluent Java DSL for building type-safe ClickHouse queries with Spring `NamedParameterJdbcTemplate`.
Zero code-gen · Zero config · Null-safe · Auto DTO mapping · Fully type-safe `Expr` column references.

---

## ⚡ Best Practice — Real-World Example

> **Use case:** E-commerce analytics dashboard — covers **every** library feature in one coherent example.

```java
import static lib.core.clickhouse.expression.CH.*;
import lib.core.clickhouse.insert.ClickHouseInsert;
import lib.core.clickhouse.query.*;
import lib.core.clickhouse.util.CHParams;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

// ══════════════════════════════════════════════════════════════════
// 1. TYPE-SAFE ALIASES  —  no more "t." string literals
// ══════════════════════════════════════════════════════════════════
Alias oi  = Alias.of("order_items").as("oi");          // main table
Alias p   = Alias.of("products").as("p");              // INNER JOIN
Alias u   = Alias.of("users").as("u");                 // LEFT JOIN
Alias cfg = Alias.of("product_config").as("cfg");      // RIGHT JOIN
Alias rb  = Alias.of("return_buckets").as("rb");       // FULL OUTER JOIN
Alias sub = Alias.of("user_summary").as("us");         // subquery alias

// ══════════════════════════════════════════════════════════════════
// 2. CTE (WITH) + ALL 4 JOIN TYPES + EVERY SELECT FEATURE
// ══════════════════════════════════════════════════════════════════
Page<SalesReport> report = ClickHouseQuery

    // ── WITH (CTE) ─────────────────────────────────────────────
    .with("active_products",
        ClickHouseQuery.select("product_id")
            .from("product_config")
            .where("status").eq("ACTIVE")
            .where("tenant_id").eq(tenantId)
    )

    // ── SELECT: aggregates + arithmetic + conditional aggs + CASE WHEN
    .select(
        // Plain columns via Alias
        oi.col("tenant_id"),
        p.col("product_name"),
        u.col("currency"),

        // Basic aggregates
        oi.sum("revenue").as("total_revenue"),
        oi.sum("cost").as("total_cost"),
        count().as("total_orders"),
        avg("revenue").as("avg_revenue"),
        min("revenue").as("min_revenue"),
        max("revenue").as("max_revenue"),

        // Multi-column countDistinct
        countDistinct(oi.col("user_id")).as("unique_users"),
        countDistinct(oi.col("user_id"), oi.col("session_id")).as("unique_sessions"),

        // Arithmetic: minus / plus / multiply / divide
        oi.sum("revenue").minus(oi.sum("cost")).as("net_profit"),
        oi.sum("revenue").minus(oi.sum("cost"))
            .divide(oi.sum("revenue")).multiply("100").as("margin_pct"),
        oi.sum("debit").plus(oi.sum("credit")).as("total_flow"),

        // Conditional aggregates — fluent
        sumIf("revenue").where("action").eq("SALE").as("actual_sales"),
        countIf("user_id").where("is_promotion").eq(1).as("promo_orders"),
        avgIf("revenue").where("status").eq("COMPLETED").as("avg_completed_revenue"),
        minIf("cost").where("action").eq("PURCHASE").as("min_cost"),
        maxIf("revenue").where("tier").in("GOLD", "PLATINUM").as("max_premium_revenue"),

        // Conditional aggregates — raw
        sumIfRaw("refund_amount", "action = 'REFUND' AND refund_amount > 0").as("total_refund"),
        countIfRaw("user_id", in("status", "CANCELLED", "ERROR")).as("failed_orders"),

        // CASE WHEN: string results, numeric results, between, in, isNull, raw, thenRaw, orElseRaw, end
        caseWhen("net_profit").gt(0).then("PROFITABLE")
            .when("net_profit").eq(0).then("BREAK_EVEN")
            .orElse("LOSS").as("profitability"),

        caseWhen("revenue").between(0, 10).then("MICRO")
            .when("revenue").between(10, 100).then("SMALL")
            .when("revenue").between(100, 1000).then("MEDIUM")
            .orElse("ENTERPRISE").as("order_tier"),

        caseWhen("currency").in("USD", "EUR", "GBP").then("FIAT")
            .orElse("CRYPTO").as("currency_group"),

        caseWhen("coupon_code").isNull().then("NO_COUPON")
            .orElseRaw("coupon_code").as("effective_coupon"),

        caseWhen("action").eq("REFUND").thenRaw("revenue")
            .orElseRaw("revenue * -1").as("adjusted_amount"),

        caseWhen("error_code").isNotNull().then("HAS_ERROR")
            .end().as("error_flag")   // no ELSE
    )

    // ── FROM + 4 JOIN types ────────────────────────────────────
    .from("active_products")                                                    // FROM (CTE)
    .join(oi).on(oi.col("product_id"), "active_products.product_id")           // INNER JOIN
    .leftJoin(u).on(u.col("id"), oi.col("user_id"))                           // LEFT JOIN
    .rightJoin(cfg).on(cfg.col("product_id"), oi.col("product_id"))           // RIGHT JOIN
    .fullJoin(rb).on(rb.col("session_id"), oi.col("session_id"))              // FULL OUTER JOIN

    // ── WHERE: every operator ─────────────────────────────────
    .where(oi.col("tenant_id")).eq(tenantId)              // eq (null-safe)
    .where(oi.col("status")).ne("CANCELLED")              // ne
    .where(oi.col("revenue")).gt(0)                       // gt
    .where(oi.col("revenue")).gte(minRevenue)             // gte — skipped if null
    .where(oi.col("revenue")).lt(maxRevenue)              // lt  — skipped if null
    .where(oi.col("line_number")).lte(9999)               // lte
    .where(oi.col("created_at")).between(fromDate, toDate)  // between(Instant)
    .where(oi.col("processing_ms")).between(100, 30_000)    // between(Number)
    .where(oi.col("product_id")).in(productIds)           // in(Collection) — skipped if empty
    .where(oi.col("user_id")).notIn(blockedUsers)         // notIn(Collection)
    .where(oi.col("completed_at")).isNotNull()            // isNotNull
    .where(oi.col("error_code")).isNull()                 // isNull
    .where(p.col("category")).eqIfNotBlank(categoryFilter)  // eqIfNotBlank
    .where(oi.col("is_sample")).eqIf(sampleOnly, 1)         // eqIf
    .where(u.col("id")).in(                               // in(subquery)
        ClickHouseQuery.select("user_id").from("premium_list")
            .where("tenant_id").eq(tenantId)
    )
    .where(u.col("id")).notIn(                            // notIn(subquery)
        ClickHouseQuery.select("user_id").from("blocked_users")
            .where("active").eq(1)
    )
    .whereRaw("toYYYYMM(oi.created_at) = toYYYYMM(now())")  // whereRaw
    .whereILike(keyword).on(u.col("username"), oi.col("session_id"))   // ILIKE multi-col
    .whereLike(sessionPrefix).onPrefix(oi.col("session_id"))           // LIKE prefix (index-friendly)
    .whereOr(or -> or                                     // OR group — all operators
        .where(oi.col("action")).eq("MANUAL_CREDIT")
        .where(oi.col("status")).ne("VOID")
        .where(oi.col("revenue")).gt(10_000)
        .where(oi.col("cost")).gte(50_000)
        .where(oi.col("latency_ms")).lt(500)
        .where(oi.col("latency_ms")).lte(1_000)
        .where(oi.col("product_id")).in(List.of("p1", "p2"))
        .where(oi.col("user_id")).notIn(premiumExclusions)
        .where(oi.col("coupon_code")).isNull()
        .where(oi.col("discount_id")).isNotNull()
        .where(oi.col("revenue")).between(500, 5_000)     // between inside OR
        .whereILike(keyword).on("session_id", "user_id")  // ILIKE inside OR
        .addRaw("oi.is_featured = 1")                     // raw inside OR
    )

    // ── GROUP BY + HAVING ─────────────────────────────────────
    .groupBy(oi.col("tenant_id"), p.col("product_name"), u.col("currency"))
    .having(sum("revenue")).gt(1_000)
    .having(count()).gte(10)
    .having(avg("revenue")).between(5, 50_000)
    .havingRaw("sum(cost) < sum(revenue) * 0.99")

    // ── ORDER BY (multiple columns) ───────────────────────────
    .orderBy("total_revenue", SortOrder.DESC)
    .orderBy(p.col("product_name"), SortOrder.ASC)

    // ── PAGINATED EXECUTION: data + total count in ONE query ──
    .queryPage(page, pageSize, namedJdbc, SalesReport.class);

// Page result
List<SalesReport> data = report.getData();
long total                = report.getTotal();
int  totalPages           = report.getTotalPages();
boolean hasNext           = report.hasNext();
boolean hasPrev           = report.hasPrevious();


// ══════════════════════════════════════════════════════════════════
// 3. SELECT DISTINCT
// ══════════════════════════════════════════════════════════════════
List<String> currencies = ClickHouseQuery
    .selectDistinct("currency")
    .from("order_items")
    .where("tenant_id").eq(tenantId)
    .query(namedJdbc, String.class);


// ══════════════════════════════════════════════════════════════════
// 4. UNION ALL — combine partitioned tables
// ══════════════════════════════════════════════════════════════════
List<UserSummary> history = ClickHouseQuery
    .select("user_id", sum("revenue").as("total"))
    .from("order_items_2024").where("tenant_id").eq(tenantId).groupBy("user_id")
    .unionAll(
        ClickHouseQuery.select("user_id", sum("revenue").as("total"))
            .from("order_items_2025").where("tenant_id").eq(tenantId).groupBy("user_id")
    )
    .orderBy("total", SortOrder.DESC)
    .limit(100)
    .query(namedJdbc, UserSummary.class);


// ══════════════════════════════════════════════════════════════════
// 5. SUBQUERY FROM — derived table
// ══════════════════════════════════════════════════════════════════
List<UserRankRow> ranked = ClickHouseQuery
    .select(sub.col("user_id"), sub.col("total"),
            "rank() OVER (ORDER BY total DESC) AS rank")
    .from(
        ClickHouseQuery.select("user_id", sum("revenue").as("total"))
            .from("order_items").where("tenant_id").eq(tenantId)
            .groupBy("user_id"),
        sub
    )
    .where(sub.col("total")).gt(minTotal)
    .orderBy("rank", SortOrder.ASC)
    .limit(50)
    .query(namedJdbc, UserRankRow.class);


// ══════════════════════════════════════════════════════════════════
// 6. queryOne (single row) + terminal count + subquery count
// ══════════════════════════════════════════════════════════════════
// Single DTO (returns null if no rows)
DailySummary today = ClickHouseQuery
    .select(sum("revenue").as("total_revenue"), count().as("total_orders"))
    .from("order_items")
    .where("tenant_id").eq(tenantId)
    .where("created_at").between(Instant.now().minus(1, ChronoUnit.DAYS), Instant.now())
    .queryOne(namedJdbc, DailySummary.class);

// Terminal .count() on the query itself
long totalRows = ClickHouseQuery.select("1")
    .from("order_items")
    .where("tenant_id").eq(tenantId)
    .count(namedJdbc);

// Static ClickHouseQuery.count(subQuery)
long distinctSessions = ClickHouseQuery.count(
    ClickHouseQuery.select("user_id", "session_id")
        .from("order_items").where("tenant_id").eq(tenantId)
        .groupBy("user_id", "session_id")
).execute(namedJdbc);


// ══════════════════════════════════════════════════════════════════
// 7. INSERT batch with CHParams — every param type
// ══════════════════════════════════════════════════════════════════
ClickHouseInsert.into("order_items")
    .columns("id", "tenant_id", "user_id", "product_id", "action",
             "revenue", "cost", "status", "currency",
             "tags", "created_at", "session_id")
    .executeBatch(namedJdbc, incomingOrders, tx -> CHParams.of()
        .set("id",           tx.getId())                               // set (any value)
        .set("tenantId",     tx.getTenantId())
        .set("userId",       tx.getUserId())
        .set("productId",    tx.getProductId())
        .setEnum("action",   tx.getAction())                          // Enum → name()
        .setOrDefault("revenue", tx.getRevenue(), BigDecimal.ZERO)    // null-safe default
        .setOrDefault("cost", tx.getCost(), BigDecimal.ZERO)
        .setEnum("status",   tx.getStatus())
        .set("currency",     tx.getCurrency())
        .setArray("tags",    tx.getTags(), String.class)              // List → SQL ARRAY
        .setTimestamp("createdAt", tx.getCreatedAt())                 // Instant → Timestamp
        .set("sessionId",    tx.getSessionId())
        .build()
    );
```

> **Every feature covered:** SELECT DISTINCT, type-safe `Alias`, CTE (`WITH`), all 4 JOINs (INNER / LEFT / RIGHT / FULL OUTER), every WHERE operator (eq / ne / gt / gte / lt / lte / in / notIn / isNull / isNotNull / between Instant / between Number / eqIfNotBlank / eqIf / in(subquery) / notIn(subquery) / whereRaw / whereILike / whereLike.onPrefix), `whereOr` (all 13 operators), CASE WHEN (all operators, thenRaw, orElseRaw, end), all 5 aggregates + 5 conditional aggregates (fluent & raw), arithmetic chain (minus / plus / multiply / divide), multi-column `countDistinct`, HAVING + `havingRaw`, multi-column ORDER BY, UNION ALL, subquery FROM, `queryPage` / `queryOne` / terminal `.count()` / subquery count, INSERT batch (`set` / `setOrDefault` / `setEnum` / `setTimestamp` / `setArray`).

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
| 7 | **Fluent JOIN** | `.join(alias).on(...)` / `.leftJoin()` / `.rightJoin()` / `.fullJoin()` |
| 8 | **Fluent OR** | `.whereOr(or -> or.where("col").eq(v).where("col2").gt(n))` — 13 operators |
| 9 | **LIKE / ILIKE** | `.whereILike(kw).on("col1", "col2")` / `.onPrefix("col")` |
| 10 | **Conditional filters** | `.eqIfNotBlank()`, `.eqIf()` skip when value is empty |
| 11 | **CASE WHEN** | `caseWhen("col").gt(0).then("HIGH").orElse("LOW").as("level")` |
| 12 | **Expression builder** | `CH.sum()`, `CH.count()`, `CH.avg()`, `CH.min()`, `CH.max()` |
| 13 | **Conditional aggregates** | `CH.sumIf()`, `CH.countIf()`, `CH.avgIf()`, `CH.minIf()`, `CH.maxIf()` |
| 14 | **Fluent arithmetic** | `sum("revenue").minus(sum("cost")).divide(100)` / `.plus()` / `.multiply()` |
| 15 | **Multi-column countDistinct** | `countDistinct(col1, col2).as("unique")` → `count(DISTINCT (col1, col2))` |
| 16 | **Fluent Subquery** | `.where("col").in(ClickHouseQuery.select(...))` |
| 17 | **Subquery FROM** | `.from(ClickHouseQuery.select(...)).as("alias")` |
| 18 | **UNION ALL** | `.unionAll(ClickHouseQuery.select(...))` |
| 19 | **WITH (CTE)** | `ClickHouseQuery.with("name", subQuery).select(...)` |
| 20 | **Type-safe Alias** | `Alias.of("orders")` → `.from(orders)` / `orders.col("amount")` |
| 21 | **INSERT batch** | `ClickHouseInsert.into("t").columns(...).executeBatch(...)` |

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
Alias orders = Alias.of("orders");     // orders.col("amount") → Expr("orders.amount")
Alias users  = Alias.of("users");      // users.col("name")    → Expr("users.name")

// With short alias:
Alias o = Alias.of("orders").as("o");  // o.col("amount") → Expr("o.amount")

ClickHouseQuery.select(
        users.col("name"),
        orders.sum("amount").as("total_revenue"),
        orders.countDistinct("order_id").as("order_count")
    )
    .from(orders)                                            // FROM orders
    .join(users).on(users.col("id"), orders.col("user_id"))  // JOIN users ON ...
    .where(orders.col("tenant_id")).eq(tenantId)
    .groupBy(users.col("name"))
    .having(orders.sum("amount")).gt(1000)
    .query(namedJdbc, Report.class);
```

**Alias methods:**

| Method | Return | Output |
|---|---|---|
| `orders.col("amount")` | `Expr` | `orders.amount` — for SELECT, WHERE, JOIN, GROUP BY |
| `orders.col("amount").as("total")` | `Expr` | `orders.amount AS total` |
| `orders.col("revenue").minus(orders.col("cost")).as("net")` | `Expr` | `orders.revenue - orders.cost AS net` |
| `orders.sum("amount")` | `Expr` | `sum(orders.amount)` |
| `orders.sum("revenue").minus(orders.sum("cost")).as("net")` | `Expr` | `sum(orders.revenue) - sum(orders.cost) AS net` |
| `orders.count("id")` | `Expr` | `count(orders.id)` |
| `orders.countDistinct("id")` | `Expr` | `countDistinct(orders.id)` |
| `orders.min("created_at")` | `Expr` | `min(orders.created_at)` |
| `orders.max("created_at")` | `Expr` | `max(orders.created_at)` |
| `orders.avg("score")` | `Expr` | `avg(orders.score)` |
| `orders.sumIf("amount").where("status").eq("ACTIVE")` | `Expr` | `sumIf(orders.amount, status = 'ACTIVE')` |
| `orders.sumIfRaw("amount", "cond")` | `Expr` | `sumIf(orders.amount, cond)` |
| `orders.caseWhen("amount").gt(5000).then("HIGH")` | — | `CASE WHEN orders.amount > 5000 ...` |

> [!TIP]
> **`col()` returns `Expr`** — a type-safe wrapper. `Expr` is accepted directly by `select()`, `groupBy()`, `on()`, `where()`, etc. No `.toString()` needed.
> `Expr.as("alias")` also returns `Expr`, so the entire chain stays type-safe.
> `Expr.equals(String)` works for convenient assertion: `assertEquals(expr, "expected")`.

> [!IMPORTANT]
> **Khi có JOIN (≥ 2 bảng), luôn dùng `Alias.col()` cho TẤT CẢ column references:**
>
> ```java
> // ✅ Đúng — mọi column đều rõ bảng
> .where(orders.col("status")).eq("ACTIVE")
> .groupBy(users.col("name"))
> .orderBy(orders.col("amount"), SortOrder.DESC)
>
> // ❌ Sai — ClickHouse báo "ambiguous column"
> .where("status").eq("ACTIVE")      // status từ bảng nào?
> .groupBy("name")                   // name từ bảng nào?
> ```
>
> **Quy tắc:** Đơn bảng → tùy chọn. JOIN → **bắt buộc** dùng `alias.col("col")`, `alias.sum("col")`, `alias.caseWhen("col")`, v.v.

### 3. Fluent JOIN

```java
Alias orders   = Alias.of("orders").as("o");
Alias users    = Alias.of("users").as("u");
Alias products = Alias.of("products").as("p");

ClickHouseQuery
    .select(users.col("name"), orders.sum("amount").as("total"))
    .from(orders)
    .join(users).on(users.col("id"), orders.col("user_id"))
    .leftJoin(products).on(products.col("id"), orders.col("product_id"))
    .where(orders.col("tenant_id")).eq(tenantId)
    .groupBy(users.col("name"))
    .having(orders.sum("amount")).gt(1000)
    .orderBy("total", SortOrder.DESC)
    .query(namedJdbc, Report.class);

// Raw ON condition for complex cases:
.join(users).on("u.id = o.user_id AND u.active = 1")
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
        .where("type").in(List.of("PREMIUM"))    //   type IN ('PREMIUM')
        .where("amount").gt(5000)                //   OR amount > 5000
        .where("name").ilike("john")             //   OR name ILIKE '%john%'
        .where("deleted_at").isNull()            //   OR deleted_at IS NULL
        .where("user_id").in(                    //   OR user_id IN (subquery)
            ClickHouseQuery.select("id").from("premium_users")
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

### 9. Fluent Arithmetic (`minus` / `plus`)

Chain arithmetic operations on any `Expr` returned by `sum()`, `col()`, `count()`, etc.:

```java
import static lib.core.clickhouse.expression.CH.*;

Alias oi = Alias.of("order_items");

// sum().minus(sum()) — aggregate arithmetic
oi.sum("revenue").minus(oi.sum("cost")).as("net_profit")
// → sum(order_items.revenue) - sum(order_items.cost) AS net_profit

// col().minus(col()) — column arithmetic
oi.col("revenue").minus(oi.col("cost")).as("net_profit")
// → order_items.revenue - order_items.cost AS net_profit

// Plus
oi.sum("debit").plus(oi.sum("credit")).as("total")
// → sum(order_items.debit) + sum(order_items.credit) AS total
```

### 10. Multi-Column `countDistinct`

Count distinct combinations of multiple columns:

```java
import static lib.core.clickhouse.expression.CH.*;

Alias oi = Alias.of("order_items");

countDistinct(oi.col("user_id"), oi.col("session_id")).as("total_sessions")
// → count(DISTINCT (order_items.user_id, order_items.session_id)) AS total_sessions
```

### 11. Expression Builder & Conditional Aggregates

**Fluent (recommended):**

```java
import static lib.core.clickhouse.expression.CH.*;

ClickHouseQuery.select(
    col("product_id"),
    sumIf("amount").where("action").eq("SALE").as("total_sales"),
    countIf("user_id").where("status").eq("ACTIVE").as("active_count"),
    minIf("amount").where("type").eq("PURCHASE").as("min_purchase"),
    maxIf("amount").where("score").gt(100).as("max_order"),
    avgIf("score").where("status").isNotNull().as("avg_score"),
    countIf("user_id").where("tier").in("GOLD", "PREMIUM").as("premium_count")
).from("orders").groupBy("product_id");
```

**Raw (explicit):**

```java
sumIfRaw("amount", "action = 'SALE'").as("total_sales")
countIfRaw("user_id", in("tier", "GOLD", "PREMIUM")).as("premium_count")
```

### 12. HAVING with Aggregates

```java
ClickHouseQuery.select("user_id", sum("amount").as("total"))
    .from("orders")
    .groupBy("user_id")
    .having(sum("amount")).gt(1000)
    .having(count()).gte(5)
    .having(avg("score")).between(50, 100)
    .query(namedJdbc, Report.class);
```

### 13. Subquery FROM

```java
Alias sub    = Alias.of("sub");
Alias orders = Alias.of("orders");

ClickHouseQuery.select(sub.col("user_id"), sub.col("total"))
    .from(
        ClickHouseQuery.select(col("user_id"), sum("amount").as("total"))
            .from(orders)
            .where(orders.col("tenant_id")).eq(tenantId)
            .groupBy("user_id"),
        sub
    )
    .where(sub.col("total")).gt(1000)
    .orderBy("total", SortOrder.DESC)
    .limit(10)
    .query(namedJdbc, Report.class);
// → SELECT sub.user_id, sub.total FROM (SELECT ... GROUP BY user_id) AS sub WHERE sub.total > ...
```

### 14. UNION ALL

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

### 15. WITH (CTE — Common Table Expressions)

```java
// Single CTE
Alias au = Alias.of("active_users").as("au");
Alias o  = Alias.of("orders").as("o");

ClickHouseQuery
    .with("active_users",
        ClickHouseQuery.select("user_id").from("users").where("status").eq("ACTIVE"))
    .select(au.col("user_id"), count().as("order_count"))
    .from(au)
    .join(o).on(au.col("user_id"), o.col("user_id"))
    .groupBy(au.col("user_id"))
    .query(namedJdbc, Report.class);

// Multiple CTEs
Alias u = Alias.of("cte_users").as("u");
Alias uo = Alias.of("cte_orders").as("uo");

ClickHouseQuery
    .with("cte_users",
        ClickHouseQuery.select("user_id").from("users").where("status").eq("ACTIVE"))
    .with("cte_orders",
        ClickHouseQuery.select("user_id", "sum(amount) AS total")
            .from("orders").groupBy("user_id"))
    .select(u.col("user_id"), uo.col("total"))
    .from(u)
    .join(uo).on(uo.col("user_id"), u.col("user_id"))
    .orderBy(uo.col("total"), SortOrder.DESC)
    .query(namedJdbc, Report.class);
```

### 16. Subquery Count

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

### 17. Single-Query Pagination (`queryPage`)

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

### 18. Auto DTO Mapping

No `RowMapper` needed — just pass your DTO class:

**Option A — Java `record` (recommended):**

```java
public record OrderReport(
    String userId,             // ← auto mapped from user_id
    BigDecimal totalAmount,    // ← auto mapped from total_amount
    long orderCount            // ← auto mapped from order_count
) {}

List<OrderReport> reports = ClickHouseQuery.select(
        col("user_id"), sum("amount").as("total_amount"), count().as("order_count")
    )
    .from("orders")
    .groupBy("user_id")
    .query(namedJdbc, OrderReport.class);   // auto RecordRowMapper
```

**Option B — POJO class:**

```java
public class OrderReport {
    private String userId;            // ← auto mapped from user_id
    private BigDecimal totalAmount;    // ← auto mapped from total_amount
    private Long orderCount;           // ← auto mapped from order_count
    // getters + setters (required)
}

List<OrderReport> reports = ClickHouseQuery.select(
        col("user_id"), sum("amount").as("total_amount"), count().as("order_count")
    )
    .from("orders")
    .groupBy("user_id")
    .query(namedJdbc, OrderReport.class);   // auto BeanPropertyRowMapper
```

**Common usage:**

```java
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

**Smart Mapper** auto-detects:
- Java `record` → uses `RecordRowMapper` (reflection-based, matches component names)
- POJO class → uses `BeanPropertyRowMapper` (Spring, `snake_case` → `camelCase`)

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
| **Auto `record`** `.query(jdbc, Record.class)` | Java 16+, immutable, column alias = component name |
| **Auto `class`** `.query(jdbc, Class)` | POJO with setters, `snake_case` → `camelCase` |
| **Manual** `.query(jdbc, RowMapper)` | Cần transform, combine fields, hoặc tên không match |

### 19. Default LIMIT (Safety Guard)

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

### 20. INSERT

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
| `countDistinct("col1", "col2")` | `count(DISTINCT (col1, col2))` — **multi-column** |
| `sum("col")` | `sum(col)` |
| `min("col")` | `min(col)` |
| `max("col")` | `max(col)` |
| `avg("col")` | `avg(col)` |
| `.minus(expr)` | `expr1 - expr2` — **arithmetic** |
| `.plus(expr)` | `expr1 + expr2` — **arithmetic** |
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
| | `.from(subQuery, alias)` / `.from(subQuery).as("alias")` | Subquery as table source |
| **JOIN** | `.join(alias).on(a.col("id"), b.col("user_id"))` | INNER JOIN |
| | `.leftJoin(alias).on(...)` | LEFT JOIN |
| | `.rightJoin(alias).on(...)` | RIGHT JOIN |
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
