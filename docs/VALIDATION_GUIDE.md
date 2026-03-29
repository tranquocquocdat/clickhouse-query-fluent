# Query Builder Validation Guide

Thư viện ClickHouse Query Builder có các validation tự động để đảm bảo SQL được tạo ra luôn hợp lệ và an toàn.

## 1. Phase Ordering Validation (Thứ tự SQL Clause)

### Thứ tự đúng:
```
SELECT → FROM → JOIN → WHERE → GROUP BY → HAVING → ORDER BY → LIMIT
```

### Ví dụ hợp lệ:
```java
ClickHouseQuery.select("user_id", "sum(amount) AS total")
    .from("orders")                          // ✅ FROM sau SELECT
    .join("users u").on("u.id = orders.user_id")  // ✅ JOIN sau FROM
    .where("status").eq("ACTIVE")            // ✅ WHERE sau JOIN
    .groupBy("user_id")                      // ✅ GROUP BY sau WHERE
    .having(sum("amount")).gt(100)           // ✅ HAVING sau GROUP BY
    .orderBy("total")                        // ✅ ORDER BY sau HAVING
    .limit(10);                              // ✅ LIMIT sau ORDER BY
```

### Ví dụ không hợp lệ:
```java
// ❌ WHERE trước FROM
ClickHouseQuery.select("*")
    .where("status").eq("ACTIVE")  // IllegalStateException!
    .from("orders");

// ❌ GROUP BY sau ORDER BY
ClickHouseQuery.select("*")
    .from("orders")
    .orderBy("created_at")
    .groupBy("user_id");  // IllegalStateException!

// ❌ HAVING trước GROUP BY
ClickHouseQuery.select("*")
    .from("orders")
    .having(sum("amount")).gt(100)  // IllegalStateException!
    .groupBy("user_id");
```

### Exception message:
```
IllegalStateException: Cannot call WHERE after GROUP_BY. 
Expected order: SELECT → FROM → JOIN → WHERE → GROUP_BY → HAVING → ORDER_BY → LIMIT
```

### Lưu ý:
- Có thể gọi cùng một phase nhiều lần (ví dụ: nhiều `.where()`, nhiều `.join()`)
- Có thể bỏ qua các phase không cần thiết (ví dụ: không cần GROUP BY, HAVING)
- Không thể quay lại phase trước đó

---

## 2. Null & Empty Value Safety

### WHERE Builder

Tất cả các operators tự động skip nếu value = `null` hoặc empty string:

```java
String status = null;
query.where("status").eq(status);  // ✅ Skip, không tạo WHERE clause

String name = "";
query.where("name").eq(name);  // ✅ Skip, không tạo WHERE clause

Integer amount = null;
query.where("amount").gt(amount);  // ✅ Skip, không tạo WHERE clause
```

### Operators có null-safety:
- `eq(value)` - skip nếu null hoặc empty string
- `ne(value)` - skip nếu null hoặc empty string
- `gt(value)`, `gte(value)`, `lt(value)`, `lte(value)` - skip nếu null hoặc empty string
- `in(collection)` - skip nếu null hoặc empty collection
- `notIn(collection)` - skip nếu null hoặc empty collection
- `between(from, to)` - chỉ apply các bound không null và không empty

### Special methods:
```java
// eqIfNotBlank - chỉ apply nếu string không null và không blank
String status = "  ";  // blank string
query.where("status").eqIfNotBlank(status);  // ✅ Skip

// eqIf - chỉ apply nếu condition = true
boolean includeFilter = false;
query.where("status").eqIf(includeFilter, "ACTIVE");  // ✅ Skip
```

### HAVING Builder

Tương tự WHERE, tất cả operators có null-safety:

```java
Integer minAmount = null;
query.having(sum("amount")).gt(minAmount);  // ✅ Skip

String category = "";
query.having(max("category")).eq(category);  // ✅ Skip
```

### WhereILike Builder

Tự động skip nếu keyword = null hoặc blank:

```java
String keyword = null;
query.whereILike(keyword).on("name", "email");  // ✅ Skip

String search = "  ";  // blank
query.whereILike(search).on("name", "email");  // ✅ Skip
```

---

## 3. Range Validation (between)

### Validation rule:
`from` phải nhỏ hơn hoặc bằng `to`, nếu không sẽ throw `InvalidRangeException`.

### Ví dụ hợp lệ:
```java
// ✅ Number range
query.where("amount").between(100, 200);

// ✅ Date range
Instant from = Instant.parse("2024-01-01T00:00:00Z");
Instant to = Instant.parse("2024-12-31T00:00:00Z");
query.where("created_at").between(from, to);

// ✅ String range
query.where("name").between("A", "Z");

// ✅ Equal bounds (inclusive range)
query.where("amount").between(100, 100);

// ✅ Null bounds (skip validation)
query.where("amount").between(null, 200);
query.where("amount").between(100, null);
```

### Ví dụ không hợp lệ:
```java
// ❌ from > to
query.where("amount").between(200, 100);
// InvalidRangeException: Invalid range for column 'amount': 
// from (200) must be less than or equal to to (100)

// ❌ Date range invalid
Instant from = Instant.parse("2024-12-31T00:00:00Z");
Instant to = Instant.parse("2024-01-01T00:00:00Z");
query.where("created_at").between(from, to);
// InvalidRangeException!

// ❌ String range invalid
query.where("name").between("Z", "A");
// InvalidRangeException!
```

### Exception details:
```java
try {
    query.where("amount").between(200, 100);
} catch (InvalidRangeException ex) {
    ex.getColumn();  // "amount"
    ex.getFrom();    // 200
    ex.getTo();      // 100
    ex.getMessage(); // "Invalid range for column 'amount': from (200) must be less than or equal to to (100)"
}
```

### Lưu ý:
- Validation chỉ áp dụng cho `Comparable` types (Number, String, Instant, etc.)
- Nếu from hoặc to = null → skip validation
- Nếu from hoặc to = empty string → skip validation
- Nếu types không comparable → skip validation
- Equal bounds (from == to) là hợp lệ (inclusive range)

---

## 4. Collection Safety

### IN / NOT IN operators

Tự động skip nếu collection = null hoặc empty:

```java
List<Integer> ids = null;
query.where("user_id").in(ids);  // ✅ Skip

List<String> statuses = List.of();  // empty
query.where("status").in(statuses);  // ✅ Skip

// ✅ Valid
List<Integer> validIds = List.of(1, 2, 3);
query.where("user_id").in(validIds);
// SQL: WHERE user_id IN (:userId0, :userId1, :userId2)
```

---

## 5. Best Practices

### 1. Luôn kiểm tra null trước khi truyền vào query:
```java
// ❌ Không tốt - dựa vào null-safety
String status = getStatusFromUser();  // có thể null
query.where("status").eq(status);

// ✅ Tốt hơn - kiểm tra rõ ràng
String status = getStatusFromUser();
if (status != null && !status.isEmpty()) {
    query.where("status").eq(status);
}

// ✅ Hoặc dùng eqIfNotBlank
query.where("status").eqIfNotBlank(getStatusFromUser());
```

### 2. Validate range trước khi gọi between():
```java
// ❌ Không tốt - để exception xảy ra
query.where("amount").between(maxAmount, minAmount);  // có thể throw!

// ✅ Tốt hơn - validate trước
if (minAmount <= maxAmount) {
    query.where("amount").between(minAmount, maxAmount);
}

// ✅ Hoặc catch exception
try {
    query.where("amount").between(from, to);
} catch (InvalidRangeException ex) {
    log.error("Invalid range: {}", ex.getMessage());
    // Handle error
}
```

### 3. Sử dụng đúng thứ tự SQL clauses:
```java
// ✅ Tốt - theo thứ tự chuẩn
query.select("*")
    .from("orders")
    .where("status").eq("ACTIVE")
    .groupBy("user_id")
    .having(sum("amount")).gt(100)
    .orderBy("created_at")
    .limit(10);

// ❌ Không tốt - sẽ throw IllegalStateException
query.select("*")
    .where("status").eq("ACTIVE")  // WHERE trước FROM!
    .from("orders");
```

### 4. Sử dụng type-safe methods:
```java
// ✅ Tốt - type-safe với Instant
Instant from = Instant.now().minus(Duration.ofDays(7));
Instant to = Instant.now();
query.where("created_at").between(from, to);

// ✅ Tốt - type-safe với SortOrder enum
query.orderBy("created_at", SortOrder.DESC);
```

---

## 6. Testing Validations

### Test Phase Ordering:
```java
@Test
void testInvalidOrder_ThrowsException() {
    assertThrows(IllegalStateException.class, () -> {
        ClickHouseQuery.select("*")
            .where("status").eq("ACTIVE")
            .from("orders");  // ❌ FROM after WHERE
    });
}
```

### Test Range Validation:
```java
@Test
void testInvalidRange_ThrowsException() {
    assertThrows(InvalidRangeException.class, () -> {
        ClickHouseQuery.select("*")
            .from("orders")
            .where("amount").between(200, 100);  // ❌ from > to
    });
}
```

### Test Null Safety:
```java
@Test
void testNullValue_SkipsCondition() {
    String sql = ClickHouseQuery.select("*")
        .from("orders")
        .where("status").eq(null)  // Skip
        .toSql();
    
    assertFalse(sql.contains("WHERE"));  // No WHERE clause
}
```

---

## Summary

Thư viện có 3 loại validation chính:

1. **Phase Ordering** - Đảm bảo thứ tự SQL clauses đúng
2. **Null & Empty Safety** - Tự động skip các điều kiện null/empty
3. **Range Validation** - Đảm bảo from <= to trong between()

Tất cả validations đều throw exceptions rõ ràng với message chi tiết, giúp debug dễ dàng.
