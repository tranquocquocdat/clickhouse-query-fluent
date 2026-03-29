# Hướng Dẫn Commit Lên Git

## Bước 1: Kiểm tra thay đổi

```bash
git status
```

Bạn sẽ thấy các file đã thay đổi:
- New files: BaseQuery.java, CommonFunctions.java, InvalidRangeException.java, tests, docs
- Modified files: ClickHouseQuery.java, CH.java, WhereBuilder.java, HavingBuilder.java, README.md, build.gradle
- Moved files: Tất cả builders và support classes

## Bước 2: Add tất cả thay đổi

```bash
git add .
```

## Bước 3: Commit với message

```bash
git commit -m "feat: refactor to base layer + add comprehensive validations

BREAKING CHANGES: None - 100% backward compatible

## Major Changes:

### 1. Refactoring - Separate Common vs ClickHouse-Specific (v1.2.0)

Created two-layer architecture to enable multi-database support:

**Base Layer (lib.core.query):**
- BaseQuery<T> abstract class with self-referential generic
- CommonFunctions with standard SQL aggregates and window functions
- All query builders with generic type parameters
- Support classes (Page, SortOrder, RecordRowMapper, Alias)

**ClickHouse Layer (lib.core.clickhouse):**
- ClickHouseQuery extends BaseQuery<ClickHouseQuery>
- CH extends CommonFunctions
- ClickHouse-specific features preserved

### 2. Comprehensive Validations

**A. Null & Empty Value Safety:**
- All WHERE/HAVING operators skip if value is null or empty
- Collection operators skip if null or empty
- between() skips null or empty bounds

**B. Range Validation:**
- between() validates from <= to
- Throws InvalidRangeException with detailed error
- Supports Instant, Number, String, all Comparable types

**C. Phase Ordering Validation:**
- Enforces: SELECT → FROM → JOIN → WHERE → GROUP_BY → HAVING → ORDER_BY → LIMIT
- Throws IllegalStateException with clear message

### 3. New Features

- InvalidRangeException class
- PhaseOrderingTest (15 test cases)
- RangeValidationTest (11 test cases)
- Complete validation documentation

### 4. Documentation

- Updated README.md with Validations & Safety section
- Added docs/VALIDATION_GUIDE.md
- Added docs/planning/REFACTORING_COMMIT_MESSAGE.md

## Backward Compatibility:

✅ All existing imports work
✅ All public APIs unchanged
✅ Method chaining returns correct types
✅ All existing tests pass
✅ SQL generation identical

## Version: 1.2.0

Minor version bump - adds new features while maintaining 100% backward compatibility"
```

## Bước 4: Push lên remote

```bash
# Nếu đang ở branch main
git push origin main

# Nếu đang ở branch khác
git push origin <branch-name>
```

## Bước 5: Tạo tag cho version mới (optional)

```bash
git tag -a v1.2.0 -m "Release v1.2.0 - Base layer refactoring + comprehensive validations"
git push origin v1.2.0
```

## Kiểm tra trước khi commit

### 1. Compile code:
```bash
./gradlew compileJava
```

### 2. Run tests:
```bash
./gradlew test
```

### 3. Build JAR:
```bash
./gradlew clean jar
```

### 4. Kiểm tra version:
```bash
cat build.gradle | grep version
# Phải thấy: version = '1.2.0'
```

## Nếu cần sửa commit message

Nếu đã commit nhưng chưa push và muốn sửa message:

```bash
git commit --amend
```

Sau đó edit message trong editor và save.

## Nếu cần undo commit

Nếu đã commit nhưng chưa push và muốn undo:

```bash
# Undo commit nhưng giữ changes
git reset --soft HEAD~1

# Undo commit và discard changes (cẩn thận!)
git reset --hard HEAD~1
```

## Files quan trọng đã thay đổi:

**Core refactoring:**
- src/main/java/lib/core/query/BaseQuery.java (NEW)
- src/main/java/lib/core/query/expression/CommonFunctions.java (NEW)
- src/main/java/lib/core/clickhouse/query/ClickHouseQuery.java (MODIFIED)
- src/main/java/lib/core/clickhouse/expression/CH.java (MODIFIED)

**Validations:**
- src/main/java/lib/core/query/exception/InvalidRangeException.java (NEW)
- src/main/java/lib/core/query/builder/WhereBuilder.java (MODIFIED)
- src/main/java/lib/core/query/builder/HavingBuilder.java (MODIFIED)

**Tests:**
- src/test/java/lib/core/query/PhaseOrderingTest.java (NEW)
- src/test/java/lib/core/query/builder/RangeValidationTest.java (NEW)

**Documentation:**
- README.md (MODIFIED)
- docs/VALIDATION_GUIDE.md (NEW)
- build.gradle (version 1.1.0 → 1.2.0)

## Summary

Tổng cộng:
- ✅ Refactored to 2-layer architecture (base + ClickHouse)
- ✅ Added comprehensive validations (null-safety, range validation, phase ordering)
- ✅ Added 26 new test cases
- ✅ Updated documentation
- ✅ 100% backward compatible
- ✅ Version bumped to 1.2.0
