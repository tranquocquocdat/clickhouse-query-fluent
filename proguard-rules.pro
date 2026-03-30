# ============================================================
# ProGuard rules for clickhouse-query-builder
# ============================================================

# Keep all public API classes and their public/protected members
-keep public class lib.core.clickhouse.query.ClickHouseQuery { public *; protected *; }
-keep public class lib.core.clickhouse.expression.CH { public *; protected *; }
-keep public class lib.core.clickhouse.expression.CH$Expr { public *; protected *; }
-keep public class lib.core.clickhouse.insert.ClickHouseInsert { public *; protected *; }
-keep public class lib.core.clickhouse.util.CHParams { public *; protected *; }
-keep public class lib.core.clickhouse.util.CHStringUtils { public *; protected *; }
-keep public class lib.core.clickhouse.util.ClickHouseDateUtil { public *; protected *; }
-keep public class lib.core.clickhouse.expression.AggIfBuilder { public *; protected *; }

# Keep query package public API
-keep public class lib.core.query.Alias { public *; protected *; }
-keep public class lib.core.query.BaseQuery { public *; protected *; }
-keep public class lib.core.query.Page { public *; protected *; }
-keep public class lib.core.query.SortOrder { public *; protected *; }
-keep public class lib.core.query.RecordRowMapper { public *; protected *; }
-keep public class lib.core.query.RecordMapperCache { public *; protected *; }

# Keep cursor pagination classes
-keep public class lib.core.query.cursor.** { public *; protected *; }

# Keep exception classes
-keep public class lib.core.query.exception.** { public *; protected *; }

# Keep observer/cache interfaces
-keep public class lib.core.query.observe.** { public *; protected *; }
-keep public class lib.core.query.cache.** { public *; protected *; }

# Keep Spring AutoConfiguration
-keep public class lib.core.autoconfigure.** { public *; protected *; }

# Keep builder classes (public methods used in fluent API chain)
-keep public class lib.core.query.builder.** { public *; protected *; }

# Keep enums
-keepclassmembers enum * { *; }

# Keep annotations
-keepattributes *Annotation*
-keepattributes Signature
-keepattributes InnerClasses
-keepattributes EnclosingMethod
-keepattributes Exceptions

# Don't warn about Spring dependencies
-dontwarn org.springframework.**
-dontwarn org.slf4j.**

# Don't optimize (keep method signatures intact for debugging)
-dontoptimize

# Keep line numbers for stack traces
-keepattributes SourceFile,LineNumberTable
-renamesourcefileattribute SourceFile
