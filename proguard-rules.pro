# ============================================================
# ProGuard rules for clickhouse-query-builder
# ============================================================

# Keep ALL public classes, their inner classes, and all public/protected members
# This ensures the entire public API surface is preserved while
# obfuscating only private internals (method bodies, local variables, etc.)
-keep public class lib.core.** { public *; protected *; }

# Keep inner classes (builder pattern chains return inner types)
-keep class lib.core.**$* { public *; protected *; }

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

# Don't optimize (keep method signatures intact)
-dontoptimize

# Keep line numbers for stack traces
-keepattributes SourceFile,LineNumberTable
-renamesourcefileattribute SourceFile
