package lib.core.clickhouse.expression;


import lib.core.clickhouse.query.ClickHouseQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link CH} fluent expression builder.
 */
class CHTest {

    @Test
    @DisplayName("count().as() generates count(*) AS alias")
    void countAll() {
        assertEquals(CH.count().as("total"), "count(*) AS total");
    }

    @Test
    @DisplayName("count(column).as() generates count(column) AS alias")
    void countColumn() {
        assertEquals(CH.count("user_id").as("user_count"), "count(user_id) AS user_count");
    }

    @Test
    @DisplayName("countDistinct().as() generates countDistinct(column) AS alias")
    void countDistinct() {
        assertEquals(CH.countDistinct("round_id").as("total_sessions"),
                "countDistinct(round_id) AS total_sessions");
    }

    @Test
    @DisplayName("sum().as() generates sum(column) AS alias")
    void sum() {
        assertEquals(CH.sum("amount").as("total_amount"), "sum(amount) AS total_amount");
    }

    @Test
    @DisplayName("sumIfRaw().as() generates sumIf(column, condition) AS alias")
    void sumIfRaw() {
        assertEquals(CH.sumIfRaw("amount", "action = 'BET'").as("total_bet"),
                "sumIf(amount, action = 'BET') AS total_bet");
    }

    @Test
    @DisplayName("sumIfRaw with in() generates correct nested expression")
    void sumIfRawWithIn() {
        var result = CH.sumIfRaw("amount", CH.in("action", "RESULT", "JACKPOT_WIN")).as("total_win");
        assertEquals(result, "sumIf(amount, action IN ('RESULT','JACKPOT_WIN')) AS total_win");
    }

    @Test
    @DisplayName("fluent sumIf().where().eq() generates sumIf(column, col = 'val')")
    void sumIfFluent() {
        assertEquals(CH.sumIf("amount").where("status").eq("COMPLETED").as("completed_revenue"),
                "sumIf(amount, status = 'COMPLETED') AS completed_revenue");
    }

    @Test
    @DisplayName("fluent sumIf().where().in() generates sumIf(column, col IN (...))")
    void sumIfFluentIn() {
        assertEquals(CH.sumIf("amount").where("action").in("SALE", "UPSELL").as("total_sales"),
                "sumIf(amount, action IN ('SALE','UPSELL')) AS total_sales");
    }

    @Test
    @DisplayName("fluent sumIf().where().gt() generates sumIf(column, col > val)")
    void sumIfFluentGt() {
        assertEquals(CH.sumIf("amount").where("score").gt(100).as("high_amount"),
                "sumIf(amount, score > 100) AS high_amount");
    }

    @Test
    @DisplayName("min().as() generates min(column) AS alias")
    void min() {
        assertEquals(CH.min("created_at").as("first_ts"), "min(created_at) AS first_ts");
    }

    @Test
    @DisplayName("max().as() generates max(column) AS alias")
    void max() {
        assertEquals(CH.max("created_at").as("last_ts"), "max(created_at) AS last_ts");
    }

    @Test
    @DisplayName("avg().as() generates avg(column) AS alias")
    void avg() {
        assertEquals(CH.avg("amount").as("avg_amount"), "avg(amount) AS avg_amount");
    }

    @Test
    @DisplayName("Expr.toString() returns raw expression without alias")
    void exprToString() {
        assertEquals("sum(amount)", CH.sum("amount").toString());
    }

    @Test
    @DisplayName("col() returns column name as-is")
    void col() {
        assertEquals(CH.col("user_id"), "user_id");
    }

    @Test
    @DisplayName("col(column, alias) generates column AS alias")
    void colWithAlias() {
        assertEquals(CH.col("user_id", "uid"), "user_id AS uid");
    }

    @Test
    @DisplayName("in() generates IN expression with quoted values")
    void in() {
        assertEquals("action IN ('BET','RESULT','JACKPOT_WIN')",
                CH.in("action", "BET", "RESULT", "JACKPOT_WIN"));
    }

    @Test
    @DisplayName("in() with single value")
    void inSingle() {
        assertEquals("status IN ('ACTIVE')", CH.in("status", "ACTIVE"));
    }

    // ── CASE WHEN ───────────────────────────────────────────────────────

    @Test
    @DisplayName("caseWhen with gt and string then/else")
    void caseWhen_gt_string() {
        var result = CH.caseWhen("amount").gt(0).then("WIN")
                .orElse("LOSE")
                .as("result");

        assertEquals(result, "CASE WHEN amount > 0 THEN 'WIN' ELSE 'LOSE' END AS result");
    }

    @Test
    @DisplayName("caseWhen with multiple WHEN clauses")
    void caseWhen_multiple() {
        var result = CH.caseWhen("amount").gt(0).then("WIN")
                .when("amount").eq(0).then("DRAW")
                .orElse("LOSE")
                .as("result");

        assertEquals(result, "CASE WHEN amount > 0 THEN 'WIN' WHEN amount = 0 THEN 'DRAW' ELSE 'LOSE' END AS result");
    }

    @Test
    @DisplayName("caseWhen with number THEN values")
    void caseWhen_numberValues() {
        var result = CH.caseWhen("action").eq("BET").then(1)
                .orElse(0)
                .as("is_bet");

        assertEquals(result, "CASE WHEN action = 'BET' THEN 1 ELSE 0 END AS is_bet");
    }

    @Test
    @DisplayName("caseWhen with lt, lte, gte, ne operators")
    void caseWhen_allOperators() {
        String lt = CH.caseWhen("score").lt(50).then("LOW").orElse("OK").toString();
        assertTrue(lt.contains("score < 50"));

        String lte = CH.caseWhen("score").lte(50).then("LOW").orElse("OK").toString();
        assertTrue(lte.contains("score <= 50"));

        String gte = CH.caseWhen("score").gte(90).then("HIGH").orElse("OK").toString();
        assertTrue(gte.contains("score >= 90"));

        String ne = CH.caseWhen("status").ne("DELETED").then(1).orElse(0).toString();
        assertTrue(ne.contains("status != 'DELETED'"));
    }

    @Test
    @DisplayName("caseWhen with IN operator")
    void caseWhen_in() {
        var result = CH.caseWhen("action").in("RESULT", "JACKPOT_WIN").then("WIN")
                .orElse("OTHER")
                .as("type");

        assertTrue(result.toString().contains("action IN ('RESULT', 'JACKPOT_WIN')"));
        assertTrue(result.toString().contains("THEN 'WIN'"));
    }

    @Test
    @DisplayName("caseWhen with isNull / isNotNull")
    void caseWhen_nullChecks() {
        String isNull = CH.caseWhen("deleted_at").isNull().then("ACTIVE")
                .orElse("DELETED").toString();
        assertTrue(isNull.contains("deleted_at IS NULL"));

        String isNotNull = CH.caseWhen("error_code").isNotNull().then("ERROR")
                .orElse("OK").toString();
        assertTrue(isNotNull.contains("error_code IS NOT NULL"));
    }

    @Test
    @DisplayName("caseWhen with thenRaw (unquoted column/expression)")
    void caseWhen_thenRaw() {
        var result = CH.caseWhen("type").eq("CREDIT").thenRaw("amount")
                .orElseRaw("amount * -1")
                .as("adjusted_amount");

        assertTrue(result.toString().contains("THEN amount"));
        assertTrue(result.toString().contains("ELSE amount * -1"));
        assertFalse(result.toString().contains("'amount'")); // should NOT be quoted
    }

    @Test
    @DisplayName("caseWhen with .end() (no ELSE)")
    void caseWhen_end() {
        var result = CH.caseWhen("status").eq("VIP").then("YES")
                .end()
                .as("is_vip");

        assertTrue(result.toString().contains("THEN 'YES' END"));
        assertFalse(result.toString().contains("ELSE"));
    }

    @Test
    @DisplayName("caseWhen works inside ClickHouseQuery.select()")
    void caseWhen_inQuery() {
        String sql = ClickHouseQuery.select(
                CH.col("user_id"),
                CH.caseWhen("amount").gt(0).then("WIN")
                        .when("amount").eq(0).then("DRAW")
                        .orElse("LOSE")
                        .as("result")
        ).from("orders").toSql();

        assertTrue(sql.contains("CASE WHEN amount > 0 THEN 'WIN'"));
        assertTrue(sql.contains("WHEN amount = 0 THEN 'DRAW'"));
        assertTrue(sql.contains("ELSE 'LOSE' END AS result"));
    }

    @Test
    @DisplayName("caseWhen with BETWEEN operator")
    void caseWhen_between() {
        var result = CH.caseWhen("score").between(0, 49).then("LOW")
                .when("score").between(50, 79).then("MEDIUM")
                .when("score").between(80, 100).then("HIGH")
                .orElse("UNKNOWN")
                .as("grade");

        assertTrue(result.toString().contains("WHEN score BETWEEN 0 AND 49 THEN 'LOW'"));
        assertTrue(result.toString().contains("WHEN score BETWEEN 50 AND 79 THEN 'MEDIUM'"));
        assertTrue(result.toString().contains("WHEN score BETWEEN 80 AND 100 THEN 'HIGH'"));
        assertTrue(result.toString().contains("ELSE 'UNKNOWN' END AS grade"));
    }

    @Test
    @DisplayName("caseWhen BETWEEN with string values")
    void caseWhen_between_strings() {
        var result = CH.caseWhen("tier").between("A", "C").then("TOP")
                .orElse("OTHER")
                .as("category");

        assertTrue(result.toString().contains("BETWEEN 'A' AND 'C'"));
    }

    // ── Conditional Aggregates ────────────────────────────────────────────

    @Test
    @DisplayName("countIfRaw() generates countIf(column, condition)")
    void countIfRaw() {
        assertEquals(CH.countIfRaw("user_id", "status = 'ACTIVE'").as("active_users"),
                "countIf(user_id, status = 'ACTIVE') AS active_users");
    }

    @Test
    @DisplayName("minIfRaw() generates minIf(column, condition)")
    void minIfRaw() {
        assertEquals(CH.minIfRaw("amount", "type = 'SALE'").as("min_sale"),
                "minIf(amount, type = 'SALE') AS min_sale");
    }

    @Test
    @DisplayName("maxIfRaw() generates maxIf(column, condition)")
    void maxIfRaw() {
        assertEquals(CH.maxIfRaw("amount", "type = 'SALE'").as("max_sale"),
                "maxIf(amount, type = 'SALE') AS max_sale");
    }

    @Test
    @DisplayName("avgIfRaw() generates avgIf(column, condition)")
    void avgIfRaw() {
        assertEquals(CH.avgIfRaw("score", "status = 'COMPLETED'").as("avg_score"),
                "avgIf(score, status = 'COMPLETED') AS avg_score");
    }

    @Test
    @DisplayName("countIfRaw with CH.in() condition")
    void countIfRawWithIn() {
        var result = CH.countIfRaw("user_id", CH.in("status", "ACTIVE", "VIP")).as("vip_count");
        assertEquals(result, "countIf(user_id, status IN ('ACTIVE','VIP')) AS vip_count");
    }

    @Test
    @DisplayName("Conditional aggregates (raw) in ClickHouseQuery.select()")
    void conditionalAggregatesRawInQuery() {
        String sql = ClickHouseQuery.select(
                CH.col("product_id"),
                CH.sumIfRaw("amount", "action = 'BET'").as("total_bet"),
                CH.countIfRaw("user_id", "status = 'ACTIVE'").as("active_count"),
                CH.minIfRaw("amount", "type = 'SALE'").as("min_sale"),
                CH.maxIfRaw("amount", "type = 'SALE'").as("max_sale"),
                CH.avgIfRaw("score", "status = 'DONE'").as("avg_score")
        ).from("orders").groupBy("product_id").toSql();

        assertTrue(sql.contains("sumIf(amount, action = 'BET') AS total_bet"));
        assertTrue(sql.contains("countIf(user_id, status = 'ACTIVE') AS active_count"));
        assertTrue(sql.contains("minIf(amount, type = 'SALE') AS min_sale"));
        assertTrue(sql.contains("maxIf(amount, type = 'SALE') AS max_sale"));
        assertTrue(sql.contains("avgIf(score, status = 'DONE') AS avg_score"));
    }

    // ── Fluent Conditional Aggregates ─────────────────────────────────────

    @Test
    @DisplayName("fluent countIf().where().eq()")
    void countIfFluent() {
        assertEquals(CH.countIf("user_id").where("type").eq("VIP").as("vip_count"),
                "countIf(user_id, type = 'VIP') AS vip_count");
    }

    @Test
    @DisplayName("fluent minIf().where().eq()")
    void minIfFluent() {
        assertEquals(CH.minIf("amount").where("type").eq("SALE").as("min_sale"),
                "minIf(amount, type = 'SALE') AS min_sale");
    }

    @Test
    @DisplayName("fluent maxIf().where().gt()")
    void maxIfFluent() {
        assertEquals(CH.maxIf("amount").where("score").gt(50).as("max_amount"),
                "maxIf(amount, score > 50) AS max_amount");
    }

    @Test
    @DisplayName("fluent avgIf().where().isNotNull()")
    void avgIfFluent() {
        assertEquals(CH.avgIf("score").where("status").isNotNull().as("avg_score"),
                "avgIf(score, status IS NOT NULL) AS avg_score");
    }

    @Test
    @DisplayName("fluent conditional aggregates in ClickHouseQuery.select()")
    void conditionalAggregatesFluentInQuery() {
        String sql = ClickHouseQuery.select(
                CH.col("product_id"),
                CH.sumIf("amount").where("action").eq("BET").as("total_bet"),
                CH.countIf("user_id").where("status").eq("ACTIVE").as("active_count"),
                CH.minIf("amount").where("type").eq("SALE").as("min_sale"),
                CH.maxIf("amount").where("type").eq("SALE").as("max_sale"),
                CH.avgIf("score").where("status").eq("DONE").as("avg_score")
        ).from("orders").groupBy("product_id").toSql();

        assertTrue(sql.contains("sumIf(amount, action = 'BET') AS total_bet"));
        assertTrue(sql.contains("countIf(user_id, status = 'ACTIVE') AS active_count"));
        assertTrue(sql.contains("minIf(amount, type = 'SALE') AS min_sale"));
        assertTrue(sql.contains("maxIf(amount, type = 'SALE') AS max_sale"));
        assertTrue(sql.contains("avgIf(score, status = 'DONE') AS avg_score"));
    }

    // ── Window Functions ──────────────────────────────────────────────────

    @Test
    @DisplayName("rowNumber().over().partitionBy().orderBy() generates window function")
    void windowFunction_rowNumber() {
        var result = CH.rowNumber().over()
                .partitionBy("user_id")
                .orderBy("created_at")
                .as("row_num");

        assertEquals("row_number() OVER(PARTITION BY user_id ORDER BY created_at ASC) AS row_num",
                result.toString());
    }

    @Test
    @DisplayName("rank().over().orderBy() without partition")
    void windowFunction_rank_noPartition() {
        var result = CH.rank().over()
                .orderBy("amount", lib.core.query.SortOrder.DESC)
                .as("rank");

        assertEquals("rank() OVER(ORDER BY amount DESC) AS rank", result.toString());
    }

    @Test
    @DisplayName("denseRank().over() generates dense_rank window function")
    void windowFunction_denseRank() {
        var result = CH.denseRank().over()
                .partitionBy("game_id")
                .orderBy("score", lib.core.query.SortOrder.DESC)
                .as("dense_rank");

        assertEquals("dense_rank() OVER(PARTITION BY game_id ORDER BY score DESC) AS dense_rank",
                result.toString());
    }

    @Test
    @DisplayName("lag(column) generates lag window function with offset 1")
    void windowFunction_lag() {
        var result = CH.lag("amount").over()
                .partitionBy("user_id")
                .orderBy("created_at")
                .as("prev_amount");

        assertEquals("lag(amount, 1) OVER(PARTITION BY user_id ORDER BY created_at ASC) AS prev_amount",
                result.toString());
    }

    @Test
    @DisplayName("lag(column, offset) generates lag with custom offset")
    void windowFunction_lag_customOffset() {
        var result = CH.lag("amount", 3).over()
                .partitionBy("user_id")
                .orderBy("created_at")
                .as("prev_3_amount");

        assertEquals("lag(amount, 3) OVER(PARTITION BY user_id ORDER BY created_at ASC) AS prev_3_amount",
                result.toString());
    }

    @Test
    @DisplayName("lead(column) generates lead window function")
    void windowFunction_lead() {
        var result = CH.lead("amount").over()
                .partitionBy("user_id")
                .orderBy("created_at")
                .as("next_amount");

        assertEquals("lead(amount, 1) OVER(PARTITION BY user_id ORDER BY created_at ASC) AS next_amount",
                result.toString());
    }

    @Test
    @DisplayName("lead(column, offset) generates lead with custom offset")
    void windowFunction_lead_customOffset() {
        var result = CH.lead("amount", 2).over()
                .orderBy("created_at")
                .as("next_2_amount");

        assertEquals("lead(amount, 2) OVER(ORDER BY created_at ASC) AS next_2_amount",
                result.toString());
    }

    @Test
    @DisplayName("firstValue(column) generates first_value window function")
    void windowFunction_firstValue() {
        var result = CH.firstValue("amount").over()
                .partitionBy("user_id")
                .orderBy("created_at")
                .as("first_order");

        assertEquals("first_value(amount) OVER(PARTITION BY user_id ORDER BY created_at ASC) AS first_order",
                result.toString());
    }

    @Test
    @DisplayName("lastValue(column) generates last_value window function")
    void windowFunction_lastValue() {
        var result = CH.lastValue("amount").over()
                .partitionBy("user_id")
                .orderBy("created_at")
                .as("last_order");

        assertEquals("last_value(amount) OVER(PARTITION BY user_id ORDER BY created_at ASC) AS last_order",
                result.toString());
    }

    @Test
    @DisplayName("ntile(n) generates ntile window function")
    void windowFunction_ntile() {
        var result = CH.ntile(4).over()
                .orderBy("amount", lib.core.query.SortOrder.DESC)
                .as("quartile");

        assertEquals("ntile(4) OVER(ORDER BY amount DESC) AS quartile", result.toString());
    }

    @Test
    @DisplayName("sum().over() generates running sum window function")
    void windowFunction_sum() {
        var result = CH.sum("amount").over()
                .partitionBy("user_id")
                .orderBy("created_at")
                .as("running_total");

        assertEquals("sum(amount) OVER(PARTITION BY user_id ORDER BY created_at ASC) AS running_total",
                result.toString());
    }

    @Test
    @DisplayName("avg().over() generates running average window function")
    void windowFunction_avg() {
        var result = CH.avg("score").over()
                .partitionBy("game_id")
                .orderBy("round_id")
                .as("running_avg");

        assertEquals("avg(score) OVER(PARTITION BY game_id ORDER BY round_id ASC) AS running_avg",
                result.toString());
    }

    @Test
    @DisplayName("count().over() generates running count window function")
    void windowFunction_count() {
        var result = CH.count().over()
                .partitionBy("user_id")
                .orderBy("created_at")
                .as("running_count");

        assertEquals("count(*) OVER(PARTITION BY user_id ORDER BY created_at ASC) AS running_count",
                result.toString());
    }

    @Test
    @DisplayName("Window function with multiple partition columns")
    void windowFunction_multiplePartitions() {
        var result = CH.rowNumber().over()
                .partitionBy("user_id", "game_id")
                .orderBy("created_at")
                .as("row_num");

        assertEquals("row_number() OVER(PARTITION BY user_id, game_id ORDER BY created_at ASC) AS row_num",
                result.toString());
    }

    @Test
    @DisplayName("Window function in ClickHouseQuery.select()")
    void windowFunction_inQuery() {
        String sql = ClickHouseQuery.select(
                CH.col("user_id"),
                CH.col("amount"),
                CH.rowNumber().over()
                        .partitionBy("user_id")
                        .orderBy("created_at", lib.core.query.SortOrder.DESC)
                        .as("rank")
        ).from("orders").toSql();

        assertTrue(sql.contains("row_number() OVER(PARTITION BY user_id ORDER BY created_at DESC) AS rank"));
    }

    @Test
    @DisplayName("raw() wraps raw SQL expression")
    void raw() {
        assertEquals("toYYYYMM(created_at)", CH.raw("toYYYYMM(created_at)").toString());
    }

    @Test
    @DisplayName("raw().as() adds alias to raw expression")
    void raw_withAlias() {
        assertEquals("toYYYYMM(created_at) AS month",
                CH.raw("toYYYYMM(created_at)").as("month").toString());
    }
}
