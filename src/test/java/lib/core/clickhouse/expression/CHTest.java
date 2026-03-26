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
        assertEquals("count(*) AS total", CH.count().as("total"));
    }

    @Test
    @DisplayName("count(column).as() generates count(column) AS alias")
    void countColumn() {
        assertEquals("count(user_id) AS user_count", CH.count("user_id").as("user_count"));
    }

    @Test
    @DisplayName("countDistinct().as() generates countDistinct(column) AS alias")
    void countDistinct() {
        assertEquals("countDistinct(round_id) AS total_sessions",
                CH.countDistinct("round_id").as("total_sessions"));
    }

    @Test
    @DisplayName("sum().as() generates sum(column) AS alias")
    void sum() {
        assertEquals("sum(amount) AS total_amount", CH.sum("amount").as("total_amount"));
    }

    @Test
    @DisplayName("sumIfRaw().as() generates sumIf(column, condition) AS alias")
    void sumIfRaw() {
        assertEquals("sumIf(amount, action = 'BET') AS total_bet",
                CH.sumIfRaw("amount", "action = 'BET'").as("total_bet"));
    }

    @Test
    @DisplayName("sumIfRaw with in() generates correct nested expression")
    void sumIfRawWithIn() {
        String result = CH.sumIfRaw("amount", CH.in("action", "RESULT", "JACKPOT_WIN")).as("total_win");
        assertEquals("sumIf(amount, action IN ('RESULT','JACKPOT_WIN')) AS total_win", result);
    }

    @Test
    @DisplayName("fluent sumIf().where().eq() generates sumIf(column, col = 'val')")
    void sumIfFluent() {
        assertEquals("sumIf(amount, status = 'COMPLETED') AS completed_revenue",
                CH.sumIf("amount").where("status").eq("COMPLETED").as("completed_revenue"));
    }

    @Test
    @DisplayName("fluent sumIf().where().in() generates sumIf(column, col IN (...))")
    void sumIfFluentIn() {
        assertEquals("sumIf(amount, action IN ('SALE','UPSELL')) AS total_sales",
                CH.sumIf("amount").where("action").in("SALE", "UPSELL").as("total_sales"));
    }

    @Test
    @DisplayName("fluent sumIf().where().gt() generates sumIf(column, col > val)")
    void sumIfFluentGt() {
        assertEquals("sumIf(amount, score > 100) AS high_amount",
                CH.sumIf("amount").where("score").gt(100).as("high_amount"));
    }

    @Test
    @DisplayName("min().as() generates min(column) AS alias")
    void min() {
        assertEquals("min(created_at) AS first_ts", CH.min("created_at").as("first_ts"));
    }

    @Test
    @DisplayName("max().as() generates max(column) AS alias")
    void max() {
        assertEquals("max(created_at) AS last_ts", CH.max("created_at").as("last_ts"));
    }

    @Test
    @DisplayName("avg().as() generates avg(column) AS alias")
    void avg() {
        assertEquals("avg(amount) AS avg_amount", CH.avg("amount").as("avg_amount"));
    }

    @Test
    @DisplayName("Expr.toString() returns raw expression without alias")
    void exprToString() {
        assertEquals("sum(amount)", CH.sum("amount").toString());
    }

    @Test
    @DisplayName("col() returns column name as-is")
    void col() {
        assertEquals("user_id", CH.col("user_id"));
    }

    @Test
    @DisplayName("col(column, alias) generates column AS alias")
    void colWithAlias() {
        assertEquals("user_id AS uid", CH.col("user_id", "uid"));
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
        String result = CH.caseWhen("amount").gt(0).then("WIN")
                .orElse("LOSE")
                .as("result");

        assertEquals("CASE WHEN amount > 0 THEN 'WIN' ELSE 'LOSE' END AS result", result);
    }

    @Test
    @DisplayName("caseWhen with multiple WHEN clauses")
    void caseWhen_multiple() {
        String result = CH.caseWhen("amount").gt(0).then("WIN")
                .when("amount").eq(0).then("DRAW")
                .orElse("LOSE")
                .as("result");

        assertEquals("CASE WHEN amount > 0 THEN 'WIN' WHEN amount = 0 THEN 'DRAW' ELSE 'LOSE' END AS result", result);
    }

    @Test
    @DisplayName("caseWhen with number THEN values")
    void caseWhen_numberValues() {
        String result = CH.caseWhen("action").eq("BET").then(1)
                .orElse(0)
                .as("is_bet");

        assertEquals("CASE WHEN action = 'BET' THEN 1 ELSE 0 END AS is_bet", result);
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
        String result = CH.caseWhen("action").in("RESULT", "JACKPOT_WIN").then("WIN")
                .orElse("OTHER")
                .as("type");

        assertTrue(result.contains("action IN ('RESULT', 'JACKPOT_WIN')"));
        assertTrue(result.contains("THEN 'WIN'"));
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
        String result = CH.caseWhen("type").eq("CREDIT").thenRaw("amount")
                .orElseRaw("amount * -1")
                .as("adjusted_amount");

        assertTrue(result.contains("THEN amount"));
        assertTrue(result.contains("ELSE amount * -1"));
        assertFalse(result.contains("'amount'")); // should NOT be quoted
    }

    @Test
    @DisplayName("caseWhen with .end() (no ELSE)")
    void caseWhen_end() {
        String result = CH.caseWhen("status").eq("VIP").then("YES")
                .end()
                .as("is_vip");

        assertTrue(result.contains("THEN 'YES' END"));
        assertFalse(result.contains("ELSE"));
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
        String result = CH.caseWhen("score").between(0, 49).then("LOW")
                .when("score").between(50, 79).then("MEDIUM")
                .when("score").between(80, 100).then("HIGH")
                .orElse("UNKNOWN")
                .as("grade");

        assertTrue(result.contains("WHEN score BETWEEN 0 AND 49 THEN 'LOW'"));
        assertTrue(result.contains("WHEN score BETWEEN 50 AND 79 THEN 'MEDIUM'"));
        assertTrue(result.contains("WHEN score BETWEEN 80 AND 100 THEN 'HIGH'"));
        assertTrue(result.contains("ELSE 'UNKNOWN' END AS grade"));
    }

    @Test
    @DisplayName("caseWhen BETWEEN with string values")
    void caseWhen_between_strings() {
        String result = CH.caseWhen("tier").between("A", "C").then("TOP")
                .orElse("OTHER")
                .as("category");

        assertTrue(result.contains("BETWEEN 'A' AND 'C'"));
    }

    // ── Conditional Aggregates ────────────────────────────────────────────

    @Test
    @DisplayName("countIfRaw() generates countIf(column, condition)")
    void countIfRaw() {
        assertEquals("countIf(user_id, status = 'ACTIVE') AS active_users",
                CH.countIfRaw("user_id", "status = 'ACTIVE'").as("active_users"));
    }

    @Test
    @DisplayName("minIfRaw() generates minIf(column, condition)")
    void minIfRaw() {
        assertEquals("minIf(amount, type = 'SALE') AS min_sale",
                CH.minIfRaw("amount", "type = 'SALE'").as("min_sale"));
    }

    @Test
    @DisplayName("maxIfRaw() generates maxIf(column, condition)")
    void maxIfRaw() {
        assertEquals("maxIf(amount, type = 'SALE') AS max_sale",
                CH.maxIfRaw("amount", "type = 'SALE'").as("max_sale"));
    }

    @Test
    @DisplayName("avgIfRaw() generates avgIf(column, condition)")
    void avgIfRaw() {
        assertEquals("avgIf(score, status = 'COMPLETED') AS avg_score",
                CH.avgIfRaw("score", "status = 'COMPLETED'").as("avg_score"));
    }

    @Test
    @DisplayName("countIfRaw with CH.in() condition")
    void countIfRawWithIn() {
        String result = CH.countIfRaw("user_id", CH.in("status", "ACTIVE", "VIP")).as("vip_count");
        assertEquals("countIf(user_id, status IN ('ACTIVE','VIP')) AS vip_count", result);
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
        assertEquals("countIf(user_id, type = 'VIP') AS vip_count",
                CH.countIf("user_id").where("type").eq("VIP").as("vip_count"));
    }

    @Test
    @DisplayName("fluent minIf().where().eq()")
    void minIfFluent() {
        assertEquals("minIf(amount, type = 'SALE') AS min_sale",
                CH.minIf("amount").where("type").eq("SALE").as("min_sale"));
    }

    @Test
    @DisplayName("fluent maxIf().where().gt()")
    void maxIfFluent() {
        assertEquals("maxIf(amount, score > 50) AS max_amount",
                CH.maxIf("amount").where("score").gt(50).as("max_amount"));
    }

    @Test
    @DisplayName("fluent avgIf().where().isNotNull()")
    void avgIfFluent() {
        assertEquals("avgIf(score, status IS NOT NULL) AS avg_score",
                CH.avgIf("score").where("status").isNotNull().as("avg_score"));
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
}
