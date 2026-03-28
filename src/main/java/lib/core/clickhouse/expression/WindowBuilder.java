package lib.core.clickhouse.expression;

import lib.core.clickhouse.query.SortOrder;

import java.util.ArrayList;
import java.util.List;

/**
 * Fluent builder for SQL window function expressions ({@code OVER(...)}).
 *
 * <pre>{@code
 * // Full window: row_number() OVER(PARTITION BY user_id ORDER BY created_at DESC)
 * CH.rowNumber().over().partitionBy("user_id").orderBy("created_at", SortOrder.DESC).as("rank")
 *
 * // Running total per game
 * CH.sum("amount").over().partitionBy("game_id").orderBy("created_at").as("running_total")
 *
 * // No partition — whole result set
 * CH.rowNumber().over().orderBy("amount", SortOrder.DESC).as("rank")
 *
 * // Alias-aware
 * spin.sum("amount").over().partitionBy(spin.col("user_id")).orderBy(spin.col("created_at")).as("run")
 * }</pre>
 *
 * @see CH#rowNumber()
 * @see CH#rank()
 * @see CH#denseRank()
 */
public final class WindowBuilder {
  private final String expression;
  private final List<String> partitionCols = new ArrayList<>();
  private final List<String> orderCols = new ArrayList<>();

  public WindowBuilder(String expression) {
    this.expression = expression;
  }

  // ── PARTITION BY ──────────────────────────────────────────────────

  /** Add columns to PARTITION BY. */
  public WindowBuilder partitionBy(String... columns) {
    partitionCols.addAll(List.of(columns));
    return this;
  }

  /** Add columns to PARTITION BY (Expr overload). */
  public WindowBuilder partitionBy(Object... columns) {
    for (Object col : columns) {
      partitionCols.add(col.toString());
    }
    return this;
  }

  // ── ORDER BY ──────────────────────────────────────────────────────

  /** Add column to ORDER BY ASC. */
  public WindowBuilder orderBy(String column) {
    orderCols.add(column + " ASC");
    return this;
  }

  /** Add column to ORDER BY with direction. */
  public WindowBuilder orderBy(String column, SortOrder direction) {
    orderCols.add(column + " " + direction.name());
    return this;
  }

  /** Add column to ORDER BY ASC (Expr overload). */
  public WindowBuilder orderBy(Object column) {
    return orderBy(column.toString());
  }

  /** Add column to ORDER BY with direction (Expr overload). */
  public WindowBuilder orderBy(Object column, SortOrder direction) {
    return orderBy(column.toString(), direction);
  }

  // ── Terminal ──────────────────────────────────────────────────────

  /**
   * Build the complete window expression and return as {@link CH.Expr}.
   * Alias can be added by chaining {@code .as("alias")}.
   *
   * <pre>{@code
   * CH.rowNumber().over().partitionBy("user_id").orderBy("created_at").build()
   * // → row_number() OVER(PARTITION BY user_id ORDER BY created_at ASC)
   * }</pre>
   */
  public CH.Expr build() {
    StringBuilder sb = new StringBuilder(expression).append(" OVER(");
    if (!partitionCols.isEmpty()) {
      sb.append("PARTITION BY ").append(String.join(", ", partitionCols));
    }
    if (!orderCols.isEmpty()) {
      if (!partitionCols.isEmpty()) sb.append(" ");
      sb.append("ORDER BY ").append(String.join(", ", orderCols));
    }
    sb.append(")");
    return new CH.Expr(sb.toString());
  }

  /**
   * Shorthand: build and add alias.
   * {@code .as("rank")} is equivalent to {@code .build().as("rank")}.
   */
  public CH.Expr as(String alias) {
    return build().as(alias);
  }

  /** Return the built expression as a string. */
  @Override
  public String toString() {
    return build().toString();
  }
}
