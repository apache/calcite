/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.benchmarks;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark that constructs a synthetic query plan consisting of a large plan.
 *
 * <p>This benchmark primarily measures planner overhead under heavy rule activity: matching and
 * firing rules, replacing RelNodes, and traversing the evolving plan during optimization.
 * It also simulates a multiphase optimization flow by running multiple HepPrograms sequentially.
 *
 * <p>Each UNION input branch contains layers of Projects and Filters that are intentionally
 *  simplifiable, so that typical planner rules (e.g., Project/Filter simplification)
 *  are repeatedly applicable.
 */

@Fork(value = 1, jvmArgsPrepend = {"-Xss200m",
    "-Dcalcite.disable.generate.type.digest.string=true"})
@Measurement(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Threads(1)
public class LargePlanBenchmark {

  @Param({"100", "1000", "10000", "100000"})
  int unionNum;

  // For large plans, "DEPTH_FIRST", "BOTTOM_UP", and "TOP_DOWN" are slower than ARBITRARY
  @Param({"ARBITRARY"})
  String matchOrder;

  // Enable validation mode to verify rule application counts across different orders
  // When enabled, all orders are tested and rule attempts are validated
  boolean enableValidation = false;

  boolean isLargePlanMode = true; // false is very slow in 10000 unions
  boolean isEnableFiredRulesCache = true;
  private static RelBuilder builder;

  // All available match orders for validation
  private static final String[] ALL_MATCH_ORDERS = {
      "ARBITRARY", "DEPTH_FIRST", "BOTTOM_UP", "TOP_DOWN"
  };

  // Validation sizes: 1, 10, 100, 1000
  private static final int[] VALIDATION_SIZES = {1, 10, 100, 1000};

  @Setup(Level.Trial)
  public void setup() {
    SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    rootSchema.add("EMP", new AbstractTable() {
      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("EMPNO", SqlTypeName.INTEGER)
            .add("ENAME", SqlTypeName.VARCHAR)
            .add("JOB", SqlTypeName.VARCHAR)
            .add("MGR", SqlTypeName.INTEGER)
            .add("HIREDATE", SqlTypeName.DATE)
            .add("SAL", SqlTypeName.INTEGER)
            .add("COMM", SqlTypeName.INTEGER)
            .add("DEPTNO", SqlTypeName.INTEGER)
            .build();
      }
    });

    builder =
        RelBuilder.create(Frameworks.newConfigBuilder()
            .defaultSchema(rootSchema)
            .build());
  }

  // select ENAME, i as cat_id, 'i' as cat_name, i as require_free_postage,
  // 0 as require_15return, 0 as require_48hour, 1 as require_insurance
  // from emp
  // where EMPNO = i and MGR >= 0 and MGR <= 0 and ENAME = 'Y' and SAL = i
  private RelNode makeSelectBranch(int i) {
    return builder.scan("EMP")
        .filter(
            builder.and(
                builder.equals(builder.field("EMPNO"), builder.literal(i)),
                builder.greaterThanOrEqual(
                    builder.field("MGR"), builder.literal(0)),
                builder.lessThanOrEqual(
                    builder.field("MGR"), builder.literal(0)),
                builder.equals(builder.field("ENAME"), builder.literal("Y")),
                builder.equals(builder.field("SAL"), builder.literal(i))
            )
        )
        .project(
            builder.field("ENAME"),
            builder.alias(builder.literal(i), "cat_id"),
            builder.alias(builder.literal(String.valueOf(i)), "cat_name"),
            builder.alias(builder.literal(i), "require_free_postage"),
            builder.alias(builder.literal(0), "require_15return"),
            builder.alias(builder.literal(0), "require_48hour"),
            builder.alias(builder.literal(1), "require_insurance")
        )
        .build();
  }

  private RelNode makeUnionTree(int unionNum) {
    RelNode union = makeSelectBranch(0);
    for (int i = 1; i < unionNum; i++) {
      RelNode right = makeSelectBranch(i);
      union = LogicalUnion.create(ImmutableList.of(union, right), true);
    }
    union = LogicalUnion.create(ImmutableList.of(union, makeSelectBranch(unionNum)), true);
    return union;
  }

  @Benchmark
  public void testLargeUnionPlan() {
    testLargeUnionPlan(unionNum, matchOrder, false);
  }

  /**
   * Executes the optimization with the given parameters.
   *
   * @param unionNum number of union branches
   * @param matchOrder the match order to use
   * @param collectStats whether to collect and return rule statistics (for validation)
   * @return rule statistics map if collectStats is true, otherwise empty map
   */
  public Map<String, Map<String, Pair<Long, Long>>> testLargeUnionPlan(
      int unionNum, String matchOrder, boolean collectStats) {

    RelNode root = makeUnionTree(unionNum);

    HepMatchOrder hepMatchOrder = HepMatchOrder.valueOf(matchOrder);

    HepProgram filterReduce = HepProgram.builder()
        .addMatchOrder(hepMatchOrder)
        .addRuleInstance(CoreRules.FILTER_REDUCE_EXPRESSIONS)
        .build();

    HepProgram projectReduce = HepProgram.builder()
        .addMatchOrder(hepMatchOrder)
        .addRuleInstance(CoreRules.PROJECT_REDUCE_EXPRESSIONS)
        .build();

    Map<String, Map<String, Pair<Long, Long>>> stats = new HashMap<>();

    if (!isLargePlanMode) {
      // Phase 1
      HepPlanner planner = new HepPlanner(filterReduce);
      planner.setRoot(root);
      planner.setEnableFiredRulesCache(isEnableFiredRulesCache);
      root = planner.findBestExp();

      // Phase 2
      planner = new HepPlanner(projectReduce);
      planner.setEnableFiredRulesCache(isEnableFiredRulesCache);
      planner.setRoot(root);
      root = planner.findBestExp();
    } else {
      HepPlanner planner = new HepPlanner();
      planner.setEnableFiredRulesCache(isEnableFiredRulesCache);
      planner.setLargePlanMode(isLargePlanMode);
      if (collectStats) {
        planner.enableRuleAttemptsTracking();
      }
      planner.setRoot(root);

      // Phase 1: Execute FILTER_REDUCE_EXPRESSIONS
      planner.executeProgram(filterReduce);
      if (collectStats) {
        stats.put("FILTER", new HashMap<>(planner.getRuleAttemptsInfo()));
      }
      planner.clear();

      // Phase 2: Execute PROJECT_REDUCE_EXPRESSIONS
      planner.executeProgram(projectReduce);
      if (collectStats) {
        stats.put("PROJECT", new HashMap<>(planner.getRuleAttemptsInfo()));
      }
      planner.clear();

      root = planner.buildFinalPlan();
    }

    return stats;
  }

  /**
   * Returns a string repeated n times (Java 8 compatible).
   */
  private static String repeat(String str, int count) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < count; i++) {
      sb.append(str);
    }
    return sb.toString();
  }

  /**
   * Runs validation mode to verify that different match orders produce
   * the same rule application counts.
   *
   * <p>This ensures that the match order does not affect optimization correctness,
   * only performance.
   */
  public void runValidation() {
    this.enableValidation = true;
    System.out.println("\n"
        + repeat("=", 80));
    System.out.println("VALIDATION MODE: Verifying rule application counts across match orders");
    System.out.println(repeat("=", 80) + "\n");

    // Collect stats for all orders and sizes
    Map<String, Map<Integer, Map<String, Map<String, Pair<Long, Long>>>>> allStats =
        new HashMap<>();

    for (String order : ALL_MATCH_ORDERS) {
      System.out.println("Testing match order: " + order);
      Map<Integer, Map<String, Map<String, Pair<Long, Long>>>> orderStats = new HashMap<>();

      for (int size : VALIDATION_SIZES) {
        System.out.println("  Size: " + size);
        Map<String, Map<String, Pair<Long, Long>>> stats =
            testLargeUnionPlan(size, order, true);
        orderStats.put(size, stats);
      }

      allStats.put(order, orderStats);
      System.out.println();
    }

    // Validate: compare all orders against ARBITRARY (as baseline)
    System.out.println("\n"
        + repeat("-", 80));
    System.out.println("VALIDATION RESULTS");
    System.out.println(repeat("-", 80) + "\n");

    boolean allPassed = true;
    Map<Integer, Map<String, Map<String, Pair<Long, Long>>>> baselineStats =
        allStats.get("ARBITRARY");

    for (String order : ALL_MATCH_ORDERS) {
      if (order.equals("ARBITRARY")) {
        continue;
      }

      System.out.println("Comparing " + order + " against ARBITRARY:");
      Map<Integer, Map<String, Map<String, Pair<Long, Long>>>> orderStats =
          allStats.get(order);

      for (int size : VALIDATION_SIZES) {
        Map<String, Map<String, Pair<Long, Long>>> baselineSizeStats = baselineStats.get(size);
        Map<String, Map<String, Pair<Long, Long>>> orderSizeStats = orderStats.get(size);

        boolean sizePassed = validateSizeStats(order, size, baselineSizeStats, orderSizeStats);
        if (!sizePassed) {
          allPassed = false;
        }
      }
      System.out.println();
    }

    System.out.println("\n"
        + repeat("=", 80));
    if (allPassed) {
      System.out.println("VALIDATION PASSED: All match orders produce "
          + "consistent rule application counts");
    } else {
      System.out.println("VALIDATION FAILED: Some match orders have "
          + "inconsistent rule application counts");
    }
    System.out.println(repeat("=", 80) + "\n");
  }

  /**
   * Validates that the rule statistics for a specific size match between
   * the baseline (ARBITRARY) and the test order.
   */
  private boolean validateSizeStats(String order, int size,
      Map<String, Map<String, Pair<Long, Long>>> baseline,
      Map<String, Map<String, Pair<Long, Long>>> test) {

    boolean passed = true;
    StringBuilder sb = new StringBuilder();
    sb.append(String.format(Locale.ROOT, "  Size %4d: ", size));

    // Validate FILTER phase
    Map<String, Pair<Long, Long>> baselineFilter = baseline.get("FILTER");
    Map<String, Pair<Long, Long>> testFilter = test.get("FILTER");

    if (!comparePhaseStats("FILTER", baselineFilter, testFilter, sb)) {
      passed = false;
    }

    // Validate PROJECT phase
    Map<String, Pair<Long, Long>> baselineProject = baseline.get("PROJECT");
    Map<String, Pair<Long, Long>> testProject = test.get("PROJECT");

    if (!comparePhaseStats("PROJECT", baselineProject, testProject, sb)) {
      passed = false;
    }

    if (passed) {
      sb.append("PASSED");
    } else {
      sb.append("FAILED");
    }

    System.out.println(sb.toString());
    return passed;
  }

  /**
   * Compares phase statistics between baseline and test.
   */
  private boolean comparePhaseStats(String phase,
      Map<String, Pair<Long, Long>> baseline,
      Map<String, Pair<Long, Long>> test,
      StringBuilder sb) {

    if (baseline == null && test == null) {
      return true;
    }
    if (baseline == null || test == null) {
      sb.append(phase).append("(null mismatch) ");
      return false;
    }

    boolean passed = true;
    for (String rule : baseline.keySet()) {
      Pair<Long, Long> baselineCount = baseline.get(rule);
      Pair<Long, Long> testCount = test.get(rule);

      if (testCount == null) {
        sb.append(phase).append("/").append(rule).append("(missing) ");
        passed = false;
        continue;
      }

      if (!baselineCount.left.equals(testCount.left)) {
        sb.append(
            String.format(Locale.ROOT, "%s/%s(%d vs %d) ",
                phase, rule, baselineCount.left, testCount.left));
        passed = false;
      }
    }

    return passed;
  }

  /**
   * Runs benchmark mode for performance testing.
   * Tests different union sizes based on the match order's scalability.
   */
  public void runBenchmark() {
    System.out.println("\n"
        + repeat("=", 80));
    System.out.println("BENCHMARK MODE: Performance testing");
    System.out.println(repeat("=", 80) + "\n");

    // Define size ranges for each match order
    // ARBITRARY: 1K, 3K, 10K, 30K, 100K, 300K (best scalability)
    // DEPTH_FIRST: 1K, 3K, 10K, 30K (good scalability)
    // BOTTOM_UP/TOP_DOWN: 1K, 3K (limited scalability)
    Map<String, int[]> orderSizes = new HashMap<>();
    orderSizes.put("ARBITRARY", new int[]{1000, 3000, 10000, 30000});
    orderSizes.put("DEPTH_FIRST", new int[]{1000, 3000, 10000, 30000});
    orderSizes.put("BOTTOM_UP", new int[]{1000, 3000});
    orderSizes.put("TOP_DOWN", new int[]{1000, 3000});

    List<String> results = new ArrayList<>();
    results.add(
        String.format(Locale.ROOT,
            "%-15s %-10s %-15s %-15s %-15s",
            "Match Order", "Union Num", "Node Count", "Rule Transforms", "Time (ms)"));
    results.add(repeat("-", 85));

    for (String order : ALL_MATCH_ORDERS) {
      System.out.println("Testing match order: " + order);
      int[] sizes = orderSizes.get(order);

      for (int size : sizes) {
        // Estimate node count: each branch has 3 nodes (Scan, Filter, Project)
        // Plus approximately 'size' Union nodes
        int nodeCount = size * 4;

        // Warmup
        testLargeUnionPlan(size, order, false);

        // Actual measurement with rule stats collection
        long startTime = System.currentTimeMillis();
        try {
          Map<String, Map<String, Pair<Long, Long>>> stats =
              testLargeUnionPlan(size, order, true);
          long endTime = System.currentTimeMillis();
          long elapsed = endTime - startTime;

          // Calculate total rule transformations
          long totalRuleTransforms = 0;
          for (Map<String, Pair<Long, Long>> phaseStats : stats.values()) {
            for (Pair<Long, Long> ruleStat : phaseStats.values()) {
              totalRuleTransforms += ruleStat.left;
            }
          }

          results.add(
              String.format(Locale.ROOT,
                  "%-15s %-10d %-15d %-15d %-15d",
                  order, size, nodeCount, totalRuleTransforms, elapsed));
          System.out.println("  Size " + size + ": " + elapsed + " ms, "
              + "nodes=" + nodeCount + ", ruleTransforms=" + totalRuleTransforms);
        } catch (Exception e) {
          results.add(
              String.format(Locale.ROOT,
                  "%-15s %-10d %-15s %-15s %-15s",
                  order, size, "N/A", "N/A", "N/A"));
          System.out.println("  Size " + size + ": FAILED - " + e.getMessage());
        }
      }
      System.out.println();
    }

    // Print summary report
    System.out.println("\n"
        + repeat("=", 85));
    System.out.println("BENCHMARK REPORT");
    System.out.println(repeat("=", 85));
    for (String line : results) {
      System.out.println(line);
    }
    System.out.println(repeat("=", 85) + "\n");
  }

  public static void main(String[] args) throws RunnerException {
    LargePlanBenchmark benchmark = new LargePlanBenchmark();
    benchmark.setup();
    benchmark.isLargePlanMode = true;
    benchmark.isEnableFiredRulesCache = true;

    // Check command line arguments for mode selection
    boolean runValidation = false;
    boolean runBenchmark = false;
    boolean runProfile = false;

    for (String arg : args) {
      if ("--validate".equals(arg) || "-v".equals(arg)) {
        runValidation = true;
      } else if ("--benchmark".equals(arg) || "-b".equals(arg)) {
        runBenchmark = true;
      } else if ("--both".equals(arg)) {
        runValidation = true;
        runBenchmark = true;
      } else if ("--profile".equals(arg) || "-p".equals(arg)) {
        runProfile = true;
      }
    }

    // Profile mode takes precedence (specialized mode)
    if (runProfile) {
      benchmark.runProfileMode();
      return;
    }

    // Default: run benchmark mode if no arguments specified
    if (!runValidation && !runBenchmark) {
      runBenchmark = true;
    }

    // Run validation first (if requested)
    if (runValidation) {
      benchmark.runValidation();
    }

    // Then run benchmark (if requested)
    if (runBenchmark) {
      benchmark.runBenchmark();
    }
  }

  /**
   * Runs profile mode for performance tuning with ARBITRARY order at 100K unions.
   * This mode is optimized for generating perf/flame graph records.
   */
  public void runProfileMode() {
    System.out.println("\n"
        + repeat("=", 80));
    System.out.println("PROFILE MODE: ARBITRARY order with 100000 unions");
    System.out.println("Optimized for perf/flame graph recording");
    System.out.println(repeat("=", 80) + "\n");

    String order = "ARBITRARY";
    int size = 100000;

    long startTime = System.currentTimeMillis();
    try {
      testLargeUnionPlan(size, order, false);
      long endTime = System.currentTimeMillis();
      long elapsed = endTime - startTime;

      System.out.println("\n"
          + repeat("=", 80));
      System.out.println("PROFILE RUN COMPLETED");
      System.out.println(repeat("=", 80));
      System.out.println("Match Order: " + order);
      System.out.println("Union Size:  " + size);
      System.out.println("Time:        " + elapsed + " ms (" + (elapsed / 1000.0) + " s)");
      System.out.println(repeat("=", 80) + "\n");
    } catch (Exception e) {
      System.err.println("Profile run failed: " + e.getMessage());
      e.printStackTrace();
    }
  }
}
