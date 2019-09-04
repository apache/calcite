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
package org.apache.calcite.test.fuzzer;

import org.apache.calcite.plan.Strong;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.test.RexProgramBuilderBase;
import org.apache.calcite.util.ImmutableBitSet;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import javax.annotation.Nonnull;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Validates that {@link org.apache.calcite.rex.RexSimplify} is able to deal with
 * randomized {@link RexNode}.
 * Note: the default fuzzing time is 5 seconds to keep overall test duration reasonable.
 * The test starts from a random point every time, so the longer it runs the more errors it detects.
 *
 * <p>Note: The test is not included to {@link org.apache.calcite.test.CalciteSuite} since it would
 * fail every build (there are lots of issues with {@link org.apache.calcite.rex.RexSimplify})
 */
public class RexProgramFuzzyTest extends RexProgramBuilderBase {
  protected static final Logger LOGGER =
      LoggerFactory.getLogger(RexProgramFuzzyTest.class);

  private static final Duration TEST_DURATION =
      Duration.of(Integer.getInteger("rex.fuzzing.duration", 5), ChronoUnit.SECONDS);
  private static final long TEST_ITERATIONS = Long.getLong("rex.fuzzing.iterations", 2000);
  // Stop fuzzing after detecting MAX_FAILURES errors
  private static final int MAX_FAILURES =
      Integer.getInteger("rex.fuzzing.max.failures", 1);
  // Number of slowest to simplify expressions to show
  private static final int TOPN_SLOWEST =
      Integer.getInteger("rex.fuzzing.max.slowest", 0);
  // 0 means use random seed
  // 42 is used to make sure tests pass in CI
  private static final long SEED =
      Long.getLong("rex.fuzzing.seed", 44);

  private static final long DEFAULT_FUZZ_TEST_SEED =
      Long.getLong("rex.fuzzing.default.seed", 0);
  private static final Duration DEFAULT_FUZZ_TEST_DURATION =
      Duration.of(Integer.getInteger("rex.fuzzing.default.duration", 5), ChronoUnit.SECONDS);
  private static final long DEFAULT_FUZZ_TEST_ITERATIONS =
      Long.getLong("rex.fuzzing.default.iterations", 0);
  private static final boolean DEFAULT_FUZZ_TEST_FAIL =
      Boolean.getBoolean("rex.fuzzing.default.fail");

  private PriorityQueue<SimplifyTask> slowestTasks;

  private long currentSeed = 0;

  private static final Strong STRONG = Strong.of(ImmutableBitSet.of());

  /**
   * A bounded variation of {@link PriorityQueue}
   *
   * @param <E> the type of elements held in this collection
   */
  private static class TopN<E extends Comparable<E>> extends PriorityQueue<E> {
    private final int n;

    private TopN(int n) {
      this.n = n;
    }

    @Override public boolean offer(E o) {
      if (size() == n) {
        E peek = peek();
        if (peek != null && peek.compareTo(o) > 0) {
          // If the smallest element in the queue exceeds the added one
          // then just ignore the offer
          return false;
        }
        // otherwise extract the smallest element, and offer a new one
        poll();
      }
      return super.offer(o);
    }

    @Override public Iterator<E> iterator() {
      throw new UnsupportedOperationException("Order of elements is not defined, please use .peek");
    }
  }

  @Before public void setUp() {
    super.setUp();
  }

  /**
   * Verifies {@code IS TRUE(IS NULL(null))} kind of expressions up to 4 level deep.
   */
  @Test public void testNestedCalls() {
    nestedCalls(trueLiteral);
    nestedCalls(falseLiteral);
    nestedCalls(nullBool);
    nestedCalls(vBool());
    nestedCalls(vBoolNotNull());
  }

  private void nestedCalls(RexNode arg) {
    SqlOperator[] operators = {
        SqlStdOperatorTable.NOT,
        SqlStdOperatorTable.IS_FALSE,
        SqlStdOperatorTable.IS_NOT_FALSE,
        SqlStdOperatorTable.IS_TRUE,
        SqlStdOperatorTable.IS_NOT_TRUE,
        SqlStdOperatorTable.IS_NULL,
        SqlStdOperatorTable.IS_NOT_NULL,
        SqlStdOperatorTable.IS_UNKNOWN,
        SqlStdOperatorTable.IS_NOT_UNKNOWN
    };
    for (SqlOperator op1 : operators) {
      RexNode n1 = rexBuilder.makeCall(op1, arg);
      checkUnknownAs(n1);
      for (SqlOperator op2 : operators) {
        RexNode n2 = rexBuilder.makeCall(op2, n1);
        checkUnknownAs(n2);
        for (SqlOperator op3 : operators) {
          RexNode n3 = rexBuilder.makeCall(op3, n2);
          checkUnknownAs(n3);
          for (SqlOperator op4 : operators) {
            RexNode n4 = rexBuilder.makeCall(op4, n3);
            checkUnknownAs(n4);
          }
        }
      }
    }
  }

  private void checkUnknownAs(RexNode node) {
    checkUnknownAs(node, RexUnknownAs.FALSE);
    checkUnknownAs(node, RexUnknownAs.UNKNOWN);
    checkUnknownAs(node, RexUnknownAs.TRUE);
  }

  private void checkUnknownAs(RexNode node, RexUnknownAs unknownAs) {
    RexNode opt;
    final String uaf = unknownAsString(unknownAs);
    try {
      long start = System.nanoTime();
      opt = simplify.simplifyUnknownAs(node, unknownAs);
      long end = System.nanoTime();
      if (end - start > 1000 && slowestTasks != null) {
        slowestTasks.add(new SimplifyTask(node, currentSeed, opt, end - start));
      }
    } catch (AssertionError a) {
      String message = a.getMessage();
      if (message != null && message.startsWith("result mismatch")) {
        throw a;
      }
      throw new IllegalStateException("Unable to simplify " + uaf + nodeToString(node), a);
    } catch (Throwable t) {
      throw new IllegalStateException("Unable to simplify " + uaf + nodeToString(node), t);
    }
    if (trueLiteral.equals(opt) && node.isAlwaysFalse()) {
      String msg = nodeToString(node);
      fail(msg + " optimizes to TRUE, isAlwaysFalse MUST not be true " + uaf);
//      This is a missing optimization, not a bug
//      assertFalse(msg + " optimizes to TRUE, isAlwaysTrue MUST be true",
//          !node.isAlwaysTrue());
    }
    if (falseLiteral.equals(opt) && node.isAlwaysTrue()) {
      String msg = nodeToString(node);
      fail(msg + " optimizes to FALSE, isAlwaysTrue MUST not be true " + uaf);
//      This is a missing optimization, not a bug
//      assertFalse(msg + " optimizes to FALSE, isAlwaysFalse MUST be true",
//          !node.isAlwaysFalse());
    }
    if (STRONG.isNull(opt)) {
      if (node.isAlwaysTrue()) {
        fail(nodeToString(node) + " optimizes to NULL: " + nodeToString(opt)
            + ", isAlwaysTrue MUST be FALSE " + uaf);
      }
      if (node.isAlwaysFalse()) {
        fail(nodeToString(node) + " optimizes to NULL: " + nodeToString(opt)
            + ", isAlwaysFalse MUST be FALSE " + uaf);
      }
    }
    if (node.isAlwaysTrue()) {
      if (!trueLiteral.equals(opt)) {
        assertEquals(nodeToString(node) + " isAlwaysTrue, so it should simplify to TRUE "
                + uaf,
            trueLiteral, opt);
      }
    }
    if (node.isAlwaysFalse()) {
      if (!falseLiteral.equals(opt)) {
        assertEquals(nodeToString(node) + " isAlwaysFalse, so it should simplify to FALSE "
                + uaf,
            falseLiteral, opt);
      }
    }
    if (STRONG.isNull(node)) {
      switch (unknownAs) {
      case FALSE:
        if (node.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
          if (!falseLiteral.equals(opt)) {
            assertEquals(nodeToString(node)
                    + " is always null boolean, so it should simplify to FALSE " + uaf,
                falseLiteral, opt);
          }
        } else {
          if (!RexLiteral.isNullLiteral(opt)) {
            assertEquals(nodeToString(node)
                    + " is always null (non boolean), so it should simplify to NULL " + uaf,
                rexBuilder.makeNullLiteral(node.getType()), opt);
          }
        }
        break;
      case TRUE:
        if (node.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
          if (!trueLiteral.equals(opt)) {
            assertEquals(nodeToString(node)
                    + " is always null boolean, so it should simplify to TRUE " + uaf,
                trueLiteral, opt);
          }
        } else {
          if (!RexLiteral.isNullLiteral(opt)) {
            assertEquals(nodeToString(node)
                    + " is always null (non boolean), so it should simplify to NULL " + uaf,
                rexBuilder.makeNullLiteral(node.getType()), opt);
          }
        }
        break;
      case UNKNOWN:
        if (!RexUtil.isNull(opt)) {
          assertEquals(nodeToString(node)
                  + " is always null, so it should simplify to NULL " + uaf,
              nullBool, opt);
        }
      }
    }
    if (unknownAs == RexUnknownAs.UNKNOWN
        && opt.getType().isNullable()
        && !node.getType().isNullable()) {
      fail(nodeToString(node) + " had non-nullable type " + opt.getType()
          + ", and it was optimized to " + nodeToString(opt)
          + " that has nullable type " + opt.getType());
    }
    if (!SqlTypeUtil.equalSansNullability(typeFactory, node.getType(), opt.getType())) {
      assertEquals(nodeToString(node) + " has different type after simplification to "
          + nodeToString(opt), node.getType(), opt.getType());
    }
  }

  @Nonnull private String unknownAsString(RexUnknownAs unknownAs) {
    switch (unknownAs) {
    case UNKNOWN:
    default:
      return "";
    case FALSE:
      return "unknownAsFalse";
    case TRUE:
      return "unknownAsTrue";
    }
  }

  private static String nodeToString(RexNode node) {
    return node + "\n"
        + node.accept(new RexToTestCodeShuttle());
  }

  private static void trimStackTrace(Throwable t, int maxStackLines) {
    StackTraceElement[] stackTrace = t.getStackTrace();
    if (stackTrace == null || stackTrace.length <= maxStackLines) {
      return;
    }
    stackTrace = Arrays.copyOf(stackTrace, maxStackLines);
    t.setStackTrace(stackTrace);
  }

  @Test public void defaultFuzzTest() {
    try {
      runRexFuzzer(DEFAULT_FUZZ_TEST_SEED, DEFAULT_FUZZ_TEST_DURATION, 1,
          DEFAULT_FUZZ_TEST_ITERATIONS, 0);
    } catch (Throwable e) {
      for (Throwable t = e; t != null; t = t.getCause()) {
        trimStackTrace(t, DEFAULT_FUZZ_TEST_FAIL ? 8 : 4);
      }
      if (DEFAULT_FUZZ_TEST_FAIL) {
        throw e;
      }
      LOGGER.info("Randomized test identified a potential defect. Feel free to fix that issue", e);
    }
  }

  @Test public void testFuzzy() {
    runRexFuzzer(SEED, TEST_DURATION, MAX_FAILURES, TEST_ITERATIONS, TOPN_SLOWEST);
  }

  private void runRexFuzzer(long startSeed, Duration testDuration, int maxFailures,
      long testIterations, int topnSlowest) {
    if (testDuration.toMillis() == 0) {
      return;
    }
    slowestTasks = new TopN<>(topnSlowest > 0 ? topnSlowest : 1);
    Random r = new Random();
    if (startSeed != 0) {
      LOGGER.info("Using seed {} for rex fuzzing", startSeed);
      r.setSeed(startSeed);
    }
    long start = System.currentTimeMillis();
    long deadline = start + testDuration.toMillis();
    List<Throwable> exceptions = new ArrayList<>();
    Set<String> duplicates = new HashSet<>();
    long total = 0;
    int dup = 0;
    int fail = 0;
    RexFuzzer fuzzer = new RexFuzzer(rexBuilder, typeFactory);
    while (System.currentTimeMillis() < deadline && exceptions.size() < maxFailures
        && (testIterations == 0 || total < testIterations)) {
      long seed = r.nextLong();
      this.currentSeed = seed;
      r.setSeed(seed);
      try {
        total++;
        generateRexAndCheckTrueFalse(fuzzer, r);
      } catch (Throwable e) {
        if (!duplicates.add(e.getMessage())) {
          dup++;
          // known exception, nothing to see here
          continue;
        }
        fail++;
        StackTraceElement[] stackTrace = e.getStackTrace();
        for (int j = 0; j < stackTrace.length; j++) {
          if (stackTrace[j].getClassName().endsWith("RexProgramTest")) {
            e.setStackTrace(Arrays.copyOf(stackTrace, j + 1));
            break;
          }
        }
        e.addSuppressed(new Throwable("seed " + seed) {
          @Override public synchronized Throwable fillInStackTrace() {
            return this;
          }
        });
        exceptions.add(e);
      }
    }
    long rate = total * 1000 / (System.currentTimeMillis() - start);
    LOGGER.info(
        "Rex fuzzing results: number of cases tested={}, failed cases={}, duplicate failures={}, fuzz rate={} per second",
        total, fail, dup, rate);

    if (topnSlowest > 0) {
      LOGGER.info("The 5 slowest to simplify nodes were");
      SimplifyTask task;
      RexToTestCodeShuttle v = new RexToTestCodeShuttle();
      while ((task = slowestTasks.poll()) != null) {
        LOGGER.info(task.duration / 1000 + " us (" + task.seed + ")");
        LOGGER.info("      " + task.node.toString());
        LOGGER.info("      " + task.node.accept(v));
        LOGGER.info("    =>" + task.result.toString());
      }
    }

    if (exceptions.isEmpty()) {
      return;
    }

    // Print the shortest fails first
    exceptions.sort(
        Comparator.
            <Throwable>comparingInt(t -> t.getMessage() == null ? -1 : t.getMessage().length())
            .thenComparing(Throwable::getMessage));

    // The first exception will be thrown, so the others go to printStackTrace
    for (int i = 1; i < exceptions.size() && i < 100; i++) {
      Throwable exception = exceptions.get(i);
      exception.printStackTrace();
    }

    Throwable ex = exceptions.get(0);
    if (ex instanceof Error) {
      throw (Error) ex;
    }
    if (ex instanceof RuntimeException) {
      throw (RuntimeException) ex;
    }
    throw new RuntimeException("Exception in runRexFuzzer", ex);
  }

  private void generateRexAndCheckTrueFalse(RexFuzzer fuzzer, Random r) {
    RexNode expression = fuzzer.getExpression(r, r.nextInt(10));
    checkUnknownAs(expression);
  }

  @Ignore("This is just a scaffold for quick investigation of a single fuzz test")
  @Test public void singleFuzzyTest() {
    Random r = new Random();
    r.setSeed(-179916965778405462L);
    RexFuzzer fuzzer = new RexFuzzer(rexBuilder, typeFactory);
    generateRexAndCheckTrueFalse(fuzzer, r);
  }
}

// End RexProgramFuzzyTest.java
