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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.RexProgramBuilderBase;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Validates that {@link org.apache.calcite.rex.RexSimplify} is able to deal with
 * randomized {@link RexNode}.
 * Note: the default fuzzing time is 5 seconds to keep overall test duration reasonable.
 * You might increase
 */
public class RexProgrammFuzzyTest extends RexProgramBuilderBase {
  private static final int TEST_DURATION = Integer.getInteger("rex.fuzzing.duration", 5);
  private static final int MAX_FAILURES =
      Integer.getInteger("rex.fuzzing.max.failures", 5000);
  private static final int TOPN_SLOWEST =
      Integer.getInteger("rex.fuzzing.max.slowest", 5);
  private static final long SEED =
      Long.getLong("rex.fuzzing.seed", 0);

  private PriorityQueue<SimplifyTask> slowestTasks;

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
          return false;
        }
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

  @Test public void testAlwaysTrueFalse() {
    RelDataType in = typeFactory.createSqlType(SqlTypeName.BOOLEAN);

    SqlOperator[] operators = new SqlOperator[]{
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
    for (boolean argNullability : new boolean[]{true, false}) {
      RexInputRef arg = rexBuilder.makeInputRef(
          typeFactory.createTypeWithNullability(
              in, argNullability),
          argNullability ? 10 : 0);
      for (SqlOperator op1 : operators) {
        RexNode n1 = rexBuilder.makeCall(op1, arg);
        checkTrueFalse(n1);
        for (SqlOperator op2 : operators) {
          RexNode n2 = rexBuilder.makeCall(op2, n1);
          checkTrueFalse(n2);
          for (SqlOperator op3 : operators) {
            RexNode n3 = rexBuilder.makeCall(op3, n2);
            checkTrueFalse(n3);
            for (SqlOperator op4 : operators) {
              RexNode n4 = rexBuilder.makeCall(op4, n3);
              checkTrueFalse(n4);
            }
          }
        }
      }
    }
  }

  private void checkTrueFalse(RexNode node) {
    RexNode opt;
    try {
      long start = System.nanoTime();
      opt = this.simplify.simplify(node);
      long end = System.nanoTime();
      if (end - start > 1000 && slowestTasks != null) {
        slowestTasks.add(new SimplifyTask(node, opt, end - start));
      }
    } catch (AssertionError a) {
      String message = a.getMessage();
      if (message != null && message.startsWith("result mismatch")) {
        throw a;
      }
      throw new IllegalStateException("Unable to simplify " + node, a);
    } catch (Throwable t) {
      throw new IllegalStateException("Unable to simplify " + node, t);
    }
    if (trueLiteral.equals(opt)) {
      String msg = node.toString();
      assertFalse(msg + " optimizes to TRUE, isAlwaysFalse MUST not be true",
          node.isAlwaysFalse());
//      This is a missing optimization, not a bug
//      assertFalse(msg + " optimizes to TRUE, isAlwaysTrue MUST be true",
//          !node.isAlwaysTrue());
    }
    if (falseLiteral.equals(opt)) {
      String msg = node.toString();
      assertFalse(msg + " optimizes to FALSE, isAlwaysTrue MUST not be true",
          node.isAlwaysTrue());
//      This is a missing optimization, not a bug
//      assertFalse(msg + " optimizes to FALSE, isAlwaysFalse MUST be true",
//          !node.isAlwaysFalse());
    }
    if (nullLiteral.equals(opt)) {
      String msg = node.toString();
      assertFalse(msg + " optimizes to NULL, isAlwaysTrue MUST be FALSE",
          node.isAlwaysTrue());
      assertFalse(msg + " optimizes to NULL, isAlwaysFalse MUST be FALSE",
          node.isAlwaysFalse());
    }
    if (node.isAlwaysTrue()) {
      String msg = node.toString();
      assertEquals(msg + " isAlwaysTrue, so it should simplify to TRUE",
          trueLiteral, opt);
    }
    if (node.isAlwaysFalse()) {
      String msg = node.toString();
      assertEquals(msg + " isAlwaysFalse, so it should simplify to FALSE",
          falseLiteral, opt);
    }
  }

  @Test public void testFuzzy() {
    slowestTasks = new TopN<>(TOPN_SLOWEST);
    Random r = new Random();
    if (SEED != 0) {
      r.setSeed(SEED);
    }
    long start = System.currentTimeMillis();
    long deadline = start + TimeUnit.SECONDS.toMillis(TEST_DURATION);
    List<Throwable> exceptions = new ArrayList<>();
    Set<String> duplicates = new HashSet<>();
    int total = 0;
    int dup = 0;
    int fail = 0;
    RexFuzzer fuzzer = new RexFuzzer(rexBuilder, typeFactory);
    while (System.currentTimeMillis() < deadline && exceptions.size() < MAX_FAILURES) {
      long seed = r.nextLong();
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
    System.out.println("total           = " + total);
    System.out.println("failed          = " + fail);
    System.out.println("duplicate fails = " + dup);
    long rate = total * 1000 / (System.currentTimeMillis() - start);
    System.out.println("fuzz rate       = " + rate + " per second");

    System.out.println("The 5 slowest to simplify nodes were");
    SimplifyTask task;
    while ((task = slowestTasks.poll()) != null) {
      System.out.println();
      System.out.println(task.duration / 1000 + " us");
      System.out.println("      " + task.node.toString());
      System.out.println("    =>" + task.result.toString());
    }

    if (exceptions.isEmpty()) {
      return;
    }

    // Print the shortest fails first
    exceptions.sort(Comparator.<Throwable>comparingInt(t -> t.getMessage().length())
        .thenComparing(Throwable::getMessage));

    for (Throwable exception : exceptions) {
      exception.printStackTrace();
    }

    Throwable ex = exceptions.get(0);
    if (ex instanceof Error) {
      throw (Error) ex;
    }
    throw new RuntimeException("Exception in testFuzzy", ex);
  }

  private void generateRexAndCheckTrueFalse(RexFuzzer fuzzer, Random r) {
    RexNode expression = fuzzer.getExpression(r, r.nextInt(10));
    checkTrueFalse(expression);
  }

  @Test public void singleFuzzyTest() {
    Random r = new Random();
    r.setSeed(-6539797260474794707L);
//    r.setSeed(-7306345575755792631L);
    RexFuzzer fuzzer = new RexFuzzer(rexBuilder, typeFactory);
    generateRexAndCheckTrueFalse(fuzzer, r);
  }


  private void assertSimplifies(String expected, RexNode node) {
    RexNode opt = this.simplify.simplify(node);
    assertEquals(node.toString(), expected, opt.toString());
  }

  /**
   * Manually coded tests go here. It ensures the cases identified by fuzzer are kept
   * even in case fuzzer implementation changes over time.
   */
  @Test public void testSimplifies() {
    assertSimplifies("true", isNull(nullBool));
    assertSimplifies("null", not(nullBool));
    assertSimplifies("false", not(trueLiteral));
    assertSimplifies("true", not(falseLiteral));
    assertSimplifies("false", isFalse(nullLiteral));
    assertSimplifies("null", rexBuilder.makeCall(SqlStdOperatorTable.UNARY_MINUS, nullLiteral));
    assertSimplifies("IS NULL($100)", isNull(input(nullableInt, 100)));
    assertSimplifies("true", isNotNull(input(nonNullableInt, 0)));
    assertSimplifies("false", isFalse(input(nullableInt, 100)));
    assertSimplifies("false", isFalse(input(nonNullableInt, 0)));
    assertSimplifies("IS FALSE($100)", isFalse(input(nullableBool, 100)));
    assertSimplifies("NOT($0)", isFalse(input(nonNullableBool, 0)));
    // {$34=-1, $31=-1, $13=true}, COALESCE(=($34, null), IS NOT FALSE($31), $13) yielded true,
    // and $31 yielded -1
    assertSimplifies("true",
        coalesce(
            eq(input(nullableInt, 34), nullLiteral),
            isNotFalse(input(nonNullableInt, 31)), input(nullableBool, 13)));
    assertSimplifies("true",
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_TRUE, nullLiteral));
    assertSimplifies("true",
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_FALSE, nullLiteral));
    assertSimplifies("false", and(falseLiteral, falseLiteral));
    assertSimplifies("false", and(falseLiteral, trueLiteral));
    assertSimplifies("false", and(falseLiteral, nullLiteral));
    assertSimplifies("true", and(trueLiteral, trueLiteral));
    assertSimplifies("null", and(trueLiteral, nullLiteral));
    assertSimplifies("null", and(nullLiteral, nullLiteral));
    assertSimplifies("true",
        or(
            ne(nullBool, input(nonNullableBool, 2)), // => NULL
            eq(isNotNull(nullBool), falseLiteral), // => TRUE
            falseLiteral));
    assertSimplifies("true",
        le(coalesce(falseLiteral, nullBool),
            or(
                ne(nullBool, input(nonNullableBool, 2)),
                le(nullBool, nullBool),
                eq(isNotNull(nullBool), isFalse(nullBool)),
                input(nullableBool, 12))));
    assertSimplifies("OR(null, $11, AND(null, $14))",
        or(nullLiteral,
            or(
                input(nullableBool, 11),
                and(nullLiteral,
                    input(nullableBool, 14)), nullLiteral)));
    assertSimplifies("true", gt(trueLiteral, falseLiteral));
    assertSimplifies("false",
        and(
            input(nullableBool, 14),
            isNotNull(and(trueLiteral, trueLiteral, nullLiteral)),
            isNotNull(coalesce(nullLiteral)),
            input(nonNullableBool, 0)));
    assertSimplifies("null", ge(nullLiteral, falseLiteral));
    assertSimplifies("false",
        isNotNull(
            ge(
                coalesce(nullLiteral),
                falseLiteral)));
    assertSimplifies("true",
        isNull(
            ge(
                coalesce(nullLiteral),
                falseLiteral)));
  }
}

// End RexProgrammFuzzyTest.java
