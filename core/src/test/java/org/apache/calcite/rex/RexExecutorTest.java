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
package org.apache.calcite.rex;
import org.apache.calcite.DataContext;
import org.apache.calcite.DataContexts;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlMonotonicBinaryOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.Matchers;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.TimeZone;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.fail;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * Unit test for {@link org.apache.calcite.rex.RexExecutorImpl}.
 */
class RexExecutorTest {
  protected void check(final Action action) {
    Frameworks.withPrepare((cluster, relOptSchema, rootSchema, statement) -> {
      final RexBuilder rexBuilder = cluster.getRexBuilder();
      final ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
      builder.put(DataContext.Variable.TIME_ZONE.camelName, TimeZone.getTimeZone("GMT"));
      builder.put(DataContext.Variable.LOCALE.camelName, Locale.US);
      final DataContext dataContext = DataContexts.of(builder.build());
      final RexExecutorImpl executor = new RexExecutorImpl(dataContext);
      action.check(rexBuilder, executor);
      return null;
    });
  }

  /** Tests an executor that uses variables stored in a {@link DataContext}.
   * Can change the value of the variable and execute again. */
  @Test void testVariableExecution() {
    check((rexBuilder, executor) -> {
      Object[] values = new Object[1];
      final DataContext testContext =
          DataContexts.of(name ->
              name.equals("inputRecord") ? values : fail("unknown: " + name));
      final RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
      final RelDataType varchar =
          typeFactory.createSqlType(SqlTypeName.VARCHAR);
      final RelDataType integer =
          typeFactory.createSqlType(SqlTypeName.INTEGER);
      // Calcite is internally creating the input ref via a RexRangeRef
      // which eventually leads to a RexInputRef. So we are good.
      final RexInputRef input = rexBuilder.makeInputRef(varchar, 0);
      final RexNode lengthArg = rexBuilder.makeLiteral(3, integer, true);
      final RexNode substr =
          rexBuilder.makeCall(SqlStdOperatorTable.SUBSTRING, input,
              lengthArg);
      ImmutableList<RexNode> constExps = ImmutableList.of(substr);

      final RelDataType rowType = typeFactory.builder()
          .add("someStr", varchar)
          .build();

      final RexExecutable exec =
          RexExecutorImpl.getExecutable(rexBuilder, constExps, rowType);
      exec.setDataContext(testContext);
      values[0] = "Hello World";
      Object[] result = exec.execute();
      assertThat(result[0], instanceOf(String.class));
      assertThat((String) result[0], equalTo("llo World"));
      values[0] = "Calcite";
      result = exec.execute();
      assertThat(result[0], instanceOf(String.class));
      assertThat((String) result[0], equalTo("lcite"));
    });
  }

  @Test void testConstant() {
    check((rexBuilder, executor) -> {
      final List<RexNode> reducedValues = new ArrayList<>();
      final RexLiteral ten = rexBuilder.makeExactLiteral(BigDecimal.TEN);
      executor.reduce(rexBuilder, ImmutableList.of(ten),
          reducedValues);
      assertThat(reducedValues, hasSize(1));
      assertThat(reducedValues.get(0), instanceOf(RexLiteral.class));
      assertThat(((RexLiteral) reducedValues.get(0)).getValue2(), equalTo(10L));
    });
  }

  /** Reduces several expressions to constants. */
  @Test void testConstant2() {
    // Same as testConstant; 10 -> 10
    checkConstant(10L,
        rexBuilder -> rexBuilder.makeExactLiteral(BigDecimal.TEN));
    // 10 + 1 -> 11
    checkConstant(11L,
        rexBuilder -> rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
            rexBuilder.makeExactLiteral(BigDecimal.TEN),
            rexBuilder.makeExactLiteral(BigDecimal.ONE)));
    // date 'today' <= date 'today' -> true
    checkConstant(true, rexBuilder -> {
      final DateString d =
          DateString.fromCalendarFields(Util.calendar());
      return rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
          rexBuilder.makeDateLiteral(d),
          rexBuilder.makeDateLiteral(d));
    });
    // date 'today' < date 'today' -> false
    checkConstant(false, rexBuilder -> {
      final DateString d =
          DateString.fromCalendarFields(Util.calendar());
      return rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN,
          rexBuilder.makeDateLiteral(d),
          rexBuilder.makeDateLiteral(d));
    });
  }

  private void checkConstant(final Object operand,
      final Function<RexBuilder, RexNode> function) {
    check((rexBuilder, executor) -> {
      final List<RexNode> reducedValues = new ArrayList<>();
      final RexNode expression = requireNonNull(function.apply(rexBuilder));
      executor.reduce(rexBuilder, ImmutableList.of(expression),
          reducedValues);
      assertThat(reducedValues, hasSize(1));
      final RexNode reducedValue = reducedValues.get(0);
      assertThat(reducedValue, instanceOf(RexLiteral.class));
      final Matcher<Object> matcher;
      if (((RexLiteral) reducedValue).getTypeName() == SqlTypeName.TIMESTAMP) {
        final long current = System.currentTimeMillis();
        //noinspection unchecked
        matcher = (Matcher) Matchers.between((long) operand, current);
      } else {
        matcher = equalTo(operand);
      }
      assertThat(((RexLiteral) reducedValue).getValue2(), matcher);
    });
  }

  @Test void testUserFromContext() {
    testContextLiteral(SqlStdOperatorTable.USER,
        DataContext.Variable.USER, "happyCalciteUser");
  }

  @Test void testSystemUserFromContext() {
    testContextLiteral(SqlStdOperatorTable.SYSTEM_USER,
        DataContext.Variable.SYSTEM_USER, "");
  }

  @Test void testTimestampFromContext() {
    // CURRENT_TIMESTAMP actually rounds the value to nearest second
    // and that's why we do currentTimeInMillis / 1000 * 1000
    long val = System.currentTimeMillis() / 1000 * 1000;
    testContextLiteral(SqlStdOperatorTable.CURRENT_TIMESTAMP,
        DataContext.Variable.CURRENT_TIMESTAMP, val);
  }

  /**
   * Ensures that for a given context operator,
   * the correct value is retrieved from the {@link DataContext}.
   *
   * @param operator The Operator to check
   * @param variable The DataContext variable this operator should be bound to
   * @param value The expected value to retrieve.
   */
  private void testContextLiteral(
      final SqlOperator operator,
      final DataContext.Variable variable,
      final Object value) {
    Frameworks.withPrepare((cluster, relOptSchema, rootSchema, statement) -> {
      final RexBuilder rexBuilder = cluster.getRexBuilder();
      final RexExecutorImpl executor =
          new RexExecutorImpl(
              DataContexts.of(name ->
                  name.equals(variable.camelName) ? value
                      : fail("unknown: " + name)));
      try {
        checkConstant(value, builder -> {
          final List<RexNode> output = new ArrayList<>();
          executor.reduce(rexBuilder,
              ImmutableList.of(rexBuilder.makeCall(operator)), output);
          return output.get(0);
        });
      } catch (Exception e) {
        throw TestUtil.rethrow(e);
      }
      return null;
    });
  }

  @Test void testSubstring() {
    check((rexBuilder, executor) -> {
      final List<RexNode> reducedValues = new ArrayList<>();
      final RexLiteral hello =
          rexBuilder.makeCharLiteral(
              new NlsString("Hello world!", null, null));
      final RexNode plus =
          rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
              rexBuilder.makeExactLiteral(BigDecimal.ONE),
              rexBuilder.makeExactLiteral(BigDecimal.ONE));
      RexLiteral four = rexBuilder.makeExactLiteral(BigDecimal.valueOf(4));
      final RexNode substring =
          rexBuilder.makeCall(SqlStdOperatorTable.SUBSTRING,
              hello, plus, four);
      executor.reduce(rexBuilder, ImmutableList.of(substring, plus),
          reducedValues);
      assertThat(reducedValues, hasSize(2));
      assertThat(reducedValues.get(0), instanceOf(RexLiteral.class));
      assertThat(((RexLiteral) reducedValues.get(0)).getValue2(),
          equalTo("ello")); // substring('Hello world!, 2, 4)
      assertThat(reducedValues.get(1), instanceOf(RexLiteral.class));
      assertThat(((RexLiteral) reducedValues.get(1)).getValue2(),
          equalTo(2L));
    });
  }

  @Test void testBinarySubstring() {
    check((rexBuilder, executor) -> {
      final List<RexNode> reducedValues = new ArrayList<>();
      // hello world! -> 48656c6c6f20776f726c6421
      final RexLiteral binaryHello =
          rexBuilder.makeBinaryLiteral(
              new ByteString("Hello world!".getBytes(UTF_8)));
      final RexNode plus =
          rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
              rexBuilder.makeExactLiteral(BigDecimal.ONE),
              rexBuilder.makeExactLiteral(BigDecimal.ONE));
      RexLiteral four = rexBuilder.makeExactLiteral(BigDecimal.valueOf(4));
      final RexNode substring =
          rexBuilder.makeCall(SqlStdOperatorTable.SUBSTRING,
              binaryHello, plus, four);
      executor.reduce(rexBuilder, ImmutableList.of(substring, plus),
          reducedValues);
      assertThat(reducedValues, hasSize(2));
      assertThat(reducedValues.get(0), instanceOf(RexLiteral.class));
      assertThat(((RexLiteral) reducedValues.get(0)).getValue2(),
          hasToString("656c6c6f")); // substring('Hello world!, 2, 4)
      assertThat(reducedValues.get(1), instanceOf(RexLiteral.class));
      assertThat(((RexLiteral) reducedValues.get(1)).getValue2(),
          equalTo(2L));
    });
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6775">[CALCITE-6775]
   * ToChar and ToTimestamp PG implementors should use translator's root instead of
   * creating a new root expression</a>. */
  @Test void testToCharPg() {
    check((rexBuilder, executor) -> {
      final List<RexNode> reducedValues = new ArrayList<>();
      // GMT: Wednesday, November 12, 1975 11:00:00 AM
      final TimestampString timestamp = TimestampString.fromMillisSinceEpoch(185022000000L);
      final RexNode toChar =
          rexBuilder.makeCall(SqlLibraryOperators.TO_CHAR_PG,
              rexBuilder.makeTimestampLiteral(timestamp, 0),
              rexBuilder.makeLiteral("ID")); // ISO 8601 day of the week (Wednesday = 3)
      executor.reduce(rexBuilder, ImmutableList.of(toChar), reducedValues);
      assertThat(reducedValues, hasSize(1));
      assertThat(reducedValues.get(0), instanceOf(RexLiteral.class));
      assertThat(((RexLiteral) reducedValues.get(0)).getValueAs(String.class), equalTo("3"));
    });
  }

  @Test void testDeterministic1() {
    check((rexBuilder, executor) -> {
      final RexNode plus =
          rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
              rexBuilder.makeExactLiteral(BigDecimal.ONE),
              rexBuilder.makeExactLiteral(BigDecimal.ONE));
      assertThat(RexUtil.isDeterministic(plus), equalTo(true));
    });
  }

  @Test void testDeterministic2() {
    check((rexBuilder, executor) -> {
      final RexNode plus =
          rexBuilder.makeCall(PLUS_RANDOM,
              rexBuilder.makeExactLiteral(BigDecimal.ONE),
              rexBuilder.makeExactLiteral(BigDecimal.ONE));
      assertThat(RexUtil.isDeterministic(plus), equalTo(false));
    });
  }

  @Test void testDeterministic3() {
    check((rexBuilder, executor) -> {
      final RexNode plus =
          rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
              rexBuilder.makeCall(PLUS_RANDOM,
                  rexBuilder.makeExactLiteral(BigDecimal.ONE),
                  rexBuilder.makeExactLiteral(BigDecimal.ONE)),
              rexBuilder.makeExactLiteral(BigDecimal.ONE));
      assertThat(RexUtil.isDeterministic(plus), equalTo(false));
    });
  }

  private static final SqlBinaryOperator PLUS_RANDOM =
      new SqlMonotonicBinaryOperator(
          "+",
          SqlKind.PLUS,
          40,
          true,
          ReturnTypes.NULLABLE_SUM,
          InferTypes.FIRST_KNOWN,
          OperandTypes.PLUS_OPERATOR) {
        @Override public boolean isDeterministic() {
          return false;
        }
      };

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1009">[CALCITE-1009]
   * SelfPopulatingList is not thread-safe</a>. */
  @Test void testSelfPopulatingList() {
    final List<Thread> threads = new ArrayList<>();
    //noinspection MismatchedQueryAndUpdateOfCollection
    final List<String> list = new RexSlot.SelfPopulatingList("$", 1);
    final Random random = new Random();
    for (int i = 0; i < 10; i++) {
      threads.add(
          new Thread(() -> {
            for (int j = 0; j < 1000; j++) {
              // Random numbers between 0 and ~1m, smaller values more common
              final int index = random.nextInt(1234567)
                  >> random.nextInt(16) >> random.nextInt(16);
              list.get(index);
            }
          }));
    }
    for (Thread runnable : threads) {
      runnable.start();
    }
    for (Thread runnable : threads) {
      try {
        runnable.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    final int size = list.size();
    for (int i = 0; i < size; i++) {
      assertThat(list.get(i), is("$" + i));
    }
  }

  @Test void testSelfPopulatingList30() {
    //noinspection MismatchedQueryAndUpdateOfCollection
    final List<String> list = new RexSlot.SelfPopulatingList("$", 30);
    final String s = list.get(30);
    assertThat(s, is("$30"));
  }

  /** Callback for {@link #check}. Test code will typically use {@code builder}
   * to create some expressions, call
   * {@link org.apache.calcite.rex.RexExecutorImpl#reduce} to evaluate them into
   * a list, then check that the results are as expected. */
  interface Action {
    void check(RexBuilder rexBuilder, RexExecutorImpl executor);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5949">[CALCITE-5949]
   * RexExecutable should return unchanged original expressions when it fails</a>.
   */
  @Test void testInvalidExpressionInList() {
    check((rexBuilder, executor) -> {
      final List<RexNode> reducedValues = new ArrayList<>();
      final RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
      final RelDataType integer =
          typeFactory.createSqlType(SqlTypeName.INTEGER);
      final RexCall first =
          (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.LN,
          rexBuilder.makeLiteral(3, integer, true));
      // Division by zero causes an exception during evaluation
      final RexCall second =
          (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE_INTEGER,
              rexBuilder.makeLiteral(-2, integer, true),
              rexBuilder.makeLiteral(0, integer, true));
      executor.reduce(rexBuilder, ImmutableList.of(first, second),
          reducedValues);
      assertThat(reducedValues, hasSize(2));
      assertThat(reducedValues.get(0), instanceOf(RexCall.class));
      assertThat(reducedValues.get(1), instanceOf(RexCall.class));
    });
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6168">[CALCITE-6168]
   * RexExecutor can throw during compilation</a>. */
  @Test void testCompileTimeException() {
    check((rexBuilder, executor) -> {
      final List<RexNode> reducedValues = new ArrayList<>();
      final RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
      // CAST(200 as TINYINT)
      final RelDataType tinyint =
          typeFactory.createSqlType(SqlTypeName.TINYINT);
      final RelDataType integer  =
          typeFactory.createSqlType(SqlTypeName.INTEGER);
      final RexNode cast =
          rexBuilder.makeCast(tinyint,
              rexBuilder.makeLiteral(200, integer, true));
      executor.reduce(rexBuilder, ImmutableList.of(cast),
          reducedValues);
      assertThat(reducedValues, hasSize(1));
      assertThat(reducedValues.get(0), instanceOf(RexCall.class));
    });
  }
}
