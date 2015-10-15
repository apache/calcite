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
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.server.CalciteServerStatement;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.NlsString;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link org.apache.calcite.rex.RexExecutorImpl}.
 */
public class RexExecutorTest {
  public RexExecutorTest() {
  }

  protected void check(final Action action) throws Exception {
    Frameworks.withPrepare(
        new Frameworks.PrepareAction<Void>() {
          public Void apply(RelOptCluster cluster, RelOptSchema relOptSchema,
              SchemaPlus rootSchema, CalciteServerStatement statement) {
            final RexBuilder rexBuilder = cluster.getRexBuilder();
            DataContext dataContext =
                Schemas.createDataContext(statement.getConnection());
            final RexExecutorImpl executor = new RexExecutorImpl(dataContext);
            action.check(rexBuilder, executor);
            return null;
          }
        });
  }

  /** Tests an executor that uses variables stored in a {@link DataContext}.
   * Can change the value of the variable and execute again. */
  @Test public void testVariableExecution() throws Exception {
    check(
        new Action() {
          public void check(RexBuilder rexBuilder, RexExecutorImpl executor) {
            Object[] values = new Object[1];
            final DataContext testContext = new TestDataContext(values);
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

            final RexExecutable exec = executor.getExecutable(rexBuilder,
                constExps, rowType);
            exec.setDataContext(testContext);
            values[0] = "Hello World";
            Object[] result = exec.execute();
            assertTrue(result[0] instanceof String);
            assertThat((String) result[0], equalTo("llo World"));
            values[0] = "Calcite";
            result = exec.execute();
            assertTrue(result[0] instanceof String);
            assertThat((String) result[0], equalTo("lcite"));
          }
        });
  }

  @Test public void testConstant() throws Exception {
    check(new Action() {
      public void check(RexBuilder rexBuilder, RexExecutorImpl executor) {
        final List<RexNode> reducedValues = new ArrayList<>();
        final RexLiteral ten = rexBuilder.makeExactLiteral(BigDecimal.TEN);
        executor.reduce(rexBuilder, ImmutableList.<RexNode>of(ten),
            reducedValues);
        assertThat(reducedValues.size(), equalTo(1));
        assertThat(reducedValues.get(0), instanceOf(RexLiteral.class));
        assertThat(((RexLiteral) reducedValues.get(0)).getValue2(),
            equalTo((Object) 10L));
      }
    });
  }

  /** Reduces several expressions to constants. */
  @Test public void testConstant2() throws Exception {
    // Same as testConstant; 10 -> 10
    checkConstant(10L,
        new Function<RexBuilder, RexNode>() {
          public RexNode apply(RexBuilder rexBuilder) {
            return rexBuilder.makeExactLiteral(BigDecimal.TEN);
          }
        });
    // 10 + 1 -> 11
    checkConstant(11L,
        new Function<RexBuilder, RexNode>() {
          public RexNode apply(RexBuilder rexBuilder) {
            return rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
                rexBuilder.makeExactLiteral(BigDecimal.TEN),
                rexBuilder.makeExactLiteral(BigDecimal.ONE));
          }
        });
    // date 'today' <= date 'today' -> true
    checkConstant(true,
        new Function<RexBuilder, RexNode>() {
          public RexNode apply(RexBuilder rexBuilder) {
            Calendar calendar = Calendar.getInstance();
            return rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                rexBuilder.makeDateLiteral(calendar),
                rexBuilder.makeDateLiteral(calendar));
          }
        });
    // date 'today' < date 'today' -> false
    checkConstant(false,
        new Function<RexBuilder, RexNode>() {
          public RexNode apply(RexBuilder rexBuilder) {
            Calendar calendar = Calendar.getInstance();
            return rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN,
                rexBuilder.makeDateLiteral(calendar),
                rexBuilder.makeDateLiteral(calendar));
          }
        });
  }

  private void checkConstant(final Object operand,
      final Function<RexBuilder, RexNode> function) throws Exception {
    check(
        new Action() {
          public void check(RexBuilder rexBuilder, RexExecutorImpl executor) {
            final List<RexNode> reducedValues = new ArrayList<>();
            final RexNode expression = function.apply(rexBuilder);
            assert expression != null;
            executor.reduce(rexBuilder, ImmutableList.of(expression),
                reducedValues);
            assertThat(reducedValues.size(), equalTo(1));
            assertThat(reducedValues.get(0), instanceOf(RexLiteral.class));
            assertThat(((RexLiteral) reducedValues.get(0)).getValue2(),
                equalTo(operand));
          }
        });
  }

  @Test public void testSubstring() throws Exception {
    check(new Action() {
      public void check(RexBuilder rexBuilder, RexExecutorImpl executor) {
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
        assertThat(reducedValues.size(), equalTo(2));
        assertThat(reducedValues.get(0), instanceOf(RexLiteral.class));
        assertThat(((RexLiteral) reducedValues.get(0)).getValue2(),
            equalTo((Object) "ello")); // substring('Hello world!, 2, 4)
        assertThat(reducedValues.get(1), instanceOf(RexLiteral.class));
        assertThat(((RexLiteral) reducedValues.get(1)).getValue2(),
            equalTo((Object) 2L));
      }
    });
  }

  /** Callback for {@link #check}. Test code will typically use {@code builder}
   * to create some expressions, call
   * {@link org.apache.calcite.rex.RexExecutorImpl#reduce} to evaluate them into
   * a list, then check that the results are as expected. */
  interface Action {
    void check(RexBuilder rexBuilder, RexExecutorImpl executor);
  }

  /**
   * ArrayList-based DataContext to check Rex execution.
   */
  public static class TestDataContext implements DataContext {
    private final Object[] values;

    public TestDataContext(Object[] values) {
      this.values = values;
    }

    public SchemaPlus getRootSchema() {
      throw new RuntimeException("Unsupported");
    }

    public JavaTypeFactory getTypeFactory() {
      throw new RuntimeException("Unsupported");
    }

    public QueryProvider getQueryProvider() {
      throw new RuntimeException("Unsupported");
    }

    public Object get(String name) {
      if (name.equals("inputRecord")) {
        return values;
      } else {
        Assert.fail("Wrong DataContext access");
        return null;
      }
    }
  }
}

// End RexExecutorTest.java
