/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.apache.optiq.rex;


import java.math.BigDecimal;
import java.util.*;

import org.apache.optiq.relopt.RelOptCluster;
import org.apache.optiq.relopt.RelOptSchema;
import org.apache.optiq.reltype.RelDataType;
import org.apache.optiq.reltype.RelDataTypeFactory;
import org.apache.optiq.sql.fun.SqlStdOperatorTable;
import org.apache.optiq.sql.type.SqlTypeName;
import org.apache.optiq.util.NlsString;

import org.apache.linq4j.QueryProvider;

import org.apache.optiq.DataContext;
import org.apache.optiq.SchemaPlus;
import org.apache.optiq.Schemas;
import org.apache.optiq.impl.enumerable.JavaTypeFactory;
import org.apache.optiq.server.OptiqServerStatement;
import org.apache.optiq.tools.Frameworks;

import com.google.common.collect.ImmutableList;

import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link org.apache.optiq.rex.RexExecutorImpl}.
 */
public class RexExecutorTest {
  public RexExecutorTest() {
  }

  protected void check(final Action action) throws Exception {
    Frameworks.withPrepare(
        new Frameworks.PrepareAction<Void>() {
          public Void apply(RelOptCluster cluster, RelOptSchema relOptSchema,
              SchemaPlus rootSchema, OptiqServerStatement statement) {
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
    check(new Action() {
      public void check(RexBuilder rexBuilder, RexExecutorImpl executor) {
        Object[] values = new Object[1];
        final DataContext testContext = new TestDataContext(values);
        final RelDataType varchar = rexBuilder.getTypeFactory().createSqlType(
            SqlTypeName.VARCHAR);
        final RelDataType integer = rexBuilder.getTypeFactory().createSqlType(
            SqlTypeName.INTEGER);
        // optiq is internally creating the creating the input ref via a
        // RexRangeRef
        // which eventually leads to a RexInputRef. So we are good.
        final RexInputRef input = rexBuilder.makeInputRef(varchar, 0);
        final RexNode lengthArg = rexBuilder.makeLiteral(3, integer, true);
        final RexNode substr =
            rexBuilder.makeCall(SqlStdOperatorTable.SUBSTRING, input,
                lengthArg);
        ImmutableList<RexNode> constExps = ImmutableList.of(substr);

        final RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
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
        values[0] = "Optiq";
        result = exec.execute();
        assertTrue(result[0] instanceof String);
        assertThat((String) result[0], equalTo("tiq"));
      }
    });
  }

  @Test public void testConstant() throws Exception {
    check(new Action() {
      public void check(RexBuilder rexBuilder, RexExecutorImpl executor) {
        final List<RexNode> reducedValues = new ArrayList<RexNode>();
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

  @Test public void testSubstring() throws Exception {
    check(new Action() {
      public void check(RexBuilder rexBuilder, RexExecutorImpl executor) {
        final List<RexNode> reducedValues = new ArrayList<RexNode>();
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
   * {@link org.apache.optiq.rex.RexExecutorImpl#reduce} to evaluate them into
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
