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
package net.hydromatic.optiq.test;

import net.hydromatic.linq4j.QueryProvider;

import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.config.Lex;
import net.hydromatic.optiq.impl.interpreter.Interpreter;
import net.hydromatic.optiq.impl.interpreter.Row;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.tools.FrameworkConfig;
import net.hydromatic.optiq.tools.Frameworks;
import net.hydromatic.optiq.tools.Planner;

import org.eigenbase.rel.RelNode;
import org.eigenbase.sql.SqlNode;

import com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for {@link net.hydromatic.optiq.impl.interpreter.Interpreter}.
 */
public class InterpreterTest {
  private SchemaPlus rootSchema;
  private Planner planner;

  /** Implementation of {@link DataContext} for executing queries without a
   * connection. */
  private class MyDataContext implements DataContext {
    private final Planner planner;

    public MyDataContext(Planner planner) {
      this.planner = planner;
    }

    public SchemaPlus getRootSchema() {
      return rootSchema;
    }

    public JavaTypeFactory getTypeFactory() {
      return (JavaTypeFactory) planner.getTypeFactory();
    }

    public QueryProvider getQueryProvider() {
      return null;
    }

    public Object get(String name) {
      return null;
    }
  }

  @Before public void setUp() {
    rootSchema = Frameworks.createRootSchema(true);
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .lex(Lex.ORACLE)
        .defaultSchema(
            OptiqAssert.addSchema(rootSchema, OptiqAssert.SchemaSpec.HR))
        .build();
    planner = Frameworks.getPlanner(config);
  }

  @After public void tearDown() {
    rootSchema = null;
    planner = null;
  }

  /** Tests executing a simple plan using an interpreter. */
  @Test public void testInterpretProjectFilterValues() throws Exception {
    SqlNode parse =
        planner.parse("select y, x\n"
            + "from (values (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y)\n"
            + "where x > 1");

    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.convert(validate);

    final Interpreter interpreter = new Interpreter(null, convert);
    assertRows(interpreter,
        "[_ISO-8859-1'b', 2]",
        "[_ISO-8859-1'c', 3]");
  }

  private static void assertRows(Interpreter interpreter, String... rows) {
    final List<String> list = Lists.newArrayList();
    for (Row row : interpreter) {
      list.add(row.toString());
    }
    assertThat(list, equalTo(Arrays.asList(rows)));
  }

  /** Tests executing a simple plan using an interpreter. */
  @Test public void testInterpretTable() throws Exception {
    SqlNode parse =
        planner.parse("select * from \"hr\".\"emps\" order by \"empid\"");

    SqlNode validate = planner.validate(parse);
    RelNode convert = planner.convert(validate);

    final Interpreter interpreter =
        new Interpreter(new MyDataContext(planner), convert);
    assertRows(interpreter,
        "[100, 10, Bill, 10000.0, 1000]",
        "[110, 10, Theodore, 11500.0, 250]",
        "[150, 10, Sebastian, 7000.0, null]",
        "[200, 20, Eric, 8000.0, 500]");
  }
}

// End InterpreterTest.java
