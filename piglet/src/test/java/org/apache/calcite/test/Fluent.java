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
package org.apache.calcite.test;

import org.apache.calcite.piglet.Ast;
import org.apache.calcite.piglet.Handler;
import org.apache.calcite.piglet.parser.ParseException;
import org.apache.calcite.piglet.parser.PigletParser;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.tools.PigRelBuilder;
import org.apache.calcite.util.Util;

import com.google.common.collect.Ordering;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Fluent API to perform Piglet test actions. */
class Fluent {
  private final String pig;

  Fluent(String pig) {
    this.pig = pig;
  }

  private Ast.Program parseProgram(String s) throws ParseException {
    return new PigletParser(new StringReader(s)).stmtListEof();
  }

  public Fluent explainContains(String expected) throws ParseException {
    final Ast.Program program = parseProgram(pig);
    final PigRelBuilder builder =
        PigRelBuilder.create(PigRelBuilderTest.config().build());
    new Handler(builder).handle(program);
    assertThat(Util.toLinux(RelOptUtil.toString(builder.peek())), is(expected));
    return this;
  }

  public Fluent returns(final String out)
      throws ParseException {
    return returns(s -> {
      assertThat(s, is(out));
      return null;
    });
  }

  public Fluent returnsUnordered(String... lines) throws ParseException {
    final List<String> expectedLines =
        Ordering.natural().immutableSortedCopy(Arrays.asList(lines));
    return returns(s -> {
      final List<String> actualLines = new ArrayList<>();
      for (;;) {
        int i = s.indexOf('\n');
        if (i < 0) {
          if (!s.isEmpty()) {
            actualLines.add(s);
          }
          break;
        } else {
          actualLines.add(s.substring(0, i));
          s = s.substring(i + 1);
        }
      }
      assertThat(Ordering.natural().sortedCopy(actualLines),
          is(expectedLines));
      return null;
    });
  }

  public Fluent returns(Function<String, Void> checker) throws ParseException {
    final Ast.Program program = parseProgram(pig);
    final PigRelBuilder builder =
        PigRelBuilder.create(PigRelBuilderTest.config().build());
    final StringWriter sw = new StringWriter();
    new CalciteHandler(builder, sw).handle(program);
    checker.apply(Util.toLinux(sw.toString()));
    return this;
  }

  public Fluent parseContains(String expected) throws ParseException {
    final Ast.Program program = parseProgram(pig);
    assertThat(Util.toLinux(Ast.toString(program)), is(expected));
    return this;
  }
}

// End Fluent.java
