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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link RexToLixTranslator}.
 */
public final class RexToLixTranslatorTest {

  @Test public void testDateTypeToInnerTypeConvert() {
    // java.sql.Date x;
    final ParameterExpression date =
        Expressions.parameter(0, java.sql.Date.class, "x");
    final Expression dateToInt =
        RexToLixTranslator.convert(date, int.class);
    final Expression dateToInteger =
        RexToLixTranslator.convert(date, Integer.class);
    assertThat(Expressions.toString(dateToInt),
        is("org.apache.calcite.runtime.SqlFunctions.toInt(x)"));
    assertThat(Expressions.toString(dateToInteger),
        is("org.apache.calcite.runtime.SqlFunctions.toIntOptional(x)"));

    // java.sql.Time x;
    final ParameterExpression time =
        Expressions.parameter(0, java.sql.Time.class, "x");
    final Expression timeToInt =
        RexToLixTranslator.convert(time, int.class);
    final Expression timeToInteger =
        RexToLixTranslator.convert(time, Integer.class);
    assertThat(Expressions.toString(timeToInt),
        is("org.apache.calcite.runtime.SqlFunctions.toInt(x)"));
    assertThat(Expressions.toString(timeToInteger),
        is("org.apache.calcite.runtime.SqlFunctions.toIntOptional(x)"));

    // java.sql.TimeStamp x;
    final ParameterExpression timestamp =
        Expressions.parameter(0, java.sql.Timestamp.class, "x");
    final Expression timeStampToLongPrimitive =
        RexToLixTranslator.convert(timestamp, long.class);
    final Expression timeStampToLong =
        RexToLixTranslator.convert(timestamp, Long.class);
    assertThat(Expressions.toString(timeStampToLongPrimitive),
        is("org.apache.calcite.runtime.SqlFunctions.toLong(x)"));
    assertThat(Expressions.toString(timeStampToLong),
        is("org.apache.calcite.runtime.SqlFunctions.toLongOptional(x)"));
  }
}

// End RexToLixTranslatorTest.java
