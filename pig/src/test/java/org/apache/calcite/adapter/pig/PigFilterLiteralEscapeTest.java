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
package org.apache.calcite.adapter.pig;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests for {@link PigFilter}'s literal-to-Pig-Latin serialization.
 */
class PigFilterLiteralEscapeTest {

  private static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl();
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

  private static RexLiteral charLiteral(String value) {
    return (RexLiteral) REX_BUILDER.makeLiteral(value,
        TYPE_FACTORY.createSqlType(SqlTypeName.CHAR, value.length()), false);
  }

  private static String call(RexLiteral literal) throws Throwable {
    Method m = PigFilter.class.getDeclaredMethod("getLiteralAsString", RexLiteral.class);
    m.setAccessible(true);
    try {
      return (String) m.invoke(null, literal);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  @Test void plainValueQuoted() throws Throwable {
    assertThat(call(charLiteral("alice")), is("'alice'"));
  }

  @Test void valueWithApostrophe() throws Throwable {
    assertThat(call(charLiteral("O'Brien")), is("'O''Brien'"));
  }

  @Test void valueWithApostropheAtTheEnd() throws Throwable {
    assertThat(call(charLiteral("a'")), is("'a'''"));
  }

  @Test void valueWithApostropheAtTheStart() throws Throwable {
    assertThat(call(charLiteral("'a")), is("'''a'"));
  }

  @Test void valueWithMultipleApostrophes() throws Throwable {
    assertThat(call(charLiteral("a''b")), is("'a''''b'"));
  }

  @Test void emptyValue() throws Throwable {
    assertThat(call(charLiteral("")), is("''"));
  }
}
