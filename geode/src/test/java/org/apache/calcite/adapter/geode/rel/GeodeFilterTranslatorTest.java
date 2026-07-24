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
package org.apache.calcite.adapter.geode.rel;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests for {@link GeodeFilter.Translator} covering OQL literal serialization.
 *
 * <p>The tests exercise the translator directly against a synthetic
 * row type; they do not require a running Geode cluster.
 */
class GeodeFilterTranslatorTest {

  private static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl();
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

  private static RelDataType rowType() {
    return TYPE_FACTORY.builder()
        .add("code", SqlTypeName.CHAR, 32)
        .build();
  }

  /** Invokes the (private) translator's translateMatch entry
   * point via reflection so this test can live in the same package
   * without widening the Translator's visibility. Any exception the
   * target throws is unwrapped from InvocationTargetException. */
  private static String translate(RexNode condition) throws Throwable {
    GeodeFilter.Translator t =
        new GeodeFilter.Translator(rowType(), REX_BUILDER);
    Method m = GeodeFilter.Translator.class
        .getDeclaredMethod("translateMatch", RexNode.class);
    m.setAccessible(true);
    try {
      return (String) m.invoke(t, condition);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  /** Builds a {@code code = <value>} predicate where the literal is
   * typed as {@code CHAR(N)} matching {@code value.length()} exactly. */
  private static RexNode eqLit(String value) {
    RelDataType t = TYPE_FACTORY.createSqlType(SqlTypeName.CHAR, value.length());
    RexInputRef ref = REX_BUILDER.makeInputRef(t, 0);
    RexNode lit = REX_BUILDER.makeLiteral(value, t, false);
    return REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, ref, lit);
  }

  @Test void charColumnPlainValueQuoted() throws Throwable {
    assertThat(translate(eqLit("alpha")), is("code = 'alpha'"));
  }

  @Test void charColumnValueWithApostrophe() throws Throwable {
    assertThat(translate(eqLit("O'Brien")), is("code = 'O''Brien'"));
  }

  @Test void charColumnValueWithMultipleApostrophes() throws Throwable {
    assertThat(translate(eqLit("a''b")), is("code = 'a''''b'"));
  }

  @Test void charColumnValueWithApostropheAtTheStart() throws Throwable {
    assertThat(translate(eqLit("'a")), is("code = '''a'"));
  }

  @Test void charColumnValueWithApostropheAtTheEnd() throws Throwable {
    assertThat(translate(eqLit("a'")), is("code = 'a'''"));
  }
}
