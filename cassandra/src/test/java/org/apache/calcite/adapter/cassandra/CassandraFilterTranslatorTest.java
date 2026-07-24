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
package org.apache.calcite.adapter.cassandra;

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
import java.util.Collections;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link CassandraFilter.Translator} covering CQL literal serialization.
 *
 * <p>The tests exercise the translator directly against a synthetic row
 * type; they do not require a running Cassandra cluster.
 */
class CassandraFilterTranslatorTest {

  private static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl();
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

  /** Two-column row type mirroring how CqlToSqlTypeConversionRules
   * maps Cassandra types: a CHAR column stands in for `uuid`/`timeuuid`
   * and a VARCHAR column stands in for `text`. */
  private static RelDataType rowType() {
    return TYPE_FACTORY.builder()
        .add("id", SqlTypeName.CHAR, 36)
        .add("name", SqlTypeName.VARCHAR)
        .build();
  }

  /** Invokes the (private) translator's translateMatch entry point via reflection.
   * Any exception is unwrapped from InvocationTargetException so callers can
   * see the real cause. */
  private static String translate(RexNode condition) throws Throwable {
    CassandraFilter.Translator t =
        new CassandraFilter.Translator(rowType(),
            Collections.singletonList("id"),
            Collections.emptyList(),
            Collections.emptyList());
    Method m = CassandraFilter.Translator.class
        .getDeclaredMethod("translateMatch", RexNode.class);
    m.setAccessible(true);
    try {
      return (String) m.invoke(t, condition);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  private static RexNode eqLit(int fieldIndex, SqlTypeName fieldType,
      int precision, String value) {
    RelDataType t = precision > 0
        ? TYPE_FACTORY.createSqlType(fieldType, precision)
        : TYPE_FACTORY.createSqlType(fieldType);
    RexInputRef ref = REX_BUILDER.makeInputRef(t, fieldIndex);
    RexNode lit = REX_BUILDER.makeLiteral(value, t, false);
    return REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, ref, lit);
  }

  @Test void uuidColumnValidUuidEmittedUnquoted() throws Throwable {
    String cql =
        translate(eqLit(0, SqlTypeName.CHAR, 36, "037f7c30-abcd-11ee-8000-000000000001"));
    assertThat(cql, is("id = 037f7c30-abcd-11ee-8000-000000000001"));
  }

  @Test void uuidColumnNonUuidValueRejected() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> translate(
                eqLit(0, SqlTypeName.CHAR, 36, "not-a-uuid")));
    assertThat(e.getMessage(), containsString("not a well-formed UUID"));
  }

  @Test void uuidColumnEmptyRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> translate(eqLit(0, SqlTypeName.CHAR, 36, "")));
  }

  @Test void uuidColumnAlmostUuidRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> translate(
            eqLit(0, SqlTypeName.CHAR, 36, "037f7c30-abcd-11ee-8000-000000000001x")));
  }

  @Test void varcharColumnPlainValueQuoted() throws Throwable {
    String cql = translate(eqLit(1, SqlTypeName.VARCHAR, -1, "alice"));
    assertThat(cql, is("name = 'alice'"));
  }

  @Test void varcharColumnValueWithApostrophe() throws Throwable {
    String cql = translate(eqLit(1, SqlTypeName.VARCHAR, -1, "O'Brien"));
    assertThat(cql, is("name = 'O''Brien'"));
  }

  @Test void varcharColumnValueWithMultipleApostrophes() throws Throwable {
    String cql = translate(eqLit(1, SqlTypeName.VARCHAR, -1, "a''b"));
    assertThat(cql, is("name = 'a''''b'"));
  }

  @Test void varcharColumnValueWithApostropheAtTheStart() throws Throwable {
    String cql = translate(eqLit(1, SqlTypeName.VARCHAR, -1, "'a"));
    assertThat(cql, is("name = '''a'"));
  }

  @Test void varcharColumnValueWithApostropheAtTheEnd() throws Throwable {
    String cql = translate(eqLit(1, SqlTypeName.VARCHAR, -1, "a'"));
    assertThat(cql, is("name = 'a'''"));
  }
}
