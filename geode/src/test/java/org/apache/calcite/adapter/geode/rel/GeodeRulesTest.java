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

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for {@link GeodeRules} covering the ITEM-key plan-time
 * short-circuit and the dotted-path emission for {@code ITEM(field, 'key')}
 * calls.
 *
 * <p>OQL provides no way to quote identifiers, so a CHAR literal that
 * becomes part of a dotted field path must match a strict allowlist before
 * it can be copied verbatim into the generated statement; any other key
 * keeps the expression on the Calcite side.
 */
class GeodeRulesTest {

  private static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl();
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

  /** Builds a translator over a single-column ANY-typed row so ITEM
   * calls type-check. */
  private static GeodeRules.RexToGeodeTranslator translator(List<String> fields)
      throws Exception {
    Constructor<GeodeRules.RexToGeodeTranslator> ctor =
        GeodeRules.RexToGeodeTranslator.class
            .getDeclaredConstructor(List.class);
    ctor.setAccessible(true);
    return ctor.newInstance(fields);
  }

  private static RexNode itemCall(String key) {
    RelDataType any = TYPE_FACTORY.createSqlType(SqlTypeName.ANY);
    RexInputRef ref = REX_BUILDER.makeInputRef(any, 0);
    RexNode lit = REX_BUILDER.makeLiteral(key);
    return REX_BUILDER.makeCall(SqlStdOperatorTable.ITEM, ref, lit);
  }

  @Test void plainKeyAccepted() throws Exception {
    GeodeRules.RexToGeodeTranslator t =
        translator(Collections.singletonList("root"));
    assertThat(itemCall("child").accept(t), is("root.child"));
  }

  @Test void syntheticExprKeyAccepted() throws Exception {
    GeodeRules.RexToGeodeTranslator t =
        translator(Collections.singletonList("root"));
    assertThat(itemCall("EXPR$0").accept(t), is("root.EXPR$0"));
  }

  @Test void keyWithSpaceRejected() throws Exception {
    GeodeRules.RexToGeodeTranslator t =
        translator(Collections.singletonList("root"));
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class,
            () -> itemCall("a b").accept(t));
    assertThat(e.getMessage(), containsString("not a plain identifier"));
  }

  @Test void keyWithSingleQuoteRejected() throws Exception {
    // An apostrophe in the key would terminate the surrounding OQL string
    // token and shift the boundaries of the emitted statement.
    GeodeRules.RexToGeodeTranslator t =
        translator(Collections.singletonList("root"));
    assertThrows(IllegalArgumentException.class,
        () -> itemCall("a'b").accept(t));
  }

  @Test void keyWithPunctuationRejected() throws Exception {
    GeodeRules.RexToGeodeTranslator t =
        translator(Collections.singletonList("root"));
    assertThrows(IllegalArgumentException.class,
        () -> itemCall("a,b").accept(t));
    assertThrows(IllegalArgumentException.class,
        () -> itemCall("a(b)").accept(t));
    assertThrows(IllegalArgumentException.class,
        () -> itemCall("a.b").accept(t));
  }

  @Test void safeItemKeyPredicate() {
    assertThat(GeodeRules.isSafeItemKey("postalCode"), is(true));
    assertThat(GeodeRules.isSafeItemKey("_key$1"), is(true));
    assertThat(GeodeRules.isSafeItemKey("EXPR$0"), is(true));
    assertThat(GeodeRules.isSafeItemKey(""), is(false));
    assertThat(GeodeRules.isSafeItemKey("a b"), is(false));
    assertThat(GeodeRules.isSafeItemKey("a.b"), is(false));
    assertThat(GeodeRules.isSafeItemKey("e.getClass()"), is(false));
  }

  @Test void hasOnlySafeItemKeysInspectsWholeTree() {
    // A plain expression walks through as safe.
    RexNode safe =
        REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, itemCall("k"),
            REX_BUILDER.makeLiteral("v"));
    assertThat(GeodeRules.hasOnlySafeItemKeys(safe), is(true));

    // An unsafe key anywhere in the tree flips the answer.
    RexNode unsafe =
        REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
            itemCall("k' OR '1' = '1"),
            REX_BUILDER.makeLiteral("v"));
    assertThat(GeodeRules.hasOnlySafeItemKeys(unsafe), is(false));
  }
}
