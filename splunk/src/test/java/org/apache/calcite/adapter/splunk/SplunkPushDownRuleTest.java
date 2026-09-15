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
package org.apache.calcite.adapter.splunk;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests for the field-name allowlist inside
 * {@link SplunkPushDownRule}. SPL has no generic identifier-quoting
 * mechanism, so a field name that is copied verbatim into the generated
 * search must match a strict allowlist; anything else could shift the
 * boundaries of the emitted command, and the corresponding filter or
 * rename is evaluated by Calcite instead.
 *
 * <p>The tests exercise the filter translation directly and do not
 * require a running Splunk instance.
 */
class SplunkPushDownRuleTest {

  private static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl();
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

  /** Translates {@code <field> = 'foo'} via the rule's (private)
   * {@code getFilter}, returning the SPL string, or null if the rule
   * refuses the push down. */
  private static String filterString(String fieldName) throws Throwable {
    RelDataType varchar = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
    RexInputRef ref = REX_BUILDER.makeInputRef(varchar, 0);
    RexNode lit = REX_BUILDER.makeLiteral("foo");
    StringBuilder buf = new StringBuilder();
    final boolean ok =
        SplunkPushDownRule.getFilter(SqlStdOperatorTable.EQUALS, ImmutableList.of(ref, lit), buf,
            ImmutableList.of(fieldName));
    return ok ? buf.toString() : null;
  }

  @Test void plainFieldNamePushedDown() throws Throwable {
    assertThat(filterString("source"), is("source = foo"));
    assertThat(filterString("date_hour"), is("date_hour = foo"));
  }

  @Test void splunkStyleFieldNamesAccepted() {
    // Real-world Splunk field-name shapes: the allowlist must not
    // reject these, or legitimate pushdown would stop working.
    assertThat(SplunkPushDownRule.isSafeFieldName("_raw"), is(true));
    assertThat(SplunkPushDownRule.isSafeFieldName("props{}.value"), is(true));
    assertThat(SplunkPushDownRule.isSafeFieldName("EXPR$0"), is(true));
    assertThat(SplunkPushDownRule.isSafeFieldName("host-name"), is(true));
    assertThat(SplunkPushDownRule.isSafeFieldName("a.b.c"), is(true));
    assertThat(SplunkPushDownRule.isSafeFieldName("date_hour"), is(true));
  }

  @Test void aliasWithPipeNotPushedDown() throws Throwable {
    // A pipe character starts a new SPL command; a rename containing
    // one would chain unintended pipeline stages onto the emitted
    // search. The rule must therefore refuse this push down.
    assertThat(filterString("x | delete | search a"), nullValue());
  }

  @Test void namesWithSplMetacharactersRejected() {
    // Characters that would let a rename fragment or field reference
    // shift the boundaries of the emitted SPL command.
    assertThat(SplunkPushDownRule.isSafeFieldName("a b"), is(false));
    assertThat(SplunkPushDownRule.isSafeFieldName("a\"b"), is(false));
    assertThat(SplunkPushDownRule.isSafeFieldName("a'b"), is(false));
    assertThat(SplunkPushDownRule.isSafeFieldName("a=b"), is(false));
    assertThat(SplunkPushDownRule.isSafeFieldName("a[b]"), is(false));
    assertThat(SplunkPushDownRule.isSafeFieldName("a`b"), is(false));
    assertThat(SplunkPushDownRule.isSafeFieldName("a\\b"), is(false));
    assertThat(SplunkPushDownRule.isSafeFieldName("a,b"), is(false));
    assertThat(SplunkPushDownRule.isSafeFieldName("(a)"), is(false));
  }

  @Test void nullAndEmptyRejected() {
    assertThat(SplunkPushDownRule.isSafeFieldName(null), is(false));
    assertThat(SplunkPushDownRule.isSafeFieldName(""), is(false));
  }
}
