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
package org.apache.calcite.test.enumerable;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.JavaCollation;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.JdbcTest;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;

import org.junit.jupiter.api.Test;

import java.text.Collator;
import java.util.Collections;
import java.util.Locale;
import java.util.function.Consumer;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_EQUALS;

/**
 * Test cases for
 * <a href="https://issues.apache.org/jira/browse/CALCITE-3951">[CALCITE-3951]
 * Support different string comparison based on SqlCollation</a>.
 */
class EnumerableStringComparisonTest {

  private static final SqlCollation SPECIAL_COLLATION_PRIMARY =
      new JavaCollation(SqlCollation.Coercibility.IMPLICIT, Locale.US,
          Util.getDefaultCharset(), Collator.PRIMARY);

  private static final SqlCollation SPECIAL_COLLATION_SECONDARY =
      new JavaCollation(SqlCollation.Coercibility.IMPLICIT, Locale.US,
          Util.getDefaultCharset(), Collator.SECONDARY);

  private static final SqlCollation SPECIAL_COLLATION_TERTIARY =
      new JavaCollation(SqlCollation.Coercibility.IMPLICIT, Locale.US,
          Util.getDefaultCharset(), Collator.TERTIARY);

  private static final SqlCollation SPECIAL_COLLATION_IDENTICAL =
      new JavaCollation(SqlCollation.Coercibility.IMPLICIT, Locale.US,
          Util.getDefaultCharset(), Collator.IDENTICAL);

  private RelDataType createRecordVarcharSpecialCollation(RelBuilder builder) {
    return builder.getTypeFactory().builder()
        .add(
            "name",
            builder.getTypeFactory().createTypeWithCharsetAndCollation(
                builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR),
                builder.getTypeFactory().getDefaultCharset(),
                SPECIAL_COLLATION_TERTIARY))
        .build();
  }

  private RelDataType createVarcharSpecialCollation(RelBuilder builder, SqlCollation collation) {
    return builder.getTypeFactory().createTypeWithCharsetAndCollation(
        builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR),
        builder.getTypeFactory().getDefaultCharset(),
        collation);
  }

  @Test void testSortStringDefault() {
    tester()
        .query("?")
        .withRel(builder -> builder
            .values(
                builder.getTypeFactory().builder()
                    .add("name",
                        builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR)).build(),
                "Legal", "presales", "hr", "Administration", "MARKETING")
            .sort(
                builder.field(1, 0, "name"))
            .build())
        .explainHookMatches(""
            + "EnumerableSort(sort0=[$0], dir0=[ASC])\n"
            + "  EnumerableValues(tuples=[[{ 'Legal' }, { 'presales' }, { 'hr' }, { 'Administration' }, { 'MARKETING' }]])\n")
        .returnsOrdered("name=Administration\n"
            + "name=Legal\n"
            + "name=MARKETING\n"
            + "name=hr\n"
            + "name=presales");
  }

  @Test void testSortStringSpecialCollation() {
    tester()
        .query("?")
        .withRel(builder -> builder
            .values(
                createRecordVarcharSpecialCollation(builder),
                "Legal", "presales", "hr", "Administration", "MARKETING")
            .sort(
                builder.field(1, 0, "name"))
            .build())
        .explainHookMatches(""
            + "EnumerableSort(sort0=[$0], dir0=[ASC])\n"
            + "  EnumerableValues(tuples=[[{ 'Legal' }, { 'presales' }, { 'hr' }, { 'Administration' }, { 'MARKETING' }]])\n")
        .returnsOrdered("name=Administration\n"
            + "name=hr\n"
            + "name=Legal\n"
            + "name=MARKETING\n"
            + "name=presales");
  }

  @Test void testMergeJoinOnStringSpecialCollation() {
    tester()
        .query("?")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner -> {
          planner.addRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
          planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
        })
        .withRel(builder -> builder
              .values(createRecordVarcharSpecialCollation(builder),
                  "Legal", "presales", "HR", "Administration", "Marketing").as("v1")
              .values(createRecordVarcharSpecialCollation(builder),
                  "Marketing", "bureaucracy", "Sales", "HR").as("v2")
              .join(JoinRelType.INNER,
                  builder.equals(
                      builder.field(2, 0, "name"),
                      builder.field(2, 1, "name")))
              .project(
                  builder.field("v1", "name"),
                  builder.field("v2", "name"))
              .build())
        .explainHookMatches("" // It is important that we have MergeJoin in the plan
            + "EnumerableMergeJoin(condition=[=($0, $1)], joinType=[inner])\n"
            + "  EnumerableSort(sort0=[$0], dir0=[ASC])\n"
            + "    EnumerableValues(tuples=[[{ 'Legal' }, { 'presales' }, { 'HR' }, { 'Administration' }, { 'Marketing' }]])\n"
            + "  EnumerableSort(sort0=[$0], dir0=[ASC])\n"
            + "    EnumerableValues(tuples=[[{ 'Marketing' }, { 'bureaucracy' }, { 'Sales' }, { 'HR' }]])\n")
        .returnsOrdered("name=HR; name0=HR\n"
            + "name=Marketing; name0=Marketing");
  }

  @Test void testStringComparison() {
    testStringComparison("a", "A", LESS_THAN, true);
    testStringComparison("a", "A", GREATER_THAN, false);
    testStringComparison("A", "a", LESS_THAN, false);
    testStringComparison("A", "a", GREATER_THAN, true);

    testStringComparison("aaa", "AAA", EQUALS, false);
    testStringComparison("aaa", "AAA", NOT_EQUALS, true);
    testStringComparison("AAA", "AAA", EQUALS, true);
    testStringComparison("AAA", "AAA", NOT_EQUALS, false);
    testStringComparison("AAA", "BBB", EQUALS, false);
    testStringComparison("AAA", "BBB", NOT_EQUALS, true);

    testStringComparison("a", "b", LESS_THAN, true);
    testStringComparison("A", "B", LESS_THAN, true);
    testStringComparison("a", "B", LESS_THAN, true);
    testStringComparison("A", "b", LESS_THAN, true);
    testStringComparison("a", "b", GREATER_THAN, false);
    testStringComparison("A", "B", GREATER_THAN, false);
    testStringComparison("a", "B", GREATER_THAN, false);
    testStringComparison("A", "b", GREATER_THAN, false);

    testStringComparison("b", "a", GREATER_THAN, true);
    testStringComparison("B", "A", GREATER_THAN, true);
    testStringComparison("B", "a", GREATER_THAN, true);
    testStringComparison("b", "A", GREATER_THAN, true);
    testStringComparison("b", "a", LESS_THAN, false);
    testStringComparison("B", "A", LESS_THAN, false);
    testStringComparison("B", "a", LESS_THAN, false);
    testStringComparison("b", "A", LESS_THAN, false);

    // Check differences regarding strength:

    testStringComparison("ABC", "ABC", EQUALS, SPECIAL_COLLATION_PRIMARY, true);
    testStringComparison("ABC", "ABC", EQUALS, SPECIAL_COLLATION_SECONDARY, true);
    testStringComparison("ABC", "ABC", EQUALS, SPECIAL_COLLATION_TERTIARY, true);
    testStringComparison("ABC", "ABC", EQUALS, SPECIAL_COLLATION_IDENTICAL, true);

    testStringComparison("abc", "ÀBC", EQUALS, SPECIAL_COLLATION_PRIMARY, true);
    testStringComparison("abc", "ÀBC", EQUALS, SPECIAL_COLLATION_SECONDARY, false);
    testStringComparison("abc", "ÀBC", EQUALS, SPECIAL_COLLATION_TERTIARY, false);
    testStringComparison("abc", "ÀBC", EQUALS, SPECIAL_COLLATION_IDENTICAL, false);

    testStringComparison("abc", "ABC", EQUALS, SPECIAL_COLLATION_PRIMARY, true);
    testStringComparison("abc", "ABC", EQUALS, SPECIAL_COLLATION_SECONDARY, true);
    testStringComparison("abc", "ABC", EQUALS, SPECIAL_COLLATION_TERTIARY, false);
    testStringComparison("abc", "ABC", EQUALS, SPECIAL_COLLATION_IDENTICAL, false);

    testStringComparison("\u0001", "\u0002", EQUALS, SPECIAL_COLLATION_PRIMARY, true);
    testStringComparison("\u0001", "\u0002", EQUALS, SPECIAL_COLLATION_SECONDARY, true);
    testStringComparison("\u0001", "\u0002", EQUALS, SPECIAL_COLLATION_TERTIARY, true);
    testStringComparison("\u0001", "\u0002", EQUALS, SPECIAL_COLLATION_IDENTICAL, false);
  }

  private void testStringComparison(String str1, String str2,
                                    SqlOperator operator, boolean expectedResult) {
    testStringComparison(str1, str2, operator, SPECIAL_COLLATION_TERTIARY, expectedResult);
  }

  private void testStringComparison(String str1, String str2,
                                    SqlOperator operator, SqlCollation col,
                                    boolean expectedResult) {
    tester()
        .query("?")
        .withRel(builder -> {
          final RexBuilder rexBuilder = builder.getRexBuilder();
          final RelDataType varcharSpecialCollation = createVarcharSpecialCollation(builder, col);
          return builder
              .values(new String[]{"aux"}, false)
              .project(
                  Collections.singletonList(
                      builder.call(
                          operator,
                          rexBuilder.makeCast(varcharSpecialCollation, builder.literal(str1)),
                          rexBuilder.makeCast(varcharSpecialCollation, builder.literal(str2)))),
                  Collections.singletonList("result"))
              .build();
        })
        .returnsUnordered("result=" + expectedResult);
  }

  private CalciteAssert.AssertThat tester() {
    return CalciteAssert.that()
        .with(CalciteConnectionProperty.LEX, Lex.JAVA)
        .with(CalciteConnectionProperty.FORCE_DECORRELATE, false)
        .withSchema("s", new ReflectiveSchema(new JdbcTest.HrSchema()));
  }
}
