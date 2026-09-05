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

import org.apache.calcite.adapter.enumerable.EnumerableIEJoin;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.JavaCollation;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.schemata.hr.HrSchema;
import org.apache.calcite.test.schemata.hr.HrSchemaBig;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

import java.text.Collator;
import java.util.Locale;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import static java.util.Objects.requireNonNull;

/** Unit tests for {@link EnumerableIEJoin}. */
class EnumerableIEJoinTest {
  private static final SqlCollation PRIMARY_COLLATION =
      new JavaCollation(SqlCollation.Coercibility.IMPLICIT, Locale.US,
          Util.getDefaultCharset(), Collator.PRIMARY);

  @Test void ieJoin() {
    final Holder<@Nullable RelRoot> root = Holder.empty();
    tester(new HrSchema())
        .withRel(builder -> {
          builder
              .values(new String[]{"lx", "ly"},
                  2, 8,
                  5, 4,
                  null, 3)
              .values(new String[]{"rx", "ry"},
                  4, 3,
                  7, 6,
                  5, 4)
              .join(JoinRelType.INNER,
                  builder.and(
                      builder.lessThan(
                          builder.field(2, 1, "rx"),
                          builder.field(2, 0, "lx")),
                      builder.greaterThan(
                          builder.field(2, 0, "ly"),
                          builder.field(2, 1, "ry"))));
          return builder.build();
        })
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner ->
            planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE))
        .withHook(Hook.PLAN_BEFORE_IMPLEMENTATION,
            (Consumer<RelRoot>) root::set)
        .explainHookMatches(
            "EnumerableIEJoin(condition=[AND(<($2, $0), >($1, $3))], joinType=[inner])\n"
                + "  EnumerableValues(tuples=[[{ 2, 8 }, { 5, 4 }, { null, 3 }]])\n"
                + "  EnumerableValues(tuples=[[{ 4, 3 }, { 7, 6 }, { 5, 4 }]])\n")
        .returnsUnordered("lx=5; ly=4; rx=4; ry=3");

    final EnumerableIEJoin join =
        (EnumerableIEJoin) requireNonNull(root.get()).rel;
    assertThrows(IllegalArgumentException.class,
        () -> join.copy(join.getTraitSet(), join.getCondition(),
            join.getLeft(), join.getRight(), JoinRelType.LEFT, false));
    assertThrows(IllegalArgumentException.class,
        () -> join.copy(join.getTraitSet(),
            join.getCluster().getRexBuilder().makeLiteral(true),
            join.getLeft(), join.getRight(), JoinRelType.INNER, false));
  }

  @Test void ieJoinDoesNotSupportApproximateNumbers() {
    final Holder<@Nullable RelRoot> root = Holder.empty();
    tester(new TestSchema())
        .query("select l.name as left_name, r.name as right_name "
            + "from lefts l join rights r "
            + "on l.x >= r.x and l.y > r.y")
        .withHook(Hook.PLAN_BEFORE_IMPLEMENTATION,
            (Consumer<RelRoot>) root::set)
        .explainHookMatches(
            allOf(
                containsString("EnumerableNestedLoopJoin"),
                not(containsString("EnumerableIEJoin"))))
        .returnsUnordered(
            "left_name=minusZero; right_name=zero",
            "left_name=plusZero; right_name=zero");
    final Join join = findJoin(requireNonNull(root.get()).rel);
    assertThat(
        assertThrows(IllegalArgumentException.class,
            () -> EnumerableIEJoin.create(
                join.getLeft(), join.getRight(), join.getCondition()))
            .getMessage(),
        allOf(containsString("left JavaType(double)"),
            containsString("right JavaType(double)")));
  }

  @Test void ieJoinDoesNotSupportAny() {
    final Holder<@Nullable RelNode> root = Holder.empty();
    tester(new TestSchema())
        .query("select l.name as left_name, r.name as right_name "
            + "from anyLefts l join anyRights r "
            + "on l.x < r.x and l.y > r.y")
        .withHook(Hook.TRIMMED, (Consumer<RelNode>) root::set)
        .explainContains("EnumerableNestedLoopJoin");
    final Join join = findJoin(requireNonNull(root.get()));
    assertThrows(IllegalArgumentException.class,
        () -> EnumerableIEJoin.create(
            join.getLeft(), join.getRight(), join.getCondition()));
  }

  @Test void ieJoinCostAndDefaultSelection() {
    final Holder<@Nullable RelRoot> root = Holder.empty();
    tester(new HrSchemaBig())
        .query("select count(*) from emps l join emps r "
            + "on l.empid < r.empid and l.deptno >= r.deptno")
        .withHook(Hook.PLAN_BEFORE_IMPLEMENTATION,
            (Consumer<RelRoot>) root::set)
        .explainHookMatches(containsString("EnumerableIEJoin"))
        .returns("EXPR$0=600\n");

    final Join join = findJoin(requireNonNull(root.get()).rel);
    final EnumerableIEJoin selfJoin =
        EnumerableIEJoin.create(join.getLeft(), join.getLeft(),
            join.getCondition());
    final RelMetadataQuery mq = new RelMetadataQuery() {
      @Override public Double getRowCount(RelNode rel) {
        return rel == selfJoin ? 3D : 10D;
      }
    };
    assertThat(
        requireNonNull(
            selfJoin.computeSelfCost(selfJoin.getCluster().getPlanner(), mq))
            .getRows(),
        is(2D * Util.nLogN(20D) + 23D));
  }

  @Test void ieJoinWithBooleanAndDecimalKeys() {
    tester(new HrSchema())
        .query("select l.id as left_id, r.id as right_id "
            + "from (values (1, true, cast(2 as decimal(5, 2))), "
            + "(2, false, cast(4 as decimal(5, 2)))) as l(id, b, d) "
            + "join (values (3, false, cast(3 as decimal(5, 2))), "
            + "(4, true, cast(1 as decimal(5, 2)))) as r(id, b, d) "
            + "on l.b > r.b and l.d < r.d")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner ->
            planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE))
        .explainHookMatches(containsString("EnumerableIEJoin"))
        .returnsUnordered("left_id=1; right_id=3");
  }

  @Test void ieJoinWithBinaryAndNullableDateKeys() {
    tester(new HrSchema())
        .query("select l.id as left_id, r.id as right_id "
            + "from (values (1, cast(X'01' as varbinary(2)), date '2020-01-02'), "
            + "(2, cast(X'01' as varbinary(2)), cast(null as date))) as l(id, b, d) "
            + "join (values (3, cast(X'02' as varbinary(2)), date '2020-01-01'), "
            + "(4, cast(X'00' as varbinary(2)), date '2020-01-03')) "
            + "as r(id, b, d) on l.b < r.b and l.d > r.d")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner ->
            planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE))
        .explainHookMatches(containsString("EnumerableIEJoin"))
        .returnsUnordered("left_id=1; right_id=3");
  }

  @Test void ieJoinWithIntervalKeys() {
    tester(new HrSchema())
        .query("select l.id as left_id, r.id as right_id "
            + "from (values (1, interval '2' day, interval '4' hour), "
            + "(2, interval '4' day, interval '1' hour)) "
            + "as l(id, d, h) "
            + "join (values (3, interval '3' day, interval '2' hour), "
            + "(4, interval '1' day, interval '5' hour)) "
            + "as r(id, d, h) on l.d < r.d and l.h > r.h")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner ->
            planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE))
        .explainHookMatches(containsString("EnumerableIEJoin"))
        .returnsUnordered("left_id=1; right_id=3");
  }

  @Test void ieJoinWithCollatedVarcharKeys() {
    tester(new HrSchema())
        .withRel(builder -> {
          final RelDataType stringType =
              builder.getTypeFactory().createTypeWithCharsetAndCollation(
                  builder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR),
                  builder.getTypeFactory().getDefaultCharset(), PRIMARY_COLLATION);
          final RelDataType rowType = builder.getTypeFactory().builder()
              .add("s", stringType)
              .add("n", SqlTypeName.INTEGER)
              .build();
          return builder
              .values(rowType, "abc", 2, "z", 1).as("l")
              .values(rowType, "ÀBC", 1, "b", 3).as("r")
              .join(JoinRelType.INNER,
                  builder.and(
                      builder.greaterThanOrEqual(
                          builder.field(2, "l", "s"),
                          builder.field(2, "r", "s")),
                      builder.greaterThan(
                          builder.field(2, "l", "n"),
                          builder.field(2, "r", "n"))))
              .project(builder.field("l", "s"), builder.field("r", "s"))
              .build();
        })
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner ->
            planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE))
        .explainHookMatches(containsString("EnumerableIEJoin"))
        .returnsUnordered("s=abc; s0=ÀBC");
  }

  @Test void ieJoinWithApproximateRemainingPredicate() {
    tester(new TestSchema())
        .query("select l.name as left_name, r.name as right_name "
            + "from residualLefts l join residualRights r "
            + "on l.x < r.x and l.y > r.y and l.z >= r.z")
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner ->
            planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE))
        .explainHookMatches(
            allOf(
                containsString("EnumerableIEJoin"),
                containsString(">=($t3, $t7)")))
        .returnsUnordered(
            "left_name=minusZero; right_name=zero",
            "left_name=plusZero; right_name=zero");
  }

  @Test void ieJoinUnsupportedConditionsUseExistingRules() {
    for (String condition : ImmutableList.of(
        "e.empid < d.deptno",
        "e.empid < d.deptno and e.deptno > d.deptno "
            + "and e.empid = d.deptno",
        "e.empid + 1 < d.deptno and e.deptno > d.deptno",
        "e.empid < d.deptno or e.deptno > d.deptno",
        "e.empid < e.deptno and e.deptno > d.deptno")) {
      tester(new HrSchema())
          .query("select * from emps e join depts d on " + condition)
          .explainHookMatches(not(containsString("EnumerableIEJoin")))
          .runs();
    }
    tester(new HrSchema())
        .query("select * from emps e left join depts d "
            + "on e.empid < d.deptno and e.deptno > d.deptno")
        .explainHookMatches(not(containsString("EnumerableIEJoin")))
        .runs();
  }

  private CalciteAssert.AssertThat tester(Object schema) {
    return CalciteAssert.that()
        .with(CalciteConnectionProperty.LEX, Lex.JAVA)
        .with(CalciteConnectionProperty.FORCE_DECORRELATE, false)
        .withSchema("s", new ReflectiveSchema(schema));
  }

  private static Join findJoin(RelNode rel) {
    final Holder<@Nullable Join> join = Holder.empty();
    new RelVisitor() {
      @Override public void visit(RelNode node, int ordinal,
          @Nullable RelNode parent) {
        if (node instanceof Join) {
          join.set((Join) node);
        } else {
          super.visit(node, ordinal, parent);
        }
      }
    }.go(rel);
    return requireNonNull(join.get());
  }

  /** Test schema for unsupported and residual key types. */
  public static class TestSchema {
    public final ApproximatePoint[] lefts = {
        new ApproximatePoint("nan", Double.NaN, 1),
        new ApproximatePoint("minusZero", -0D, 1),
        new ApproximatePoint("plusZero", 0D, 1)
    };
    public final ApproximatePoint[] rights = {
        new ApproximatePoint("zero", 0D, 0)
    };
    public final AnyPoint[] anyLefts = {
        new AnyPoint("one", 1, 2)
    };
    public final AnyPoint[] anyRights = {
        new AnyPoint("two", 2, 1)
    };
    public final ResidualPoint[] residualLefts = {
        new ResidualPoint("nan", 2, 8, Double.NaN),
        new ResidualPoint("minusZero", 2, 8, -0D),
        new ResidualPoint("plusZero", 2, 8, 0D)
    };
    public final ResidualPoint[] residualRights = {
        new ResidualPoint("zero", 4, 3, 0D)
    };
  }

  /** Row with an approximate key. */
  public static class ApproximatePoint {
    public final String name;
    public final double x;
    public final int y;

    ApproximatePoint(String name, double x, int y) {
      this.name = name;
      this.x = x;
      this.y = y;
    }
  }

  /** Row with dynamically typed keys. */
  public static class AnyPoint {
    public final String name;
    public final Object x;
    public final Object y;

    AnyPoint(String name, Object x, Object y) {
      this.name = name;
      this.x = x;
      this.y = y;
    }
  }

  /** Row with an approximate residual key. */
  public static class ResidualPoint {
    public final String name;
    public final int x;
    public final int y;
    public final double z;

    ResidualPoint(String name, int x, int y, double z) {
      this.name = name;
      this.x = x;
      this.y = y;
      this.z = z;
    }
  }
}
