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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.LogicVisitor;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlQuantifyOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.calcite.util.Util.last;

import static java.util.Objects.requireNonNull;

/**
 * Transform that converts IN, EXISTS and scalar sub-queries into joins.
 *
 * <p>Sub-queries are represented by {@link RexSubQuery} expressions.
 *
 * <p>A sub-query may or may not be correlated. If a sub-query is correlated,
 * the wrapped {@link RelNode} will contain a {@link RexCorrelVariable} before
 * the rewrite, and the product of the rewrite will be a {@link Correlate}.
 * The Correlate can be removed using {@link RelDecorrelator}.
 *
 * @see CoreRules#FILTER_SUB_QUERY_TO_CORRELATE
 * @see CoreRules#PROJECT_SUB_QUERY_TO_CORRELATE
 * @see CoreRules#JOIN_SUB_QUERY_TO_CORRELATE
 */
@Value.Enclosing
public class SubQueryRemoveRule
    extends RelRule<SubQueryRemoveRule.Config>
    implements TransformationRule {

  /** Creates a SubQueryRemoveRule. */
  protected SubQueryRemoveRule(Config config) {
    super(config);
    requireNonNull(config.matchHandler());
  }

  @Override public void onMatch(RelOptRuleCall call) {
    config.matchHandler().accept(this, call);
  }

  protected RexNode apply(RexSubQuery e, Set<CorrelationId> variablesSet,
      RelOptUtil.Logic logic,
      RelBuilder builder, int inputCount, int offset, int subQueryIndex) {
    switch (e.getKind()) {
    case SCALAR_QUERY:
      return rewriteScalarQuery(e, variablesSet, builder, inputCount, offset);
    case ARRAY_QUERY_CONSTRUCTOR:
    case MAP_QUERY_CONSTRUCTOR:
    case MULTISET_QUERY_CONSTRUCTOR:
      return rewriteCollection(e, variablesSet, builder,
          inputCount, offset);
    case SOME:
      return rewriteSome(e, variablesSet, builder, subQueryIndex);
    case IN:
      return rewriteIn(e, variablesSet, logic, builder, offset, subQueryIndex);
    case EXISTS:
      return rewriteExists(e, variablesSet, logic, builder);
    case UNIQUE:
      return rewriteUnique(e, builder);
    default:
      throw new AssertionError(e.getKind());
    }
  }

  /**
   * Rewrites a scalar sub-query into an
   * {@link org.apache.calcite.rel.core.Aggregate}.
   *
   * @param e            Scalar sub-query to rewrite
   * @param variablesSet A set of variables used by a relational
   *                     expression of the specified RexSubQuery
   * @param builder      Builder
   * @param offset       Offset to shift {@link RexInputRef}
   *
   * @return Expression that may be used to replace the RexSubQuery
   */
  private static RexNode rewriteScalarQuery(RexSubQuery e, Set<CorrelationId> variablesSet,
      RelBuilder builder, int inputCount, int offset) {
    builder.push(e.rel);
    final RelMetadataQuery mq = e.rel.getCluster().getMetadataQuery();
    final Boolean unique =
        mq.areColumnsUnique(builder.peek(), ImmutableBitSet.of());
    if (unique == null || !unique) {
      builder.aggregate(builder.groupKey(),
          builder.aggregateCall(SqlStdOperatorTable.SINGLE_VALUE,
              builder.field(0)));
    }
    builder.join(JoinRelType.LEFT, builder.literal(true), variablesSet);
    return field(builder, inputCount, offset);
  }

  /**
   * Rewrites a sub-query into a
   * {@link org.apache.calcite.rel.core.Collect}.
   *
   * @param e            Sub-query to rewrite
   * @param variablesSet A set of variables used by a relational
   *                     expression of the specified RexSubQuery
   * @param builder      Builder
   * @param offset       Offset to shift {@link RexInputRef}
   * @return Expression that may be used to replace the RexSubQuery
   */
  private static RexNode rewriteCollection(RexSubQuery e,
      Set<CorrelationId> variablesSet, RelBuilder builder,
      int inputCount, int offset) {
    builder.push(e.rel);
    builder.push(
        Collect.create(builder.build(), e.getKind(), "x"));
    builder.join(JoinRelType.INNER, builder.literal(true), variablesSet);
    return field(builder, inputCount, offset);
  }

  /**
   * Rewrites a SOME sub-query into a {@link Join}.
   *
   * @param e               SOME sub-query to rewrite
   * @param builder         Builder
   * @param subQueryIndex   sub-query index in multiple sub-queries
   *
   * @return Expression that may be used to replace the RexSubQuery
   */
  private static RexNode rewriteSome(RexSubQuery e, Set<CorrelationId> variablesSet,
      RelBuilder builder, int subQueryIndex) {
    // If the sub-query is guaranteed empty, just return
    // FALSE.
    final RelMetadataQuery mq = e.rel.getCluster().getMetadataQuery();
    if (RelMdUtil.isRelDefinitelyEmpty(mq, e.rel)) {
      return builder.getRexBuilder().makeLiteral(Boolean.FALSE, e.getType(), true);
    }
    // Most general case, where the left and right keys might have nulls, and
    // caller requires 3-valued logic return.
    //
    // select e.deptno, e.deptno < some (select deptno from emp) as v
    // from emp as e
    //
    // becomes
    //
    // select e.deptno,
    //   case
    //   when q.c = 0 then false // sub-query is empty
    //   when (e.deptno < q.m) is true then true
    //   when q.c > q.d then unknown // sub-query has at least one null
    //   else e.deptno < q.m
    //   end as v
    // from emp as e
    // cross join (
    //   select max(deptno) as m, count(*) as c, count(deptno) as d
    //   from emp) as q
    //
    final SqlQuantifyOperator op = (SqlQuantifyOperator) e.op;
    switch (op.comparisonKind) {
    case GREATER_THAN_OR_EQUAL:
    case LESS_THAN_OR_EQUAL:
    case LESS_THAN:
    case GREATER_THAN:
    case NOT_EQUALS:
      break;

    default:
      // "SOME =" should have been rewritten into IN.
      throw new AssertionError("unexpected " + op);
    }

    final RexNode caseRexNode;
    final RexNode literalFalse = builder.literal(false);
    final RexNode literalTrue = builder.literal(true);
    final RexLiteral literalUnknown =
        builder.getRexBuilder().makeNullLiteral(literalFalse.getType());

    final SqlAggFunction minMax = op.comparisonKind == SqlKind.GREATER_THAN
        || op.comparisonKind == SqlKind.GREATER_THAN_OR_EQUAL
        ? SqlStdOperatorTable.MIN
        : SqlStdOperatorTable.MAX;

    String qAlias = "q";
    if (subQueryIndex != 0) {
      qAlias = "q" + subQueryIndex;
    }

    if (variablesSet.isEmpty()) {
      switch (op.comparisonKind) {
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN_OR_EQUAL:
      case LESS_THAN:
      case GREATER_THAN:
        // for non-correlated case queries such as
        // select e.deptno, e.deptno < some (select deptno from emp) as v
        // from emp as e
        //
        // becomes
        //
        // select e.deptno,
        //   case
        //   when q.c = 0 then false // sub-query is empty
        //   when (e.deptno < q.m) is true then true
        //   when q.c > q.d then unknown // sub-query has at least one null
        //   else e.deptno < q.m
        //   end as v
        // from emp as e
        // cross join (
        //   select max(deptno) as m, count(*) as c, count(deptno) as d
        //   from emp) as q
        builder.push(e.rel)
            .aggregate(builder.groupKey(),
                builder.aggregateCall(minMax, builder.field(0)).as("m"),
                builder.count(false, "c"),
                builder.count(false, "d", builder.field(0)))
            .as(qAlias)
            .join(JoinRelType.INNER);
        caseRexNode =
            builder.call(SqlStdOperatorTable.CASE,
                builder.equals(builder.field(qAlias, "c"), builder.literal(0)),
                literalFalse,
                builder.call(SqlStdOperatorTable.IS_TRUE,
                    builder.call(RexUtil.op(op.comparisonKind),
                        e.operands.get(0), builder.field(qAlias, "m"))),
                literalTrue,
                builder.greaterThan(builder.field(qAlias, "c"),
                    builder.field(qAlias, "d")),
                literalUnknown,
                builder.call(RexUtil.op(op.comparisonKind),
                    e.operands.get(0), builder.field(qAlias, "m")));
        break;

      case NOT_EQUALS:
        // for non-correlated case queries such as
        // select e.deptno, e.deptno <> some (select deptno from emp) as v
        // from emp as e
        //
        // becomes
        //
        // select e.deptno,
        //   case
        //   when q.c = 0 then false // sub-query is empty
        //   when e.deptno is null then unknown
        //   when q.c <> q.d && q.d <= 1 then e.deptno != m || unknown
        //   when q.d = 1
        //     then e.deptno != m // sub-query has the distinct result
        //   else true
        //   end as v
        // from emp as e
        // cross join (
        //   select count(*) as c, count(deptno) as d, max(deptno) as m
        //   from (select distinct deptno from emp)) as q
        builder.push(e.rel);
        builder.distinct()
            .aggregate(builder.groupKey(),
                builder.count(false, "c"),
                builder.count(false, "d", builder.field(0)),
                builder.max(builder.field(0)).as("m"))
            .as(qAlias)
            .join(JoinRelType.INNER);
        caseRexNode =
            builder.call(SqlStdOperatorTable.CASE,
                builder.equals(builder.field("c"), builder.literal(0)),
                literalFalse,
                builder.isNull(e.getOperands().get(0)),
                literalUnknown,
                builder.and(
                    builder.notEquals(builder.field("d"), builder.field("c")),
                    builder.lessThanOrEqual(builder.field("d"),
                        builder.literal(1))),
                builder.or(
                    builder.notEquals(e.operands.get(0), builder.field(qAlias, "m")),
                    literalUnknown),
                builder.equals(builder.field("d"), builder.literal(1)),
                builder.notEquals(e.operands.get(0), builder.field(qAlias, "m")),
                literalTrue);
        break;

      default:
        throw new AssertionError("not possible - per above check");
      }
    } else {
      final String indicator = "trueLiteral";
      switch (op.comparisonKind) {
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN_OR_EQUAL:
      case LESS_THAN:
      case GREATER_THAN:
        // for correlated case queries such as
        //
        // select e.deptno, e.deptno < some (
        //   select deptno from emp where emp.name = e.name) as v
        // from emp as e
        //
        // becomes
        //
        // select e.deptno,
        //   case
        //   when indicator is null then false // sub-query is empty for corresponding corr value
        //   when q.c = 0 then false // sub-query is empty
        //   when (e.deptno < q.m) is true then true
        //   when q.c > q.d then unknown // sub-query has at least one null
        //   else e.deptno < q.m
        //   end as v
        // from emp as e
        // left outer join (
        //   select name, max(deptno) as m, count(*) as c, count(deptno) as d,
        //       LITERAL_AGG(true) as indicator
        //   from emp group by name) as q on e.name = q.name
        builder.push(e.rel)
            .aggregate(builder.groupKey(),
                builder.aggregateCall(minMax, builder.field(0)).as("m"),
                builder.count(false, "c"),
                builder.count(false, "d", builder.field(0)),
                builder.literalAgg(true, indicator));
        builder.as(qAlias);
        builder.join(JoinRelType.LEFT, literalTrue, variablesSet);
        caseRexNode =
            builder.call(SqlStdOperatorTable.CASE,
                builder.isNull(builder.field(qAlias, indicator)),
                literalFalse,
                builder.equals(builder.field(qAlias, "c"), builder.literal(0)),
                literalFalse,
                builder.call(SqlStdOperatorTable.IS_TRUE,
                    builder.call(RexUtil.op(op.comparisonKind),
                        e.operands.get(0), builder.field(qAlias, "m"))),
                literalTrue,
                builder.greaterThan(builder.field(qAlias, "c"),
                    builder.field(qAlias, "d")),
                literalUnknown,
                builder.call(RexUtil.op(op.comparisonKind),
                    e.operands.get(0), builder.field(qAlias, "m")));
        break;

      case NOT_EQUALS:
        // for correlated case queries such as
        //
        // select e.deptno, e.deptno <> some (
        //   select deptno from emp where emp.name = e.name) as v
        // from emp as e
        //
        // becomes
        //
        // select e.deptno,
        //   case
        //   when indicator is null
        //     then false // sub-query is empty for corresponding corr value
        //   when q.c = 0 then false // sub-query is empty
        //   when e.deptno is null then unknown
        //   when q.c <> q.d && q.dd <= 1
        //     then e.deptno != m || unknown
        //   when q.dd = 1
        //     then e.deptno != m // sub-query has the distinct result
        //   else true
        //   end as v
        // from emp as e
        // left outer join (
        //   select name, count(*) as c, count(deptno) as d, count(distinct deptno) as dd,
        //       max(deptno) as m, LITERAL_AGG(true) as indicator
        //   from emp group by name) as q on e.name = q.name

        // Additional details on the `q.c <> q.d && q.dd <= 1` clause:
        // the q.c <> q.d comparison identifies if there are any null values,
        // since count(*) counts null values and count(deptno) does not.
        // if there's no null value, c should be equal to d.
        // the q.dd <= 1 part means: true if there is at most one non-null value
        // so this clause means:
        // "if there are any null values and there is at most one non-null value".
        builder.push(e.rel)
            .aggregate(builder.groupKey(),
                builder.count(false, "c"),
                builder.count(false, "d", builder.field(0)),
                builder.count(true, "dd", builder.field(0)),
                builder.max(builder.field(0)).as("m"),
                builder.literalAgg(true, indicator));
        builder.as(qAlias);
        builder.join(JoinRelType.LEFT, literalTrue, variablesSet);
        caseRexNode =
            builder.call(SqlStdOperatorTable.CASE,
                builder.isNull(builder.field(qAlias, indicator)),
                literalFalse,
                builder.equals(builder.field("c"), builder.literal(0)),
                literalFalse,
                builder.isNull(e.getOperands().get(0)),
                literalUnknown,
                builder.and(
                    builder.notEquals(builder.field("d"), builder.field("c")),
                    builder.lessThanOrEqual(builder.field("dd"),
                        builder.literal(1))),
                builder.or(
                    builder.notEquals(e.operands.get(0), builder.field(qAlias, "m")),
                    literalUnknown),
                builder.equals(builder.field("dd"), builder.literal(1)),
                builder.notEquals(e.operands.get(0), builder.field(qAlias, "m")),
                literalTrue);
        break;

      default:
        throw new AssertionError("not possible - per above check");
      }
    }

    // CASE statement above is created with nullable boolean type, but it might
    // not be correct.  If the original sub-query node's type is not nullable it
    // is guaranteed for case statement to not produce NULLs. Therefore to avoid
    // planner complaining we need to add cast.  Note that nullable type is
    // created due to the MIN aggregate call, since there is no GROUP BY.
    if (!e.getType().isNullable()) {
      return builder.cast(caseRexNode, e.getType().getSqlTypeName());
    }
    return caseRexNode;
  }

  /**
   * Rewrites an EXISTS RexSubQuery into a {@link Join}.
   *
   * @param e            EXISTS sub-query to rewrite
   * @param variablesSet A set of variables used by a relational
   *                     expression of the specified RexSubQuery
   * @param logic        Logic for evaluating
   * @param builder      Builder
   *
   * @return Expression that may be used to replace the RexSubQuery
   */
  private static RexNode rewriteExists(RexSubQuery e, Set<CorrelationId> variablesSet,
      RelOptUtil.Logic logic, RelBuilder builder) {
    // If the sub-query is guaranteed never empty, just return
    // TRUE.
    final RelMetadataQuery mq = e.rel.getCluster().getMetadataQuery();
    if (RelMdUtil.isRelDefinitelyNotEmpty(mq, e.rel)) {
      return builder.literal(true);
    }
    if (RelMdUtil.isRelDefinitelyEmpty(mq, e.rel)) {
      return builder.literal(false);
    }
    builder.push(e.rel);
    builder.project(builder.alias(builder.literal(true), "i"));
    switch (logic) {
    case TRUE:
      // Handles queries with single EXISTS in filter condition:
      // select e.deptno from emp as e
      // where exists (select deptno from emp)
      builder.aggregate(builder.groupKey(0));
      builder.as("dt");
      builder.join(JoinRelType.INNER, builder.literal(true), variablesSet);
      return builder.literal(true);
    default:
      builder.distinct();
    }

    builder.as("dt");

    builder.join(JoinRelType.LEFT, builder.literal(true), variablesSet);

    return builder.isNotNull(last(builder.fields()));
  }

  /**
   * Rewrites a UNIQUE RexSubQuery into an EXISTS RexSubQuery.
   *
   * <p>For example, rewrites the UNIQUE sub-query:
   *
   * <pre>{@code
   * UNIQUE (SELECT PUBLISHED_IN
   * FROM BOOK
   * WHERE AUTHOR_ID = 3)
   * }</pre>
   *
   * <p>to the following EXISTS sub-query:
   *
   * <pre>{@code
   * NOT EXISTS (
   *   SELECT * FROM (
   *     SELECT PUBLISHED_IN
   *     FROM BOOK
   *     WHERE AUTHOR_ID = 3
   *   ) T
   *   WHERE (T.PUBLISHED_IN) IS NOT NULL
   *   GROUP BY T.PUBLISHED_IN
   *   HAVING COUNT(*) > 1
   * )
   * }</pre>
   *
   * @param e            UNIQUE sub-query to rewrite
   * @param builder      Builder
   *
   * @return Expression that may be used to replace the RexSubQuery
   */
  private static RexNode rewriteUnique(RexSubQuery e, RelBuilder builder) {
    // if sub-query always return unique value.
    final RelMetadataQuery mq = e.rel.getCluster().getMetadataQuery();
    Boolean isUnique = mq.areRowsUnique(e.rel, true);
    if (isUnique != null && isUnique) {
      return builder.getRexBuilder().makeLiteral(true);
    }
    builder.push(e.rel);
    List<RexNode> notNullCondition =
        builder.fields().stream()
            .map(builder::isNotNull)
            .collect(Collectors.toList());
    builder
        .filter(notNullCondition)
        .aggregate(builder.groupKey(builder.fields()), builder.countStar("c"))
        .filter(
            builder.greaterThan(last(builder.fields()), builder.literal(1)));
    RelNode relNode = builder.build();
    return builder.call(SqlStdOperatorTable.NOT, RexSubQuery.exists(relNode));
  }

  /**
   * Rewrites an IN RexSubQuery into a {@link Join}.
   *
   * @param e               IN sub-query to rewrite
   * @param variablesSet    A set of variables used by a relational
   *                        expression of the specified RexSubQuery
   * @param logic           Logic for evaluating
   * @param builder         Builder
   * @param offset          Offset to shift {@link RexInputRef}
   * @param subQueryIndex   sub-query index in multiple sub-queries
   *
   * @return Expression that may be used to replace the RexSubQuery
   */
  private static RexNode rewriteIn(RexSubQuery e, Set<CorrelationId> variablesSet,
      RelOptUtil.Logic logic, RelBuilder builder, int offset, int subQueryIndex) {
    // If the sub-query is guaranteed empty, just return
    // FALSE.
    final RelMetadataQuery mq = e.rel.getCluster().getMetadataQuery();
    if (RelMdUtil.isRelDefinitelyEmpty(mq, e.rel)) {
      return builder.getRexBuilder().makeLiteral(Boolean.FALSE, e.getType(), true);
    }
    // Most general case, where the left and right keys might have nulls, and
    // caller requires 3-valued logic return.
    //
    // select e.deptno, e.deptno in (select deptno from emp)
    // from emp as e
    //
    // becomes
    //
    // select e.deptno,
    //   case
    //   when ct.c = 0 then false              -- (1) empty subquery check
    //   when e.deptno is null then null       -- (2) key NULL check
    //   when dt.i is not null then true       -- (3) match found
    //   when ct.ck < ct.c then null           -- (4) NULLs exist in subquery
    //   else false                             -- (5) no match
    //   end
    // from emp as e
    // left join (
    //   (select count(*) as c, count(deptno) as ck from emp) as ct
    //   cross join (select distinct deptno, true as i from emp)) as dt
    //   on e.deptno = dt.deptno
    //
    // If both keys (e.deptno) and subquery columns (deptno) are NOT NULL,
    // we can drop checks (1), (2), and (4), which eliminates the need for ct:
    //
    // select e.deptno,
    //   case
    //   when dt.i is not null then true       -- (3) match found
    //   else false                             -- (5) no match
    //   end
    // from emp as e
    // left join (select distinct deptno, true as i from emp) as dt
    //   on e.deptno = dt.deptno
    //
    // Check (1) is not needed: if the subquery is empty, all dt.i are NULL,
    // and the LEFT JOIN pattern correctly returns FALSE for IN (TRUE for NOT IN).
    //
    // NULL-safety checks are required if either the keys or the subquery
    // columns are nullable, due to SQL three-valued logic.
    //
    // If the logic is TRUE (as opposed to TRUE_FALSE_UNKNOWN), we only care about
    // matches, so the query simplifies to an inner join regardless of nullability:
    //
    // select e.deptno,
    //   true
    // from emp as e
    // inner join (select distinct deptno from emp) as dt
    //   on e.deptno = dt.deptno

    builder.push(e.rel);
    final List<RexNode> fields = new ArrayList<>(builder.fields());

    // for the case when IN has only literal operands, it may be handled
    // in the simpler way:
    //
    // select e.deptno, 123456 in (select deptno from emp)
    // from emp as e
    //
    // becomes
    //
    // select e.deptno,
    //   case
    //   when dt.c IS NULL THEN FALSE
    //   when e.deptno IS NULL THEN NULL
    //   when dt.cs IS FALSE THEN NULL
    //   when dt.cs IS NOT NULL THEN TRUE
    //   else false
    //   end
    // from emp AS e
    // cross join (
    //   select distinct deptno is not null as cs, count(*) as c
    //   from emp
    //   where deptno = 123456 or deptno is null or e.deptno is null
    //   order by cs desc limit 1) as dt
    //

    String ctAlias = "ct";
    if (subQueryIndex != 0) {
      ctAlias = "ct" + subQueryIndex;
    }

    boolean allLiterals = RexUtil.allLiterals(e.getOperands());
    final List<RexNode> expressionOperands = new ArrayList<>(e.getOperands());

    final List<RexNode> keyIsNulls = e.getOperands().stream()
        .filter(operand -> operand.getType().isNullable())
        .map(builder::isNull)
        .collect(Collectors.toList());

    final RexLiteral trueLiteral = builder.literal(true);
    final RexLiteral falseLiteral = builder.literal(false);
    final RexLiteral unknownLiteral =
        builder.getRexBuilder().makeNullLiteral(trueLiteral.getType());
    boolean needsNullSafety = false;
    if (allLiterals) {
      final List<RexNode> conditions =
          Pair.zip(expressionOperands, fields).stream()
              .map(pair -> builder.equals(pair.left, pair.right))
              .collect(Collectors.toList());
      switch (logic) {
      case TRUE:
      case TRUE_FALSE:
        builder.filter(conditions);
        builder.project(builder.alias(trueLiteral, "cs"));
        builder.distinct();
        break;
      default:
        List<RexNode> isNullOperands = fields.stream()
            .map(builder::isNull)
            .collect(Collectors.toList());
        // uses keyIsNulls conditions in the filter to avoid empty results
        isNullOperands.addAll(keyIsNulls);
        builder.filter(
            builder.or(
                builder.and(conditions),
                builder.or(isNullOperands)));
        RexNode project =
            builder.and(
                fields.stream()
                    .map(builder::isNotNull)
                    .collect(Collectors.toList()));
        builder.project(builder.alias(project, "cs"));

        if (variablesSet.isEmpty()) {
          builder.aggregate(builder.groupKey(builder.field("cs")),
              builder.count(false, "c"));
        } else {
          builder.distinct();
        }
        // sorts input with desc order since we are interested
        // only in the case when one of the values is true.
        // When true value is absent then we are interested
        // only in false value.
        builder.sortLimit(0, 1,
            ImmutableList.of(builder.desc(builder.field("cs"))));
      }
      // clears expressionOperands and fields lists since
      // all expressions were used in the filter
      expressionOperands.clear();
      fields.clear();
    } else {
      boolean anyFieldNullable = fields.stream()
          .anyMatch(field -> field.getType().isNullable());

      // we can skip NULL-safety checks only if both keys
      // and subquery columns are NOT NULL
      needsNullSafety =
          (logic == RelOptUtil.Logic.TRUE_FALSE_UNKNOWN
           || logic == RelOptUtil.Logic.UNKNOWN_AS_TRUE)
          && (!keyIsNulls.isEmpty() || anyFieldNullable);

      switch (logic) {
      case TRUE:
        builder.aggregate(builder.groupKey(fields));
        break;
      case TRUE_FALSE_UNKNOWN:
      case UNKNOWN_AS_TRUE:
        if (needsNullSafety) {
          // Builds the cross join
          // Some databases don't support use FILTER clauses for aggregate functions
          // like {@code COUNT(*) FILTER (WHERE not(a is null))}
          // So use count(*) when only one column
          if (builder.fields().size() <= 1) {
            builder.aggregate(builder.groupKey(),
                builder.count(false, "c"),
                builder.count(builder.fields()).as("ck"));
          } else {
            builder.aggregate(builder.groupKey(),
                builder.count(false, "c"),
                builder.count()
                    .filter(builder
                        .not(builder
                            .and(builder.fields().stream()
                                .map(builder::isNull)
                                .collect(Collectors.toList()))))
                    .as("ck"));
          }
          builder.as(ctAlias);
          if (!variablesSet.isEmpty()) {
            builder.join(JoinRelType.LEFT, trueLiteral, variablesSet);
          } else {
            builder.join(JoinRelType.INNER, trueLiteral, variablesSet);
          }
          offset += 2;
          builder.push(e.rel);
        }
        // fall through
      default:
        builder.aggregate(builder.groupKey(fields),
            builder.literalAgg(true).as("i"));
      }
    }

    String dtAlias = "dt";
    if (subQueryIndex != 0) {
      dtAlias = "dt" + subQueryIndex;
    }
    builder.as(dtAlias);
    int refOffset = offset;
    final List<RexNode> conditions =
        Pair.zip(expressionOperands, builder.fields()).stream()
            .map(pair -> builder.equals(pair.left, RexUtil.shift(pair.right, refOffset)))
            .collect(Collectors.toList());
    switch (logic) {
    case TRUE:
      builder.join(JoinRelType.INNER, builder.and(conditions), variablesSet);
      return trueLiteral;
    default:
      break;
    }
    // Now the left join
    builder.join(JoinRelType.LEFT, builder.and(conditions), variablesSet);

    final ImmutableList.Builder<RexNode> operands = ImmutableList.builder();
    RexLiteral b = trueLiteral;
    switch (logic) {
    case TRUE_FALSE_UNKNOWN:
      b = unknownLiteral;
      // fall through
    case UNKNOWN_AS_TRUE:
      if (allLiterals) {
        // Considers case when right side of IN is empty
        // for the case of non-correlated sub-queries
        if (variablesSet.isEmpty()) {
          operands.add(
              builder.isNull(builder.field(dtAlias, "c")),
              falseLiteral);
        }
        operands.add(
            builder.equals(builder.field(dtAlias, "cs"), falseLiteral),
            b);
      } else {
        // only reference ctAlias if we created it
        if (needsNullSafety) {
          operands.add(
              builder.equals(builder.field(ctAlias, "c"), builder.literal(0)),
              falseLiteral);
        }
      }
      break;
    default:
      break;
    }

    if (!keyIsNulls.isEmpty()) {
      operands.add(builder.or(keyIsNulls), unknownLiteral);
    }

    if (allLiterals) {
      operands.add(builder.isNotNull(builder.field(dtAlias, "cs")),
          trueLiteral);
    } else {
      operands.add(builder.isNotNull(last(builder.fields())),
          trueLiteral);
    }

    if (!allLiterals) {
      switch (logic) {
      case TRUE_FALSE_UNKNOWN:
      case UNKNOWN_AS_TRUE:
        // only reference ctAlias if we created it
        if (needsNullSafety) {
          operands.add(
              builder.lessThan(builder.field(ctAlias, "ck"),
                  builder.field(ctAlias, "c")),
              b);
        }
        break;
      default:
        break;
      }
    }
    operands.add(falseLiteral);
    RexNode result = builder.call(SqlStdOperatorTable.CASE, operands.build());

    // When we skip NULL-safety checks, the result might be NOT NULL
    // but the original IN expression was nullable, so we need to preserve that
    if (e.getType().isNullable() && !result.getType().isNullable()) {
      result = builder.getRexBuilder().makeCast(e.getType(), result, false, false);
    }

    return result;
  }

  /** Returns a reference to a particular field, by offset, across several
   * inputs on a {@link RelBuilder}'s stack. */
  private static RexInputRef field(RelBuilder builder, int inputCount, int offset) {
    for (int inputOrdinal = 0;;) {
      final RelNode r = builder.peek(inputCount, inputOrdinal);
      if (offset < r.getRowType().getFieldCount()) {
        return builder.field(inputCount, inputOrdinal, offset);
      }
      ++inputOrdinal;
      offset -= r.getRowType().getFieldCount();
    }
  }

  /** Returns a list of expressions that project the first {@code fieldCount}
   * fields of the top input on a {@link RelBuilder}'s stack. */
  private static List<RexNode> fields(RelBuilder builder, int fieldCount) {
    final List<RexNode> projects = new ArrayList<>();
    for (int i = 0; i < fieldCount; i++) {
      projects.add(builder.field(i));
    }
    return projects;
  }

  private static void matchProject(SubQueryRemoveRule rule,
      RelOptRuleCall call) {
    final Project project = call.rel(0);
    final RelBuilder builder = call.builder();
    final RexSubQuery e =
        requireNonNull(RexUtil.SubQueryFinder.find(project.getProjects()));
    final RelOptUtil.Logic logic =
        LogicVisitor.find(RelOptUtil.Logic.TRUE_FALSE_UNKNOWN,
            project.getProjects(), e);
    builder.push(project.getInput());
    final int fieldCount = builder.peek().getRowType().getFieldCount();
    final Set<CorrelationId>  variablesSet =
        RelOptUtil.getVariablesUsed(e.rel);
    final RexNode target =
        rule.apply(e, variablesSet, logic, builder, 1, fieldCount, 0);
    final RexShuttle shuttle = new ReplaceSubQueryShuttle(e, target);
    builder.project(shuttle.apply(project.getProjects()),
        project.getRowType().getFieldNames());
    call.transformTo(builder.build());
  }

  private static void matchFilter(SubQueryRemoveRule rule,
      RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final Set<CorrelationId> filterVariablesSet = filter.getVariablesSet();
    final RelBuilder builder = call.builder();
    builder.push(filter.getInput());
    int count = 0;
    RexNode c = filter.getCondition();
    while (true) {
      final RexSubQuery e = RexUtil.SubQueryFinder.find(c);
      if (e == null) {
        assert count > 0;
        break;
      }
      ++count;
      final RelOptUtil.Logic logic =
          LogicVisitor.find(RelOptUtil.Logic.TRUE, ImmutableList.of(c), e);
      final Set<CorrelationId>  variablesSet =
          RelOptUtil.getVariablesUsed(e.rel);
      // Only consider the correlated variables which originated from this sub-query level.
      variablesSet.retainAll(filterVariablesSet);
      final RexNode target =
          rule.apply(e, variablesSet, logic,
              builder, 1, builder.peek().getRowType().getFieldCount(), count);
      final RexShuttle shuttle = new ReplaceSubQueryShuttle(e, target);
      c = c.accept(shuttle);
    }
    builder.filter(c);
    builder.project(fields(builder, filter.getRowType().getFieldCount()));
    call.transformTo(builder.build());
  }

  private static void matchJoin(SubQueryRemoveRule rule, RelOptRuleCall call) {
    final Join join = call.rel(0);
    final RelBuilder builder = call.builder();
    final RexSubQuery e =
        requireNonNull(RexUtil.SubQueryFinder.find(join.getCondition()));

    ImmutableBitSet inputSet = RelOptUtil.InputFinder.bits(e.getOperands(), null);
    int nFieldsLeft = join.getLeft().getRowType().getFieldCount();
    int nFieldsRight = join.getRight().getRowType().getFieldCount();

    // Correlation columns should also be considered.
    // For example:
    //                                   LogicalJoin
    //              left                                          right
    //                |                                             |
    // LogicalProject.NONE.[0, 1]                            LogicalValues.NONE.[0]
    // RecordType(INTEGER DEPTNO, CHAR(11) DNAME)            RecordType(INTEGER DEPTNO)
    //
    // and subquery: $SCALAR_QUERY with correlate
    // LogicalProject(DEPTNO=[$1])
    //   LogicalFilter(condition=[=(CAST($0):CHAR(11) NOT NULL, $cor0.DNAME)])
    //
    // In such a case $cor0.DNAME need to be accounted as input form left side.
    final Set<CorrelationId> variablesSet = RelOptUtil.getVariablesUsed(e.rel);
    for (CorrelationId id : variablesSet) {
      ImmutableBitSet requiredColumns = RelOptUtil.correlationColumns(id, e.rel);
      inputSet = ImmutableBitSet.union(ImmutableList.of(requiredColumns, inputSet));
    }

    boolean inputIntersectsLeftSide = inputSet.intersects(ImmutableBitSet.range(0, nFieldsLeft));
    boolean inputIntersectsRightSide =
        inputSet.intersects(ImmutableBitSet.range(nFieldsLeft, nFieldsLeft + nFieldsRight));
    if (inputIntersectsLeftSide && inputIntersectsRightSide) {
      rewriteSubQueryOnDomain(rule, call, e, join, nFieldsLeft, nFieldsRight,
          inputSet, builder, variablesSet);
      return;
    }

    if (inputIntersectsLeftSide) {
      builder.push(join.getLeft());

      final RelOptUtil.Logic logic =
          LogicVisitor.find(join.getJoinType().generatesNullsOnRight()
                  ? RelOptUtil.Logic.TRUE_FALSE_UNKNOWN : RelOptUtil.Logic.TRUE,
              ImmutableList.of(join.getCondition()), e);

      final RexNode target =
          rule.apply(e, variablesSet, logic, builder, 1, nFieldsLeft, 0);
      final RexShuttle shuttle = new ReplaceSubQueryShuttle(e, target);

      final RexNode newCond =
          shuttle.apply(
              RexUtil.shift(join.getCondition(), nFieldsLeft,
                  builder.fields().size() - nFieldsLeft));
      builder.push(join.getRight());
      builder.join(join.getJoinType(), newCond);

      final int nFields = builder.fields().size();
      ImmutableList<RexNode> fields =
          builder.fields(ImmutableBitSet.range(0, nFieldsLeft)
              .union(ImmutableBitSet.range(nFields - nFieldsRight, nFields)));
      builder.project(fields);
    } else {
      builder.push(join.getRight());

      final RelOptUtil.Logic logic =
          LogicVisitor.find(join.getJoinType().generatesNullsOnLeft()
                  ? RelOptUtil.Logic.TRUE_FALSE_UNKNOWN : RelOptUtil.Logic.TRUE,
              ImmutableList.of(join.getCondition()), e);

      RexSubQuery subQuery = e;

      if (!variablesSet.isEmpty()) {
        // Original correlates reference joint row type, but we are about to create
        // new join of original right side and correlated sub-query. Therefore we have
        // to adjust correlated variables in following way:
        //   1) new correlation variable must reference row type of right side only
        //   2) field index must be shifted on the size of the left side
        // Example:
        // SELECT e1.*
        // FROM emp e1
        // JOIN dept d
        //   ON e1.deptno = d.deptno
        //   AND d.deptno IN (
        //     SELECT e3.empno
        //     FROM emp e3
        //     WHERE d.deptno > e3.comm
        //   )
        // ORDER BY e1.empno, e1.deptno;
        //
        // LogicalJoin(condition=[AND(=($7, $8), IN(CAST($8):SMALLINT NOT NULL, {
        // LogicalProject(EMPNO=[$0])
        //   LogicalFilter(condition=[>(CAST($cor0.DEPTNO0):DECIMAL(7, 2) NOT NULL, $6)])
        //     LogicalTableScan(table=[[scott, EMP]])
        // }))], joinType=[inner])
        //   LogicalTableScan(table=[[scott, EMP]])
        //   LogicalProject(DEPTNO=[$0])
        //     LogicalTableScan(table=[[scott, DEPT]])
        //
        // Rewrite to:
        //
        // LogicalProject(EMPNO=[$0], ENAME=[$1], ..., COMM=[$6], DEPTNO=[$7], DEPTNO0=[$8])
        //   LogicalJoin(condition=[=($7, $8)], joinType=[inner])
        //     LogicalTableScan(table=[[scott, EMP]])
        //     LogicalFilter(condition=[=(CAST($0):SMALLINT NOT NULL, $1)])
        //       LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])
        //         LogicalProject(DEPTNO=[$0])
        //           LogicalTableScan(table=[[scott, DEPT]])
        //         LogicalProject(EMPNO=[$0])
        //           LogicalFilter(condition=[>(CAST($cor0.DEPTNO):DECIMAL(7, 2) NOT NULL, $6)])
        //             LogicalTableScan(table=[[scott, EMP]])
        CorrelationId id = Iterables.getOnlyElement(variablesSet);
        RexBuilder rexBuilder = builder.getRexBuilder();

        RelNode newSubQueryRel = e.rel.accept(new RelHomogeneousShuttle() {
          @Override public RelNode visit(RelNode other) {
            RelNode node =
                RexUtil.shiftFieldAccess(rexBuilder, other, id, join.getRight(), -nFieldsLeft);
            return super.visit(node);
          }
        });
        subQuery = e.clone(newSubQueryRel);
      }

      subQuery =
          subQuery.clone(subQuery.getType(), RexUtil.shift(subQuery.getOperands(), -nFieldsLeft));

      final int nFields = join.getRowType().getFieldCount();
      final RexNode target =
          rule.apply(subQuery, variablesSet, logic, builder, 1, nFieldsRight, 0);
      final RexShuttle shuttle = new ReplaceSubQueryShuttle(e, RexUtil.shift(target, nFieldsLeft));

      RelNode newRight = builder.build();
      builder.push(join.getLeft());
      builder.push(newRight);

      builder.join(join.getJoinType(), shuttle.apply(join.getCondition()));
      builder.project(fields(builder, nFields));
    }

    call.transformTo(builder.build());
  }

  /**
   * Rewrites a sub-query that references columns from both the left and right inputs of a Join.
   *
   * <p>This method handles the complex case where a sub-query in a Join condition is correlated
   * with both sides of the Join. It performs the following steps:
   * <ol>
   *   <li>Identifies the "Domain" of values from the left and right inputs that are relevant
   *       to the sub-query.</li>
   *   <li>Constructs a "Computation Domain" by cross-joining the distinct keys from the left
   *       and right domains.</li>
   *   <li>Remaps the sub-query to operate on this Computation Domain.</li>
   *   <li>Rewrites the sub-query using the standard {@link #apply} method, but applied to the
   *       Domain.</li>
   *   <li>Re-integrates the result of the sub-query rewrite back into the original Join structure,
   *       ensuring correct join types and conditions are maintained.</li>
   * </ol>
   *
   * @param rule         The rule instance
   * @param call         The rule call
   * @param e            The sub-query to rewrite
   * @param join         The join containing the sub-query
   * @param nFieldsLeft  Number of fields in the left input
   * @param nFieldsRight Number of fields in the right input
   * @param inputSet     BitSet of columns used by the sub-query
   * @param builder      The RelBuilder
   * @param variablesSet Set of correlation variables used by the sub-query
   */
  private static void rewriteSubQueryOnDomain(SubQueryRemoveRule rule,
      RelOptRuleCall call,
      RexSubQuery e,
      Join join,
      int nFieldsLeft,
      int nFieldsRight,
      ImmutableBitSet inputSet,
      RelBuilder builder,
      Set<CorrelationId> variablesSet) {
    // Map to store the offset of each correlation variable
    final Map<CorrelationId, Integer> idToOffset = new HashMap<>();
    // Helper to determine offset for each correlation variable
    e.rel.accept(new CorrelationOffsetFinder(idToOffset, join, nFieldsLeft));

    // 1. Identify which columns from Left and Right are used by the subquery.
    // These will form the "Domain" on which the subquery is calculated.
    final ImmutableBitSet leftUsed =
        inputSet.intersect(ImmutableBitSet.range(0, nFieldsLeft));
    final ImmutableBitSet rightUsed =
        inputSet.intersect(ImmutableBitSet.range(nFieldsLeft, nFieldsLeft + nFieldsRight));

    // 2. Build the "Computation Domain".
    // This is a Cross Join of the distinct keys from Left and Right.
    // Domain = Distinct(Project(LeftUsed)) x Distinct(Project(RightUsed))

    // 2a. Left Domain
    builder.push(join.getLeft());
    builder.project(builder.fields(leftUsed));
    builder.distinct();

    // 2b. Right Domain
    builder.push(join.getRight());
    // We must shift the bitset to be 0-based for the Right input
    ImmutableBitSet rightUsedShifted = rightUsed.shift(-nFieldsLeft);
    builder.project(builder.fields(rightUsedShifted));
    builder.distinct();

    // 2c. Create Domain Cross Join
    builder.join(JoinRelType.INNER, builder.literal(true));

    // 3. Remap the SubQuery to run on the Domain.
    // We need to map original field indices to their new positions in the Domain.
    // Original: [LeftFields... | RightFields...]
    // Domain:   [LeftUsed...   | RightUsed...]
    final Map<Integer, Integer> mapping = new HashMap<>();
    int targetIdx = 0;
    for (int source : leftUsed) {
      mapping.put(source, targetIdx++);
    }
    for (int source : rightUsed) {
      mapping.put(source, targetIdx++);
    }

    final RexBuilder rexBuilder = builder.getRexBuilder();
    final CorrelationId domainCorrId = join.getCluster().createCorrel();
    final RexNode domainCorrVar = rexBuilder.makeCorrel(builder.peek().getRowType(), domainCorrId);

    // Shuttle to replace InputRefs and Correlations with references to the Domain
    RexShuttle shuttle = new InputRefAndCorrelationReplacer(mapping, variablesSet, idToOffset);
    // Create the new subquery with operands remapped to the Domain
    RexNode newSubQueryNode = e.accept(shuttle);

    // Rewrite e.rel to use domainCorrId
    RelNode newRel =
            e.rel.accept(
                new DomainRewriter(variablesSet, idToOffset, mapping, rexBuilder, domainCorrVar));

    if (newSubQueryNode instanceof RexSubQuery) {
      newSubQueryNode = ((RexSubQuery) newSubQueryNode).clone(newRel);
    }

    // We introduced a new correlation variable domainCorrId.
    Set<CorrelationId> newVariablesSet = ImmutableSet.of(domainCorrId);

    final RelOptUtil.Logic logic =
        LogicVisitor.find(join.getJoinType().generatesNullsOnRight()
                ? RelOptUtil.Logic.TRUE_FALSE_UNKNOWN : RelOptUtil.Logic.TRUE,
            ImmutableList.of(join.getCondition()), e);

    // 4. Apply the standard rewriting rule to the Domain.
    // The builder is currently sitting on the Domain Join.
    // 'target' is the CASE expression (or similar) resulting from the rewrite.
    // The builder stack now has the result of the rewrite (e.g. Domain Left Join Aggregate).
    assert newSubQueryNode instanceof RexSubQuery;
    final RexNode target =
        rule.apply((RexSubQuery) newSubQueryNode, newVariablesSet, logic, builder,
            1, builder.peek().getRowType().getFieldCount(), 0);

    // The target references the Domain Result (which is currently at the top of the builder).
    // In the final plan, the Domain Result will be joined to the right of the original inputs.
    // Furthermore, since we use a LEFT JOIN, the Domain Result columns become nullable.
    // So we need to shift the references in target AND make them nullable.
    final int offset = nFieldsLeft + nFieldsRight;
    final RexShuttle shiftAndNullableShuttle = new RexShuttle() {
      @Override public RexNode visitInputRef(RexInputRef inputRef) {
        // Shift the index
        int newIndex = inputRef.getIndex() + offset;
        return new RexInputRef(newIndex, inputRef.getType());
      }
    };
    final RexNode shiftedTarget = target.accept(shiftAndNullableShuttle);

    // 5. Re-integrate with Original Inputs
    // Stack has: [RewriteResult]
    RelNode domainResult = builder.build();

    // Rebuild the original Join structure
    // We want to construct: Left JOIN (Right JOIN Domain) ON ...
    // This preserves the JoinRelType of the original join.
    JoinRelType joinType = join.getJoinType();
    if (joinType == JoinRelType.RIGHT) {
      // Symmetric to LEFT/INNER/FULL but attached to Left
      builder.push(join.getLeft());
      builder.push(domainResult);

      // Join Left and Domain on Left Keys
      List<RexNode> leftJoinConditions = new ArrayList<>();
      int domainIdx = 0; // Left Keys are at start of Domain
      for (int source : leftUsed) {
        leftJoinConditions.add(
            builder.equals(
                builder.field(2, 0, source),
                builder.field(2, 1, domainIdx++)));
      }
      builder.join(JoinRelType.INNER, builder.and(leftJoinConditions));

      // Now Join Right
      builder.push(join.getRight());
      // Stack: (Left+Domain), Right

      // Join Condition: Original + Right Keys match
      List<RexNode> rightJoinConditions = new ArrayList<>();
      // Domain starts after Left. Right Keys in Domain are after Left Keys.
      int domainRightKeyIdx = nFieldsLeft + leftUsed.cardinality();
      for (int source : rightUsed) {
        // Right input (index 1)
        RexInputRef field = builder.field(2, 1, source - nFieldsLeft);
        // (Left+Domain) input (index 0)
        RexInputRef field1 = builder.field(2, 0, domainRightKeyIdx++);
        rightJoinConditions.add(builder.equals(field, field1));
      }

      RexShuttle replaceShuttle = new ReplaceSubQueryShuttle(e, shiftedTarget);
      RexNode newJoinCondition = join.getCondition().accept(replaceShuttle);

      builder.join(joinType, builder.and(builder.and(rightJoinConditions), newJoinCondition));

      builder.project(fields(builder, nFieldsLeft + nFieldsRight));
    } else {
      // For INNER, LEFT, FULL join, we can attach Domain to Right, then Join Left.
      // 1. Build (Right JOIN Domain)
      builder.push(join.getRight());
      builder.push(domainResult);

      // Join Right and Domain on Right Keys
      // Domain layout: [LeftKeys, RightKeys]
      List<RexNode> rightJoinConditions = new ArrayList<>();
      // Skip Left Keys
      int domainIdx = leftUsed.cardinality();
      for (int source : rightUsed) {
        rightJoinConditions.add(
            builder.equals(
                builder.field(2, 0, source - nFieldsLeft),  // Right input
                builder.field(2, 1, domainIdx++)));  // Domain input
      }
      // We use INNER join here to expand Right with Domain values.
      // Since Domain contains all distinct Right keys, this is safe.
      builder.join(JoinRelType.INNER, builder.and(rightJoinConditions));

      // 2. Join Left with (Right JOIN Domain)
      RelNode rightWithDomain = builder.build();
      builder.push(join.getLeft());
      builder.push(rightWithDomain);

      // Join Condition: Original Condition (rewritten) AND Left.LeftKeys = Domain.LeftKeys
      List<RexNode> leftJoinConditions = new ArrayList<>();
      // In (Right+Domain), Domain fields start after Right fields
      int domainStartInCombined = nFieldsRight;
      int domainLeftKeyIdx = domainStartInCombined; // Left Keys are at start of Domain

      for (int source : leftUsed) {
        // Left input
        RexInputRef field = builder.field(2, 0, source);
        // (Right+Domain) input
        RexInputRef field1 = builder.field(2, 1, domainLeftKeyIdx++);
        leftJoinConditions.add(builder.equals(field, field1));
      }

      RexShuttle replaceShuttle = new ReplaceSubQueryShuttle(e, shiftedTarget);
      RexNode newJoinCondition = join.getCondition().accept(replaceShuttle);

      builder.join(joinType, builder.and(builder.and(leftJoinConditions), newJoinCondition));

      // Project original fields (remove Domain columns)
      builder.project(fields(builder, nFieldsLeft + nFieldsRight));
    }

    call.transformTo(builder.build());
  }

  private static void matchFilterEnableMarkJoin(SubQueryRemoveRule rule, RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final Set<CorrelationId> variablesSet = filter.getVariablesSet();
    final RelBuilder builder = call.builder();
    builder.push(filter.getInput());
    List<RexNode> newCondition
        = rule.applyEnableMarkJoin(variablesSet, ImmutableList.of(filter.getCondition()), builder);
    assert newCondition.size() == 1;
    builder.filter(newCondition.get(0));
    builder.project(fields(builder, filter.getRowType().getFieldCount()));
    call.transformTo(builder.build());
  }

  private static void matchProjectEnableMarkJoin(SubQueryRemoveRule rule, RelOptRuleCall call) {
    final Project project = call.rel(0);
    final Set<CorrelationId> variablesSet = project.getVariablesSet();
    final RelBuilder builder = call.builder();
    builder.push(project.getInput());
    List<RexNode> newProjects
        = rule.applyEnableMarkJoin(variablesSet, project.getProjects(), builder);
    builder.project(newProjects, project.getRowType().getFieldNames());
    call.transformTo(builder.build());
  }

  private List<RexNode> applyEnableMarkJoin(Set<CorrelationId> variablesSetOfRelNode,
      List<RexNode> expressions, RelBuilder builder) {
    List<RexNode> newExpressions = new ArrayList<>(expressions);
    int count = 0;
    while (true) {
      final RexSubQuery e = RexUtil.SubQueryFinder.find(newExpressions);
      if (e == null) {
        assert count > 0;
        break;
      }
      ++count;
      final Set<CorrelationId> variablesSet = RelOptUtil.getVariablesUsed(e.rel);
      // Only keep the correlation that are defined in the current RelNode level, to avoid creating
      // wrong Correlate node.
      variablesSet.retainAll(variablesSetOfRelNode);

      RexNode target;
      // rewrite EXISTS/IN/SOME to left mark join/correlate
      switch (e.getKind()) {
      case EXISTS:
      case IN:
      case SOME:
        target =
            rewriteToMarkJoin(e, variablesSet, builder,
                builder.peek().getRowType().getFieldCount());
        break;
      case SCALAR_QUERY:
        target =
            rewriteScalarQuery(e, variablesSet, builder, 1,
                builder.peek().getRowType().getFieldCount());
        break;
      case ARRAY_QUERY_CONSTRUCTOR:
      case MAP_QUERY_CONSTRUCTOR:
      case MULTISET_QUERY_CONSTRUCTOR:
        target =
            rewriteCollection(e, variablesSet, builder, 1,
                builder.peek().getRowType().getFieldCount());
        break;
      case UNIQUE:
        target = rewriteUnique(e, builder);
        break;
      default:
        throw new AssertionError(e.getKind());
      }
      final RexShuttle shuttle = new ReplaceSubQueryShuttle(e, target);
      newExpressions = shuttle.apply(newExpressions);
    }
    return newExpressions;
  }

  /**
   * Rewrites a IN/SOME/EXISTS RexSubQuery into a {@link Join} of LEFT MARK type.
   *
   * @param e             IN/SOME/EXISTS Sub-query to rewrite
   * @param variablesSet  A set of variables used by a relational
   *                      expression of the specified RexSubQuery
   * @param builder       Builder
   * @param offset        Offset to shift {@link RexInputRef}
   * @return  Expression that may be used to replace the RexSubQuery
   */
  private static RexNode rewriteToMarkJoin(RexSubQuery e, Set<CorrelationId> variablesSet,
      RelBuilder builder, int offset) {
    builder.push(e.rel);
    final List<RexNode> rightShiftRef = RexUtil.shift(builder.fields(), offset);
    final List<RexNode> externalPredicate = new ArrayList<>();
    final SqlOperator externalOperator;
    switch (e.getKind()) {
    case SOME:
      SqlQuantifyOperator op = (SqlQuantifyOperator) e.op;
      externalOperator = RelOptUtil.op(op.comparisonKind, SqlStdOperatorTable.EQUALS);
      break;
    case IN:
      externalOperator = SqlStdOperatorTable.EQUALS;
      break;
    case EXISTS:
      externalOperator = SqlStdOperatorTable.EQUALS;
      assert e.getOperands().isEmpty();
      break;
    default:
      throw new IllegalArgumentException("Only IN/SOME/EXISTS sub-query can be rewritten to "
          + "left mark join, but got: " + e.getKind());
    }
    Pair.zip(e.getOperands(), rightShiftRef, false).stream()
        .map(pair -> builder.call(externalOperator, pair.left, pair.right))
        .forEach(externalPredicate::add);

    builder.join(
        JoinRelType.LEFT_MARK,
        RexUtil.composeConjunction(builder.getRexBuilder(), externalPredicate),
        variablesSet);
    return last(builder.fields());
  }

  /** Shuttle that replaces occurrences of a given
   * {@link org.apache.calcite.rex.RexSubQuery} with a replacement
   * expression. */
  private static class ReplaceSubQueryShuttle extends RexShuttle {
    private final RexSubQuery subQuery;
    private final RexNode replacement;

    ReplaceSubQueryShuttle(RexSubQuery subQuery, RexNode replacement) {
      this.subQuery = subQuery;
      this.replacement = replacement;
    }

    @Override public RexNode visitSubQuery(RexSubQuery subQuery) {
      return subQuery.equals(this.subQuery) ? replacement : subQuery;
    }
  }

  /**
   * Shuttle that finds correlation variables and determines their offset.
   */
  private static class CorrelationOffsetFinder extends RelHomogeneousShuttle {
    private final Map<CorrelationId, Integer> idToOffset;
    private final Join join;
    private final int nFieldsLeft;

    CorrelationOffsetFinder(Map<CorrelationId, Integer> idToOffset, Join join, int nFieldsLeft) {
      this.idToOffset = idToOffset;
      this.join = join;
      this.nFieldsLeft = nFieldsLeft;
    }

    @Override public RelNode visit(RelNode other) {
      other.accept(new RexShuttle() {
        @Override public RexNode visitCorrelVariable(RexCorrelVariable correlVariable) {
          if (!idToOffset.containsKey(correlVariable.id)) {
            // Check if type matches Left
            if (RelOptUtil.eq("type1", correlVariable.getType(),
                "type2", join.getLeft().getRowType(), Litmus.IGNORE)) {
              idToOffset.put(correlVariable.id, 0);
            } else if (RelOptUtil.eq("type1", correlVariable.getType(),
                "type2", join.getRight().getRowType(), Litmus.IGNORE)) {
              idToOffset.put(correlVariable.id, nFieldsLeft);
            } else {
              // Default to 0 if unknown
              idToOffset.put(correlVariable.id, 0);
            }
          }
          return super.visitCorrelVariable(correlVariable);
        }
      });
      return super.visit(other);
    }
  }

  /**
   * Shuttle that replaces InputRefs and Correlations with references to the Domain.
   */
  private static class InputRefAndCorrelationReplacer extends RexShuttle {
    private final Map<Integer, Integer> mapping;
    private final Set<CorrelationId> variablesSet;
    private final Map<CorrelationId, Integer> idToOffset;

    InputRefAndCorrelationReplacer(Map<Integer, Integer> mapping,
        Set<CorrelationId> variablesSet, Map<CorrelationId, Integer> idToOffset) {
      this.mapping = mapping;
      this.variablesSet = variablesSet;
      this.idToOffset = idToOffset;
    }

    @Override public RexNode visitInputRef(RexInputRef inputRef) {
      Integer newIndex = mapping.get(inputRef.getIndex());
      if (newIndex != null) {
        return new RexInputRef(newIndex, inputRef.getType());
      }
      return super.visitInputRef(inputRef);
    }

    @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      RexNode refExpr = fieldAccess.getReferenceExpr();
      if (refExpr instanceof RexCorrelVariable) {
        CorrelationId id = ((RexCorrelVariable) refExpr).id;
        if (variablesSet.contains(id)) {
          int fieldIdx = fieldAccess.getField().getIndex();
          int offset = idToOffset.getOrDefault(id, 0);
          Integer newIndex = mapping.get(fieldIdx + offset);
          if (newIndex != null) {
            return new RexInputRef(newIndex, fieldAccess.getType());
          }
        }
      }
      return super.visitFieldAccess(fieldAccess);
    }
  }

  /**
   * Shuttle that rewrites RelNodes to use the Domain correlation variable.
   */
  private static class DomainRewriter extends RelHomogeneousShuttle {
    private final Set<CorrelationId> variablesSet;
    private final Map<CorrelationId, Integer> idToOffset;
    private final Map<Integer, Integer> mapping;
    private final RexBuilder rexBuilder;
    private final RexNode domainCorrVar;

    DomainRewriter(Set<CorrelationId> variablesSet, Map<CorrelationId, Integer> idToOffset,
        Map<Integer, Integer> mapping, RexBuilder rexBuilder, RexNode domainCorrVar) {
      this.variablesSet = variablesSet;
      this.idToOffset = idToOffset;
      this.mapping = mapping;
      this.rexBuilder = rexBuilder;
      this.domainCorrVar = domainCorrVar;
    }

    @Override public RelNode visit(RelNode other) {
      return super.visit(
          other.accept(new RexShuttle() {
            @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
              RexNode refExpr = fieldAccess.getReferenceExpr();
              if (refExpr instanceof RexCorrelVariable) {
                CorrelationId id = ((RexCorrelVariable) refExpr).id;
                if (variablesSet.contains(id)) {
                  int fieldIdx = fieldAccess.getField().getIndex();
                  int offset = idToOffset.getOrDefault(id, 0);
                  Integer newIndex = mapping.get(fieldIdx + offset);
                  if (newIndex != null) {
                    return rexBuilder.makeFieldAccess(domainCorrVar, newIndex);
                  }
                }
              }
              return super.visitFieldAccess(fieldAccess);
            }
          }));
    }
  }

  /** Rule configuration. */
  @Value.Immutable(singleton = false)
  public interface Config extends RelRule.Config {
    Config PROJECT = ImmutableSubQueryRemoveRule.Config.builder()
        .withMatchHandler(SubQueryRemoveRule::matchProject)
        .build()
        .withOperandSupplier(b ->
            b.operand(Project.class)
                .predicate(RexUtil.SubQueryFinder::containsSubQuery).anyInputs())
        .withDescription("SubQueryRemoveRule:Project");

    Config FILTER = ImmutableSubQueryRemoveRule.Config.builder()
        .withMatchHandler(SubQueryRemoveRule::matchFilter)
        .build()
        .withOperandSupplier(b ->
            b.operand(Filter.class)
                .predicate(RexUtil.SubQueryFinder::containsSubQuery).anyInputs())
        .withDescription("SubQueryRemoveRule:Filter");

    Config JOIN = ImmutableSubQueryRemoveRule.Config.builder()
        .withMatchHandler(SubQueryRemoveRule::matchJoin)
        .build()
        .withOperandSupplier(b ->
            b.operand(Join.class)
                .predicate(RexUtil.SubQueryFinder::containsSubQuery)
                .anyInputs())
        .withDescription("SubQueryRemoveRule:Join");

    Config PROJECT_ENABLE_MARK_JOIN = ImmutableSubQueryRemoveRule.Config.builder()
        .withMatchHandler(SubQueryRemoveRule::matchProjectEnableMarkJoin)
        .build()
        .withOperandSupplier(b ->
            b.operand(Project.class)
                .predicate(RexUtil.SubQueryFinder::containsSubQuery).anyInputs())
        .withDescription("SubQueryRemoveRule:ProjectEnableMarkJoin");

    Config FILTER_ENABLE_MARK_JOIN = ImmutableSubQueryRemoveRule.Config.builder()
        .withMatchHandler(SubQueryRemoveRule::matchFilterEnableMarkJoin)
        .build()
        .withOperandSupplier(b ->
            b.operand(Filter.class)
                .predicate(RexUtil.SubQueryFinder::containsSubQuery).anyInputs())
        .withDescription("SubQueryRemoveRule:FilterEnableMarkJoin");

    @Override default SubQueryRemoveRule toRule() {
      return new SubQueryRemoveRule(this);
    }

    /** Forwards a call to {@link #onMatch(RelOptRuleCall)}. */
    MatchHandler<SubQueryRemoveRule> matchHandler();

    /** Sets {@link #matchHandler()}. */
    Config withMatchHandler(MatchHandler<SubQueryRemoveRule> matchHandler);
  }
}
