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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.LogicVisitor;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
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
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.DeduplicateCorrelateVariables;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
 * @see CoreRules#TABLE_FUNCTION_SCAN_SCALAR_QUERY_TO_CORRELATE
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
    //   else false                            -- (5) no match
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
    //   else false                            -- (5) no match
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
    //
    // For multi-column IN where at least one key or RHS column is nullable, a
    // single wildcard LEFT JOIN handles both exact and partial-null matches,
    // while all-null RHS rows are excluded from dt and detected via ct.ck < ct.c:
    //
    // select e.empno, (e.empno, e.comm) in (select empno, comm from emp)
    // from emp as e
    //
    // becomes
    //
    // select e.empno,
    //   case
    //   when ct.c = 0 then false                     -- (1) empty subquery check
    //   when e.comm is null then null                -- (2) nullable key NULL check
    //   when dt.i is not null and dt.em then true    -- (3) exact match (all cols non-null)
    //   when dt.i is not null then null              -- (4) partial-null match (UNKNOWN)
    //   when ct.ck < ct.c then null                  -- (5) all-null row exists (UNKNOWN)
    //   else false                                   -- (6) no match
    //   end
    // from emp as e
    // inner join (
    //   select count(*) as c,
    //          count(*) filter (where not (empno is null and comm is null)) as ck
    //   from emp) as ct on true
    // left join (
    //   select empno, comm, true as i,
    //          max(empno is not null and comm is not null) as em
    //   from emp
    //   where empno is not null or comm is not null  -- all-null rows excluded
    //   group by empno, comm) as dt
    //   on (e.empno = dt.empno or dt.empno is null)  -- wildcard per-column condition
    //   and (e.comm = dt.comm or dt.comm is null)
    //
    // All-null rows (empno IS NULL AND comm IS NULL) are excluded from dt because
    // the wildcard condition matches every LHS key, causing duplicate join output
    // rows for LHS keys that also have exact group matches. They are instead
    // caught by the global ct.ck < ct.c check (branch 5).

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
    boolean needsNullRowJoin = false;
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
      needsNullRowJoin = needsNullSafety && fields.size() > 1;

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
        if (needsNullRowJoin) {
          // Exclude all-null rows from dt (they are detected by ct.ck < ct.c instead).
          // Add em (exact-match) column to distinguish exact groups from partial-null groups.
          List<RexNode> anyFieldNotNull =
              fields.stream().map(builder::isNotNull).collect(Collectors.toList());
          builder.filter(builder.or(anyFieldNotNull));
          RexNode allNotNull =
              builder.and(fields.stream().map(builder::isNotNull).collect(Collectors.toList()));
          builder.aggregate(builder.groupKey(fields),
              builder.literalAgg(true).as("i"),
              builder.max(allNotNull).as("em"));
        } else {
          builder.aggregate(builder.groupKey(fields),
              builder.literalAgg(true).as("i"));
        }
      }
    }

    String dtAlias = "dt";
    if (subQueryIndex != 0) {
      dtAlias = "dt" + subQueryIndex;
    }
    builder.as(dtAlias);
    int refOffset = offset;
    final List<RexNode> conditions;
    if (needsNullRowJoin) {
      // Per-column wildcard condition: (key = col OR col IS NULL).
      final List<RexNode> dtFields = builder.fields();
      conditions = IntStream.range(0, expressionOperands.size())
          .mapToObj(k -> {
            RexNode col = RexUtil.shift(dtFields.get(k), refOffset);
            return builder.or(builder.equals(expressionOperands.get(k), col),
                builder.isNull(col));
          })
          .collect(Collectors.toList());
    } else {
      conditions = Pair.zip(expressionOperands, builder.fields()).stream()
          .map(pair -> builder.equals(pair.left, RexUtil.shift(pair.right, refOffset)))
          .collect(Collectors.toList());
    }
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
      if (needsNullRowJoin) {
        // em=true: exact match (all RHS cols non-null) → TRUE.
        operands.add(
            builder.and(
                ImmutableList.of(
                builder.isNotNull(builder.field(dtAlias, "i")),
                builder.call(SqlStdOperatorTable.IS_TRUE,
                    builder.field(dtAlias, "em")))),
            trueLiteral);
        // em=false: partial-null match → UNKNOWN.
        operands.add(builder.isNotNull(builder.field(dtAlias, "i")), b);
      } else {
        operands.add(builder.isNotNull(builder.field(dtAlias, "i")),
            trueLiteral);
      }
    }

    if (!allLiterals) {
      switch (logic) {
      case TRUE_FALSE_UNKNOWN:
      case UNKNOWN_AS_TRUE:
        if (needsNullSafety) {
          // ct.ck < ct.c: RHS has a null (single-col) or all-null row (multi-col) → UNKNOWN.
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
    final Set<CorrelationId> projectVariablesSet = project.getVariablesSet();
    final RelBuilder builder = call.builder();
    builder.push(project.getInput());
    // Rewrite all sub-queries in one call, as matchFilter does. If only one
    // were rewritten per call, the Project created for the remaining ones
    // would no longer define the correlation variables that they use, and a
    // later call could not tell this Project's variables from those of an
    // outer scope.
    int count = 0;
    List<RexNode> projects = project.getProjects();
    while (true) {
      final RexSubQuery e = RexUtil.SubQueryFinder.find(projects);
      if (e == null) {
        assert count > 0;
        break;
      }
      ++count;
      final RelOptUtil.Logic logic =
          LogicVisitor.find(RelOptUtil.Logic.TRUE_FALSE_UNKNOWN, projects, e);
      final Set<CorrelationId> variablesSet =
          RelOptUtil.getVariablesUsed(e.rel);
      // Only consider the correlated variables which originated from this sub-query level.
      variablesSet.retainAll(projectVariablesSet);
      final RexNode target =
          rule.apply(e, variablesSet, logic,
              builder, 1, builder.peek().getRowType().getFieldCount(), count);
      final RexShuttle shuttle = new ReplaceSubQueryShuttle(e, target);
      projects = shuttle.apply(projects);
    }
    builder.project(projects, project.getRowType().getFieldNames());
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

  /**
   * Rewrites one scalar sub-query in a table-function call into a
   * {@link Correlate}.
   *
   * <p>For example, converts:
   *
   * <pre>{@code
   * LogicalTableFunctionScan(invocation=[F(SCALAR_QUERY(SUB_QUERY_REL), 20)])
   * }</pre>
   *
   * <p>into:
   *
   * <pre>{@code
   * LogicalProject(TABLE_FUNCTION_FIELDS)
   *   LogicalCorrelate(correlation=[$cor0], joinType=[inner],
   *       requiredColumns=[{0}])
   *     LogicalAggregate(group=[{}], scalarValue=[SINGLE_VALUE($0)])
   *       SUB_QUERY_REL
   *     LogicalTableFunctionScan(invocation=[F($cor0.scalarValue, 20)])
   * }</pre>
   *
   * <p>The aggregate implements scalar-query cardinality: zero rows produce
   * {@code null}, and more than one row raises an error. The correlate makes
   * the resulting value available to the table-function invocation, and the
   * project removes the helper scalar field from the output. The rule rewrites
   * one scalar sub-query per invocation; subsequent invocations rewrite any
   * remaining scalar sub-queries.
   */
  private static void matchTableFunctionScan(SubQueryRemoveRule rule,
      RelOptRuleCall call) {
    final TableFunctionScan scan = call.rel(0);
    final RexSubQuery e =
        requireNonNull(
            RexUtil.SubQueryFinder.find(scan.getCall(), SqlKind.SCALAR_QUERY));

    final RelBuilder builder = call.builder();
    builder.push(e.rel);
    builder.aggregate(builder.groupKey(),
        builder.aggregateCall(SqlStdOperatorTable.SINGLE_VALUE,
            builder.field(0)));
    final RelNode scalarValue = builder.build();

    final CorrelationId correlationId =
        scan.getCluster().createCorrel();
    final RexCorrelVariable correlationVariable =
        (RexCorrelVariable) scan.getCluster().getRexBuilder()
            .makeCorrel(scalarValue.getRowType(), correlationId);
    final RexNode target =
        scan.getCluster().getRexBuilder()
            .makeFieldAccess(correlationVariable, 0);
    final RexNode newCall =
        scan.getCall().accept(new ReplaceSubQueryShuttle(e, target));
    final TableFunctionScan newScan =
        (TableFunctionScan) scan.copy(scan.getTraitSet(), scan.getInputs(),
            newCall, scan.getElementType(), scan.getRowType(),
            scan.getColumnMappings())
            .withHints(scan.getHints());

    final RelNode correlate =
        LogicalCorrelate.create(scalarValue, newScan, ImmutableList.of(),
            correlationId, ImmutableBitSet.of(0), JoinRelType.INNER);
    builder.push(correlate);
    final int scalarFieldCount =
        scalarValue.getRowType().getFieldCount();
    builder.project(
        IntStream.range(0, scan.getRowType().getFieldCount())
            .mapToObj(i -> builder.field(scalarFieldCount + i))
            .collect(Collectors.toList()),
        scan.getRowType().getFieldNames());
    call.transformTo(builder.build());
  }

  private static void matchJoin(SubQueryRemoveRule rule, RelOptRuleCall call) {
    final Join join = call.rel(0);
    final RelBuilder builder = call.builder();
    // rewrites one sub-query per call and relies on the rule firing repeatedly
    final RexSubQuery e =
        requireNonNull(RexUtil.SubQueryFinder.find(join.getCondition()));

    final int nFieldsLeft = join.getLeft().getRowType().getFieldCount();
    final int nFieldsRight = join.getRight().getRowType().getFieldCount();

    // Correlation columns should also be considered.
    // For example:
    // SELECT t1.deptno FROM dept AS t0 JOIN emp AS t1 ON
    // (t1.deptno = (SELECT inner_t1.deptno FROM emp AS inner_t1 WHERE inner_t1.ENAME = t0.DNAME));
    //
    // LogicalProject(DEPTNO=[$11])
    //   LogicalJoin(condition=[=($11, $SCALAR_QUERY({
    // LogicalProject(DEPTNO=[$8])
    //   LogicalFilter(condition=[=($1, $cor0.dname)])
    //     LogicalTableScan(table=[emp])
    // }))], joinType=[inner])
    //     LogicalTableScan(table=[dept])
    //     LogicalTableScan(table=[emp])
    //
    // In such a case $cor0.dname need to be accounted as input form left side.
    final ImmutableBitSet subqueryUsedFields =
        RelOptUtil.InputFinder.analyze(e, join.getVariablesSet()).build();

    final boolean inputIntersectsLeftSide =
        subqueryUsedFields.intersects(ImmutableBitSet.range(0, nFieldsLeft));
    final boolean inputIntersectsRightSide =
        subqueryUsedFields.intersects(ImmutableBitSet.range(nFieldsLeft, nFieldsLeft + nFieldsRight));
    if (inputIntersectsLeftSide && inputIntersectsRightSide
        && join.getJoinType() != JoinRelType.INNER) {
      // Rewriting requires flattening the join into a cross-product first (see below).
      // That transformation is only valid for INNER JOIN (A JOIN B ON c ≡ (A × B) WHERE c).
      // For OUTER JOINs the semantics differ (NULL-padding), so we bail out. We bail
      // out before creating any correlation id, so that an abandoned match does not
      // shift the ids of unrelated plans.
      return;
    }

    // This rule rewrites one sub-query, "e", per call; the other sub-queries
    // remain in the condition, to be rewritten by later calls. Split the
    // correlation variables of the join accordingly:
    //  * subQueryVariables, used by "e", are bound by the rewrite of "e"
    //    (rule.apply), against the row type of one input or of the
    //    cross-product;
    //  * remainingVariables, used by the other sub-queries, must still be
    //    defined by the relational expression that replaces the join, against
    //    its joint row type; otherwise those sub-queries would become
    //    uncorrelated, with their variables undefined.
    // A variable in both sets would be defined twice, with different row types,
    // so "e" gets fresh ids for such variables (see renameSharedVariables),
    // whereas the other sub-queries keep the original ids. Variables that are
    // bound by an outer scope are not the join's, and are left alone.
    //
    // Sub-queries are compared by value, as in ReplaceSubQueryShuttle: those
    // equal to "e" are all rewritten at once, so none of them remains. The
    // condition still contains "e" with its original ids, so "e", not
    // unsharedSubQuery, is used to locate the sub-query in the condition.
    final RexUtil.SubQueryCollector collector = new RexUtil.SubQueryCollector(true);
    join.getCondition().accept(collector);
    final List<RexSubQuery> otherSubQueries = new ArrayList<>();
    for (RexSubQuery subQuery : collector.getSubQueries()) {
      if (!subQuery.equals(e)) {
        otherSubQueries.add(subQuery);
      }
    }

    final Set<CorrelationId> subQueryVariables = new LinkedHashSet<>(join.getVariablesSet());
    subQueryVariables.retainAll(RelOptUtil.getVariablesUsed(e.rel));
    final Set<CorrelationId> remainingVariables = new LinkedHashSet<>(join.getVariablesSet());
    remainingVariables.retainAll(RelOptUtil.getVariablesUsed(otherSubQueries));

    final Set<CorrelationId> sharedVariables = new LinkedHashSet<>(subQueryVariables);
    sharedVariables.retainAll(remainingVariables);
    final RexSubQuery unsharedSubQuery;
    final Set<CorrelationId> variablesSet;
    if (sharedVariables.isEmpty()) {
      unsharedSubQuery = e;
      variablesSet = subQueryVariables;
    } else {
      final Pair<RexNode, Set<CorrelationId>> unshared =
          renameSharedVariables(e, subQueryVariables, sharedVariables, builder);
      unsharedSubQuery = (RexSubQuery) unshared.left;
      variablesSet = unshared.right;
    }

    if (inputIntersectsLeftSide && inputIntersectsRightSide) {
      // The sub-query operands span both sides of the join, e.g.:
      //   SELECT empno FROM emp JOIN dept
      //   ON emp.deptno + dept.deptno >= SOME(SELECT deptno FROM dept)
      //
      // Because the sub-query references fields from both the left (emp) and right (dept)
      // inputs, we cannot attach the sub-query rewrite to either side alone.
      // The logic is to exploit the INNER JOIN equivalence:
      //   L INNER JOIN R ON cond  ≡  (L CROSS JOIN R) WHERE cond

      // Step 1 – flatten to a cross-product so every field is visible in one relation:
      //
      //   Before:                        After (builder stack):
      //   LogicalJoin(INNER, cond)   →   LogicalJoin(INNER, true)   ← cross-product
      //     LogicalTableScan(EMP)          LogicalTableScan(EMP)
      //     LogicalTableScan(DEPT)         LogicalTableScan(DEPT)
      builder.push(join.getLeft());
      builder.push(join.getRight());
      builder.join(JoinRelType.INNER, builder.literal(true));

      // Step 2 – expand the sub-query against the flattened relation (rule.apply).
      //   The sub-query rewrite pushes an auxiliary relation onto the stack, e.g. for SOME:
      //
      //   LogicalJoin(INNER, true)                ← sub-query auxiliary join
      //     LogicalJoin(INNER, true)              ← cross-product from step 1
      //       LogicalTableScan(EMP)
      //       LogicalTableScan(DEPT)
      //     LogicalAggregate(m, c, d)             ← aggregate over sub-query
      //       LogicalTableScan(DEPT)
      final int count = builder.peek().getRowType().getFieldCount();
      final RelOptUtil.Logic logic =
          LogicVisitor.find(RelOptUtil.Logic.TRUE, ImmutableList.of(join.getCondition()), e);
      final RexNode target =
          rule.apply(unsharedSubQuery, variablesSet, logic, builder, 1, count, 0);

      // Step 3 – replace the sub-query in the ON condition with the expression (target)
      //   produced by rule.apply (e.g. a CASE expression for SOME, IS NOT NULL for EXISTS),
      //   then materialize it as a Filter (valid because the cross-product is already in place).
      final RexShuttle shuttle = new ReplaceSubQueryShuttle(e, target);
      RexNode newCond = shuttle.apply(join.getCondition());

      // Keep the variables of the remaining sub-queries defined on the new Filter.
      // The auxiliary fields are appended after both inputs, so field indexes do not
      // change; only the row type that the variables reference is updated.
      newCond =
          remapCorrelationVariables(newCond, remainingVariables,
              builder.peek().getRowType(), 0, 0, builder.getRexBuilder());
      builder.filter(remainingVariables, newCond);

      // Step 4 – project away the auxiliary columns introduced by the sub-query rewrite,
      //   restoring the original output schema (fields 0 .. count-1).
      builder.project(fields(builder, count));
      call.transformTo(builder.build());
      return;
    }

    if (inputIntersectsLeftSide) {
      builder.push(join.getLeft());

      final RelOptUtil.Logic logic =
          LogicVisitor.find(join.getJoinType().generatesNullsOnRight()
                  ? RelOptUtil.Logic.TRUE_FALSE_UNKNOWN : RelOptUtil.Logic.TRUE,
              ImmutableList.of(join.getCondition()), e);

      // The sub-query's variables now reference the left input only; the fields
      // that it uses are at the same indexes, so only the row type changes.
      final RexSubQuery subQuery =
          (RexSubQuery) remapCorrelationVariables(unsharedSubQuery, variablesSet,
              join.getLeft().getRowType(), 0, 0, builder.getRexBuilder());
      final RexNode target =
          rule.apply(subQuery, variablesSet, logic, builder, 1, nFieldsLeft, 0);
      final RexShuttle shuttle = new ReplaceSubQueryShuttle(e, target);

      final int nFieldsNewLeft = builder.fields().size();
      final RexNode newCond =
          shuttle.apply(
              RexUtil.shift(join.getCondition(), nFieldsLeft,
                  nFieldsNewLeft - nFieldsLeft));

      if (remainingVariables.isEmpty()) {
        builder.push(join.getRight());
        builder.join(join.getJoinType(), newCond);
      } else {
        // The remaining sub-queries reference right-side fields through the join's
        // correlation variables, so their indexes must be shifted past the auxiliary
        // fields that the rewrite appended to the left input.
        rebuildJoin(builder, join, builder.build(), join.getRight(), newCond,
            remainingVariables, nFieldsLeft, nFieldsNewLeft - nFieldsLeft);
      }

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

      RexSubQuery subQuery = unsharedSubQuery;

      if (!variablesSet.isEmpty()) {
        // Original correlates reference joint row type, but we are about to create
        // new join of original right side and correlated sub-query. Therefore we have
        // to adjust correlated variables in following way:
        //   1) new correlation variable must reference row type of right side only
        //   2) field index must be shifted on the size of the left side
        //      (offset -nFieldsLeft; start is 0 because the sub-query references
        //      only right-side fields). In the example below, $cor0.DEPTNO0 is
        //      field 8 of EMP + DEPT and becomes $cor0.DEPTNO, field 0 of DEPT.
        // This applies to every variable in variablesSet, including references
        // inside nested sub-queries.
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
        subQuery =
            (RexSubQuery) remapCorrelationVariables(subQuery, variablesSet,
                join.getRight().getRowType(), 0, -nFieldsLeft, builder.getRexBuilder());
      }

      subQuery =
          subQuery.clone(subQuery.getType(), RexUtil.shift(subQuery.getOperands(), -nFieldsLeft));

      final int nFields = join.getRowType().getFieldCount();
      final RexNode target =
          rule.apply(subQuery, variablesSet, logic, builder, 1, nFieldsRight, 0);
      final RexShuttle shuttle = new ReplaceSubQueryShuttle(e, RexUtil.shift(target, nFieldsLeft));

      final RelNode newRight = builder.build();
      final RexNode newCond = shuttle.apply(join.getCondition());

      if (remainingVariables.isEmpty()) {
        builder.push(join.getLeft());
        builder.push(newRight);
        builder.join(join.getJoinType(), newCond);
      } else {
        // The auxiliary fields are appended after the right-side fields, so the
        // indexes that the remaining sub-queries use do not change (hence start
        // and offset 0).
        rebuildJoin(builder, join, join.getLeft(), newRight, newCond,
            remainingVariables, 0, 0);
      }
      builder.project(fields(builder, nFields));
    }

    call.transformTo(builder.build());
  }

  /** Gives {@code node} fresh correlation ids for {@code sharedVariables}, a
   * subset of {@code variablesSet}, and returns the rewritten expression along
   * with its new set of variables.
   *
   * <p>Visits the complete expression, including operands and relational
   * expressions of nested sub-queries. Otherwise a sub-query in an operand
   * could retain an id that its new binder no longer defines. Variables that
   * are not shared, including those bound by an outer scope, are unchanged.
   *
   * <p>Rewriting the sub-query binds its variables to a new {@link Correlate},
   * whereas the remaining sub-queries need the
   * original join to keep defining the same variables over the joint row
   * type. For example, in
   *
   * <pre>{@code
   * SELECT e.ename
   * FROM emp e
   * JOIN dept d
   *   ON EXISTS (SELECT 1 FROM emp s WHERE s.mgr = e.empno)
   *   AND EXISTS (SELECT 1 FROM emp s WHERE s.ename = d.dname)
   * }</pre>
   *
   * <p>both sub-queries use {@code $cor0}. Rewriting the first one renames its
   * {@code $cor0} to a fresh id, so that {@code $cor0} is not defined twice: by
   * the Correlate over EMP (8 fields) and by the Join over EMP + auxiliary field
   * + DEPT (12 fields). */
  private static Pair<RexNode, Set<CorrelationId>> renameSharedVariables(
      RexNode node, Set<CorrelationId> variablesSet,
      Set<CorrelationId> sharedVariables, RelBuilder builder) {
    final Set<CorrelationId> newVariablesSet = new LinkedHashSet<>(variablesSet);
    for (CorrelationId id : sharedVariables) {
      final CorrelationId newId = builder.getCluster().createCorrel();
      node =
          DeduplicateCorrelateVariables.go(builder.getRexBuilder(), newId,
              ImmutableSet.of(id), node);
      newVariablesSet.remove(id);
      newVariablesSet.add(newId);
    }
    return Pair.of(node, newVariablesSet);
  }

  /** Creates the join that replaces {@code join} once one of the sub-queries in
   * its condition has been rewritten, and pushes it onto {@code builder}.
   *
   * <p>The sub-queries that are left in {@code condition} use the join's
   * correlation variables {@code remainingVariables}, so the new join must keep
   * defining them, otherwise they would be treated as uncorrelated and left
   * undefined. Their field indexes are remapped as {@code start} and
   * {@code offset} describe (see {@link #remapCorrelationVariables}), because
   * the rewrite may have added auxiliary fields to one of the inputs. */
  private static void rebuildJoin(RelBuilder builder, Join join, RelNode left,
      RelNode right, RexNode condition, Set<CorrelationId> remainingVariables,
      int start, int offset) {
    // The correlation row type comes from createJoinType rather than from the
    // join's output, because the ON condition sees both inputs even for the
    // SEMI and ANTI joins that do not output the right one.
    final RelDataType correlRowType =
        SqlValidatorUtil.createJoinType(builder.getTypeFactory(),
            left.getRowType(), right.getRowType(), null,
            join.getSystemFieldList());
    final RexNode newCondition =
        remapCorrelationVariables(condition, remainingVariables, correlRowType,
            start, offset, builder.getRexBuilder());

    // Not RelBuilder#join: given a non-empty variablesSet it may create a
    // Correlate (and a Correlate has no room for a condition that references
    // both inputs). Not Join#copy either, because that would keep the original,
    // now stale, variablesSet.
    builder.push(
        LogicalJoin.create(left, right, join.getHints(), newCondition,
            remainingVariables, join.getJoinType(), join.isSemiJoinDone(),
            ImmutableList.copyOf(join.getSystemFieldList())));
  }

  /** Rewrites references to the correlation variables {@code ids} inside the
   * sub-queries of {@code node} so that they reference a row of type
   * {@code rowType}, shifting fields whose index is at least {@code start} by
   * {@code offset}, as {@link RexUtil#shift(RexNode, int, int)} does for input
   * references. An {@code offset} of 0 leaves the indexes alone and only
   * changes the row type that the variables reference.
   *
   * <p>For example, in
   *
   * <pre>{@code
   * SELECT e.ename
   * FROM emp e
   * LEFT JOIN dept d
   *   ON e.deptno IN (SELECT s.deptno FROM emp s)
   *   AND e.deptno = d.deptno
   *   AND EXISTS (SELECT 1 FROM emp s WHERE s.ename = d.dname)
   * }</pre>
   *
   * <p>{@code $cor0} is a row of EMP (fields 0..7) + DEPT (fields 8..10), so
   * {@code $cor0.DNAME} is field 9. Rewriting the IN appends 4 auxiliary fields
   * to the left input, which moves DEPT to fields 12..14; with {@code start = 8}
   * and {@code offset = 4}, {@code $cor0.DNAME} becomes field 13 of 15.
   *
   * <p>Fields that the shift pushes outside {@code rowType}, which can only
   * happen when {@code offset} is negative, are left unmapped, so that a
   * reference to one of them is an error rather than a silently wrong index. */
  private static RexNode remapCorrelationVariables(RexNode node,
      Set<CorrelationId> ids, RelDataType rowType, int start, int offset,
      RexBuilder rexBuilder) {
    if (ids.isEmpty()) {
      return node;
    }
    final int targetCount = rowType.getFieldCount();
    final Mapping mapping;
    if (offset == 0) {
      mapping = Mappings.createIdentity(targetCount);
    } else {
      // A mapping from the old positions of the correlation row to the new
      // ones; fields before "start" keep their index, the others move by
      // "offset".
      mapping =
          Mappings.create(MappingType.PARTIAL_FUNCTION, targetCount - offset,
              targetCount);
      for (int source = 0; source < mapping.getSourceCount(); source++) {
        final int target = source < start ? source : source + offset;
        if (target >= 0 && target < targetCount) {
          mapping.set(source, target);
        }
      }
    }
    for (CorrelationId id : ids) {
      node =
          node.accept(
              new RelOptUtil.RexCorrelVariableMapShuttle(id, rowType, mapping,
                  rexBuilder));
    }
    return node;
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

    Config TABLE_FUNCTION_SCAN_SCALAR_QUERY =
        ImmutableSubQueryRemoveRule.Config.builder()
            .withMatchHandler(SubQueryRemoveRule::matchTableFunctionScan)
            .build()
            .withOperandSupplier(b ->
                b.operand(TableFunctionScan.class)
                    .predicate(scan ->
                        RexUtil.SubQueryFinder.find(scan.getCall(),
                            SqlKind.SCALAR_QUERY) != null)
                    .anyInputs())
            .withDescription("SubQueryRemoveRule:TableFunctionScanScalarQuery");

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
