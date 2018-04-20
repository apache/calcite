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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

/**
 * Utility to infer Predicates that are applicable above a RelNode.
 *
 * <p>This is currently used by
 * {@link org.apache.calcite.rel.rules.JoinPushTransitivePredicatesRule} to
 * infer <em>Predicates</em> that can be inferred from one side of a Join
 * to the other.
 *
 * <p>The PullUp Strategy is sound but not complete. Here are some of the
 * limitations:
 * <ol>
 *
 * <li> For Aggregations we only PullUp predicates that only contain
 * Grouping Keys. This can be extended to infer predicates on Aggregation
 * expressions from  expressions on the aggregated columns. For e.g.
 * <pre>
 * select a, max(b) from R1 where b &gt; 7
 *   &rarr; max(b) &gt; 7 or max(b) is null
 * </pre>
 *
 * <li> For Projections we only look at columns that are projected without
 * any function applied. So:
 * <pre>
 * select a from R1 where a &gt; 7
 *   &rarr; "a &gt; 7" is pulled up from the Projection.
 * select a + 1 from R1 where a + 1 &gt; 7
 *   &rarr; "a + 1 gt; 7" is not pulled up
 * </pre>
 *
 * <li> There are several restrictions on Joins:
 *   <ul>
 *   <li> We only pullUp inferred predicates for now. Pulling up existing
 *   predicates causes an explosion of duplicates. The existing predicates
 *   are pushed back down as new predicates. Once we have rules to eliminate
 *   duplicate Filter conditions, we should pullUp all predicates.
 *
 *   <li> For Left Outer: we infer new predicates from the left and set them
 *   as applicable on the Right side. No predicates are pulledUp.
 *
 *   <li> Right Outer Joins are handled in an analogous manner.
 *
 *   <li> For Full Outer Joins no predicates are pulledUp or inferred.
 *   </ul>
 * </ol>
 */
public class RelMdPredicates
    implements MetadataHandler<BuiltInMetadata.Predicates> {
  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider
      .reflectiveSource(BuiltInMethod.PREDICATES.method, new RelMdPredicates());

  private static final List<RexNode> EMPTY_LIST = ImmutableList.of();

  public MetadataDef<BuiltInMetadata.Predicates> getDef() {
    return BuiltInMetadata.Predicates.DEF;
  }

  /** Catch-all implementation for
   * {@link BuiltInMetadata.Predicates#getPredicates()},
   * invoked using reflection.
   *
   * @see org.apache.calcite.rel.metadata.RelMetadataQuery#getPulledUpPredicates(RelNode)
   */
  public RelOptPredicateList getPredicates(RelNode rel, RelMetadataQuery mq) {
    return RelOptPredicateList.EMPTY;
  }

  public RelOptPredicateList getPredicates(HepRelVertex rel,
      RelMetadataQuery mq) {
    return mq.getPulledUpPredicates(rel.getCurrentRel());
  }

  /**
   * Infers predicates for a table scan.
   */
  public RelOptPredicateList getPredicates(TableScan table,
      RelMetadataQuery mq) {
    return RelOptPredicateList.EMPTY;
  }

  /**
   * Infers predicates for a project.
   *
   * <ol>
   * <li>create a mapping from input to projection. Map only positions that
   * directly reference an input column.
   * <li>Expressions that only contain above columns are retained in the
   * Project's pullExpressions list.
   * <li>For e.g. expression 'a + e = 9' below will not be pulled up because 'e'
   * is not in the projection list.
   *
   * <blockquote><pre>
   * inputPullUpExprs:      {a &gt; 7, b + c &lt; 10, a + e = 9}
   * projectionExprs:       {a, b, c, e / 2}
   * projectionPullupExprs: {a &gt; 7, b + c &lt; 10}
   * </pre></blockquote>
   *
   * </ol>
   */
  public RelOptPredicateList getPredicates(Project project,
      RelMetadataQuery mq) {
    final RelNode input = project.getInput();
    final RexBuilder rexBuilder = project.getCluster().getRexBuilder();
    final RelOptPredicateList inputInfo = mq.getPulledUpPredicates(input);
    final List<RexNode> projectPullUpPredicates = new ArrayList<>();

    ImmutableBitSet.Builder columnsMappedBuilder = ImmutableBitSet.builder();
    Mapping m = Mappings.create(MappingType.PARTIAL_FUNCTION,
        input.getRowType().getFieldCount(),
        project.getRowType().getFieldCount());

    for (Ord<RexNode> o : Ord.zip(project.getProjects())) {
      if (o.e instanceof RexInputRef) {
        int sIdx = ((RexInputRef) o.e).getIndex();
        m.set(sIdx, o.i);
        columnsMappedBuilder.set(sIdx);
      }
    }

    // Go over childPullUpPredicates. If a predicate only contains columns in
    // 'columnsMapped' construct a new predicate based on mapping.
    final ImmutableBitSet columnsMapped = columnsMappedBuilder.build();
    for (RexNode r : inputInfo.pulledUpPredicates) {
      RexNode r2 = projectPredicate(rexBuilder, input, r, columnsMapped);
      if (!r2.isAlwaysTrue()) {
        r2 = r2.accept(new RexPermuteInputsShuttle(m, input));
        projectPullUpPredicates.add(r2);
      }
    }

    // Project can also generate constants. We need to include them.
    for (Ord<RexNode> expr : Ord.zip(project.getProjects())) {
      if (RexLiteral.isNullLiteral(expr.e)) {
        projectPullUpPredicates.add(
            rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL,
                rexBuilder.makeInputRef(project, expr.i)));
      } else if (RexUtil.isConstant(expr.e)) {
        final List<RexNode> args =
            ImmutableList.of(rexBuilder.makeInputRef(project, expr.i), expr.e);
        final SqlOperator op = args.get(0).getType().isNullable()
            || args.get(1).getType().isNullable()
            ? SqlStdOperatorTable.IS_NOT_DISTINCT_FROM
            : SqlStdOperatorTable.EQUALS;
        projectPullUpPredicates.add(rexBuilder.makeCall(op, args));
      }
    }
    return RelOptPredicateList.of(rexBuilder, projectPullUpPredicates);
  }

  /** Converts a predicate on a particular set of columns into a predicate on
   * a subset of those columns, weakening if necessary.
   *
   * <p>If not possible to simplify, returns {@code true}, which is the weakest
   * possible predicate.
   *
   * <p>Examples:<ol>
   * <li>The predicate {@code $7 = $9} on columns [7]
   *     becomes {@code $7 is not null}
   * <li>The predicate {@code $7 = $9 + $11} on columns [7, 9]
   *     becomes {@code $7 is not null or $9 is not null}
   * <li>The predicate {@code $7 = $9 and $9 = 5} on columns [7] becomes
   *   {@code $7 = 5}
   * <li>The predicate
   *   {@code $7 = $9 and ($9 = $1 or $9 = $2) and $1 > 3 and $2 > 10}
   *   on columns [7] becomes {@code $7 > 3}
   * </ol>
   *
   * <p>We currently only handle examples 1 and 2.
   *
   * @param rexBuilder Rex builder
   * @param input Input relational expression
   * @param r Predicate expression
   * @param columnsMapped Columns which the final predicate can reference
   * @return Predicate expression narrowed to reference only certain columns
   */
  private RexNode projectPredicate(final RexBuilder rexBuilder, RelNode input,
      RexNode r, ImmutableBitSet columnsMapped) {
    ImmutableBitSet rCols = RelOptUtil.InputFinder.bits(r);
    if (columnsMapped.contains(rCols)) {
      // All required columns are present. No need to weaken.
      return r;
    }
    if (columnsMapped.intersects(rCols)) {
      final List<RexNode> list = new ArrayList<>();
      for (int c : columnsMapped.intersect(rCols)) {
        if (input.getRowType().getFieldList().get(c).getType().isNullable()
            && Strong.isNull(r, ImmutableBitSet.of(c))) {
          list.add(
              rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL,
                  rexBuilder.makeInputRef(input, c)));
        }
      }
      if (!list.isEmpty()) {
        return RexUtil.composeDisjunction(rexBuilder, list);
      }
    }
    // Cannot weaken to anything non-trivial
    return rexBuilder.makeLiteral(true);
  }

  /**
   * Add the Filter condition to the pulledPredicates list from the input.
   */
  public RelOptPredicateList getPredicates(Filter filter, RelMetadataQuery mq) {
    final RelNode input = filter.getInput();
    final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    final RelOptPredicateList inputInfo = mq.getPulledUpPredicates(input);

    return Util.first(inputInfo, RelOptPredicateList.EMPTY)
        .union(rexBuilder,
            RelOptPredicateList.of(rexBuilder,
                RexUtil.retainDeterministic(
                    RelOptUtil.conjunctions(filter.getCondition()))));
  }

  /**
   * Infers predicates for a {@link org.apache.calcite.rel.core.Join} (including
   * {@link org.apache.calcite.rel.core.SemiJoin}).
   */
  public RelOptPredicateList getPredicates(Join join, RelMetadataQuery mq) {
    RelOptCluster cluster = join.getCluster();
    RexBuilder rexBuilder = cluster.getRexBuilder();
    final RexExecutor executor =
        Util.first(cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR);
    final RelNode left = join.getInput(0);
    final RelNode right = join.getInput(1);

    final RelOptPredicateList leftInfo = mq.getPulledUpPredicates(left);
    final RelOptPredicateList rightInfo = mq.getPulledUpPredicates(right);

    final RexSimplify simplifier =
        new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, true, executor);

    JoinConditionBasedPredicateInference joinInference =
        new JoinConditionBasedPredicateInference(join,
            RexUtil.composeConjunction(rexBuilder, leftInfo.pulledUpPredicates, false),
            RexUtil.composeConjunction(rexBuilder, rightInfo.pulledUpPredicates, false),
            simplifier);

    return joinInference.inferPredicates(false);
  }

  /**
   * Infers predicates for an Aggregate.
   *
   * <p>Pulls up predicates that only contains references to columns in the
   * GroupSet. For e.g.
   *
   * <blockquote><pre>
   * inputPullUpExprs : { a &gt; 7, b + c &lt; 10, a + e = 9}
   * groupSet         : { a, b}
   * pulledUpExprs    : { a &gt; 7}
   * </pre></blockquote>
   */
  public RelOptPredicateList getPredicates(Aggregate agg, RelMetadataQuery mq) {
    final RelNode input = agg.getInput();
    final RexBuilder rexBuilder = agg.getCluster().getRexBuilder();
    final RelOptPredicateList inputInfo = mq.getPulledUpPredicates(input);
    final List<RexNode> aggPullUpPredicates = new ArrayList<>();

    ImmutableBitSet groupKeys = agg.getGroupSet();
    if (groupKeys.isEmpty()) {
      // "GROUP BY ()" can convert an empty relation to a non-empty relation, so
      // it is not valid to pull up predicates. In particular, consider the
      // predicate "false": it is valid on all input rows (trivially - there are
      // no rows!) but not on the output (there is one row).
      return RelOptPredicateList.EMPTY;
    }
    Mapping m = Mappings.create(MappingType.PARTIAL_FUNCTION,
        input.getRowType().getFieldCount(), agg.getRowType().getFieldCount());

    int i = 0;
    for (int j : groupKeys) {
      m.set(j, i++);
    }

    for (RexNode r : inputInfo.pulledUpPredicates) {
      ImmutableBitSet rCols = RelOptUtil.InputFinder.bits(r);
      if (groupKeys.contains(rCols)) {
        r = r.accept(new RexPermuteInputsShuttle(m, input));
        aggPullUpPredicates.add(r);
      }
    }
    return RelOptPredicateList.of(rexBuilder, aggPullUpPredicates);
  }

  /**
   * Infers predicates for a Union.
   */
  public RelOptPredicateList getPredicates(Union union, RelMetadataQuery mq) {
    RexBuilder rexBuilder = union.getCluster().getRexBuilder();

    Map<String, RexNode> finalPreds = new HashMap<>();
    List<RexNode> finalResidualPreds = new ArrayList<>();
    for (int i = 0; i < union.getInputs().size(); i++) {
      RelNode input = union.getInputs().get(i);
      RelOptPredicateList info = mq.getPulledUpPredicates(input);
      if (info.pulledUpPredicates.isEmpty()) {
        return RelOptPredicateList.EMPTY;
      }
      Map<String, RexNode> preds = new HashMap<>();
      List<RexNode> residualPreds = new ArrayList<>();
      for (RexNode pred : info.pulledUpPredicates) {
        final String predDigest = pred.toString();
        if (i == 0) {
          preds.put(predDigest, pred);
          continue;
        }
        if (finalPreds.containsKey(predDigest)) {
          preds.put(predDigest, pred);
        } else {
          residualPreds.add(pred);
        }
      }
      // Add new residual preds
      finalResidualPreds.add(RexUtil.composeConjunction(rexBuilder, residualPreds, false));
      // Add those that are not part of the final set to residual
      for (Entry<String, RexNode> e : finalPreds.entrySet()) {
        if (!preds.containsKey(e.getKey())) {
          // This node was in previous union inputs, but it is not in this one
          for (int j = 0; j < i; j++) {
            finalResidualPreds.set(j,
                RexUtil.composeConjunction(rexBuilder,
                    Lists.newArrayList(finalResidualPreds.get(j), e.getValue()), false));
          }
        }
      }
      // Final preds
      finalPreds = preds;
    }

    List<RexNode> preds = new ArrayList<>(finalPreds.values());
    final RelOptCluster cluster = union.getCluster();
    final RexExecutor executor =
        Util.first(cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR);
    final RelOptPredicateList predicates = RelOptPredicateList.EMPTY;
    final RexSimplify simplify =
        new RexSimplify(rexBuilder, predicates, true, executor);
    RexNode disjPred = simplify.simplifyOrs(finalResidualPreds);
    if (!disjPred.isAlwaysTrue()) {
      preds.add(disjPred);
    }
    return RelOptPredicateList.of(rexBuilder, preds);
  }

  /**
   * Infers predicates for a Sort.
   */
  public RelOptPredicateList getPredicates(Sort sort, RelMetadataQuery mq) {
    RelNode input = sort.getInput();
    return mq.getPulledUpPredicates(input);
  }

  /**
   * Infers predicates for an Exchange.
   */
  public RelOptPredicateList getPredicates(Exchange exchange,
      RelMetadataQuery mq) {
    RelNode input = exchange.getInput();
    return mq.getPulledUpPredicates(input);
  }

  /** @see RelMetadataQuery#getPulledUpPredicates(RelNode) */
  public RelOptPredicateList getPredicates(RelSubset r,
      RelMetadataQuery mq) {
    if (!Bug.CALCITE_1048_FIXED) {
      return RelOptPredicateList.EMPTY;
    }
    final RexBuilder rexBuilder = r.getCluster().getRexBuilder();
    RelOptPredicateList list = null;
    for (RelNode r2 : r.getRels()) {
      RelOptPredicateList list2 = mq.getPulledUpPredicates(r2);
      if (list2 != null) {
        list = list == null ? list2 : list.union(rexBuilder, list2);
      }
    }
    return Util.first(list, RelOptPredicateList.EMPTY);
  }

  /**
   * Utility to infer predicates from one side of the join that apply on the
   * other side.
   *
   * <p>Contract is:<ul>
   *
   * <li>initialize with a {@link org.apache.calcite.rel.core.Join} and
   * optional predicates applicable on its left and right subtrees.
   *
   * <li>you can
   * then ask it for equivalentPredicate(s) given a predicate.
   *
   * </ul>
   *
   * <p>So for:
   * <ol>
   * <li>'<code>R1(x) join R2(y) on x = y</code>' a call for
   * equivalentPredicates on '<code>x &gt; 7</code>' will return '
   * <code>[y &gt; 7]</code>'
   * <li>'<code>R1(x) join R2(y) on x = y join R3(z) on y = z</code>' a call for
   * equivalentPredicates on the second join '<code>x &gt; 7</code>' will return
   * </ol>
   */
  static class JoinConditionBasedPredicateInference {
    final Join joinRel;
    final boolean isSemiJoin;
    final int nSysFields;
    final int nFieldsLeft;
    final int nFieldsRight;
    final ImmutableBitSet leftFieldsBitSet;
    final ImmutableBitSet rightFieldsBitSet;
    final ImmutableBitSet allFieldsBitSet;
    SortedMap<Integer, BitSet> equivalence;
    final Map<String, ImmutableBitSet> exprFields;
    final Set<String> allExprDigests;
    final Set<String> equalityPredicates;
    final RexNode leftChildPredicates;
    final RexNode rightChildPredicates;
    final RexSimplify simplifier;

    JoinConditionBasedPredicateInference(Join joinRel,
            RexNode lPreds, RexNode rPreds, RexSimplify simplifier) {
      this(joinRel, joinRel instanceof SemiJoin, lPreds, rPreds, simplifier);
    }

    private JoinConditionBasedPredicateInference(Join joinRel, boolean isSemiJoin,
        RexNode lPreds, RexNode rPreds, RexSimplify simplifier) {
      super();
      this.joinRel = joinRel;
      this.isSemiJoin = isSemiJoin;
      this.simplifier = simplifier;
      nFieldsLeft = joinRel.getLeft().getRowType().getFieldList().size();
      nFieldsRight = joinRel.getRight().getRowType().getFieldList().size();
      nSysFields = joinRel.getSystemFieldList().size();
      leftFieldsBitSet = ImmutableBitSet.range(nSysFields,
          nSysFields + nFieldsLeft);
      rightFieldsBitSet = ImmutableBitSet.range(nSysFields + nFieldsLeft,
          nSysFields + nFieldsLeft + nFieldsRight);
      allFieldsBitSet = ImmutableBitSet.range(0,
          nSysFields + nFieldsLeft + nFieldsRight);

      exprFields = Maps.newHashMap();
      allExprDigests = new HashSet<>();

      if (lPreds == null) {
        leftChildPredicates = null;
      } else {
        Mappings.TargetMapping leftMapping = Mappings.createShiftMapping(
            nSysFields + nFieldsLeft, nSysFields, 0, nFieldsLeft);
        leftChildPredicates = lPreds.accept(
            new RexPermuteInputsShuttle(leftMapping, joinRel.getInput(0)));

        allExprDigests.add(leftChildPredicates.toString());
        for (RexNode r : RelOptUtil.conjunctions(leftChildPredicates)) {
          exprFields.put(r.toString(), RelOptUtil.InputFinder.bits(r));
          allExprDigests.add(r.toString());
        }
      }
      if (rPreds == null) {
        rightChildPredicates = null;
      } else {
        Mappings.TargetMapping rightMapping = Mappings.createShiftMapping(
            nSysFields + nFieldsLeft + nFieldsRight,
            nSysFields + nFieldsLeft, 0, nFieldsRight);
        rightChildPredicates = rPreds.accept(
            new RexPermuteInputsShuttle(rightMapping, joinRel.getInput(1)));

        allExprDigests.add(rightChildPredicates.toString());
        for (RexNode r : RelOptUtil.conjunctions(rightChildPredicates)) {
          exprFields.put(r.toString(), RelOptUtil.InputFinder.bits(r));
          allExprDigests.add(r.toString());
        }
      }

      equivalence = Maps.newTreeMap();
      equalityPredicates = new HashSet<>();
      for (int i = 0; i < nSysFields + nFieldsLeft + nFieldsRight; i++) {
        equivalence.put(i, BitSets.of(i));
      }

      // Only process equivalences found in the join conditions. Processing
      // Equivalences from the left or right side infer predicates that are
      // already present in the Tree below the join.
      RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
      List<RexNode> exprs =
          RelOptUtil.conjunctions(
              compose(rexBuilder, ImmutableList.of(joinRel.getCondition())));

      final EquivalenceFinder eF = new EquivalenceFinder();
      new ArrayList<>(
          Lists.transform(exprs,
              new Function<RexNode, Void>() {
                public Void apply(RexNode input) {
                  return input.accept(eF);
                }
              }));

      equivalence = BitSets.closure(equivalence);
    }

    /**
     * The PullUp Strategy is sound but not complete.
     * <ol>
     * <li>We only pullUp inferred predicates for now. Pulling up existing
     * predicates causes an explosion of duplicates. The existing predicates are
     * pushed back down as new predicates. Once we have rules to eliminate
     * duplicate Filter conditions, we should pullUp all predicates.
     * <li>For Left Outer: we infer new predicates from the left and set them as
     * applicable on the Right side. No predicates are pulledUp.
     * <li>Right Outer Joins are handled in an analogous manner.
     * <li>For Full Outer Joins no predicates are pulledUp or inferred.
     * </ol>
     */
    public RelOptPredicateList inferPredicates(
        boolean includeEqualityInference) {
      final List<RexNode> inferredPredicates = new ArrayList<>();
      final Set<String> allExprDigests = new HashSet<>(this.allExprDigests);
      final JoinRelType joinType = joinRel.getJoinType();
      switch (joinType) {
      case INNER:
      case LEFT:
        infer(leftChildPredicates, allExprDigests, inferredPredicates,
            includeEqualityInference,
            joinType == JoinRelType.LEFT ? rightFieldsBitSet
                : allFieldsBitSet);
        break;
      }
      switch (joinType) {
      case INNER:
      case RIGHT:
        infer(rightChildPredicates, allExprDigests, inferredPredicates,
            includeEqualityInference,
            joinType == JoinRelType.RIGHT ? leftFieldsBitSet
                : allFieldsBitSet);
        break;
      }

      Mappings.TargetMapping rightMapping = Mappings.createShiftMapping(
          nSysFields + nFieldsLeft + nFieldsRight,
          0, nSysFields + nFieldsLeft, nFieldsRight);
      final RexPermuteInputsShuttle rightPermute =
          new RexPermuteInputsShuttle(rightMapping, joinRel);
      Mappings.TargetMapping leftMapping = Mappings.createShiftMapping(
          nSysFields + nFieldsLeft, 0, nSysFields, nFieldsLeft);
      final RexPermuteInputsShuttle leftPermute =
          new RexPermuteInputsShuttle(leftMapping, joinRel);
      final List<RexNode> leftInferredPredicates = new ArrayList<>();
      final List<RexNode> rightInferredPredicates = new ArrayList<>();

      for (RexNode iP : inferredPredicates) {
        ImmutableBitSet iPBitSet = RelOptUtil.InputFinder.bits(iP);
        if (leftFieldsBitSet.contains(iPBitSet)) {
          leftInferredPredicates.add(iP.accept(leftPermute));
        } else if (rightFieldsBitSet.contains(iPBitSet)) {
          rightInferredPredicates.add(iP.accept(rightPermute));
        }
      }

      final RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
      switch (joinType) {
      case INNER:
        Iterable<RexNode> pulledUpPredicates;
        if (isSemiJoin) {
          pulledUpPredicates = Iterables.concat(
                RelOptUtil.conjunctions(leftChildPredicates),
                leftInferredPredicates);
        } else {
          pulledUpPredicates = Iterables.concat(
                RelOptUtil.conjunctions(leftChildPredicates),
                RelOptUtil.conjunctions(rightChildPredicates),
                RexUtil.retainDeterministic(
                  RelOptUtil.conjunctions(joinRel.getCondition())),
                inferredPredicates);
        }
        return RelOptPredicateList.of(rexBuilder, pulledUpPredicates,
          leftInferredPredicates, rightInferredPredicates);
      case LEFT:
        return RelOptPredicateList.of(rexBuilder,
            RelOptUtil.conjunctions(leftChildPredicates),
            leftInferredPredicates, rightInferredPredicates);
      case RIGHT:
        return RelOptPredicateList.of(rexBuilder,
            RelOptUtil.conjunctions(rightChildPredicates),
            inferredPredicates, EMPTY_LIST);
      default:
        assert inferredPredicates.size() == 0;
        return RelOptPredicateList.EMPTY;
      }
    }

    public RexNode left() {
      return leftChildPredicates;
    }

    public RexNode right() {
      return rightChildPredicates;
    }

    private void infer(RexNode predicates, Set<String> allExprsDigests,
        List<RexNode> inferredPredicates, boolean includeEqualityInference,
        ImmutableBitSet inferringFields) {
      for (RexNode r : RelOptUtil.conjunctions(predicates)) {
        if (!includeEqualityInference
            && equalityPredicates.contains(r.toString())) {
          continue;
        }
        for (Mapping m : mappings(r)) {
          RexNode tr = r.accept(
              new RexPermuteInputsShuttle(m, joinRel.getInput(0),
                  joinRel.getInput(1)));
          // Filter predicates can be already simplified, so we should work with
          // simplified RexNode versions as well. It also allows prevent of having
          // some duplicates in in result pulledUpPredicates
          RexNode simplifiedTarget =
              simplifier.simplifyFilterPredicates(RelOptUtil.conjunctions(tr));
          if (checkTarget(inferringFields, allExprsDigests, tr)
              && checkTarget(inferringFields, allExprsDigests, simplifiedTarget)) {
            inferredPredicates.add(simplifiedTarget);
            allExprsDigests.add(simplifiedTarget.toString());
          }
        }
      }
    }

    Iterable<Mapping> mappings(final RexNode predicate) {
      return new Iterable<Mapping>() {
        public Iterator<Mapping> iterator() {
          ImmutableBitSet fields = exprFields.get(predicate.toString());
          if (fields.cardinality() == 0) {
            return Collections.emptyIterator();
          }
          return new ExprsItr(fields);
        }
      };
    }

    private boolean checkTarget(ImmutableBitSet inferringFields,
        Set<String> allExprsDigests, RexNode tr) {
      return inferringFields.contains(RelOptUtil.InputFinder.bits(tr))
          && !allExprsDigests.contains(tr.toString())
          && !isAlwaysTrue(tr);
    }

    private void equivalent(int p1, int p2) {
      BitSet b = equivalence.get(p1);
      b.set(p2);

      b = equivalence.get(p2);
      b.set(p1);
    }

    RexNode compose(RexBuilder rexBuilder, Iterable<RexNode> exprs) {
      exprs = Linq4j.asEnumerable(exprs).where(new Predicate1<RexNode>() {
        public boolean apply(RexNode expr) {
          return expr != null;
        }
      });
      return RexUtil.composeConjunction(rexBuilder, exprs, false);
    }

    /**
     * Find expressions of the form 'col_x = col_y'.
     */
    class EquivalenceFinder extends RexVisitorImpl<Void> {
      protected EquivalenceFinder() {
        super(true);
      }

      @Override public Void visitCall(RexCall call) {
        if (call.getOperator().getKind() == SqlKind.EQUALS) {
          int lPos = pos(call.getOperands().get(0));
          int rPos = pos(call.getOperands().get(1));
          if (lPos != -1 && rPos != -1) {
            JoinConditionBasedPredicateInference.this.equivalent(lPos, rPos);
            JoinConditionBasedPredicateInference.this.equalityPredicates
                .add(call.toString());
          }
        }
        return null;
      }
    }

    /**
     * Given an expression returns all the possible substitutions.
     *
     * <p>For example, for an expression 'a + b + c' and the following
     * equivalences: <pre>
     * a : {a, b}
     * b : {a, b}
     * c : {c, e}
     * </pre>
     *
     * <p>The following Mappings will be returned:
     * <pre>
     * {a &rarr; a, b &rarr; a, c &rarr; c}
     * {a &rarr; a, b &rarr; a, c &rarr; e}
     * {a &rarr; a, b &rarr; b, c &rarr; c}
     * {a &rarr; a, b &rarr; b, c &rarr; e}
     * {a &rarr; b, b &rarr; a, c &rarr; c}
     * {a &rarr; b, b &rarr; a, c &rarr; e}
     * {a &rarr; b, b &rarr; b, c &rarr; c}
     * {a &rarr; b, b &rarr; b, c &rarr; e}
     * </pre>
     *
     * <p>which imply the following inferences:
     * <pre>
     * a + a + c
     * a + a + e
     * a + b + c
     * a + b + e
     * b + a + c
     * b + a + e
     * b + b + c
     * b + b + e
     * </pre>
     */
    class ExprsItr implements Iterator<Mapping> {
      final int[] columns;
      final BitSet[] columnSets;
      final int[] iterationIdx;
      Mapping nextMapping;
      boolean firstCall;

      ExprsItr(ImmutableBitSet fields) {
        nextMapping = null;
        columns = new int[fields.cardinality()];
        columnSets = new BitSet[fields.cardinality()];
        iterationIdx = new int[fields.cardinality()];
        for (int j = 0, i = fields.nextSetBit(0); i >= 0; i = fields
            .nextSetBit(i + 1), j++) {
          columns[j] = i;
          columnSets[j] = equivalence.get(i);
          iterationIdx[j] = 0;
        }
        firstCall = true;
      }

      public boolean hasNext() {
        if (firstCall) {
          initializeMapping();
          firstCall = false;
        } else {
          computeNextMapping(iterationIdx.length - 1);
        }
        return nextMapping != null;
      }

      public Mapping next() {
        return nextMapping;
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }

      private void computeNextMapping(int level) {
        int t = columnSets[level].nextSetBit(iterationIdx[level]);
        if (t < 0) {
          if (level == 0) {
            nextMapping = null;
          } else {
            int tmp = columnSets[level].nextSetBit(0);
            nextMapping.set(columns[level], tmp);
            iterationIdx[level] = tmp + 1;
            computeNextMapping(level - 1);
          }
        } else {
          nextMapping.set(columns[level], t);
          iterationIdx[level] = t + 1;
        }
      }

      private void initializeMapping() {
        nextMapping = Mappings.create(MappingType.PARTIAL_FUNCTION,
            nSysFields + nFieldsLeft + nFieldsRight,
            nSysFields + nFieldsLeft + nFieldsRight);
        for (int i = 0; i < columnSets.length; i++) {
          BitSet c = columnSets[i];
          int t = c.nextSetBit(iterationIdx[i]);
          if (t < 0) {
            nextMapping = null;
            return;
          }
          nextMapping.set(columns[i], t);
          iterationIdx[i] = t + 1;
        }
      }
    }

    private int pos(RexNode expr) {
      if (expr instanceof RexInputRef) {
        return ((RexInputRef) expr).getIndex();
      }
      return -1;
    }

    private boolean isAlwaysTrue(RexNode predicate) {
      if (predicate instanceof RexCall) {
        RexCall c = (RexCall) predicate;
        if (c.getOperator().getKind() == SqlKind.EQUALS) {
          int lPos = pos(c.getOperands().get(0));
          int rPos = pos(c.getOperands().get(1));
          return lPos != -1 && lPos == rPos;
        }
      }
      return predicate.isAlwaysTrue();
    }
  }
}

// End RelMdPredicates.java
