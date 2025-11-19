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
package org.apache.calcite.sql2rel;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql2rel.RelDecorrelator.CorDef;
import org.apache.calcite.sql2rel.RelDecorrelator.Frame;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;

/**
 * A top‑down, generic decorrelation algorithm that can handle deep nestings of correlated
 * subqueries and that generalizes to complex query constructs. More details are in paper:
 * <a href="https://dl.gi.de/items/b9df4765-d1b0-4267-a77c-4ce4ab0ee62d">
 *   Improving Unnesting of Complex Queries</a>
 */
public class TopDownGeneralDecorrelator implements ReflectiveVisitor {

  private final RelBuilder builder;

  // record the CorDef in the current context (including those in the parent Correlate).
  private final NavigableSet<CorDef> corDefs;

  // a map from RelNode to whether existing correlated expressions (according to corDefs).
  private final Map<RelNode, Boolean> hasCorrelatedExpressions;

  // a map from RelNode to its UnnestQuery.
  private final Map<RelNode, UnnestQuery> mapRelToUnnestQuery;

  private final boolean hasParent;

  // whether allow empty output resulting from decorrelate rewriting.
  private boolean emptyOutputDisable;

  // the domain of the free variables (i.e. corDefs) D, it's duplicate free.
  private DedupFreeVarsNode dedupFreeVarsNode;

  @SuppressWarnings("method.invocation.invalid")
  private final ReflectUtil.MethodDispatcher<RelNode> dispatcher =
      ReflectUtil.createMethodDispatcher(
          RelNode.class, getVisitor(), "unnestInternal", RelNode.class);

  @SuppressWarnings("initialization.fields.uninitialized")
  public TopDownGeneralDecorrelator(
      RelBuilder builder,
      boolean hasParent,
      boolean emptyOutputDisable,
      @Nullable Set<CorDef> parentCorDefs,
      @Nullable Map<RelNode, Boolean> parentHasCorrelatedExpressions,
      @Nullable Map<RelNode, UnnestQuery> parentMapRelToUnnestInfo) {
    this.builder = builder;
    this.hasParent = hasParent;
    this.emptyOutputDisable = emptyOutputDisable;
    this.corDefs = new TreeSet<>();
    if (parentCorDefs != null) {
      this.corDefs.addAll(parentCorDefs);
    }
    this.hasCorrelatedExpressions = parentHasCorrelatedExpressions == null
        ? new HashMap<>()
        : parentHasCorrelatedExpressions;
    this.mapRelToUnnestQuery = parentMapRelToUnnestInfo == null
        ? new HashMap<>()
        : parentMapRelToUnnestInfo;
  }

  private static TopDownGeneralDecorrelator createEmptyDecorrelator(RelBuilder builder) {
    return new TopDownGeneralDecorrelator(builder, false, false, null, null, null);
  }

  private TopDownGeneralDecorrelator createSubDecorrelator() {
    TopDownGeneralDecorrelator subDecorrelator =
        new TopDownGeneralDecorrelator(
            builder,
            true,
            emptyOutputDisable,
            corDefs,
            hasCorrelatedExpressions,
            mapRelToUnnestQuery);
    subDecorrelator.dedupFreeVarsNode = this.dedupFreeVarsNode;
    return subDecorrelator;
  }

  /**
   * Decorrelates a query. This is the entry point for this class.
   *
   * @param rel     Root node of the query
   * @param builder RelBuilder
   * @return  Equivalent node without correlation
   */
  public static RelNode decorrelateQuery(RelNode rel, RelBuilder builder) {
    HepProgram preProgram = HepProgram.builder()
        .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
        .addRuleInstance(CoreRules.FILTER_CORRELATE)
        .build();
    HepPlanner prePlanner = new HepPlanner(preProgram);
    prePlanner.setRoot(rel);
    RelNode preparedRel = prePlanner.findBestExp();

    // start decorrelating
    TopDownGeneralDecorrelator decorrelator = createEmptyDecorrelator(builder);
    RelNode decorrelateNode = rel;
    try {
      decorrelateNode = decorrelator.correlateElimination(preparedRel);
    } catch (UnsupportedOperationException e) {
      // if the correlation exists in an unsupported operator, retain the original plan.
    }

    HepProgram postProgram = HepProgram.builder()
        .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
        .addRuleInstance(CoreRules.MARK_TO_SEMI_OR_ANTI_JOIN_RULE)
        .addRuleInstance(CoreRules.PROJECT_MERGE)
        .addRuleInstance(CoreRules.PROJECT_REMOVE)
        .build();
    HepPlanner postPlanner = new HepPlanner(postProgram);
    postPlanner.setRoot(decorrelateNode);
    return postPlanner.findBestExp();
  }

  /**
   * Eliminates Correlate.
   *
   * @param rel RelNode
   * @return  Equivalent RelNode without Correlate
   */
  private RelNode correlateElimination(RelNode rel) {
    if (rel instanceof Correlate) {
      final Correlate correlate = (Correlate) rel;
      final RelNode newLeft;
      if (hasParent) {
        // if the current decorrelator has a parent, it means that the Correlate must have
        // correlation from above.
        assert hasCorrelatedExpressions.containsKey(correlate)
            && hasCorrelatedExpressions.get(correlate);
        newLeft = unnest(correlate.getLeft());
      } else {
        // otherwise, start a new decorrelation for the left side.
        newLeft = decorrelateQuery(correlate.getLeft(), builder);
      }

      // create or update UnnestQuery of left side and corDefs of this decorrelator.
      UnnestQuery leftInfo = mapRelToUnnestQuery.get(correlate.getLeft());
      TreeMap<CorDef, Integer> corDefOutputs = new TreeMap<>();
      Map<Integer, Integer> oldToNewOutputs = new HashMap<>();
      for (int i = 0; i < correlate.getLeft().getRowType().getFieldCount(); i++) {
        int newColumnIndex = leftInfo == null ? i : requireNonNull(leftInfo.oldToNewOutputs.get(i));
        oldToNewOutputs.put(i, newColumnIndex);
        if (correlate.getRequiredColumns().get(i)) {
          CorDef corDef = new CorDef(correlate.getCorrelationId(), i);
          corDefs.add(corDef);
          corDefOutputs.put(corDef, newColumnIndex);
        }
      }
      if (leftInfo != null) {
        corDefOutputs.putAll(leftInfo.corDefOutputs);
      }
      leftInfo = new UnnestQuery(correlate.getLeft(), newLeft, corDefOutputs, oldToNewOutputs);
      dedupFreeVarsNode = generateDedupFreeVarsNode(newLeft, leftInfo);

      // decorrelate right side
      detectCorrelatedExpressions(correlate.getRight());
      emptyOutputDisable |= correlate.getJoinType() == JoinRelType.MARK;
      RelNode newRight = unnest(correlate.getRight());
      UnnestQuery rightInfo = requireNonNull(mapRelToUnnestQuery.get(correlate.getRight()));

      // rewrite condition, adding the natural join condition between the left side and
      // the domain D that in right side.
      //
      //         Correlate(condition=[p])
      //           /   \
      //          L     ... with correlation
      //                  \
      //                   R
      // =>
      //         Join(condition=[p AND (L is not distinct from D)])
      //          /   \
      //         L     ... without correlation
      //                \
      //                 x
      //                / \
      //               D   R
      builder.push(newLeft).push(newRight);
      RexNode unnestedJoinCondition =
          createUnnestedJoinCondition(correlate.getCondition(), leftInfo, rightInfo, true);
      RelNode unnestedRel = builder.join(correlate.getJoinType(), unnestedJoinCondition).build();

      if (!hasParent) {
        // ensure that the fields are in the same order as in the original plan.
        builder.push(unnestedRel);
        UnnestQuery unnestQuery =
            createJoinUnnestInfo(
                leftInfo,
                rightInfo,
                correlate,
                unnestedRel,
                correlate.getJoinType());
        List<RexNode> projects
            = builder.fields(new ArrayList<>(unnestQuery.oldToNewOutputs.values()));
        unnestedRel = builder.project(projects).build();
      }
      return unnestedRel;
    } else {
      for (int i = 0; i < rel.getInputs().size(); i++) {
        rel.replaceInput(i, correlateElimination(rel.getInput(i)));
      }
    }
    return rel;
  }

  /**
   * Generate the domain of the free variables D.
   *
   * @param newLeft   the left side (without correlation) of Correlate
   * @param leftInfo  the UnnestQuery of the left side of Correlate
   * @return  the domain of the free variables D
   */
  private DedupFreeVarsNode generateDedupFreeVarsNode(RelNode newLeft, UnnestQuery leftInfo) {
    List<Integer> columnIndexes = new ArrayList<>();
    for (CorDef corDef : corDefs) {
      int fieldIndex = requireNonNull(leftInfo.corDefOutputs.get(corDef));
      columnIndexes.add(fieldIndex);
    }
    List<RexNode> inputRefs = builder.push(newLeft)
        .fields(columnIndexes);
    RelNode rel = builder.project(inputRefs).distinct().build();
    return new DedupFreeVarsNode(rel);
  }

  /**
   * Detects whether existing correlated expressions in the RelNode (according to corDefs).
   *
   * @param rel RelNode
   * @return true when there are correlated expressions
   */
  private boolean detectCorrelatedExpressions(RelNode rel) {
    boolean hasCorrelation = false;
    for (RelNode input : rel.getInputs()) {
      hasCorrelation |= detectCorrelatedExpressions(input);
    }
    if (!hasCorrelation) {
      RelOptUtil.VariableUsedVisitor variableUsedVisitor =
          new RelOptUtil.VariableUsedVisitor(null);
      rel.accept(variableUsedVisitor);
      Set<CorrelationId> corrIdSet
          = corDefs.stream()
          .map(corDef -> corDef.corr)
          .collect(ImmutableSet.toImmutableSet());
      hasCorrelation =
          !variableUsedVisitor.variables.isEmpty()
              && corrIdSet.containsAll(variableUsedVisitor.variables);
    }
    hasCorrelatedExpressions.put(rel, hasCorrelation);
    return hasCorrelation;
  }

  /**
   * Create the new join condition after decorrelating.
   *
   * @param oriCondition        original Correlate/Join condition
   * @param leftInfo            UnnestQuery of the left side
   * @param rightInfo           UnnestQuery of the right side
   * @param needNatureJoinCond  whether need to add the natural join condition for domain D
   * @return the new join condition
   */
  private RexNode createUnnestedJoinCondition(
      RexNode oriCondition,
      UnnestQuery leftInfo,
      UnnestQuery rightInfo,
      boolean needNatureJoinCond) {
    // create a virtual inner join and its UnnestQuery to help rewrite the
    // original condition by CorrelatedExprRewriter
    Map<Integer, Integer> virtualOldToNewOutputs = new HashMap<>();
    int oriLeftFieldCount = leftInfo.oldRel.getRowType().getFieldCount();
    int newLeftFieldCount = leftInfo.r.getRowType().getFieldCount();
    virtualOldToNewOutputs.putAll(leftInfo.oldToNewOutputs);
    rightInfo.oldToNewOutputs.forEach((oriIndex, newIndex) ->
        virtualOldToNewOutputs.put(
            requireNonNull(oriIndex, "oriIndex") + oriLeftFieldCount,
            requireNonNull(newIndex, "newIndex") + newLeftFieldCount));

    TreeMap<CorDef, Integer> virtualCorDefOutputs = new TreeMap<>();
    if (!leftInfo.corDefOutputs.isEmpty()) {
      virtualCorDefOutputs.putAll(leftInfo.corDefOutputs);
    } else if (!rightInfo.corDefOutputs.isEmpty()) {
      rightInfo.corDefOutputs.forEach((corDef, index) ->
          virtualCorDefOutputs.put(corDef, index + newLeftFieldCount));
    } else {
      throw new IllegalArgumentException("The UnnestInfo for both sides of Join/Correlate that has "
          + "correlation should not all be empty.");
    }
    RelNode virtualOldRel = builder.push(leftInfo.oldRel).push(rightInfo.oldRel)
        .join(JoinRelType.INNER)
        .build();
    RelNode virtualNewRel = builder.push(leftInfo.r).push(rightInfo.r)
        .join(JoinRelType.INNER)
        .build();
    UnnestQuery virtualInfo =
        new UnnestQuery(virtualOldRel, virtualNewRel, virtualCorDefOutputs, virtualOldToNewOutputs);
    RexNode rewriteOriCondition = CorrelatedExprRewriter.rewrite(oriCondition, virtualInfo);
    List<RexNode> unnestedJoinConditions = new ArrayList<>();
    unnestedJoinConditions.add(rewriteOriCondition);

    if (needNatureJoinCond) {
      for (CorDef corDef : corDefs) {
        int leftIndex = requireNonNull(leftInfo.corDefOutputs.get(corDef));
        RelDataType leftColumnType
            = leftInfo.r.getRowType().getFieldList().get(leftIndex).getType();
        int rightIndex = requireNonNull(rightInfo.corDefOutputs.get(corDef));
        RelDataType rightColumnType
            = rightInfo.r.getRowType().getFieldList().get(rightIndex).getType();
        RexNode notDistinctFrom =
            builder.isNotDistinctFrom(
                new RexInputRef(leftIndex, leftColumnType),
                new RexInputRef(rightIndex + newLeftFieldCount, rightColumnType));
        unnestedJoinConditions.add(notDistinctFrom);
      }
    }
    return RexUtil.composeConjunction(builder.getRexBuilder(), unnestedJoinConditions);
  }

  /**
   * Create UnnestQuery for Join/Correlate after decorrelating.
   *
   * @param leftInfo          UnnestQuery of the left side
   * @param rightInfo         UnnestQuery of the right side
   * @param oriJoinNode       original Join/Correlate node
   * @param unnestedJoinNode  new node after decorrelating
   * @param joinRelType       join type of original Join/Correlate
   * @return UnnestQuery
   */
  private UnnestQuery createJoinUnnestInfo(
      UnnestQuery leftInfo,
      UnnestQuery rightInfo,
      RelNode oriJoinNode,
      RelNode unnestedJoinNode,
      JoinRelType joinRelType) {
    Map<Integer, Integer> oldToNewOutputs = new HashMap<>();
    oldToNewOutputs.putAll(leftInfo.oldToNewOutputs);
    int oriLeftFieldCount = leftInfo.oldRel.getRowType().getFieldCount();
    int newLeftFieldCount = leftInfo.r.getRowType().getFieldCount();
    switch (joinRelType) {
    case SEMI:
    case ANTI:
      break;
    case MARK:
      oldToNewOutputs.put(oriLeftFieldCount, newLeftFieldCount);
      break;
    default:
      rightInfo.oldToNewOutputs.forEach((oriIndex, newIndex) ->
          oldToNewOutputs.put(
              requireNonNull(oriIndex, "oriIndex") + oriLeftFieldCount,
              requireNonNull(newIndex, "newIndex") + newLeftFieldCount));
      break;
    }

    TreeMap<CorDef, Integer> corDefOutputs = new TreeMap<>();
    if (!leftInfo.corDefOutputs.isEmpty()) {
      corDefOutputs.putAll(leftInfo.corDefOutputs);
    } else if (!rightInfo.corDefOutputs.isEmpty()) {
      Litmus.THROW.check(joinRelType.projectsRight(),
          "If the joinType doesn't project right, its left side must have UnnestInfo.");
      rightInfo.corDefOutputs.forEach((corDef, index) ->
          corDefOutputs.put(corDef, index + newLeftFieldCount));
    } else {
      throw new IllegalArgumentException("The UnnestInfo for both sides of Join/Correlate that has "
          + "correlation should not all be empty.");
    }
    return new UnnestQuery(oriJoinNode, unnestedJoinNode, corDefOutputs, oldToNewOutputs);
  }

  /**
   * Unnests a RelNode. If there is no correlation in the node, create the cross product
   * with domain D; otherwise, dispatch to specific method to push down D.
   *
   * @param rel RelNode
   * @return new node (contains domain D) without correlation
   */
  private RelNode unnest(RelNode rel) {
    if (!requireNonNull(hasCorrelatedExpressions.get(rel))) {
      RelNode newRel
          = builder.push(decorrelateQuery(rel, builder))
              .push(dedupFreeVarsNode.r)
              .join(JoinRelType.INNER)
              .build();
      Map<Integer, Integer> oldToNewOutputs = new HashMap<>();
      IntStream.range(0, rel.getRowType().getFieldCount())
          .forEach(i -> oldToNewOutputs.put(i, i));

      int offset = rel.getRowType().getFieldCount();
      TreeMap<CorDef, Integer> corDefOutputs = new TreeMap<>();
      for (CorDef corDef : corDefs) {
        corDefOutputs.put(corDef, offset++);
      }

      UnnestQuery unnestQuery
          = new UnnestQuery(rel, newRel, corDefOutputs, oldToNewOutputs);
      mapRelToUnnestQuery.put(rel, unnestQuery);
      return newRel;
    }
    return dispatcher.invoke(rel);
  }

  public RelNode unnestInternal(Filter filter) {
    Map<Integer, Integer> oldToNewOutputs = new HashMap<>();
    TreeMap<CorDef, Integer> corDefOutputs = new TreeMap<>();
    List<RexNode> newConditions = new ArrayList<>();
    // try to replace all free variables to input refs according to equi-conditions
    if (tryReplaceFreeVarsToInputRef(filter, corDefOutputs, newConditions)) {
      // all free variables can be replaced, no need to push down D, that is, D is eliminated.
      builder.push(filter.getInput()).filter(newConditions);
      for (int i = 0; i < filter.getRowType().getFieldCount(); i++) {
        oldToNewOutputs.put(i, i);
      }
    } else {
      // push down D
      RelNode newInput = unnest(filter.getInput());
      UnnestQuery inputInfo = requireNonNull(mapRelToUnnestQuery.get(filter.getInput()));
      RexNode newCondition =
          CorrelatedExprRewriter.rewrite(filter.getCondition(), inputInfo);
      builder.push(newInput).filter(newCondition);
      oldToNewOutputs = inputInfo.oldToNewOutputs;
      corDefOutputs.putAll(inputInfo.corDefOutputs);
    }
    RelNode newFilter = builder.build();
    UnnestQuery unnestQuery = new UnnestQuery(filter, newFilter, corDefOutputs, oldToNewOutputs);
    mapRelToUnnestQuery.put(filter, unnestQuery);
    return newFilter;
  }

  public RelNode unnestInternal(Project project) {
    for (RexNode expr : project.getProjects()) {
      if (!Strong.isStrong(expr)) {
        emptyOutputDisable = true;
      }
    }
    RelNode newInput = unnest(project.getInput());
    UnnestQuery inputInfo = requireNonNull(mapRelToUnnestQuery.get(project.getInput()));
    List<RexNode> newProjects
        = CorrelatedExprRewriter.rewrite(project.getProjects(), inputInfo);

    int oriFieldCount = newProjects.size();
    Map<Integer, Integer> oldToNewOutputs = new HashMap<>();
    IntStream.range(0, oriFieldCount).forEach(i -> oldToNewOutputs.put(i, i));

    builder.push(newInput);
    TreeMap<CorDef, Integer> corDefOutputs = new TreeMap<>();
    for (CorDef corDef : corDefs) {
      newProjects.add(builder.field(requireNonNull(inputInfo.corDefOutputs.get(corDef))));
      corDefOutputs.put(corDef, oriFieldCount++);
    }
    RelNode newProject = builder.project(newProjects, ImmutableList.of(), true).build();
    UnnestQuery unnestQuery
        = new UnnestQuery(project, newProject, corDefOutputs, oldToNewOutputs);
    mapRelToUnnestQuery.put(project, unnestQuery);
    return newProject;
  }

  public RelNode unnestInternal(Aggregate aggregate) {
    RelNode newInput = unnest(aggregate.getInput());
    UnnestQuery inputUnnestQuery =
        requireNonNull(mapRelToUnnestQuery.get(aggregate.getInput()));
    builder.push(newInput);

    // create new groupSet and groupSets, adding the fields in D to group keys
    ImmutableBitSet.Builder corKeyBuilder = ImmutableBitSet.builder();
    for (CorDef corDef : corDefs) {
      int corKeyIndex = requireNonNull(inputUnnestQuery.corDefOutputs.get(corDef));
      corKeyBuilder.set(corKeyIndex);
    }
    ImmutableBitSet corKeyBitSet = corKeyBuilder.build();
    ImmutableBitSet newGroupSet
        = aggregate.getGroupSet().permute(inputUnnestQuery.oldToNewOutputs)
            .union(corKeyBitSet);
    List<ImmutableBitSet> newGroupSets = new ArrayList<>();
    for (ImmutableBitSet bitSet : aggregate.getGroupSets()) {
      ImmutableBitSet newBitSet
          = bitSet.permute(inputUnnestQuery.oldToNewOutputs).union(corKeyBitSet);
      newGroupSets.add(newBitSet);
    }

    // create new aggregate functions
    boolean hasCountFunction = false;
    List<AggregateCall> permutedAggCalls = new ArrayList<>();
    Mappings.TargetMapping targetMapping =
        Mappings.target(
            inputUnnestQuery.oldToNewOutputs,
            inputUnnestQuery.oldRel.getRowType().getFieldCount(),
            inputUnnestQuery.r.getRowType().getFieldCount());
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      hasCountFunction |= aggCall.getAggregation() instanceof SqlCountAggFunction;
      permutedAggCalls.add(aggCall.transform(targetMapping));
    }
    // create new Aggregate node
    RelNode newAggregate
        = builder.aggregate(builder.groupKey(newGroupSet, newGroupSets), permutedAggCalls).build();

    // create UnnestQuery
    Map<Integer, Integer> oldToNewOutputs = new HashMap<>();
    for (int groupKey : aggregate.getGroupSet()) {
      int oriIndex = aggregate.getGroupSet().indexOf(groupKey);
      int newIndex = newGroupSet.indexOf(groupKey);
      oldToNewOutputs.put(oriIndex, newIndex);
    }
    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
      oldToNewOutputs.put(
          aggregate.getGroupCount() + i,
          newGroupSet.cardinality() + i);
    }
    TreeMap<CorDef, Integer> corDefOutputs = new TreeMap<>();
    for (CorDef corDef : corDefs) {
      int index = requireNonNull(inputUnnestQuery.corDefOutputs.get(corDef));
      corDefOutputs.put(corDef, newGroupSet.indexOf(index));
    }

    if (aggregate.hasEmptyGroup()
        && (emptyOutputDisable || hasCountFunction)) {
      // create a left join with D to avoid rewriting from non-empty to empty output
      builder.push(dedupFreeVarsNode.r).push(newAggregate);
      List<RexNode> leftJoinConditions = new ArrayList<>();
      int freeVarsIndex = 0;
      for (CorDef corDef : corDefs) {
        RexNode notDistinctFrom =
            builder.isNotDistinctFrom(
                builder.field(2, 0, freeVarsIndex),
                builder.field(2, 1, requireNonNull(corDefOutputs.get(corDef))));
        leftJoinConditions.add(notDistinctFrom);

        corDefOutputs.put(corDef, freeVarsIndex++);
      }
      builder.join(JoinRelType.LEFT, leftJoinConditions);

      // rewrite COUNT to case when
      List<RexNode> aggCallProjects = new ArrayList<>();
      final int aggCallStartIndex =
          dedupFreeVarsNode.r.getRowType().getFieldCount() + newGroupSet.cardinality();
      for (int i = 0; i < permutedAggCalls.size(); i++) {
        int index = aggCallStartIndex + i;
        SqlAggFunction aggregation = permutedAggCalls.get(i).getAggregation();
        if (aggregation instanceof SqlCountAggFunction) {
          RexNode caseWhenRewrite =
              builder.call(
                  SqlStdOperatorTable.CASE,
                  builder.isNotNull(builder.field(index)),
                  builder.field(index),
                  builder.literal(0));
          aggCallProjects.add(caseWhenRewrite);
        } else {
          aggCallProjects.add(builder.field(index));
        }
      }
      List<RexNode> projects =
          new ArrayList<>(builder.fields(ImmutableBitSet.range(0, aggCallStartIndex)));
      projects.addAll(aggCallProjects);
      newAggregate = builder.project(projects).build();


      for (Map.Entry<Integer, Integer> entry : oldToNewOutputs.entrySet()) {
        int value = requireNonNull(entry.getValue());
        entry.setValue(value + corDefs.size());
      }
    }
    UnnestQuery unnestQuery
        = new UnnestQuery(aggregate, newAggregate, corDefOutputs, oldToNewOutputs);
    mapRelToUnnestQuery.put(aggregate, unnestQuery);
    return newAggregate;
  }

  public RelNode unnestInternal(Sort sort) {
    RelNode newInput = unnest(sort.getInput());
    UnnestQuery inputInfo =
        requireNonNull(mapRelToUnnestQuery.get(sort.getInput()));
    Mappings.TargetMapping targetMapping =
        Mappings.target(
            inputInfo.oldToNewOutputs,
            inputInfo.oldRel.getRowType().getFieldCount(),
            inputInfo.r.getRowType().getFieldCount());
    RelCollation shiftCollation = sort.getCollation().apply(targetMapping);
    builder.push(newInput);

    if (!sort.collation.getFieldCollations().isEmpty()
        && (sort.offset != null || sort.fetch != null)) {
      // the Sort with ORDER BY and LIMIT or OFFSET have to be changed during rewriting because
      // now the limit has to be enforced per value of the outer bindings instead of globally.
      // It can be rewritten using ROW_NUMBER() window function and filtering on it,
      // see section 4.4 in paper
      List<RexNode> partitionKeys = new ArrayList<>();
      for (CorDef corDef : corDefs) {
        int partitionKeyIndex = requireNonNull(inputInfo.corDefOutputs.get(corDef));
        partitionKeys.add(builder.field(partitionKeyIndex));
      }
      RexNode rowNumber = builder.aggregateCall(SqlStdOperatorTable.ROW_NUMBER)
          .over()
          .partitionBy(partitionKeys)
          .orderBy(builder.fields(shiftCollation))
          .toRex();
      List<RexNode> projectsWithRowNumber = new ArrayList<>(builder.fields());
      projectsWithRowNumber.add(rowNumber);
      builder.project(projectsWithRowNumber);

      List<RexNode> conditions = new ArrayList<>();
      if (sort.offset != null) {
        RexNode greaterThenLowerBound =
            builder.call(
                SqlStdOperatorTable.GREATER_THAN,
                builder.field(projectsWithRowNumber.size() - 1),
                sort.offset);
        conditions.add(greaterThenLowerBound);
      }
      if (sort.fetch != null) {
        RexNode upperBound = sort.offset == null
            ? sort.fetch
            : builder.call(SqlStdOperatorTable.PLUS, sort.offset, sort.fetch);
        RexNode lessThenOrEqualUpperBound =
            builder.call(
                SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                builder.field(projectsWithRowNumber.size() - 1),
                upperBound);
        conditions.add(lessThenOrEqualUpperBound);
      }
      builder.filter(conditions);
    } else {
      builder.sortLimit(sort.offset, sort.fetch, builder.fields(shiftCollation));
    }
    RelNode newSort = builder.build();
    UnnestQuery unnestQuery
        = new UnnestQuery(sort, newSort, inputInfo.corDefOutputs, inputInfo.oldToNewOutputs);
    mapRelToUnnestQuery.put(sort, unnestQuery);
    return newSort;
  }

  public RelNode unnestInternal(Correlate correlate) {
    // when nesting Correlate and there are still correlation, create a sub-decorrelator and merge
    // this decorrelator's context to the sub-decorrelator.
    TopDownGeneralDecorrelator subDecorrelator = createSubDecorrelator();
    Join newJoin = (Join) subDecorrelator.correlateElimination(correlate);

    UnnestQuery leftInfo
        = requireNonNull(subDecorrelator.mapRelToUnnestQuery.get(correlate.getLeft()));
    UnnestQuery rightInfo
        = requireNonNull(subDecorrelator.mapRelToUnnestQuery.get(correlate.getRight()));
    UnnestQuery unnestQuery
        = createJoinUnnestInfo(leftInfo, rightInfo, correlate, newJoin, correlate.getJoinType());
    mapRelToUnnestQuery.put(correlate, unnestQuery);
    return newJoin;
  }

  public RelNode unnestInternal(Join join) {
    boolean leftHasCorrelation =
        requireNonNull(hasCorrelatedExpressions.get(join.getLeft()));
    boolean rightHasCorrelation =
        requireNonNull(hasCorrelatedExpressions.get(join.getRight()));
    boolean pushDownToLeft = false;
    boolean pushDownToRight = false;
    RelNode newLeft;
    RelNode newRight;
    UnnestQuery leftInfo;
    UnnestQuery rightInfo;

    if (!leftHasCorrelation && !join.getJoinType().generatesNullsOnRight()
        && join.getJoinType().projectsRight()) {
      // there is no need to push down domain D to left side when both are satisfied:
      // 1. there is no correlation on left side
      // 2. join type will not generate NULL values on right side and will project right
      // the left side will start a decorrelation independently
      newLeft = decorrelateQuery(join.getLeft(), builder);
      Map<Integer, Integer> leftOldToNewOutputs = new HashMap<>();
      IntStream.range(0, newLeft.getRowType().getFieldCount())
          .forEach(i -> leftOldToNewOutputs.put(i, i));
      leftInfo = new UnnestQuery(join.getLeft(), newLeft, new TreeMap<>(), leftOldToNewOutputs);
    } else {
      newLeft = unnest(join.getLeft());
      pushDownToLeft = true;
      leftInfo = requireNonNull(mapRelToUnnestQuery.get(join.getLeft()));
    }
    if (!rightHasCorrelation && !join.getJoinType().generatesNullsOnLeft()) {
      // there is no need to push down domain D to right side when both are satisfied:
      // 1. there is no correlation on right side
      // 2. join type will not generate NULL values on left side
      // the right side will start a decorrelation independently
      newRight = decorrelateQuery(join.getRight(), builder);
      Map<Integer, Integer> rightOldToNewOutputs = new HashMap<>();
      IntStream.range(0, newRight.getRowType().getFieldCount())
          .forEach(i -> rightOldToNewOutputs.put(i, i));
      rightInfo = new UnnestQuery(join.getRight(), newRight, new TreeMap<>(), rightOldToNewOutputs);
    } else {
      emptyOutputDisable |= join.getJoinType() == JoinRelType.MARK;
      newRight = unnest(join.getRight());
      pushDownToRight = true;
      rightInfo = requireNonNull(mapRelToUnnestQuery.get(join.getRight()));
    }

    builder.push(newLeft).push(newRight);
    // if domain D is pushed down to both sides, the new join condition need to add the natural
    // condition between D
    RexNode newJoinCondition =
        createUnnestedJoinCondition(
            join.getCondition(),
            leftInfo,
            rightInfo,
            pushDownToLeft && pushDownToRight);
    RelNode newJoin = builder.join(join.getJoinType(), newJoinCondition).build();
    UnnestQuery unnestQuery =
        createJoinUnnestInfo(
            leftInfo,
            rightInfo,
            join,
            newJoin,
            join.getJoinType());
    mapRelToUnnestQuery.put(join, unnestQuery);
    return newJoin;
  }

  public RelNode unnestInternal(SetOp setOp) {
    List<RelNode> newInputs = new ArrayList<>();
    for (RelNode input : setOp.getInputs()) {
      // push down the domain D to each input
      RelNode newInput = unnest(input);
      builder.push(newInput);
      UnnestQuery inputInfo = requireNonNull(mapRelToUnnestQuery.get(input));
      // ensure that the rowType remains consistent after each input is rewritten:
      // [original fields that maintain their original order, the domain D]
      List<Integer> projectIndexes = new ArrayList<>();
      for (int i = 0; i < inputInfo.oldRel.getRowType().getFieldCount(); i++) {
        projectIndexes.add(requireNonNull(inputInfo.oldToNewOutputs.get(i)));
      }
      for (CorDef corDef : corDefs) {
        projectIndexes.add(requireNonNull(inputInfo.corDefOutputs.get(corDef)));
      }
      builder.project(builder.fields(projectIndexes));
      newInputs.add(builder.build());
    }
    builder.pushAll(newInputs);
    switch (setOp.kind) {
    case UNION:
      builder.union(setOp.all, newInputs.size());
      break;
    case INTERSECT:
      builder.intersect(setOp.all, newInputs.size());
      break;
    case EXCEPT:
      builder.minus(setOp.all, newInputs.size());
      break;
    }
    RelNode newSetOp = builder.build();

    int oriSetOpFieldCount = setOp.getRowType().getFieldCount();
    Map<Integer, Integer> oldToNewOutputs = new HashMap<>();
    IntStream.range(0, oriSetOpFieldCount).forEach(i -> oldToNewOutputs.put(i, i));
    TreeMap<CorDef, Integer> corDefOutputs = new TreeMap<>();
    for (CorDef corDef : corDefs) {
      corDefOutputs.put(corDef, oriSetOpFieldCount++);
    }
    UnnestQuery unnestQuery = new UnnestQuery(setOp, newSetOp, corDefOutputs, oldToNewOutputs);
    mapRelToUnnestQuery.put(setOp, unnestQuery);
    return newSetOp;
  }

  public RelNode unnestInternal(RelNode other) {
    throw new UnsupportedOperationException("Top-down general decorrelator does not support: "
        + other.getClass().getSimpleName());
  }

  /**
   * Try to replace all free variables (i.e. the attributes of domain D) to RexInputRef.
   * When the decorrelation process reaches:
   *
   * <pre>{@code
   *              Filter    with correlation
   *                |
   *              Input     without correlation
   * }</pre>
   *
   * <p> It will introduce the domain D by creating a cross product with Input. However, if all free
   * variables in the condition are filtered using equality conditions with local attributes, then
   * we can instead derive the domain from the local attributes. This substitution results in a
   * superset (compared to creating a cross product between D and input), because the filter effect
   * of the equality conditions is removed. However, this does not affect the final result,
   * because the filter will still happen at a later stage (at the original Correlate). Although
   * this substitution will result in more intermediate results, we assume that introducing a join
   * is more costly.
   *
   * @param filter              Filter node
   * @param corDefToInputIndex  a map from CorDef to input index
   * @param newConditions       new conditions after replacing free variables
   * @return  true when all free variables are replaced
   */
  private boolean tryReplaceFreeVarsToInputRef(
      Filter filter,
      Map<CorDef, Integer> corDefToInputIndex,
      List<RexNode> newConditions) {
    if (requireNonNull(hasCorrelatedExpressions.get(filter.getInput()))) {
      return false;
    }
    List<RexNode> oriConditions = RelOptUtil.conjunctions(filter.getCondition());
    for (RexNode condition : oriConditions) {
      if (RexUtil.containsCorrelation(condition)) {
        Pair<CorDef, RexInputRef> pair = getPairOfFreeVarAndInputRefInEqui(condition);
        if (pair != null) {
          // equi-condition will filter NULL values, so need to add IS NOT NULL for input ref
          newConditions.add(builder.isNotNull(pair.right));
          corDefToInputIndex.put(pair.left, pair.right.getIndex());
          continue;
        }
        // if the condition is correlated but it's not an equi-condition between free variable
        // and input ref, then cannot replaced.
        return false;
      } else {
        newConditions.add(condition);
      }
    }
    Set<CorDef> replacedCorDef = corDefToInputIndex.keySet();
    // ensure all free variables can be replaced
    return replacedCorDef.size() == corDefs.size() && corDefs.containsAll(replacedCorDef);
  }

  private @Nullable Pair<CorDef, RexInputRef> getPairOfFreeVarAndInputRefInEqui(RexNode condition) {
    if (!condition.isA(SqlKind.EQUALS)) {
      return null;
    }
    RexCall equiCond = (RexCall) condition;
    RexNode left = equiCond.getOperands().get(0);
    RexNode right = equiCond.getOperands().get(1);
    CorDef leftCorDef = unwrapCorDef(left);
    CorDef rightCorDef = unwrapCorDef(right);
    if (left instanceof RexInputRef && rightCorDef != null) {
      return Pair.of(rightCorDef, (RexInputRef) left);
    }
    if (right instanceof RexInputRef && leftCorDef != null) {
      return Pair.of(leftCorDef, (RexInputRef) right);
    }
    return null;
  }

  private @Nullable CorDef unwrapCorDef(RexNode expr) {
    if (expr instanceof RexFieldAccess) {
      RexFieldAccess fieldAccess = (RexFieldAccess) expr;
      if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
        RexCorrelVariable v = (RexCorrelVariable) fieldAccess.getReferenceExpr();
        CorDef corDef = new CorDef(v.id, fieldAccess.getField().getIndex());
        return corDefs.contains(corDef) ? corDef : null;
      }
    }
    return null;
  }

  /**
   * Rewrites correlated expressions, window function and shift input references.
   */
  static class CorrelatedExprRewriter extends RexShuttle {
    final UnnestQuery unnestQuery;

    CorrelatedExprRewriter(UnnestQuery unnestQuery) {
      this.unnestQuery = unnestQuery;
    }

    static RexNode rewrite(
        RexNode expr,
        UnnestQuery unnestQuery) {
      CorrelatedExprRewriter rewriter = new CorrelatedExprRewriter(unnestQuery);
      return expr.accept(rewriter);
    }

    static List<RexNode> rewrite(
        List<RexNode> exprs,
        UnnestQuery unnestQuery) {
      CorrelatedExprRewriter rewriter = new CorrelatedExprRewriter(unnestQuery);
      return new ArrayList<>(rewriter.apply(exprs));
    }

    @Override public RexNode visitInputRef(RexInputRef inputRef) {
      int newIndex = requireNonNull(unnestQuery.oldToNewOutputs.get(inputRef.getIndex()));
      if (newIndex == inputRef.getIndex()) {
        return inputRef;
      }
      return new RexInputRef(newIndex, inputRef.getType());
    }

    @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
        RexCorrelVariable v =
            (RexCorrelVariable) fieldAccess.getReferenceExpr();
        CorDef corDef = new CorDef(v.id, fieldAccess.getField().getIndex());
        int newIndex = requireNonNull(unnestQuery.corDefOutputs.get(corDef));
        return new RexInputRef(newIndex, fieldAccess.getType());
      }
      return super.visitFieldAccess(fieldAccess);
    }

    @Override public RexWindow visitWindow(RexWindow window) {
      RexWindow shiftedWindow = super.visitWindow(window);
      List<RexNode> newPartitionKeys = new ArrayList<>(shiftedWindow.partitionKeys);
      for (Integer corIndex : unnestQuery.corDefOutputs.values()) {
        RexInputRef inputRef =
            new RexInputRef(
                corIndex,
                unnestQuery.r.getRowType().getFieldList().get(corIndex).getType());
        newPartitionKeys.add(inputRef);
      }
      return unnestQuery.r.getCluster().getRexBuilder().makeWindow(
          newPartitionKeys,
          window.orderKeys,
          window.getLowerBound(),
          window.getUpperBound(),
          window.isRows(),
          window.getExclude());
    }
  }

  public TopDownGeneralDecorrelator getVisitor() {
    return this;
  }

  /**
   * Unnesting information.
   */
  static class UnnestQuery extends Frame {
    final RelNode oldRel;

    UnnestQuery(RelNode oldRel, RelNode r, NavigableMap<CorDef, Integer> corDefOutputs,
        Map<Integer, Integer> oldToNewOutputs) {
      super(oldRel, r, corDefOutputs, oldToNewOutputs);
      this.oldRel = oldRel;
    }
  }

  /**
   * The domain of the free variables. It's duplicate free.
   */
  static class DedupFreeVarsNode {
    final RelNode r;

    DedupFreeVarsNode(RelNode r) {
      this.r = r;
    }
  }

}
