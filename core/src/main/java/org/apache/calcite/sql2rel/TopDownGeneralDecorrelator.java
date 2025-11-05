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

import org.apache.calcite.linq4j.function.Experimental;
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
import java.util.Collections;
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
 *   Improving Unnesting of Complex Queries</a>. It's an improved version of the paper:
 * <a href="https://dl.gi.de/items/137d6917-d8fe-43aa-940b-e27da7c01625">
 *   Unnesting Arbitrary Queries</a>.
 *
 * <p> Usage notes for TopDownGeneralDecorrelator:
 *
 * <p> TopDownGeneralDecorrelator is not yet integrated into other modules and needs to be called
 * separately. If you want to use it to replace {@link RelDecorrelator}, we recommend:
 *
 * <ol>
 *   <li>When generating the initial plan by {@link SqlToRelConverter}, do not remove subqueries
 *   and do not enable decorrelation.</li>
 *   <li>Build a {@link HepPlanner} and apply rules for removing subqueries to the initial
 *   plan. With subqueries removed correctly, TopDownGeneralDecorrelator can in theory eliminate all
 *   correlation. We recommend using {@link CoreRules#FILTER_SUB_QUERY_TO_MARK_CORRELATE}
 *   and {@link CoreRules#PROJECT_SUB_QUERY_TO_MARK_CORRELATE} to remove subqueries from Filter and
 *   Project. These rules produce LEFT MARK Join/Correlate which are better suited for
 *   TopDownGeneralDecorrelator. There is not yet a corresponding, specially tailored rule for
 *   Join; you may choose to use {@link CoreRules#JOIN_SUB_QUERY_TO_CORRELATE}. Alternatively, for
 *   greater stability, you can run TopDownGeneralDecorrelator first and then apply
 *   {@link CoreRules#JOIN_SUB_QUERY_TO_CORRELATE} together with {@link RelDecorrelator}.</li>
 *   <li>Call {@link TopDownGeneralDecorrelator#decorrelateQuery(RelNode, RelBuilder)} to obtain
 *   the decorrelated plan.</li>
 *   <li>Continue with other optimizations.</li>
 * </ol>
 *
 * <p> See
 * <code>org.apache.calcite.test.RelOptRulesTest#testTopDownGeneralDecorrelateForFilterExists()
 * </code> and <code>org.apache.calcite.test.RelOptFixture#checkPlanning(boolean)</code> for
 * working examples.
 */
@Experimental
public class TopDownGeneralDecorrelator implements ReflectiveVisitor {

  private final RelBuilder builder;

  // record the CorDef in the current context (including those in the parent Correlate).
  // NavigableSet is used to ensure a stable iteration order.
  private final NavigableSet<CorDef> corDefs;

  // a map from RelNode to whether existing correlated expressions (according to corDefs).
  private final Map<RelNode, Boolean> hasCorrelatedExpressions;

  // a map from RelNode to its UnnestedQuery.
  private final Map<RelNode, UnnestedQuery> mapRelToUnnestedQuery;

  private final boolean hasParent;

  // the domain of the free variables (i.e. corDefs) D, it's duplicate free.
  private DedupFreeVarsNode dedupFreeVarsNode;

  // invokes using reflection a method named unnestInternal based on the
  // runtime type of the argument.
  @SuppressWarnings("method.invocation.invalid")
  private final ReflectUtil.MethodDispatcher<RelNode> dispatcher =
      ReflectUtil.createMethodDispatcher(
          RelNode.class, getVisitor(), "unnestInternal", RelNode.class, boolean.class);

  /**
   * Creates a TopDownGeneralDecorrelator. If parent context arguments are provided,
   * they are reused/merged into this instance.
   *
   * @param builder                         RelBuilder
   * @param hasParent                       whether has parent decorrelator
   * @param parentCorDefs                   corDefs from parent decorrelator
   * @param parentHasCorrelatedExpressions  a map from RelNode to whether existing correlated
   *                                        expressions
   * @param parentMapRelToUnnestedQuery       a map from RelNode to its UnnestedQuery
   */
  @SuppressWarnings("initialization.fields.uninitialized")
  private TopDownGeneralDecorrelator(
      RelBuilder builder,
      boolean hasParent,
      @Nullable Set<CorDef> parentCorDefs,
      @Nullable Map<RelNode, Boolean> parentHasCorrelatedExpressions,
      @Nullable Map<RelNode, UnnestedQuery> parentMapRelToUnnestedQuery) {
    this.builder = builder;
    this.hasParent = hasParent;
    this.corDefs = new TreeSet<>();
    if (parentCorDefs != null) {
      this.corDefs.addAll(parentCorDefs);
    }
    this.hasCorrelatedExpressions = parentHasCorrelatedExpressions == null
        ? new HashMap<>()
        : parentHasCorrelatedExpressions;
    this.mapRelToUnnestedQuery = parentMapRelToUnnestedQuery == null
        ? new HashMap<>()
        : parentMapRelToUnnestedQuery;
  }

  public static TopDownGeneralDecorrelator createEmptyDecorrelator(RelBuilder builder) {
    return new TopDownGeneralDecorrelator(builder, false, null, null, null);
  }

  private TopDownGeneralDecorrelator createSubDecorrelator() {
    TopDownGeneralDecorrelator subDecorrelator =
        new TopDownGeneralDecorrelator(
            builder,
            true,
            corDefs,
            hasCorrelatedExpressions,
            mapRelToUnnestedQuery);
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
        .addRuleCollection(
            ImmutableList.of(
                CoreRules.FILTER_PROJECT_TRANSPOSE,
                CoreRules.FILTER_INTO_JOIN,
                CoreRules.FILTER_CORRELATE))
        .build();
    HepPlanner prePlanner = new HepPlanner(preProgram);
    prePlanner.setRoot(rel);
    RelNode preparedRel = prePlanner.findBestExp();

    // start decorrelating
    TopDownGeneralDecorrelator decorrelator = createEmptyDecorrelator(builder);
    RelNode decorrelateNode = rel;
    try {
      decorrelateNode = decorrelator.correlateElimination(preparedRel, true);
    } catch (UnsupportedOperationException e) {
      // if the correlation exists in an unsupported operator, retain the original plan.
    }

    HepProgram postProgram = HepProgram.builder()
        .addRuleCollection(
            ImmutableList.of(
                CoreRules.FILTER_PROJECT_TRANSPOSE,
                CoreRules.FILTER_INTO_JOIN,
                CoreRules.MARK_TO_SEMI_OR_ANTI_JOIN_RULE,
                CoreRules.PROJECT_MERGE,
                CoreRules.PROJECT_REMOVE))
        .build();
    HepPlanner postPlanner = new HepPlanner(postProgram);
    postPlanner.setRoot(decorrelateNode);
    return postPlanner.findBestExp();
  }

  /**
   * Eliminates Correlate.
   *
   * @param rel                           RelNode
   * @param allowEmptyOutputFromRewrite   whether allow empty output resulting from
   *                                      decorrelate rewriting.
   * @return  Equivalent RelNode without Correlate
   */
  private RelNode correlateElimination(RelNode rel, boolean allowEmptyOutputFromRewrite) {
    if (!(rel instanceof Correlate)) {
      for (int i = 0; i < rel.getInputs().size(); i++) {
        rel.replaceInput(i, correlateElimination(rel.getInput(i), allowEmptyOutputFromRewrite));
      }
      return rel;
    }

    final Correlate correlate = (Correlate) rel;
    final RelNode newLeft;
    if (hasParent) {
      // if the current decorrelator has a parent, it means that the Correlate must have
      // correlation from above.
      assert hasCorrelatedExpressions.containsKey(correlate)
          && hasCorrelatedExpressions.get(correlate);
      newLeft = unnest(correlate.getLeft(), allowEmptyOutputFromRewrite);
    } else {
      // otherwise, start a new decorrelation for the left side.
      newLeft = decorrelateQuery(correlate.getLeft(), builder);
    }

    // create or update UnnestedQuery of left side and corDefs of this decorrelator.
    UnnestedQuery leftInfo = mapRelToUnnestedQuery.get(correlate.getLeft());
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
    leftInfo = new UnnestedQuery(correlate.getLeft(), newLeft, corDefOutputs, oldToNewOutputs);
    dedupFreeVarsNode = DedupFreeVarsNode.create(newLeft, leftInfo, corDefs, builder);

    // decorrelate right side
    detectCorrelatedExpressions(correlate.getRight());
    allowEmptyOutputFromRewrite &= correlate.getJoinType() != JoinRelType.LEFT_MARK;
    RelNode newRight = unnest(correlate.getRight(), allowEmptyOutputFromRewrite);
    UnnestedQuery rightInfo = requireNonNull(mapRelToUnnestedQuery.get(correlate.getRight()));

    // rewrite condition, adding the natural join condition between the left side and
    // the domain D that is produced from the right side. This the fundamental equation from the
    // paper Improving Unnesting of Complex Queries, shown in Section 2.2
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
        UnnestedQuery.createUnnestedJoinCondition(correlate.getCondition(), leftInfo, rightInfo,
            true, builder, corDefs);
    RelNode unnestedRel = builder.join(correlate.getJoinType(), unnestedJoinCondition).build();

    if (!hasParent) {
      // ensure that the fields are in the same order as in the original plan.
      builder.push(unnestedRel);
      UnnestedQuery unnestedQuery =
          UnnestedQuery.createJoinUnnestInfo(
              leftInfo,
              rightInfo,
              correlate,
              unnestedRel,
              correlate.getJoinType());
      List<RexNode> projects
          = builder.fields(new ArrayList<>(unnestedQuery.oldToNewOutputs.values()));
      unnestedRel = builder.project(projects).build();
    }
    return unnestedRel;
  }

  /**
   * Detects whether any expression in the relational tree rooted at {@code rel} refers to any
   * variables that appear in {@link #corDefs} and populates the {@link #hasCorrelatedExpressions}.
   *
   * <p> It is necessary to detect correlation for every node, for example:
   *
   * <blockquote><pre>
   *      Union
   *    /   |   \
   *  r1    r2   r3     all with correlation
   *  |     |     |
   * r11   r22   r33    all without correlation
   * </pre></blockquote>
   *
   * <p> If we stop after detecting correlation in the r1 branch, we lose correlation information
   * for the r2 and r3 branches. Without that information we cannot know the correct stopping point
   * when pushing down D to r2/r3 branches. In addition, accurately knowing the correlation of each
   * input enables useful optimizations when pushing down D to Join.
   *
   * @param rel RelNode
   * @return true when there are correlated expressions
   */
  private boolean detectCorrelatedExpressions(RelNode rel) {
    if (!hasParent && hasCorrelatedExpressions.containsKey(rel)) {
      // for shared sub-trees, check the map hasCorrelatedExpressions first. However, this is only
      // valid when there is no parent decorrelator. For example:
      //    Correlate0 => cor0
      //   /        \
      //  r1        r2
      //             \
      //          Correlate1 => cor1
      //          /        \
      //   r3 with cor0    r5 with cor1
      //         /           \
      //       r4            r6
      // for the parent decorrelator-0 of Correlate0, r5 doesn't have correlation. However, for the
      // decorrelator-1 of Correlate1, its construction merge information from the parent
      // decorrelation-0, at this point, r5 still doesn't have correlation. Once the decorrelator-1
      // completes the decorrelation on the left side of Correlate1, it need to detect the
      // correlation on Correlate1 right side (based on cor0 and cor1). Now r5 has correlation,
      // and the value in hasCorrelatedExpressions will change from FALSE to TRUE.
      return hasCorrelatedExpressions.get(rel);
    }
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
              && !Collections.disjoint(corrIdSet, variableUsedVisitor.variables);
    }
    hasCorrelatedExpressions.put(rel, hasCorrelation);
    return hasCorrelation;
  }

  /**
   * Unnests a RelNode. If there is no correlation in the node, create the cross product
   * with domain D; otherwise, dispatch to specific method to push down D based on the type of rel.
   *
   * @param rel                           RelNode
   * @param allowEmptyOutputFromRewrite   whether allow empty output resulting from
   *                                      decorrelate rewriting.
   * @return new node (contains domain D) without correlation
   */
  private RelNode unnest(RelNode rel, boolean allowEmptyOutputFromRewrite) {
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

      UnnestedQuery unnestedQuery
          = new UnnestedQuery(rel, newRel, corDefOutputs, oldToNewOutputs);
      mapRelToUnnestedQuery.put(rel, unnestedQuery);
      return newRel;
    }
    return dispatcher.invoke(rel, allowEmptyOutputFromRewrite);
  }

  public RelNode unnestInternal(Filter filter, boolean allowEmptyOutputFromRewrite) {
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
      RelNode newInput = unnest(filter.getInput(), allowEmptyOutputFromRewrite);
      UnnestedQuery inputInfo = requireNonNull(mapRelToUnnestedQuery.get(filter.getInput()));
      RexNode newCondition =
          CorrelatedExprRewriter.rewrite(filter.getCondition(), inputInfo);
      builder.push(newInput).filter(newCondition);
      oldToNewOutputs = inputInfo.oldToNewOutputs;
      corDefOutputs.putAll(inputInfo.corDefOutputs);
    }
    RelNode newFilter = builder.build();
    UnnestedQuery unnestedQuery =
        new UnnestedQuery(filter, newFilter, corDefOutputs, oldToNewOutputs);
    mapRelToUnnestedQuery.put(filter, unnestedQuery);
    return newFilter;
  }

  public RelNode unnestInternal(Project project, boolean allowEmptyOutputFromRewrite) {
    for (RexNode expr : project.getProjects()) {
      if (!allowEmptyOutputFromRewrite) {
        break;
      }
      allowEmptyOutputFromRewrite &= Strong.isStrong(expr);
    }
    RelNode newInput = unnest(project.getInput(), allowEmptyOutputFromRewrite);
    UnnestedQuery inputInfo = requireNonNull(mapRelToUnnestedQuery.get(project.getInput()));
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
    UnnestedQuery unnestedQuery
        = new UnnestedQuery(project, newProject, corDefOutputs, oldToNewOutputs);
    mapRelToUnnestedQuery.put(project, unnestedQuery);
    return newProject;
  }

  public RelNode unnestInternal(Aggregate aggregate, boolean allowEmptyOutputFromRewrite) {
    RelNode newInput = unnest(aggregate.getInput(), allowEmptyOutputFromRewrite);
    UnnestedQuery inputUnnestedQuery =
        requireNonNull(mapRelToUnnestedQuery.get(aggregate.getInput()));
    builder.push(newInput);

    // create new groupSet and groupSets, adding the fields in D to group keys
    ImmutableBitSet.Builder corKeyBuilder = ImmutableBitSet.builder();
    for (CorDef corDef : corDefs) {
      int corKeyIndex = requireNonNull(inputUnnestedQuery.corDefOutputs.get(corDef));
      corKeyBuilder.set(corKeyIndex);
    }
    ImmutableBitSet corKeyBitSet = corKeyBuilder.build();
    ImmutableBitSet newGroupSet
        = aggregate.getGroupSet().permute(inputUnnestedQuery.oldToNewOutputs)
            .union(corKeyBitSet);
    List<ImmutableBitSet> newGroupSets = new ArrayList<>();
    for (ImmutableBitSet bitSet : aggregate.getGroupSets()) {
      ImmutableBitSet newBitSet
          = bitSet.permute(inputUnnestedQuery.oldToNewOutputs).union(corKeyBitSet);
      newGroupSets.add(newBitSet);
    }

    // create new aggregate functions
    boolean hasCountFunction = false;
    List<AggregateCall> permutedAggCalls = new ArrayList<>();
    Mappings.TargetMapping targetMapping =
        Mappings.target(
            inputUnnestedQuery.oldToNewOutputs,
            inputUnnestedQuery.oldRel.getRowType().getFieldCount(),
            inputUnnestedQuery.r.getRowType().getFieldCount());
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      hasCountFunction |= aggCall.getAggregation() instanceof SqlCountAggFunction;
      permutedAggCalls.add(aggCall.transform(targetMapping));
    }
    // create new Aggregate node
    RelNode newAggregate
        = builder.aggregate(builder.groupKey(newGroupSet, newGroupSets), permutedAggCalls).build();

    // create UnnestedQuery
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
      int index = requireNonNull(inputUnnestedQuery.corDefOutputs.get(corDef));
      corDefOutputs.put(corDef, newGroupSet.indexOf(index));
    }

    if (aggregate.hasEmptyGroup()
        && (!allowEmptyOutputFromRewrite || hasCountFunction)) {
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

      // replace the reference to COUNT with CASE WHEN COUNT(*) IS NULL THEN 0 ELSE COUNT(*) END
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
    UnnestedQuery unnestedQuery
        = new UnnestedQuery(aggregate, newAggregate, corDefOutputs, oldToNewOutputs);
    mapRelToUnnestedQuery.put(aggregate, unnestedQuery);
    return newAggregate;
  }

  public RelNode unnestInternal(Sort sort, boolean allowEmptyOutputFromRewrite) {
    RelNode newInput = unnest(sort.getInput(), allowEmptyOutputFromRewrite);
    UnnestedQuery inputInfo =
        requireNonNull(mapRelToUnnestedQuery.get(sort.getInput()));
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
      // see section 4.4 in paper Improving Unnesting of Complex Queries
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
    UnnestedQuery unnestedQuery
        = new UnnestedQuery(sort, newSort, inputInfo.corDefOutputs, inputInfo.oldToNewOutputs);
    mapRelToUnnestedQuery.put(sort, unnestedQuery);
    return newSort;
  }

  public RelNode unnestInternal(Correlate correlate, boolean allowEmptyOutputFromRewrite) {
    // when nesting Correlate and there are still correlation, create a sub-decorrelator and merge
    // this decorrelator's context to the sub-decorrelator.
    TopDownGeneralDecorrelator subDecorrelator = createSubDecorrelator();
    Join newJoin =
        (Join) subDecorrelator.correlateElimination(correlate, allowEmptyOutputFromRewrite);

    UnnestedQuery leftInfo
        = requireNonNull(subDecorrelator.mapRelToUnnestedQuery.get(correlate.getLeft()));
    UnnestedQuery rightInfo
        = requireNonNull(subDecorrelator.mapRelToUnnestedQuery.get(correlate.getRight()));
    UnnestedQuery unnestedQuery =
        UnnestedQuery.createJoinUnnestInfo(leftInfo, rightInfo, correlate,
            newJoin, correlate.getJoinType());
    mapRelToUnnestedQuery.put(correlate, unnestedQuery);
    return newJoin;
  }

  public RelNode unnestInternal(Join join, boolean allowEmptyOutputFromRewrite) {
    boolean leftHasCorrelation =
        requireNonNull(hasCorrelatedExpressions.get(join.getLeft()));
    boolean rightHasCorrelation =
        requireNonNull(hasCorrelatedExpressions.get(join.getRight()));
    boolean pushDownToLeft = false;
    boolean pushDownToRight = false;
    RelNode newLeft;
    RelNode newRight;
    UnnestedQuery leftInfo;
    UnnestedQuery rightInfo;

    if (!leftHasCorrelation && !join.getJoinType().generatesNullsOnRight()
        && join.getJoinType().projectsRight()) {
      // there is no need to push down domain D to left side when both following conditions
      // are satisfied:
      // 1. there is no correlation on left side
      // 2. join type will not generate NULL values on right side and will project right
      // In this case, the left side will start a decorrelation independently
      newLeft = decorrelateQuery(join.getLeft(), builder);
      Map<Integer, Integer> leftOldToNewOutputs = new HashMap<>();
      IntStream.range(0, newLeft.getRowType().getFieldCount())
          .forEach(i -> leftOldToNewOutputs.put(i, i));
      leftInfo = new UnnestedQuery(join.getLeft(), newLeft, new TreeMap<>(), leftOldToNewOutputs);
    } else {
      newLeft = unnest(join.getLeft(), allowEmptyOutputFromRewrite);
      pushDownToLeft = true;
      leftInfo = requireNonNull(mapRelToUnnestedQuery.get(join.getLeft()));
    }
    if (!rightHasCorrelation && !join.getJoinType().generatesNullsOnLeft()) {
      // there is no need to push down domain D to right side when both following conditions
      // are satisfied:
      // 1. there is no correlation on right side
      // 2. join type will not generate NULL values on left side
      // In this case, the right side will start a decorrelation independently
      newRight = decorrelateQuery(join.getRight(), builder);
      Map<Integer, Integer> rightOldToNewOutputs = new HashMap<>();
      IntStream.range(0, newRight.getRowType().getFieldCount())
          .forEach(i -> rightOldToNewOutputs.put(i, i));
      rightInfo =
          new UnnestedQuery(join.getRight(), newRight, new TreeMap<>(), rightOldToNewOutputs);
    } else {
      allowEmptyOutputFromRewrite &= join.getJoinType() != JoinRelType.LEFT_MARK;
      newRight = unnest(join.getRight(), allowEmptyOutputFromRewrite);
      pushDownToRight = true;
      rightInfo = requireNonNull(mapRelToUnnestedQuery.get(join.getRight()));
    }

    builder.push(newLeft).push(newRight);
    // if domain D is pushed down to both sides, the new join condition need to add the natural
    // condition between D
    RexNode newJoinCondition =
        UnnestedQuery.createUnnestedJoinCondition(
            join.getCondition(),
            leftInfo,
            rightInfo,
            pushDownToLeft && pushDownToRight,
            builder,
            corDefs);
    RelNode newJoin = builder.join(join.getJoinType(), newJoinCondition).build();
    UnnestedQuery unnestedQuery =
        UnnestedQuery.createJoinUnnestInfo(
            leftInfo,
            rightInfo,
            join,
            newJoin,
            join.getJoinType());
    mapRelToUnnestedQuery.put(join, unnestedQuery);
    return newJoin;
  }

  public RelNode unnestInternal(SetOp setOp, boolean allowEmptyOutputFromRewrite) {
    List<RelNode> newInputs = new ArrayList<>();
    for (RelNode input : setOp.getInputs()) {
      // push down the domain D to each input
      RelNode newInput = unnest(input, allowEmptyOutputFromRewrite);
      builder.push(newInput);
      UnnestedQuery inputInfo = requireNonNull(mapRelToUnnestedQuery.get(input));
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
    default:
      throw new AssertionError("Not a set op: " + setOp);
    }
    RelNode newSetOp = builder.build();

    int oriSetOpFieldCount = setOp.getRowType().getFieldCount();
    Map<Integer, Integer> oldToNewOutputs = new HashMap<>();
    IntStream.range(0, oriSetOpFieldCount).forEach(i -> oldToNewOutputs.put(i, i));
    TreeMap<CorDef, Integer> corDefOutputs = new TreeMap<>();
    for (CorDef corDef : corDefs) {
      corDefOutputs.put(corDef, oriSetOpFieldCount++);
    }
    UnnestedQuery unnestedQuery =
        new UnnestedQuery(setOp, newSetOp, corDefOutputs, oldToNewOutputs);
    mapRelToUnnestedQuery.put(setOp, unnestedQuery);
    return newSetOp;
  }

  public RelNode unnestInternal(RelNode other) {
    throw new UnsupportedOperationException("Top-down general decorrelator does not support: "
        + other.getClass().getSimpleName());
  }

  /**
   * Try to replace all free variables (i.e. the attributes of domain D) with RexInputRef.
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
   * is more costly. See section 3.3 in paper Unnesting Arbitrary Queries.
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
          if (condition.isA(SqlKind.EQUALS)) {
            newConditions.add(builder.isNotNull(pair.right));
          }
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
    if (!condition.isA(SqlKind.EQUALS) && !condition.isA(SqlKind.IS_NOT_DISTINCT_FROM)) {
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
    final UnnestedQuery unnestedQuery;

    CorrelatedExprRewriter(UnnestedQuery unnestedQuery) {
      this.unnestedQuery = unnestedQuery;
    }

    static RexNode rewrite(
        RexNode expr,
        UnnestedQuery unnestedQuery) {
      CorrelatedExprRewriter rewriter = new CorrelatedExprRewriter(unnestedQuery);
      return expr.accept(rewriter);
    }

    static List<RexNode> rewrite(
        List<RexNode> exprs,
        UnnestedQuery unnestedQuery) {
      CorrelatedExprRewriter rewriter = new CorrelatedExprRewriter(unnestedQuery);
      return new ArrayList<>(rewriter.apply(exprs));
    }

    @Override public RexNode visitInputRef(RexInputRef inputRef) {
      int newIndex = requireNonNull(unnestedQuery.oldToNewOutputs.get(inputRef.getIndex()));
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
        int newIndex = requireNonNull(unnestedQuery.corDefOutputs.get(corDef));
        return new RexInputRef(newIndex, fieldAccess.getType());
      }
      return super.visitFieldAccess(fieldAccess);
    }

    @Override public RexWindow visitWindow(RexWindow window) {
      RexWindow shiftedWindow = super.visitWindow(window);
      List<RexNode> newPartitionKeys = new ArrayList<>(shiftedWindow.partitionKeys);
      for (Integer corIndex : unnestedQuery.corDefOutputs.values()) {
        RexInputRef inputRef =
            new RexInputRef(
                corIndex,
                unnestedQuery.r.getRowType().getFieldList().get(corIndex).getType());
        newPartitionKeys.add(inputRef);
      }
      return unnestedQuery.r.getCluster().getRexBuilder().makeWindow(
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
  static class UnnestedQuery extends Frame {
    final RelNode oldRel;

    /**
     * Creates a UnnestedQuery.
     *
     * @param oldRel          old node before unnesting
     * @param r               new node after unnesting
     * @param corDefOutputs   a sorted map from CorDef to output index in new node
     * @param oldToNewOutputs a map from old node output index to new node output index
     */
    UnnestedQuery(RelNode oldRel, RelNode r, NavigableMap<CorDef, Integer> corDefOutputs,
        Map<Integer, Integer> oldToNewOutputs) {
      super(oldRel, r, corDefOutputs, oldToNewOutputs);
      this.oldRel = oldRel;
    }

    /**
     * Create UnnestedQuery for Join/Correlate after decorrelating.
     *
     * @param leftInfo          UnnestedQuery of the left side
     * @param rightInfo         UnnestedQuery of the right side
     * @param oriJoinNode       original Join/Correlate node
     * @param unnestedJoinNode  new node after decorrelating
     * @param joinRelType       join type of original Join/Correlate
     * @return UnnestedQuery
     */
    private static UnnestedQuery createJoinUnnestInfo(
        UnnestedQuery leftInfo,
        UnnestedQuery rightInfo,
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
      case LEFT_MARK:
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
        throw new IllegalArgumentException("The UnnestInfo for both sides of Join/Correlate that "
            + "has correlation should not all be empty.");
      }
      return new UnnestedQuery(oriJoinNode, unnestedJoinNode, corDefOutputs, oldToNewOutputs);
    }

    /**
     * Create the new join condition after decorrelating.
     *
     * @param oriCondition              original Correlate/Join condition
     * @param leftInfo                  UnnestedQuery of the left side
     * @param rightInfo                 UnnestedQuery of the right side
     * @param needNaturalJoinCondition  whether need to add the natural join condition for domain D
     * @param builder                   RelBuilder
     * @param corDefs                   the CorDef in the current decorrelator context
     * @return the new join condition
     */
    private static RexNode createUnnestedJoinCondition(
        RexNode oriCondition,
        UnnestedQuery leftInfo,
        UnnestedQuery rightInfo,
        boolean needNaturalJoinCondition,
        RelBuilder builder,
        NavigableSet<CorDef> corDefs) {
      // create a temporary inner join and its UnnestedQuery to help rewrite the
      // original condition by CorrelatedExprRewriter
      Map<Integer, Integer> temporaryOldToNewOutputs = new HashMap<>();
      int oriLeftFieldCount = leftInfo.oldRel.getRowType().getFieldCount();
      int newLeftFieldCount = leftInfo.r.getRowType().getFieldCount();
      temporaryOldToNewOutputs.putAll(leftInfo.oldToNewOutputs);
      rightInfo.oldToNewOutputs.forEach((oriIndex, newIndex) ->
          temporaryOldToNewOutputs.put(
              requireNonNull(oriIndex, "oriIndex") + oriLeftFieldCount,
              requireNonNull(newIndex, "newIndex") + newLeftFieldCount));

      TreeMap<CorDef, Integer> temporaryCorDefOutputs = new TreeMap<>();
      if (!leftInfo.corDefOutputs.isEmpty()) {
        temporaryCorDefOutputs.putAll(leftInfo.corDefOutputs);
      } else if (!rightInfo.corDefOutputs.isEmpty()) {
        rightInfo.corDefOutputs.forEach((corDef, index) ->
            temporaryCorDefOutputs.put(corDef, index + newLeftFieldCount));
      } else {
        throw new IllegalArgumentException("The UnnestInfo for both sides of Join/Correlate that "
            + "has correlation should not all be empty.");
      }
      RelNode temporaryOldRel = builder.push(leftInfo.oldRel).push(rightInfo.oldRel)
          .join(JoinRelType.INNER)
          .build();
      RelNode temporaryNewRel = builder.push(leftInfo.r).push(rightInfo.r)
          .join(JoinRelType.INNER)
          .build();
      UnnestedQuery temporaryInfo =
          new UnnestedQuery(temporaryOldRel, temporaryNewRel,
              temporaryCorDefOutputs, temporaryOldToNewOutputs);
      RexNode rewriteOriCondition = CorrelatedExprRewriter.rewrite(oriCondition, temporaryInfo);
      List<RexNode> unnestedJoinConditions = new ArrayList<>();
      unnestedJoinConditions.add(rewriteOriCondition);

      if (needNaturalJoinCondition) {
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

  }

  /**
   * The domain of the free variables. It's duplicate free. Corresponds to a relation denoted
   * by D in the paper.
   */
  static class DedupFreeVarsNode {
    final RelNode r;

    DedupFreeVarsNode(RelNode r) {
      this.r = r;
    }

    /**
     * Generate the domain of the free variables D.
     *
     * @param newLeft   the left side (without correlation) of Correlate
     * @param leftInfo  the UnnestedQuery of the left side of Correlate
     * @param corDefs   the CorDef in the current decorrelator context
     * @param builder   RelBuilder
     * @return  the domain of the free variables D
     */
    static DedupFreeVarsNode create(
        RelNode newLeft,
        UnnestedQuery leftInfo,
        NavigableSet<CorDef> corDefs,
        RelBuilder builder) {
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
  }

}
