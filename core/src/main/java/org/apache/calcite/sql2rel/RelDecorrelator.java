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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitDispatcher;
import org.apache.calcite.util.ReflectiveVisitor;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.collect.SortedSetMultimap;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * RelDecorrelator replaces all correlated expressions (corExp) in a relational
 * expression (RelNode) tree with non-correlated expressions that are produced
 * from joining the RelNode that produces the corExp with the RelNode that
 * references it.
 *
 * <p>TODO:</p>
 * <ul>
 *   <li>replace {@code CorelMap} constructor parameter with a RelNode
 *   <li>make {@link #currentRel} immutable (would require a fresh
 *      RelDecorrelator for each node being decorrelated)</li>
 *   <li>make fields of {@code CorelMap} immutable</li>
 *   <li>make sub-class rules static, and have them create their own
 *   de-correlator</li>
 * </ul>
 */
public class RelDecorrelator implements ReflectiveVisitor {
  //~ Static fields/initializers ---------------------------------------------

  private static final Logger SQL2REL_LOGGER =
      CalciteTrace.getSqlToRelTracer();

  //~ Instance fields --------------------------------------------------------

  // map built during translation
  private CorelMap cm;

  private final DecorrelateRelVisitor decorrelateVisitor;

  private final RexBuilder rexBuilder;

  // The rel which is being visited
  private RelNode currentRel;

  private final Context context;

  // maps built during decorrelation
  private final Map<RelNode, RelNode> mapOldToNewRel = Maps.newHashMap();

  // map rel to all the newly created correlated variables in its output
  private final Map<RelNode, SortedMap<Correlation, Integer>>
  mapNewRelToMapCorVarToOutputPos = Maps.newHashMap();

  // another map to map old input positions to new input positions
  // this is from the view point of the parent rel of a new rel.
  private final Map<RelNode, Map<Integer, Integer>>
  mapNewRelToMapOldToNewOutputPos = Maps.newHashMap();

  private final HashSet<LogicalCorrelate> generatedCorRels = Sets.newHashSet();

  //~ Constructors -----------------------------------------------------------

  private RelDecorrelator(
      RexBuilder rexBuilder,
      CorelMap cm,
      Context context) {
    this.cm = cm;
    this.rexBuilder = rexBuilder;
    this.context = context;

    decorrelateVisitor = new DecorrelateRelVisitor();
  }

  //~ Methods ----------------------------------------------------------------

  /** Decorrelates a query.
   *
   * <p>This is the main entry point to {@code RelDecorrelator}.
   *
   * @param rootRel Root node of the query
   *
   * @return Equivalent query with all
   * {@link org.apache.calcite.rel.logical.LogicalCorrelate} instances removed
   */
  public static RelNode decorrelateQuery(RelNode rootRel) {
    final CorelMap corelMap = CorelMap.build(rootRel);
    if (!corelMap.hasCorrelation()) {
      return rootRel;
    }

    final RelOptCluster cluster = rootRel.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final RelDecorrelator decorrelator =
        new RelDecorrelator(rexBuilder, corelMap,
            cluster.getPlanner().getContext());


    RelNode newRootRel = decorrelator.removeCorrelationViaRule(rootRel);

    if (SQL2REL_LOGGER.isLoggable(Level.FINE)) {
      SQL2REL_LOGGER.fine(
          RelOptUtil.dumpPlan(
              "Plan after removing Correlator",
              newRootRel,
              false,
              SqlExplainLevel.EXPPLAN_ATTRIBUTES));
    }

    if (!decorrelator.cm.mapCorVarToCorRel.isEmpty()) {
      newRootRel = decorrelator.decorrelate(newRootRel);
    }

    return newRootRel;
  }

  private void setCurrent(RelNode root, LogicalCorrelate corRel) {
    currentRel = corRel;
    if (corRel != null) {
      cm = CorelMap.build(Util.first(root, corRel));
    }
  }

  private RelNode decorrelate(RelNode root) {
    // first adjust count() expression if any
    HepProgram program = HepProgram.builder()
        .addRuleInstance(new AdjustProjectForCountAggregateRule(false))
        .addRuleInstance(new AdjustProjectForCountAggregateRule(true))
        .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
        .build();

    HepPlanner planner = createPlanner(program);

    planner.setRoot(root);
    root = planner.findBestExp();

    // Perform decorrelation.
    mapOldToNewRel.clear();
    mapNewRelToMapCorVarToOutputPos.clear();
    mapNewRelToMapOldToNewOutputPos.clear();

    decorrelateVisitor.visit(root, 0, null);

    if (mapOldToNewRel.containsKey(root)) {
      // has been rewritten
      return mapOldToNewRel.get(root);
    } else {
      // not rewritten
      return root;
    }
  }

  private Function2<RelNode, RelNode, Void> createCopyHook() {
    return new Function2<RelNode, RelNode, Void>() {
      public Void apply(RelNode oldNode, RelNode newNode) {
        if (cm.mapRefRelToCorVar.containsKey(oldNode)) {
          cm.mapRefRelToCorVar.putAll(newNode,
              cm.mapRefRelToCorVar.get(oldNode));
        }
        if (oldNode instanceof LogicalCorrelate
            && newNode instanceof LogicalCorrelate) {
          LogicalCorrelate oldCor = (LogicalCorrelate) oldNode;
          CorrelationId c = oldCor.getCorrelationId();
          if (cm.mapCorVarToCorRel.get(c) == oldNode) {
            cm.mapCorVarToCorRel.put(c, (LogicalCorrelate) newNode);
          }

          if (generatedCorRels.contains(oldNode)) {
            generatedCorRels.add((LogicalCorrelate) newNode);
          }
        }
        return null;
      }
    };
  }

  private HepPlanner createPlanner(HepProgram program) {
    // Create a planner with a hook to update the mapping tables when a
    // node is copied when it is registered.
    return new HepPlanner(
        program,
        context,
        true,
        createCopyHook(),
        RelOptCostImpl.FACTORY);
  }

  public RelNode removeCorrelationViaRule(RelNode root) {
    HepProgram program = HepProgram.builder()
        .addRuleInstance(new RemoveSingleAggregateRule())
        .addRuleInstance(new RemoveCorrelationForScalarProjectRule())
        .addRuleInstance(new RemoveCorrelationForScalarAggregateRule())
        .build();

    HepPlanner planner = createPlanner(program);

    planner.setRoot(root);
    RelNode newRootRel = planner.findBestExp();

    return newRootRel;
  }

  protected RexNode decorrelateExpr(RexNode exp) {
    DecorrelateRexShuttle shuttle = new DecorrelateRexShuttle();
    return exp.accept(shuttle);
  }

  protected RexNode removeCorrelationExpr(
      RexNode exp,
      boolean projectPulledAboveLeftCorrelator) {
    RemoveCorrelationRexShuttle shuttle =
        new RemoveCorrelationRexShuttle(
            rexBuilder,
            projectPulledAboveLeftCorrelator);
    return exp.accept(shuttle);
  }

  protected RexNode removeCorrelationExpr(
      RexNode exp,
      boolean projectPulledAboveLeftCorrelator,
      RexInputRef nullIndicator) {
    RemoveCorrelationRexShuttle shuttle =
        new RemoveCorrelationRexShuttle(
            rexBuilder,
            projectPulledAboveLeftCorrelator,
            nullIndicator);
    return exp.accept(shuttle);
  }

  protected RexNode removeCorrelationExpr(
      RexNode exp,
      boolean projectPulledAboveLeftCorrelator,
      Set<Integer> isCount) {
    RemoveCorrelationRexShuttle shuttle =
        new RemoveCorrelationRexShuttle(
            rexBuilder,
            projectPulledAboveLeftCorrelator,
            isCount);
    return exp.accept(shuttle);
  }

  public void decorrelateRelGeneric(RelNode rel) {
    RelNode newRel = rel.copy(rel.getTraitSet(), rel.getInputs());

    if (rel.getInputs().size() > 0) {
      List<RelNode> oldInputs = rel.getInputs();
      List<RelNode> newInputs = Lists.newArrayList();
      for (int i = 0; i < oldInputs.size(); ++i) {
        RelNode newInputRel = mapOldToNewRel.get(oldInputs.get(i));
        if ((newInputRel == null)
            || mapNewRelToMapCorVarToOutputPos.containsKey(newInputRel)) {
          // if child is not rewritten, or if it produces correlated
          // variables, terminate rewrite
          return;
        } else {
          newInputs.add(newInputRel);
          newRel.replaceInput(i, newInputRel);
        }
      }

      if (!Util.equalShallow(oldInputs, newInputs)) {
        newRel = rel.copy(rel.getTraitSet(), newInputs);
      }
    }

    // the output position should not change since there are no corVars
    // coming from below.
    Map<Integer, Integer> mapOldToNewOutputPos = Maps.newHashMap();
    for (int i = 0; i < rel.getRowType().getFieldCount(); i++) {
      mapOldToNewOutputPos.put(i, i);
    }
    mapOldToNewRel.put(rel, newRel);
    mapNewRelToMapOldToNewOutputPos.put(newRel, mapOldToNewOutputPos);
  }

  /**
   * Rewrite Sort.
   *
   * @param rel Sort to be rewritten
   */
  public void decorrelateRel(Sort rel) {
    //
    // Rewrite logic:
    //
    // 1. change the collations field to reference the new input.
    //

    // Sort itself should not reference cor vars.
    assert !cm.mapRefRelToCorVar.containsKey(rel);

    // Sort only references field positions in collations field.
    // The collations field in the newRel now need to refer to the
    // new output positions in its input.
    // Its output does not change the input ordering, so there's no
    // need to call propagateExpr.

    RelNode oldChildRel = rel.getInput();

    RelNode newChildRel = mapOldToNewRel.get(oldChildRel);
    if (newChildRel == null) {
      // If child has not been rewritten, do not rewrite this rel.
      return;
    }

    Map<Integer, Integer> childMapOldToNewOutputPos =
        mapNewRelToMapOldToNewOutputPos.get(newChildRel);
    assert childMapOldToNewOutputPos != null;
    Mappings.TargetMapping mapping =
        Mappings.target(
            childMapOldToNewOutputPos,
            oldChildRel.getRowType().getFieldCount(),
            newChildRel.getRowType().getFieldCount());

    RelCollation oldCollation = rel.getCollation();
    RelCollation newCollation = RexUtil.apply(mapping, oldCollation);

    final Sort newRel =
        LogicalSort.create(newChildRel, newCollation, rel.offset, rel.fetch);

    mapOldToNewRel.put(rel, newRel);

    // Sort does not change input ordering
    mapNewRelToMapOldToNewOutputPos.put(newRel, childMapOldToNewOutputPos);
  }

  /**
   * Rewrites a {@link LogicalAggregate}.
   *
   * @param rel Aggregate to rewrite
   */
  public void decorrelateRel(LogicalAggregate rel) {
    if (rel.getGroupType() != Aggregate.Group.SIMPLE) {
      throw new AssertionError(Bug.CALCITE_461_FIXED);
    }
    //
    // Rewrite logic:
    //
    // 1. Permute the group by keys to the front.
    // 2. If the child of an aggregate produces correlated variables,
    //    add them to the group list.
    // 3. Change aggCalls to reference the new project.
    //

    // Aggregate itself should not reference cor vars.
    assert !cm.mapRefRelToCorVar.containsKey(rel);

    RelNode oldChildRel = rel.getInput();

    RelNode newChildRel = mapOldToNewRel.get(oldChildRel);
    if (newChildRel == null) {
      // If child has not been rewritten, do not rewrite this rel.
      return;
    }

    Map<Integer, Integer> childMapOldToNewOutputPos =
        mapNewRelToMapOldToNewOutputPos.get(newChildRel);
    assert childMapOldToNewOutputPos != null;

    // map from newChildRel
    Map<Integer, Integer> mapNewChildToProjOutputPos = Maps.newHashMap();
    final int oldGroupKeyCount = rel.getGroupSet().cardinality();

    // LogicalProject projects the original expressions,
    // plus any correlated variables the child wants to pass along.
    final List<Pair<RexNode, String>> projects = Lists.newArrayList();

    List<RelDataTypeField> newChildOutput =
        newChildRel.getRowType().getFieldList();

    int newPos;

    // oldChildRel has the original group by keys in the front.
    for (newPos = 0; newPos < oldGroupKeyCount; newPos++) {
      int newChildPos = childMapOldToNewOutputPos.get(newPos);
      projects.add(RexInputRef.of2(newChildPos, newChildOutput));
      mapNewChildToProjOutputPos.put(newChildPos, newPos);
    }

    SortedMap<Correlation, Integer> mapCorVarToOutputPos = Maps.newTreeMap();

    boolean produceCorVar =
        mapNewRelToMapCorVarToOutputPos.containsKey(newChildRel);
    if (produceCorVar) {
      // If child produces correlated variables, move them to the front,
      // right after any existing groupby fields.

      SortedMap<Correlation, Integer> childMapCorVarToOutputPos =
          mapNewRelToMapCorVarToOutputPos.get(newChildRel);

      // Now add the corVars from the child, starting from
      // position oldGroupKeyCount.
      for (Correlation corVar
          : childMapCorVarToOutputPos.keySet()) {
        int newChildPos = childMapCorVarToOutputPos.get(corVar);
        projects.add(RexInputRef.of2(newChildPos, newChildOutput));

        mapCorVarToOutputPos.put(corVar, newPos);
        mapNewChildToProjOutputPos.put(newChildPos, newPos);
        newPos++;
      }
    }

    // add the remaining fields
    final int newGroupKeyCount = newPos;
    for (int i = 0; i < newChildOutput.size(); i++) {
      if (!mapNewChildToProjOutputPos.containsKey(i)) {
        projects.add(RexInputRef.of2(i, newChildOutput));
        mapNewChildToProjOutputPos.put(i, newPos);
        newPos++;
      }
    }

    assert newPos == newChildOutput.size();

    // This LogicalProject will be what the old child maps to,
    // replacing any previous mapping from old child).
    RelNode newProjectRel =
        RelOptUtil.createProject(newChildRel, projects, false);

    // update mappings:
    // oldChildRel ----> newChildRel
    //
    //                   newProjectRel
    //                        |
    // oldChildRel ---->  newChildRel
    //
    // is transformed to
    //
    // oldChildRel ----> newProjectRel
    //                        |
    //                   newChildRel
    Map<Integer, Integer> combinedMap = Maps.newHashMap();

    for (Integer oldChildPos : childMapOldToNewOutputPos.keySet()) {
      combinedMap.put(
          oldChildPos,
          mapNewChildToProjOutputPos.get(
              childMapOldToNewOutputPos.get(oldChildPos)));
    }

    mapOldToNewRel.put(oldChildRel, newProjectRel);
    mapNewRelToMapOldToNewOutputPos.put(newProjectRel, combinedMap);

    if (produceCorVar) {
      mapNewRelToMapCorVarToOutputPos.put(
          newProjectRel,
          mapCorVarToOutputPos);
    }

    // now it's time to rewrite LogicalAggregate
    List<AggregateCall> newAggCalls = Lists.newArrayList();
    List<AggregateCall> oldAggCalls = rel.getAggCallList();

    // LogicalAggregate.Call oldAggCall;
    int oldChildOutputFieldCount = oldChildRel.getRowType().getFieldCount();
    int newChildOutputFieldCount =
        newProjectRel.getRowType().getFieldCount();

    int i = -1;
    for (AggregateCall oldAggCall : oldAggCalls) {
      ++i;
      List<Integer> oldAggArgs = oldAggCall.getArgList();

      List<Integer> aggArgs = Lists.newArrayList();

      // Adjust the aggregator argument positions.
      // Note aggregator does not change input ordering, so the child
      // output position mapping can be used to derive the new positions
      // for the argument.
      for (int oldPos : oldAggArgs) {
        aggArgs.add(combinedMap.get(oldPos));
      }

      newAggCalls.add(
          oldAggCall.adaptTo(newProjectRel, aggArgs,
              oldGroupKeyCount, newGroupKeyCount));

      // The old to new output position mapping will be the same as that
      // of newProjectRel, plus any aggregates that the oldAgg produces.
      combinedMap.put(
          oldChildOutputFieldCount + i,
          newChildOutputFieldCount + i);
    }

    LogicalAggregate newAggregate =
        LogicalAggregate.create(newProjectRel,
            false,
            ImmutableBitSet.range(newGroupKeyCount),
            null,
            newAggCalls);

    mapOldToNewRel.put(rel, newAggregate);

    mapNewRelToMapOldToNewOutputPos.put(newAggregate, combinedMap);

    if (produceCorVar) {
      // LogicalAggregate does not change input ordering so corVars will be
      // located at the same position as the input newProjectRel.
      mapNewRelToMapCorVarToOutputPos.put(
          newAggregate,
          mapCorVarToOutputPos);
    }
  }

  /**
   * Rewrite LogicalProject.
   *
   * @param rel the project rel to rewrite
   */
  public void decorrelateRel(LogicalProject rel) {
    //
    // Rewrite logic:
    //
    // 1. Pass along any correlated variables coming from the child.
    //

    RelNode oldChildRel = rel.getInput();

    RelNode newChildRel = mapOldToNewRel.get(oldChildRel);
    if (newChildRel == null) {
      // If child has not been rewritten, do not rewrite this rel.
      return;
    }
    List<RexNode> oldProj = rel.getProjects();
    List<RelDataTypeField> relOutput = rel.getRowType().getFieldList();

    Map<Integer, Integer> childMapOldToNewOutputPos =
        mapNewRelToMapOldToNewOutputPos.get(newChildRel);
    assert childMapOldToNewOutputPos != null;

    Map<Integer, Integer> mapOldToNewOutputPos = Maps.newHashMap();

    boolean produceCorVar =
        mapNewRelToMapCorVarToOutputPos.containsKey(newChildRel);

    // LogicalProject projects the original expressions,
    // plus any correlated variables the child wants to pass along.
    final List<Pair<RexNode, String>> projects = Lists.newArrayList();

    // If this LogicalProject has correlated reference, create value generator
    // and produce the correlated variables in the new output.
    if (cm.mapRefRelToCorVar.containsKey(rel)) {
      decorrelateInputWithValueGenerator(rel);

      // The old child should be mapped to the LogicalJoin created by
      // rewriteInputWithValueGenerator().
      newChildRel = mapOldToNewRel.get(oldChildRel);
      produceCorVar = true;
    }

    // LogicalProject projects the original expressions
    int newPos;
    for (newPos = 0; newPos < oldProj.size(); newPos++) {
      projects.add(
          newPos,
          Pair.of(
              decorrelateExpr(oldProj.get(newPos)),
              relOutput.get(newPos).getName()));
      mapOldToNewOutputPos.put(newPos, newPos);
    }

    SortedMap<Correlation, Integer> mapCorVarToOutputPos = Maps.newTreeMap();

    // Project any correlated variables the child wants to pass along.
    if (produceCorVar) {
      SortedMap<Correlation, Integer> childMapCorVarToOutputPos =
          mapNewRelToMapCorVarToOutputPos.get(newChildRel);

      // propagate cor vars from the new child
      List<RelDataTypeField> newChildOutput =
          newChildRel.getRowType().getFieldList();
      for (Correlation corVar
          : childMapCorVarToOutputPos.keySet()) {
        int corVarPos = childMapCorVarToOutputPos.get(corVar);
        projects.add(RexInputRef.of2(corVarPos, newChildOutput));
        mapCorVarToOutputPos.put(corVar, newPos);
        newPos++;
      }
    }

    RelNode newProjectRel =
        RelOptUtil.createProject(newChildRel, projects, false);

    mapOldToNewRel.put(rel, newProjectRel);
    mapNewRelToMapOldToNewOutputPos.put(
        newProjectRel,
        mapOldToNewOutputPos);

    if (produceCorVar) {
      mapNewRelToMapCorVarToOutputPos.put(
          newProjectRel,
          mapCorVarToOutputPos);
    }
  }

  /**
   * Create RelNode tree that produces a list of correlated variables.
   *
   * @param correlations         correlated variables to generate
   * @param valueGenFieldOffset  offset in the output that generated columns
   *                             will start
   * @param mapCorVarToOutputPos output positions for the correlated variables
   *                             generated
   * @return RelNode the root of the resultant RelNode tree
   */
  private RelNode createValueGenerator(
      Iterable<Correlation> correlations,
      int valueGenFieldOffset,
      SortedMap<Correlation, Integer> mapCorVarToOutputPos) {
    RelNode resultRel = null;

    Map<RelNode, List<Integer>> mapNewInputRelToOutputPos = Maps.newHashMap();

    Map<RelNode, Integer> mapNewInputRelToNewOffset = Maps.newHashMap();

    RelNode oldInputRel;
    RelNode newInputRel;
    List<Integer> newLocalOutputPosList;

    // inputRel provides the definition of a correlated variable.
    // Add to map all the referenced positions(relative to each input rel)
    for (Correlation corVar : correlations) {
      int oldCorVarOffset = corVar.field;

      oldInputRel = cm.mapCorVarToCorRel.get(corVar.corr).getInput(0);
      assert oldInputRel != null;
      newInputRel = mapOldToNewRel.get(oldInputRel);
      assert newInputRel != null;

      if (!mapNewInputRelToOutputPos.containsKey(newInputRel)) {
        newLocalOutputPosList = Lists.newArrayList();
      } else {
        newLocalOutputPosList =
            mapNewInputRelToOutputPos.get(newInputRel);
      }

      Map<Integer, Integer> mapOldToNewOutputPos =
          mapNewRelToMapOldToNewOutputPos.get(newInputRel);
      assert mapOldToNewOutputPos != null;

      int newCorVarOffset = mapOldToNewOutputPos.get(oldCorVarOffset);

      // Add all unique positions referenced.
      if (!newLocalOutputPosList.contains(newCorVarOffset)) {
        newLocalOutputPosList.add(newCorVarOffset);
      }
      mapNewInputRelToOutputPos.put(newInputRel, newLocalOutputPosList);
    }

    int offset = 0;

    // Project only the correlated fields out of each inputRel
    // and join the projectRel together.
    // To make sure the plan does not change in terms of join order,
    // join these rels based on their occurrence in cor var list which
    // is sorted.
    Set<RelNode> joinedInputRelSet = Sets.newHashSet();

    for (Correlation corVar : correlations) {
      oldInputRel = cm.mapCorVarToCorRel.get(corVar.corr).getInput(0);
      assert oldInputRel != null;
      newInputRel = mapOldToNewRel.get(oldInputRel);
      assert newInputRel != null;

      if (!joinedInputRelSet.contains(newInputRel)) {
        RelNode projectRel =
            RelOptUtil.createProject(
                newInputRel,
                mapNewInputRelToOutputPos.get(newInputRel));
        RelNode distinctRel = RelOptUtil.createDistinctRel(projectRel);
        RelOptCluster cluster = distinctRel.getCluster();

        joinedInputRelSet.add(newInputRel);
        mapNewInputRelToNewOffset.put(newInputRel, offset);
        offset += distinctRel.getRowType().getFieldCount();

        if (resultRel == null) {
          resultRel = distinctRel;
        } else {
          resultRel =
              LogicalJoin.create(resultRel, distinctRel,
                  cluster.getRexBuilder().makeLiteral(true),
                  JoinRelType.INNER, ImmutableSet.<String>of());
        }
      }
    }

    // Translate the positions of correlated variables to be relative to
    // the join output, leaving room for valueGenFieldOffset because
    // valueGenerators are joined with the original left input of the rel
    // referencing correlated variables.
    int newOutputPos;
    int newLocalOutputPos;
    for (Correlation corVar : correlations) {
      // The first child of a correlatorRel is always the rel defining
      // the correlated variables.
      newInputRel =
          mapOldToNewRel.get(cm.mapCorVarToCorRel.get(corVar.corr).getInput(0));
      newLocalOutputPosList = mapNewInputRelToOutputPos.get(newInputRel);

      Map<Integer, Integer> mapOldToNewOutputPos =
          mapNewRelToMapOldToNewOutputPos.get(newInputRel);
      assert mapOldToNewOutputPos != null;

      newLocalOutputPos = mapOldToNewOutputPos.get(corVar.field);

      // newOutputPos is the index of the cor var in the referenced
      // position list plus the offset of referenced position list of
      // each newInputRel.
      newOutputPos =
          newLocalOutputPosList.indexOf(newLocalOutputPos)
              + mapNewInputRelToNewOffset.get(newInputRel)
              + valueGenFieldOffset;

      if (mapCorVarToOutputPos.containsKey(corVar)) {
        assert mapCorVarToOutputPos.get(corVar) == newOutputPos;
      }
      mapCorVarToOutputPos.put(corVar, newOutputPos);
    }

    return resultRel;
  }

  private void decorrelateInputWithValueGenerator(
      RelNode rel) {
    // currently only handles one child input
    assert rel.getInputs().size() == 1;
    RelNode oldChildRel = rel.getInput(0);
    RelNode newChildRel = mapOldToNewRel.get(oldChildRel);

    Map<Integer, Integer> childMapOldToNewOutputPos =
        mapNewRelToMapOldToNewOutputPos.get(newChildRel);
    assert childMapOldToNewOutputPos != null;

    SortedMap<Correlation, Integer> mapCorVarToOutputPos = Maps.newTreeMap();

    if (mapNewRelToMapCorVarToOutputPos.containsKey(newChildRel)) {
      mapCorVarToOutputPos.putAll(
          mapNewRelToMapCorVarToOutputPos.get(newChildRel));
    }

    final Collection<Correlation> corVarList = cm.mapRefRelToCorVar.get(rel);

    RelNode newLeftChildRel = newChildRel;

    int leftChildOutputCount = newLeftChildRel.getRowType().getFieldCount();

    // can directly add positions into mapCorVarToOutputPos since join
    // does not change the output ordering from the children.
    RelNode valueGenRel =
        createValueGenerator(
            corVarList,
            leftChildOutputCount,
            mapCorVarToOutputPos);

    final Set<String> variablesStopped = Collections.emptySet();
    RelNode joinRel =
        LogicalJoin.create(newLeftChildRel, valueGenRel,
            rexBuilder.makeLiteral(true), JoinRelType.INNER, variablesStopped);

    mapOldToNewRel.put(oldChildRel, joinRel);
    mapNewRelToMapCorVarToOutputPos.put(joinRel, mapCorVarToOutputPos);

    // LogicalJoin or LogicalFilter does not change the old input ordering. All
    // input fields from newLeftInput(i.e. the original input to the old
    // LogicalFilter) are in the output and in the same position.
    mapNewRelToMapOldToNewOutputPos.put(joinRel, childMapOldToNewOutputPos);
  }

  /**
   * Rewrite LogicalFilter.
   *
   * @param rel the filter rel to rewrite
   */
  public void decorrelateRel(LogicalFilter rel) {
    //
    // Rewrite logic:
    //
    // 1. If a LogicalFilter references a correlated field in its filter
    // condition, rewrite the LogicalFilter to be
    //   LogicalFilter
    //     LogicalJoin(cross product)
    //       OriginalFilterInput
    //       ValueGenerator(produces distinct sets of correlated variables)
    // and rewrite the correlated fieldAccess in the filter condition to
    // reference the LogicalJoin output.
    //
    // 2. If LogicalFilter does not reference correlated variables, simply
    // rewrite the filter condition using new input.
    //

    RelNode oldChildRel = rel.getInput();

    RelNode newChildRel = mapOldToNewRel.get(oldChildRel);
    if (newChildRel == null) {
      // If child has not been rewritten, do not rewrite this rel.
      return;
    }

    Map<Integer, Integer> childMapOldToNewOutputPos =
        mapNewRelToMapOldToNewOutputPos.get(newChildRel);
    assert childMapOldToNewOutputPos != null;

    boolean produceCorVar =
        mapNewRelToMapCorVarToOutputPos.containsKey(newChildRel);

    // If this LogicalFilter has correlated reference, create value generator
    // and produce the correlated variables in the new output.
    if (cm.mapRefRelToCorVar.containsKey(rel)) {
      decorrelateInputWithValueGenerator(rel);

      // The old child should be mapped to the newly created LogicalJoin by
      // rewriteInputWithValueGenerator().
      newChildRel = mapOldToNewRel.get(oldChildRel);
      produceCorVar = true;
    }

    // Replace the filter expression to reference output of the join
    // Map filter to the new filter over join
    RelNode newFilterRel =
        RelOptUtil.createFilter(
            newChildRel,
            decorrelateExpr(rel.getCondition()));

    mapOldToNewRel.put(rel, newFilterRel);

    // Filter does not change the input ordering.
    mapNewRelToMapOldToNewOutputPos.put(
        newFilterRel,
        childMapOldToNewOutputPos);

    if (produceCorVar) {
      // filter rel does not permute the input all corvars produced by
      // filter will have the same output positions in the child rel.
      mapNewRelToMapCorVarToOutputPos.put(
          newFilterRel,
          mapNewRelToMapCorVarToOutputPos.get(newChildRel));
    }
  }

  /**
   * Rewrite Correlator into a left outer join.
   *
   * @param rel Correlator
   */
  public void decorrelateRel(LogicalCorrelate rel) {
    //
    // Rewrite logic:
    //
    // The original left input will be joined with the new right input that
    // has generated correlated variables propagated up. For any generated
    // cor vars that are not used in the join key, pass them along to be
    // joined later with the CorrelatorRels that produce them.
    //

    // the right input to Correlator should produce correlated variables
    RelNode oldLeftRel = rel.getInputs().get(0);
    RelNode oldRightRel = rel.getInputs().get(1);

    RelNode newLeftRel = mapOldToNewRel.get(oldLeftRel);
    RelNode newRightRel = mapOldToNewRel.get(oldRightRel);

    if ((newLeftRel == null) || (newRightRel == null)) {
      // If any child has not been rewritten, do not rewrite this rel.
      return;
    }

    SortedMap<Correlation, Integer> rightChildMapCorVarToOutputPos =
        mapNewRelToMapCorVarToOutputPos.get(newRightRel);

    if (rightChildMapCorVarToOutputPos == null) {
      return;
    }

    Map<Integer, Integer> leftChildMapOldToNewOutputPos =
        mapNewRelToMapOldToNewOutputPos.get(newLeftRel);
    assert leftChildMapOldToNewOutputPos != null;

    Map<Integer, Integer> rightChildMapOldToNewOutputPos =
        mapNewRelToMapOldToNewOutputPos.get(newRightRel);

    assert rightChildMapOldToNewOutputPos != null;

    SortedMap<Correlation, Integer> mapCorVarToOutputPos =
        rightChildMapCorVarToOutputPos;

    assert rel.getRequiredColumns().cardinality()
        <= rightChildMapCorVarToOutputPos.keySet().size();

    // Change correlator rel into a join.
    // Join all the correlated variables produced by this correlator rel
    // with the values generated and propagated from the right input
    RexNode condition = rexBuilder.makeLiteral(true);
    final List<RelDataTypeField> newLeftOutput =
        newLeftRel.getRowType().getFieldList();
    int newLeftFieldCount = newLeftOutput.size();

    final List<RelDataTypeField> newRightOutput =
        newRightRel.getRowType().getFieldList();

    int newLeftPos;
    int newRightPos;
    for (Map.Entry<Correlation, Integer> rightOutputPos
        : Lists.newArrayList(rightChildMapCorVarToOutputPos.entrySet())) {
      Correlation corVar = rightOutputPos.getKey();
      if (!corVar.corr.equals(rel.getCorrelationId())) {
        continue;
      }
      newLeftPos = leftChildMapOldToNewOutputPos.get(corVar.field);
      newRightPos = rightChildMapCorVarToOutputPos.get(corVar);
      RexNode equi =
          rexBuilder.makeCall(
              SqlStdOperatorTable.EQUALS,
              RexInputRef.of(newLeftPos, newLeftOutput),
              new RexInputRef(
                  newLeftFieldCount + newRightPos,
                  newRightOutput.get(newRightPos).getType()));
      if (condition == rexBuilder.makeLiteral(true)) {
        condition = equi;
      } else {
        condition =
            rexBuilder.makeCall(
                SqlStdOperatorTable.AND,
                condition,
                equi);
      }

      // remove this cor var from output position mapping
      mapCorVarToOutputPos.remove(corVar);
    }

    // Update the output position for the cor vars: only pass on the cor
    // vars that are not used in the join key.
    for (Correlation corVar : mapCorVarToOutputPos.keySet()) {
      int newPos = mapCorVarToOutputPos.get(corVar) + newLeftFieldCount;
      mapCorVarToOutputPos.put(corVar, newPos);
    }

    // then add any cor var from the left input. Do not need to change
    // output positions.
    if (mapNewRelToMapCorVarToOutputPos.containsKey(newLeftRel)) {
      mapCorVarToOutputPos.putAll(
          mapNewRelToMapCorVarToOutputPos.get(newLeftRel));
    }

    // Create the mapping between the output of the old correlation rel
    // and the new join rel
    Map<Integer, Integer> mapOldToNewOutputPos = Maps.newHashMap();

    int oldLeftFieldCount = oldLeftRel.getRowType().getFieldCount();

    int oldRightFieldCount = oldRightRel.getRowType().getFieldCount();
    assert rel.getRowType().getFieldCount()
        == oldLeftFieldCount + oldRightFieldCount;

    // Left input positions are not changed.
    mapOldToNewOutputPos.putAll(leftChildMapOldToNewOutputPos);

    // Right input positions are shifted by newLeftFieldCount.
    for (int i = 0; i < oldRightFieldCount; i++) {
      mapOldToNewOutputPos.put(
          i + oldLeftFieldCount,
          rightChildMapOldToNewOutputPos.get(i) + newLeftFieldCount);
    }

    final Set<String> variablesStopped = Collections.emptySet();
    RelNode newRel =
        LogicalJoin.create(newLeftRel, newRightRel, condition,
            rel.getJoinType().toJoinType(), variablesStopped);

    mapOldToNewRel.put(rel, newRel);
    mapNewRelToMapOldToNewOutputPos.put(newRel, mapOldToNewOutputPos);

    if (!mapCorVarToOutputPos.isEmpty()) {
      mapNewRelToMapCorVarToOutputPos.put(newRel, mapCorVarToOutputPos);
    }
  }

  /**
   * Rewrite LogicalJoin.
   *
   * @param rel LogicalJoin
   */
  public void decorrelateRel(LogicalJoin rel) {
    //
    // Rewrite logic:
    //
    // 1. rewrite join condition.
    // 2. map output positions and produce cor vars if any.
    //

    RelNode oldLeftRel = rel.getInputs().get(0);
    RelNode oldRightRel = rel.getInputs().get(1);

    RelNode newLeftRel = mapOldToNewRel.get(oldLeftRel);
    RelNode newRightRel = mapOldToNewRel.get(oldRightRel);

    if ((newLeftRel == null) || (newRightRel == null)) {
      // If any child has not been rewritten, do not rewrite this rel.
      return;
    }

    Map<Integer, Integer> leftChildMapOldToNewOutputPos =
        mapNewRelToMapOldToNewOutputPos.get(newLeftRel);
    assert leftChildMapOldToNewOutputPos != null;

    Map<Integer, Integer> rightChildMapOldToNewOutputPos =
        mapNewRelToMapOldToNewOutputPos.get(newRightRel);
    assert rightChildMapOldToNewOutputPos != null;

    SortedMap<Correlation, Integer> mapCorVarToOutputPos = Maps.newTreeMap();

    final Set<String> variablesStopped = Collections.emptySet();
    RelNode newRel =
        LogicalJoin.create(newLeftRel, newRightRel,
            decorrelateExpr(rel.getCondition()), rel.getJoinType(),
            variablesStopped);

    // Create the mapping between the output of the old correlation rel
    // and the new join rel
    Map<Integer, Integer> mapOldToNewOutputPos = Maps.newHashMap();

    int oldLeftFieldCount = oldLeftRel.getRowType().getFieldCount();
    int newLeftFieldCount = newLeftRel.getRowType().getFieldCount();

    int oldRightFieldCount = oldRightRel.getRowType().getFieldCount();
    assert rel.getRowType().getFieldCount()
        == oldLeftFieldCount + oldRightFieldCount;

    // Left input positions are not changed.
    mapOldToNewOutputPos.putAll(leftChildMapOldToNewOutputPos);

    // Right input positions are shifted by newLeftFieldCount.
    for (int i = 0; i < oldRightFieldCount; i++) {
      mapOldToNewOutputPos.put(
          i + oldLeftFieldCount,
          rightChildMapOldToNewOutputPos.get(i) + newLeftFieldCount);
    }

    if (mapNewRelToMapCorVarToOutputPos.containsKey(newLeftRel)) {
      mapCorVarToOutputPos.putAll(
          mapNewRelToMapCorVarToOutputPos.get(newLeftRel));
    }

    // Right input positions are shifted by newLeftFieldCount.
    int oldRightPos;
    if (mapNewRelToMapCorVarToOutputPos.containsKey(newRightRel)) {
      SortedMap<Correlation, Integer> rightChildMapCorVarToOutputPos =
          mapNewRelToMapCorVarToOutputPos.get(newRightRel);
      for (Correlation corVar : rightChildMapCorVarToOutputPos.keySet()) {
        oldRightPos = rightChildMapCorVarToOutputPos.get(corVar);
        mapCorVarToOutputPos.put(
            corVar,
            oldRightPos + newLeftFieldCount);
      }
    }
    mapOldToNewRel.put(rel, newRel);
    mapNewRelToMapOldToNewOutputPos.put(newRel, mapOldToNewOutputPos);

    if (!mapCorVarToOutputPos.isEmpty()) {
      mapNewRelToMapCorVarToOutputPos.put(newRel, mapCorVarToOutputPos);
    }
  }

  private RexInputRef getNewForOldInputRef(RexInputRef oldInputRef) {
    assert currentRel != null;

    int oldOrdinal = oldInputRef.getIndex();
    int newOrdinal = 0;

    // determine which input rel oldOrdinal references, and adjust
    // oldOrdinal to be relative to that input rel
    List<RelNode> oldInputRels = currentRel.getInputs();
    RelNode oldInputRel = null;

    for (RelNode oldInputRel0 : oldInputRels) {
      RelDataType oldInputType = oldInputRel0.getRowType();
      int n = oldInputType.getFieldCount();
      if (oldOrdinal < n) {
        oldInputRel = oldInputRel0;
        break;
      }
      RelNode newInput = mapOldToNewRel.get(oldInputRel0);
      newOrdinal += newInput.getRowType().getFieldCount();
      oldOrdinal -= n;
    }

    assert oldInputRel != null;

    RelNode newInputRel = mapOldToNewRel.get(oldInputRel);
    assert newInputRel != null;

    // now oldOrdinal is relative to oldInputRel
    int oldLocalOrdinal = oldOrdinal;

    // figure out the newLocalOrdinal, relative to the newInputRel.
    int newLocalOrdinal = oldLocalOrdinal;

    Map<Integer, Integer> mapOldToNewOutputPos =
        mapNewRelToMapOldToNewOutputPos.get(newInputRel);

    if (mapOldToNewOutputPos != null) {
      newLocalOrdinal = mapOldToNewOutputPos.get(oldLocalOrdinal);
    }

    newOrdinal += newLocalOrdinal;

    return new RexInputRef(newOrdinal,
        newInputRel.getRowType().getFieldList().get(newLocalOrdinal).getType());
  }

  /**
   * Pull projRel above the join from its RHS input. Enforce nullability
   * for join output.
   *
   * @param join          Join
   * @param projRel          the original projRel as the RHS input of the join.
   * @param nullIndicatorPos Position of null indicator
   * @return the subtree with the new LogicalProject at the root
   */
  private RelNode projectJoinOutputWithNullability(
      LogicalJoin join,
      LogicalProject projRel,
      int nullIndicatorPos) {
    RelDataTypeFactory typeFactory = join.getCluster().getTypeFactory();
    RelNode leftInputRel = join.getLeft();
    JoinRelType joinType = join.getJoinType();

    RexInputRef nullIndicator =
        new RexInputRef(
            nullIndicatorPos,
            typeFactory.createTypeWithNullability(
                join.getRowType().getFieldList().get(nullIndicatorPos)
                    .getType(),
                true));

    // now create the new project
    List<Pair<RexNode, String>> newProjExprs = Lists.newArrayList();

    // project everything from the LHS and then those from the original
    // projRel
    List<RelDataTypeField> leftInputFields =
        leftInputRel.getRowType().getFieldList();

    for (int i = 0; i < leftInputFields.size(); i++) {
      newProjExprs.add(RexInputRef.of2(i, leftInputFields));
    }

    // Marked where the projected expr is coming from so that the types will
    // become nullable for the original projections which are now coming out
    // of the nullable side of the OJ.
    boolean projectPulledAboveLeftCorrelator =
        joinType.generatesNullsOnRight();

    for (Pair<RexNode, String> pair : projRel.getNamedProjects()) {
      RexNode newProjExpr =
          removeCorrelationExpr(
              pair.left,
              projectPulledAboveLeftCorrelator,
              nullIndicator);

      newProjExprs.add(Pair.of(newProjExpr, pair.right));
    }

    RelNode newProjRel =
        RelOptUtil.createProject(join, newProjExprs, false);

    return newProjRel;
  }

  /**
   * Pulls projRel above the joinRel from its RHS input. Enforces nullability
   * for join output.
   *
   * @param corRel  Correlator
   * @param projRel the original LogicalProject as the RHS input of the join
   * @param isCount Positions which are calls to the <code>COUNT</code>
   *                aggregation function
   * @return the subtree with the new LogicalProject at the root
   */
  private RelNode aggregateCorrelatorOutput(
      LogicalCorrelate corRel,
      LogicalProject projRel,
      Set<Integer> isCount) {
    RelNode leftInputRel = corRel.getLeft();
    JoinRelType joinType = corRel.getJoinType().toJoinType();

    // now create the new project
    List<Pair<RexNode, String>> newProjects = Lists.newArrayList();

    // project everything from the LHS and then those from the original
    // projRel
    List<RelDataTypeField> leftInputFields =
        leftInputRel.getRowType().getFieldList();

    for (int i = 0; i < leftInputFields.size(); i++) {
      newProjects.add(RexInputRef.of2(i, leftInputFields));
    }

    // Marked where the projected expr is coming from so that the types will
    // become nullable for the original projections which are now coming out
    // of the nullable side of the OJ.
    boolean projectPulledAboveLeftCorrelator =
        joinType.generatesNullsOnRight();

    for (Pair<RexNode, String> pair : projRel.getNamedProjects()) {
      RexNode newProjExpr =
          removeCorrelationExpr(
              pair.left,
              projectPulledAboveLeftCorrelator,
              isCount);
      newProjects.add(Pair.of(newProjExpr, pair.right));
    }

    return RelOptUtil.createProject(corRel, newProjects, false);
  }

  /**
   * Checks whether the correlations in projRel and filter are related to
   * the correlated variables provided by corRel.
   *
   * @param corRel    Correlator
   * @param projRel   The original Project as the RHS input of the join
   * @param filter    Filter
   * @param correlatedJoinKeys Correlated join keys
   * @return true if filter and proj only references corVar provided by corRel
   */
  private boolean checkCorVars(
      LogicalCorrelate corRel,
      LogicalProject projRel,
      LogicalFilter filter,
      List<RexFieldAccess> correlatedJoinKeys) {
    if (filter != null) {
      assert correlatedJoinKeys != null;

      // check that all correlated refs in the filter condition are
      // used in the join(as field access).
      Set<Correlation> corVarInFilter =
          Sets.newHashSet(cm.mapRefRelToCorVar.get(filter));

      for (RexFieldAccess correlatedJoinKey : correlatedJoinKeys) {
        corVarInFilter.remove(
            cm.mapFieldAccessToCorVar.get(correlatedJoinKey));
      }

      if (!corVarInFilter.isEmpty()) {
        return false;
      }

      // Check that the correlated variables referenced in these
      // comparisons do come from the correlatorRel.
      corVarInFilter.addAll(cm.mapRefRelToCorVar.get(filter));

      for (Correlation corVar : corVarInFilter) {
        if (cm.mapCorVarToCorRel.get(corVar.corr) != corRel) {
          return false;
        }
      }
    }

    // if projRel has any correlated reference, make sure they are also
    // provided by the current corRel. They will be projected out of the LHS
    // of the corRel.
    if ((projRel != null) && cm.mapRefRelToCorVar.containsKey(projRel)) {
      for (Correlation corVar : cm.mapRefRelToCorVar.get(projRel)) {
        if (cm.mapCorVarToCorRel.get(corVar.corr) != corRel) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Remove correlated variables from the tree at root corRel
   *
   * @param corRel Correlator
   */
  private void removeCorVarFromTree(LogicalCorrelate corRel) {
    if (cm.mapCorVarToCorRel.get(corRel.getCorrelationId()) == corRel) {
      cm.mapCorVarToCorRel.remove(corRel.getCorrelationId());
    }
  }

  /**
   * Project all childRel output fields plus the additional expressions.
   *
   * @param childRel        Child relational expression
   * @param additionalExprs Additional expressions and names
   * @return the new LogicalProject
   */
  private RelNode createProjectWithAdditionalExprs(
      RelNode childRel,
      List<Pair<RexNode, String>> additionalExprs) {
    final List<RelDataTypeField> fieldList =
        childRel.getRowType().getFieldList();
    List<Pair<RexNode, String>> projects = Lists.newArrayList();
    for (Ord<RelDataTypeField> field : Ord.zip(fieldList)) {
      projects.add(
          Pair.of(
              (RexNode) rexBuilder.makeInputRef(
                  field.e.getType(), field.i),
              field.e.getName()));
    }
    projects.addAll(additionalExprs);
    return RelOptUtil.createProject(childRel, projects, false);
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Visitor that decorrelates. */
  private class DecorrelateRelVisitor extends RelVisitor {
    private final ReflectiveVisitDispatcher<RelDecorrelator, RelNode>
    dispatcher =
        ReflectUtil.createDispatcher(
            RelDecorrelator.class,
            RelNode.class);

    // implement RelVisitor
    public void visit(RelNode p, int ordinal, RelNode parent) {
      // rewrite children first  (from left to right)
      super.visit(p, ordinal, parent);

      currentRel = p;

      final String visitMethodName = "decorrelateRel";
      boolean found =
          dispatcher.invokeVisitor(
              RelDecorrelator.this,
              currentRel,
              visitMethodName);
      setCurrent(null, null);

      if (!found) {
        decorrelateRelGeneric(p);
      }
      // else no rewrite will occur. This will terminate the bottom-up
      // rewrite. If root node of a RelNode tree is not rewritten, the
      // original tree will be returned. See decorrelate() method.
    }
  }

  /** Shuttle that decorrelates. */
  private class DecorrelateRexShuttle extends RexShuttle {
    // override RexShuttle
    public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      int newInputRelOutputOffset = 0;
      RelNode oldInputRel;
      RelNode newInputRel;
      Integer newInputPos;

      List<RelNode> inputs = currentRel.getInputs();
      for (int i = 0; i < inputs.size(); i++) {
        oldInputRel = inputs.get(i);
        newInputRel = mapOldToNewRel.get(oldInputRel);

        if ((newInputRel != null)
            && mapNewRelToMapCorVarToOutputPos.containsKey(newInputRel)) {
          SortedMap<Correlation, Integer> childMapCorVarToOutputPos =
              mapNewRelToMapCorVarToOutputPos.get(newInputRel);

          if (childMapCorVarToOutputPos != null) {
            // try to find in this input rel the position of cor var
            Correlation corVar = cm.mapFieldAccessToCorVar.get(fieldAccess);

            if (corVar != null) {
              newInputPos = childMapCorVarToOutputPos.get(corVar);
              if (newInputPos != null) {
                // this input rel does produce the cor var
                // referenced
                newInputPos += newInputRelOutputOffset;

                // fieldAccess is assumed to have the correct
                // type info.
                RexInputRef newInput =
                    new RexInputRef(
                        newInputPos,
                        fieldAccess.getType());
                return newInput;
              }
            }
          }

          // this input rel does not produce the cor var needed
          newInputRelOutputOffset +=
              newInputRel.getRowType().getFieldCount();
        } else {
          // this input rel is not rewritten
          newInputRelOutputOffset +=
              oldInputRel.getRowType().getFieldCount();
        }
      }
      return fieldAccess;
    }

    // override RexShuttle
    public RexNode visitInputRef(RexInputRef inputRef) {
      RexInputRef newInputRef = getNewForOldInputRef(inputRef);
      return newInputRef;
    }
  }

  /** Shuttle that removes correlations. */
  private class RemoveCorrelationRexShuttle extends RexShuttle {
    RexBuilder rexBuilder;
    RelDataTypeFactory typeFactory;
    boolean projectPulledAboveLeftCorrelator;
    RexInputRef nullIndicator;
    Set<Integer> isCount;

    public RemoveCorrelationRexShuttle(
        RexBuilder rexBuilder,
        boolean projectPulledAboveLeftCorrelator) {
      this(
          rexBuilder,
          projectPulledAboveLeftCorrelator,
          null, null);
    }

    public RemoveCorrelationRexShuttle(
        RexBuilder rexBuilder,
        boolean projectPulledAboveLeftCorrelator,
        RexInputRef nullIndicator) {
      this(
          rexBuilder,
          projectPulledAboveLeftCorrelator,
          nullIndicator,
          null);
    }

    public RemoveCorrelationRexShuttle(
        RexBuilder rexBuilder,
        boolean projectPulledAboveLeftCorrelator,
        Set<Integer> isCount) {
      this(
          rexBuilder,
          projectPulledAboveLeftCorrelator,
          null, isCount);
    }

    public RemoveCorrelationRexShuttle(
        RexBuilder rexBuilder,
        boolean projectPulledAboveLeftCorrelator,
        RexInputRef nullIndicator,
        Set<Integer> isCount) {
      this.projectPulledAboveLeftCorrelator =
          projectPulledAboveLeftCorrelator;
      this.nullIndicator = nullIndicator;
      this.isCount = isCount;
      this.rexBuilder = rexBuilder;
      this.typeFactory = rexBuilder.getTypeFactory();
    }

    private RexNode createCaseExpression(
        RexInputRef nullInputRef,
        RexLiteral lit,
        RexNode rexNode) {
      RexNode[] caseOperands = new RexNode[3];

      // Construct a CASE expression to handle the null indicator.
      //
      // This also covers the case where a left correlated subquery
      // projects fields from outer relation. Since LOJ cannot produce
      // nulls on the LHS, the projection now need to make a nullable LHS
      // reference using a nullability indicator. If this this indicator
      // is null, it means the subquery does not produce any value. As a
      // result, any RHS ref by this usbquery needs to produce null value.

      // WHEN indicator IS NULL
      caseOperands[0] =
          rexBuilder.makeCall(
              SqlStdOperatorTable.IS_NULL,
              new RexInputRef(
                  nullInputRef.getIndex(),
                  typeFactory.createTypeWithNullability(
                      nullInputRef.getType(),
                      true)));

      // THEN CAST(NULL AS newInputTypeNullable)
      caseOperands[1] =
          rexBuilder.makeCast(
              typeFactory.createTypeWithNullability(
                  rexNode.getType(),
                  true),
              lit);

      // ELSE cast (newInput AS newInputTypeNullable) END
      caseOperands[2] =
          rexBuilder.makeCast(
              typeFactory.createTypeWithNullability(
                  rexNode.getType(),
                  true),
              rexNode);

      return rexBuilder.makeCall(
          SqlStdOperatorTable.CASE,
          caseOperands);
    }

    // override RexShuttle
    public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      if (cm.mapFieldAccessToCorVar.containsKey(fieldAccess)) {
        // if it is a corVar, change it to be input ref.
        Correlation corVar = cm.mapFieldAccessToCorVar.get(fieldAccess);

        // corVar offset should point to the leftInput of currentRel,
        // which is the Correlator.
        RexNode newRexNode =
            new RexInputRef(corVar.field, fieldAccess.getType());

        if (projectPulledAboveLeftCorrelator
            && (nullIndicator != null)) {
          // need to enforce nullability by applying an additional
          // cast operator over the transformed expression.
          newRexNode =
              createCaseExpression(
                  nullIndicator,
                  rexBuilder.constantNull(),
                  newRexNode);
        }
        return newRexNode;
      }
      return fieldAccess;
    }

    // override RexShuttle
    public RexNode visitInputRef(RexInputRef inputRef) {
      if ((currentRel != null) && (currentRel instanceof LogicalCorrelate)) {
        // if this rel references corVar
        // and now it needs to be rewritten
        // it must have been pulled above the Correlator
        // replace the input ref to account for the LHS of the
        // Correlator
        int leftInputFieldCount =
            ((LogicalCorrelate) currentRel).getLeft().getRowType()
                .getFieldCount();
        RelDataType newType = inputRef.getType();

        if (projectPulledAboveLeftCorrelator) {
          newType =
              typeFactory.createTypeWithNullability(newType, true);
        }

        int pos = inputRef.getIndex();
        RexInputRef newInputRef =
            new RexInputRef(leftInputFieldCount + pos, newType);

        if ((isCount != null) && isCount.contains(pos)) {
          return createCaseExpression(
              newInputRef,
              rexBuilder.makeExactLiteral(BigDecimal.ZERO),
              newInputRef);
        } else {
          return newInputRef;
        }
      }
      return inputRef;
    }

    // override RexLiteral
    public RexNode visitLiteral(RexLiteral literal) {
      // Use nullIndicator to decide whether to project null.
      // Do nothing if the literal is null.
      if (!RexUtil.isNull(literal)
          && projectPulledAboveLeftCorrelator
          && (nullIndicator != null)) {
        return createCaseExpression(
            nullIndicator,
            rexBuilder.constantNull(),
            literal);
      }
      return literal;
    }

    public RexNode visitCall(final RexCall call) {
      RexNode newCall;

      boolean[] update = {false};
      List<RexNode> clonedOperands = visitList(call.operands, update);
      if (update[0]) {
        SqlOperator operator = call.getOperator();

        boolean isSpecialCast = false;
        if (operator instanceof SqlFunction) {
          SqlFunction function = (SqlFunction) operator;
          if (function.getKind() == SqlKind.CAST) {
            if (call.operands.size() < 2) {
              isSpecialCast = true;
            }
          }
        }

        final RelDataType newType;
        if (!isSpecialCast) {
          // TODO: ideally this only needs to be called if the result
          // type will also change. However, since that requires
          // support from type inference rules to tell whether a rule
          // decides return type based on input types, for now all
          // operators will be recreated with new type if any operand
          // changed, unless the operator has "built-in" type.
          newType = rexBuilder.deriveReturnType(operator, clonedOperands);
        } else {
          // Use the current return type when creating a new call, for
          // operators with return type built into the operator
          // definition, and with no type inference rules, such as
          // cast function with less than 2 operands.

          // TODO: Comments in RexShuttle.visitCall() mention other
          // types in this category. Need to resolve those together
          // and preferably in the base class RexShuttle.
          newType = call.getType();
        }
        newCall =
            rexBuilder.makeCall(
                newType,
                operator,
                clonedOperands);
      } else {
        newCall = call;
      }

      if (projectPulledAboveLeftCorrelator && (nullIndicator != null)) {
        return createCaseExpression(
            nullIndicator,
            rexBuilder.constantNull(),
            newCall);
      }
      return newCall;
    }
  }

  /**
   * Rule to remove single_value rel. For cases like
   *
   * <blockquote>AggRel single_value proj/filter/agg/ join on unique LHS key
   * AggRel single group</blockquote>
   */
  private final class RemoveSingleAggregateRule extends RelOptRule {
    public RemoveSingleAggregateRule() {
      super(
          operand(
              LogicalAggregate.class,
              operand(
                  LogicalProject.class,
                  operand(LogicalAggregate.class, any()))));
    }

    public void onMatch(RelOptRuleCall call) {
      LogicalAggregate singleAggRel = call.rel(0);
      LogicalProject projRel = call.rel(1);
      LogicalAggregate aggRel = call.rel(2);

      // check singleAggRel is single_value agg
      if ((!singleAggRel.getGroupSet().isEmpty())
          || (singleAggRel.getAggCallList().size() != 1)
          || !(singleAggRel.getAggCallList().get(0).getAggregation()
          instanceof SqlSingleValueAggFunction)) {
        return;
      }

      // check projRel only projects one expression
      // check this project only projects one expression, i.e. scalar
      // subqueries.
      List<RexNode> projExprs = projRel.getProjects();
      if (projExprs.size() != 1) {
        return;
      }

      // check the input to projRel is an aggregate on the entire input
      if (!aggRel.getGroupSet().isEmpty()) {
        return;
      }

      // singleAggRel produces a nullable type, so create the new
      // projection that casts proj expr to a nullable type.
      final RelOptCluster cluster = projRel.getCluster();
      RelNode newProjRel =
          RelOptUtil.createProject(aggRel,
              ImmutableList.of(
                  rexBuilder.makeCast(
                      cluster.getTypeFactory().createTypeWithNullability(
                          projExprs.get(0).getType(),
                          true),
                      projExprs.get(0))),
              null);
      call.transformTo(newProjRel);
    }
  }

  /** Planner rule that removes correlations for scalar projects. */
  private final class RemoveCorrelationForScalarProjectRule extends RelOptRule {
    public RemoveCorrelationForScalarProjectRule() {
      super(
          operand(LogicalCorrelate.class,
              operand(RelNode.class, any()),
              operand(LogicalAggregate.class,
                  operand(LogicalProject.class,
                      operand(RelNode.class, any())))));
    }

    public void onMatch(RelOptRuleCall call) {
      LogicalCorrelate corRel = call.rel(0);
      RelNode leftInputRel = call.rel(1);
      LogicalAggregate aggRel = call.rel(2);
      LogicalProject projRel = call.rel(3);
      RelNode rightInputRel = call.rel(4);
      RelOptCluster cluster = corRel.getCluster();

      setCurrent(call.getPlanner().getRoot(), corRel);

      // Check for this pattern.
      // The pattern matching could be simplified if rules can be applied
      // during decorrelation.
      //
      // CorrelateRel(left correlation, condition = true)
      //   LeftInputRel
      //   LogicalAggregate (groupby (0) single_value())
      //     LogicalProject-A (may reference coVar)
      //       RightInputRel
      JoinRelType joinType = corRel.getJoinType().toJoinType();

      // corRel.getCondition was here, however Correlate was updated so it
      // never includes a join condition. The code was not modified for brevity.
      RexNode joinCond = rexBuilder.makeLiteral(true);
      if ((joinType != JoinRelType.LEFT)
          || (joinCond != rexBuilder.makeLiteral(true))) {
        return;
      }

      // check that the agg is of the following type:
      // doing a single_value() on the entire input
      if ((!aggRel.getGroupSet().isEmpty())
          || (aggRel.getAggCallList().size() != 1)
          || !(aggRel.getAggCallList().get(0).getAggregation()
          instanceof SqlSingleValueAggFunction)) {
        return;
      }

      // check this project only projects one expression, i.e. scalar
      // subqueries.
      if (projRel.getProjects().size() != 1) {
        return;
      }

      int nullIndicatorPos;

      if ((rightInputRel instanceof LogicalFilter)
          && cm.mapRefRelToCorVar.containsKey(rightInputRel)) {
        // rightInputRel has this shape:
        //
        //       LogicalFilter (references corvar)
        //         FilterInputRel

        // If rightInputRel is a filter and contains correlated
        // reference, make sure the correlated keys in the filter
        // condition forms a unique key of the RHS.

        LogicalFilter filter = (LogicalFilter) rightInputRel;
        rightInputRel = filter.getInput();

        assert rightInputRel instanceof HepRelVertex;
        rightInputRel = ((HepRelVertex) rightInputRel).getCurrentRel();

        // check filter input contains no correlation
        if (RelOptUtil.getVariablesUsed(rightInputRel).size() > 0) {
          return;
        }

        // extract the correlation out of the filter

        // First breaking up the filter conditions into equality
        // comparisons between rightJoinKeys(from the original
        // filterInputRel) and correlatedJoinKeys. correlatedJoinKeys
        // can be expressions, while rightJoinKeys need to be input
        // refs. These comparisons are AND'ed together.
        List<RexNode> tmpRightJoinKeys = Lists.newArrayList();
        List<RexNode> correlatedJoinKeys = Lists.newArrayList();
        RelOptUtil.splitCorrelatedFilterCondition(
            filter,
            tmpRightJoinKeys,
            correlatedJoinKeys,
            false);

        // check that the columns referenced in these comparisons form
        // an unique key of the filterInputRel
        List<RexInputRef> rightJoinKeys = new ArrayList<RexInputRef>();
        for (RexNode key : tmpRightJoinKeys) {
          assert key instanceof RexInputRef;
          rightJoinKeys.add((RexInputRef) key);
        }

        // check that the columns referenced in rightJoinKeys form an
        // unique key of the filterInputRel
        if (rightJoinKeys.isEmpty()) {
          return;
        }

        // The join filters out the nulls.  So, it's ok if there are
        // nulls in the join keys.
        if (!RelMdUtil.areColumnsDefinitelyUniqueWhenNullsFiltered(
            rightInputRel,
            rightJoinKeys)) {
          SQL2REL_LOGGER.fine(rightJoinKeys.toString()
              + "are not unique keys for "
              + rightInputRel.toString());
          return;
        }

        RexUtil.FieldAccessFinder visitor =
            new RexUtil.FieldAccessFinder();
        RexUtil.apply(visitor, correlatedJoinKeys, null);
        List<RexFieldAccess> correlatedKeyList =
            visitor.getFieldAccessList();

        if (!checkCorVars(corRel, projRel, filter, correlatedKeyList)) {
          return;
        }

        // Change the plan to this structure.
        // Note that the aggregateRel is removed.
        //
        // LogicalProject-A' (replace corvar to input ref from the LogicalJoin)
        //   LogicalJoin (replace corvar to input ref from LeftInputRel)
        //     LeftInputRel
        //     RightInputRel(oreviously FilterInputRel)

        // Change the filter condition into a join condition
        joinCond =
            removeCorrelationExpr(filter.getCondition(), false);

        nullIndicatorPos =
            leftInputRel.getRowType().getFieldCount()
                + rightJoinKeys.get(0).getIndex();
      } else if (cm.mapRefRelToCorVar.containsKey(projRel)) {
        // check filter input contains no correlation
        if (RelOptUtil.getVariablesUsed(rightInputRel).size() > 0) {
          return;
        }

        if (!checkCorVars(corRel, projRel, null, null)) {
          return;
        }

        // Change the plan to this structure.
        //
        // LogicalProject-A' (replace corvar to input ref from LogicalJoin)
        //   LogicalJoin (left, condition = true)
        //     LeftInputRel
        //     LogicalAggregate(groupby(0), single_value(0), s_v(1)....)
        //       LogicalProject-B (everything from input plus literal true)
        //         ProjInputRel

        // make the new projRel to provide a null indicator
        rightInputRel =
            createProjectWithAdditionalExprs(rightInputRel,
                ImmutableList.of(
                    Pair.<RexNode, String>of(
                        rexBuilder.makeLiteral(true), "nullIndicator")));

        // make the new aggRel
        rightInputRel =
            RelOptUtil.createSingleValueAggRel(cluster, rightInputRel);

        // The last field:
        //     single_value(true)
        // is the nullIndicator
        nullIndicatorPos =
            leftInputRel.getRowType().getFieldCount()
                + rightInputRel.getRowType().getFieldCount() - 1;
      } else {
        return;
      }

      // make the new join rel
      LogicalJoin join =
          LogicalJoin.create(leftInputRel, rightInputRel, joinCond, joinType,
              ImmutableSet.<String>of());

      RelNode newProjRel =
          projectJoinOutputWithNullability(join, projRel, nullIndicatorPos);

      call.transformTo(newProjRel);

      removeCorVarFromTree(corRel);
    }
  }

  /** Planner rule that removes correlations for scalar aggregates. */
  private final class RemoveCorrelationForScalarAggregateRule
      extends RelOptRule {
    public RemoveCorrelationForScalarAggregateRule() {
      super(
          operand(LogicalCorrelate.class,
              operand(RelNode.class, any()),
              operand(LogicalProject.class,
                  operand(LogicalAggregate.class, null, Aggregate.IS_SIMPLE,
                      operand(LogicalProject.class,
                          operand(RelNode.class, any()))))));
    }

    public void onMatch(RelOptRuleCall call) {
      LogicalCorrelate corRel = call.rel(0);
      RelNode leftInputRel = call.rel(1);
      LogicalProject aggOutputProjRel = call.rel(2);
      LogicalAggregate aggRel = call.rel(3);
      LogicalProject aggInputProjRel = call.rel(4);
      RelNode rightInputRel = call.rel(5);
      RelOptCluster cluster = corRel.getCluster();

      setCurrent(call.getPlanner().getRoot(), corRel);

      // check for this pattern
      // The pattern matching could be simplified if rules can be applied
      // during decorrelation,
      //
      // CorrelateRel(left correlation, condition = true)
      //   LeftInputRel
      //   LogicalProject-A (a RexNode)
      //     LogicalAggregate (groupby (0), agg0(), agg1()...)
      //       LogicalProject-B (references coVar)
      //         rightInputRel

      // check aggOutputProj projects only one expression
      List<RexNode> aggOutputProjExprs = aggOutputProjRel.getProjects();
      if (aggOutputProjExprs.size() != 1) {
        return;
      }

      JoinRelType joinType = corRel.getJoinType().toJoinType();
      // corRel.getCondition was here, however Correlate was updated so it
      // never includes a join condition. The code was not modified for brevity.
      RexNode joinCond = rexBuilder.makeLiteral(true);
      if ((joinType != JoinRelType.LEFT)
          || (joinCond != rexBuilder.makeLiteral(true))) {
        return;
      }

      // check that the agg is on the entire input
      if (!aggRel.getGroupSet().isEmpty()) {
        return;
      }

      List<RexNode> aggInputProjExprs = aggInputProjRel.getProjects();

      List<AggregateCall> aggCalls = aggRel.getAggCallList();
      Set<Integer> isCountStar = Sets.newHashSet();

      // mark if agg produces count(*) which needs to reference the
      // nullIndicator after the transformation.
      int k = -1;
      for (AggregateCall aggCall : aggCalls) {
        ++k;
        if ((aggCall.getAggregation() instanceof SqlCountAggFunction)
            && (aggCall.getArgList().size() == 0)) {
          isCountStar.add(k);
        }
      }

      if ((rightInputRel instanceof LogicalFilter)
          && cm.mapRefRelToCorVar.containsKey(rightInputRel)) {
        // rightInputRel has this shape:
        //
        //       LogicalFilter (references corvar)
        //         FilterInputRel
        LogicalFilter filter = (LogicalFilter) rightInputRel;
        rightInputRel = filter.getInput();

        assert rightInputRel instanceof HepRelVertex;
        rightInputRel = ((HepRelVertex) rightInputRel).getCurrentRel();

        // check filter input contains no correlation
        if (RelOptUtil.getVariablesUsed(rightInputRel).size() > 0) {
          return;
        }

        // check filter condition type First extract the correlation out
        // of the filter

        // First breaking up the filter conditions into equality
        // comparisons between rightJoinKeys(from the original
        // filterInputRel) and correlatedJoinKeys. correlatedJoinKeys
        // can only be RexFieldAccess, while rightJoinKeys can be
        // expressions. These comparisons are AND'ed together.
        List<RexNode> rightJoinKeys = Lists.newArrayList();
        List<RexNode> tmpCorrelatedJoinKeys = Lists.newArrayList();
        RelOptUtil.splitCorrelatedFilterCondition(
            filter,
            rightJoinKeys,
            tmpCorrelatedJoinKeys,
            true);

        // make sure the correlated reference forms a unique key check
        // that the columns referenced in these comparisons form an
        // unique key of the leftInputRel
        List<RexFieldAccess> correlatedJoinKeys = Lists.newArrayList();
        List<RexInputRef> correlatedInputRefJoinKeys = Lists.newArrayList();
        for (RexNode joinKey : tmpCorrelatedJoinKeys) {
          assert joinKey instanceof RexFieldAccess;
          correlatedJoinKeys.add((RexFieldAccess) joinKey);
          RexNode correlatedInputRef =
              removeCorrelationExpr(joinKey, false);
          assert correlatedInputRef instanceof RexInputRef;
          correlatedInputRefJoinKeys.add(
              (RexInputRef) correlatedInputRef);
        }

        // check that the columns referenced in rightJoinKeys form an
        // unique key of the filterInputRel
        if (correlatedInputRefJoinKeys.isEmpty()) {
          return;
        }

        // The join filters out the nulls.  So, it's ok if there are
        // nulls in the join keys.
        if (!RelMdUtil.areColumnsDefinitelyUniqueWhenNullsFiltered(
            leftInputRel,
            correlatedInputRefJoinKeys)) {
          SQL2REL_LOGGER.fine(correlatedJoinKeys.toString()
              + "are not unique keys for "
              + leftInputRel.toString());
          return;
        }

        // check cor var references are valid
        if (!checkCorVars(corRel,
            aggInputProjRel,
            filter,
            correlatedJoinKeys)) {
          return;
        }

        // Rewrite the above plan:
        //
        // CorrelateRel(left correlation, condition = true)
        //   LeftInputRel
        //   LogicalProject-A (a RexNode)
        //     LogicalAggregate (groupby(0), agg0(),agg1()...)
        //       LogicalProject-B (may reference coVar)
        //         LogicalFilter (references corVar)
        //           RightInputRel (no correlated reference)
        //

        // to this plan:
        //
        // LogicalProject-A' (all gby keys + rewritten nullable ProjExpr)
        //   LogicalAggregate (groupby(all left input refs)
        //                 agg0(rewritten expression),
        //                 agg1()...)
        //     LogicalProject-B' (rewriten original projected exprs)
        //       LogicalJoin(replace corvar w/ input ref from LeftInputRel)
        //         LeftInputRel
        //         RightInputRel
        //

        // In the case where agg is count(*) or count($corVar), it is
        // changed to count(nullIndicator).
        // Note:  any non-nullable field from the RHS can be used as
        // the indicator however a "true" field is added to the
        // projection list from the RHS for simplicity to avoid
        // searching for non-null fields.
        //
        // LogicalProject-A' (all gby keys + rewritten nullable ProjExpr)
        //   LogicalAggregate (groupby(all left input refs),
        //                 count(nullIndicator), other aggs...)
        //     LogicalProject-B' (all left input refs plus
        //                    the rewritten original projected exprs)
        //       LogicalJoin(replace corvar to input ref from LeftInputRel)
        //         LeftInputRel
        //         LogicalProject (everything from RightInputRel plus
        //                     the nullIndicator "true")
        //           RightInputRel
        //

        // first change the filter condition into a join condition
        joinCond =
            removeCorrelationExpr(filter.getCondition(), false);
      } else if (cm.mapRefRelToCorVar.containsKey(aggInputProjRel)) {
        // check rightInputRel contains no correlation
        if (RelOptUtil.getVariablesUsed(rightInputRel).size() > 0) {
          return;
        }

        // check cor var references are valid
        if (!checkCorVars(corRel, aggInputProjRel, null, null)) {
          return;
        }

        int nFields = leftInputRel.getRowType().getFieldCount();
        ImmutableBitSet allCols = ImmutableBitSet.range(nFields);

        // leftInputRel contains unique keys
        // i.e. each row is distinct and can group by on all the left
        // fields
        if (!RelMdUtil.areColumnsDefinitelyUnique(
            leftInputRel,
            allCols)) {
          SQL2REL_LOGGER.fine("There are no unique keys for " + leftInputRel);
          return;
        }
        //
        // Rewrite the above plan:
        //
        // CorrelateRel(left correlation, condition = true)
        //   LeftInputRel
        //   LogicalProject-A (a RexNode)
        //     LogicalAggregate (groupby(0), agg0(), agg1()...)
        //       LogicalProject-B (references coVar)
        //         RightInputRel (no correlated reference)
        //

        // to this plan:
        //
        // LogicalProject-A' (all gby keys + rewritten nullable ProjExpr)
        //   LogicalAggregate (groupby(all left input refs)
        //                 agg0(rewritten expression),
        //                 agg1()...)
        //     LogicalProject-B' (rewriten original projected exprs)
        //       LogicalJoin (LOJ cond = true)
        //         LeftInputRel
        //         RightInputRel
        //

        // In the case where agg is count($corVar), it is changed to
        // count(nullIndicator).
        // Note:  any non-nullable field from the RHS can be used as
        // the indicator however a "true" field is added to the
        // projection list from the RHS for simplicity to avoid
        // searching for non-null fields.
        //
        // LogicalProject-A' (all gby keys + rewritten nullable ProjExpr)
        //   LogicalAggregate (groupby(all left input refs),
        //                 count(nullIndicator), other aggs...)
        //     LogicalProject-B' (all left input refs plus
        //                    the rewritten original projected exprs)
        //       LogicalJoin(replace corvar to input ref from LeftInputRel)
        //         LeftInputRel
        //         LogicalProject (everything from RightInputRel plus
        //                     the nullIndicator "true")
        //           RightInputRel
      } else {
        return;
      }

      RelDataType leftInputFieldType = leftInputRel.getRowType();
      int leftInputFieldCount = leftInputFieldType.getFieldCount();
      int joinOutputProjExprCount =
          leftInputFieldCount + aggInputProjExprs.size() + 1;

      rightInputRel =
          createProjectWithAdditionalExprs(rightInputRel,
              ImmutableList.of(
                  Pair.<RexNode, String>of(rexBuilder.makeLiteral(true),
                      "nullIndicator")));

      LogicalJoin join =
          LogicalJoin.create(leftInputRel, rightInputRel, joinCond, joinType,
              ImmutableSet.<String>of());

      // To the consumer of joinOutputProjRel, nullIndicator is located
      // at the end
      int nullIndicatorPos = join.getRowType().getFieldCount() - 1;

      RexInputRef nullIndicator =
          new RexInputRef(
              nullIndicatorPos,
              cluster.getTypeFactory().createTypeWithNullability(
                  join.getRowType().getFieldList()
                      .get(nullIndicatorPos).getType(),
                  true));

      // first project all group-by keys plus the transformed agg input
      List<RexNode> joinOutputProjExprs = Lists.newArrayList();

      // LOJ Join preserves LHS types
      for (int i = 0; i < leftInputFieldCount; i++) {
        joinOutputProjExprs.add(
            rexBuilder.makeInputRef(
                leftInputFieldType.getFieldList().get(i).getType(), i));
      }

      for (RexNode aggInputProjExpr : aggInputProjExprs) {
        joinOutputProjExprs.add(
            removeCorrelationExpr(aggInputProjExpr,
                joinType.generatesNullsOnRight(),
                nullIndicator));
      }

      joinOutputProjExprs.add(
          rexBuilder.makeInputRef(join, nullIndicatorPos));

      RelNode joinOutputProjRel =
          RelOptUtil.createProject(
              join,
              joinOutputProjExprs,
              null);

      // nullIndicator is now at a different location in the output of
      // the join
      nullIndicatorPos = joinOutputProjExprCount - 1;

      final int groupCount = leftInputFieldCount;

      List<AggregateCall> newAggCalls = Lists.newArrayList();
      k = -1;
      for (AggregateCall aggCall : aggCalls) {
        ++k;
        final List<Integer> aggArgs = aggCall.getArgList();
        final List<Integer> newAggArgs;

        if (isCountStar.contains(k)) {
          // this is a count(*), transform it to count(nullIndicator)
          // the null indicator is located at the end
          newAggArgs = Collections.singletonList(nullIndicatorPos);
        } else {
          newAggArgs = Lists.newArrayList();

          for (Integer aggArg : aggArgs) {
            newAggArgs.add(aggArg + groupCount);
          }
        }

        newAggCalls.add(
            aggCall.adaptTo(joinOutputProjRel, newAggArgs,
                aggRel.getGroupCount(), groupCount));
      }

      ImmutableBitSet groupSet =
          ImmutableBitSet.range(groupCount);
      LogicalAggregate newAggRel =
          LogicalAggregate.create(joinOutputProjRel,
              false,
              groupSet,
              null,
              newAggCalls);

      List<RexNode> newAggOutputProjExprList = Lists.newArrayList();
      for (int i : groupSet) {
        newAggOutputProjExprList.add(
            rexBuilder.makeInputRef(newAggRel, i));
      }

      RexNode newAggOutputProjExpr =
          removeCorrelationExpr(aggOutputProjExprs.get(0), false);
      newAggOutputProjExprList.add(
          rexBuilder.makeCast(
              cluster.getTypeFactory().createTypeWithNullability(
                  newAggOutputProjExpr.getType(),
                  true),
              newAggOutputProjExpr));

      RelNode newAggOutputProjRel =
          RelOptUtil.createProject(
              newAggRel,
              newAggOutputProjExprList,
              null);

      call.transformTo(newAggOutputProjRel);

      removeCorVarFromTree(corRel);
    }
  }

  // REVIEW jhyde 29-Oct-2007: This rule is non-static, depends on the state
  // of members in RelDecorrelator, and has side-effects in the decorrelator.
  // This breaks the contract of a planner rule, and the rule will not be
  // reusable in other planners.

  // REVIEW jvs 29-Oct-2007:  Shouldn't it also be incorporating
  // the flavor attribute into the description?

  /** Planner rule that adjusts projects when counts are added. */
  private final class AdjustProjectForCountAggregateRule extends RelOptRule {
    final boolean flavor;

    public AdjustProjectForCountAggregateRule(boolean flavor) {
      super(
          flavor
              ? operand(LogicalCorrelate.class,
                  operand(RelNode.class, any()),
                      operand(LogicalProject.class,
                          operand(LogicalAggregate.class, any())))
              : operand(LogicalCorrelate.class,
                  operand(RelNode.class, any()),
                      operand(LogicalAggregate.class, any())));
      this.flavor = flavor;
    }

    public void onMatch(RelOptRuleCall call) {
      LogicalCorrelate corRel = call.rel(0);
      RelNode leftInputRel = call.rel(1);
      LogicalProject aggOutputProjRel;
      LogicalAggregate aggRel;
      if (flavor) {
        aggOutputProjRel = call.rel(2);
        aggRel = call.rel(3);
      } else {
        aggRel = call.rel(2);

        // Create identity projection
        List<Pair<RexNode, String>> projects = Lists.newArrayList();
        final List<RelDataTypeField> fields =
            aggRel.getRowType().getFieldList();
        for (int i = 0; i < fields.size(); i++) {
          projects.add(RexInputRef.of2(projects.size(), fields));
        }
        aggOutputProjRel =
            (LogicalProject) RelOptUtil.createProject(
                aggRel,
                projects,
                false);
      }
      onMatch2(call, corRel, leftInputRel, aggOutputProjRel, aggRel);
    }

    private void onMatch2(
        RelOptRuleCall call,
        LogicalCorrelate corRel,
        RelNode leftInputRel,
        LogicalProject aggOutputProjRel,
        LogicalAggregate aggRel) {
      RelOptCluster cluster = corRel.getCluster();

      if (generatedCorRels.contains(corRel)) {
        // This correlator was generated by a previous invocation of
        // this rule. No further work to do.
        return;
      }

      setCurrent(call.getPlanner().getRoot(), corRel);

      // check for this pattern
      // The pattern matching could be simplified if rules can be applied
      // during decorrelation,
      //
      // CorrelateRel(left correlation, condition = true)
      //   LeftInputRel
      //   LogicalProject-A (a RexNode)
      //     LogicalAggregate (groupby (0), agg0(), agg1()...)

      // check aggOutputProj projects only one expression
      List<RexNode> aggOutputProjExprs = aggOutputProjRel.getProjects();
      if (aggOutputProjExprs.size() != 1) {
        return;
      }

      JoinRelType joinType = corRel.getJoinType().toJoinType();
      // corRel.getCondition was here, however Correlate was updated so it
      // never includes a join condition. The code was not modified for brevity.
      RexNode joinCond = rexBuilder.makeLiteral(true);
      if ((joinType != JoinRelType.LEFT)
          || (joinCond != rexBuilder.makeLiteral(true))) {
        return;
      }

      // check that the agg is on the entire input
      if (!aggRel.getGroupSet().isEmpty()) {
        return;
      }

      List<AggregateCall> aggCalls = aggRel.getAggCallList();
      Set<Integer> isCount = Sets.newHashSet();

      // remember the count() positions
      int i = -1;
      for (AggregateCall aggCall : aggCalls) {
        ++i;
        if (aggCall.getAggregation() instanceof SqlCountAggFunction) {
          isCount.add(i);
        }
      }

      // now rewrite the plan to
      //
      // Project-A' (all LHS plus transformed original projections,
      //             replacing references to count() with case statement)
      //   Correlator(left correlation, condition = true)
      //     LeftInputRel
      //     LogicalAggregate (groupby (0), agg0(), agg1()...)
      //
      LogicalCorrelate newCorRel =
          LogicalCorrelate.create(leftInputRel, aggRel,
              corRel.getCorrelationId(), corRel.getRequiredColumns(),
              corRel.getJoinType());

      // remember this rel so we don't fire rule on it again
      // REVIEW jhyde 29-Oct-2007: rules should not save state; rule
      // should recognize patterns where it does or does not need to do
      // work
      generatedCorRels.add(newCorRel);

      // need to update the mapCorVarToCorRel Update the output position
      // for the cor vars: only pass on the cor vars that are not used in
      // the join key.
      if (cm.mapCorVarToCorRel.get(corRel.getCorrelationId()) == corRel) {
        cm.mapCorVarToCorRel.put(corRel.getCorrelationId(), newCorRel);
      }

      RelNode newOutputRel =
          aggregateCorrelatorOutput(newCorRel, aggOutputProjRel, isCount);

      call.transformTo(newOutputRel);
    }
  }

  /**
   * {@code Correlation} here represents a unique reference to a correlation
   * field.
   * For instance, if a RelNode references emp.name multiple times, it would
   * result in multiple {@code Correlation} objects that differ just in
   * {@link Correlation#uniqueKey}.
   */
  static class Correlation
      implements Comparable<Correlation> {
    public final int uniqueKey;
    public final CorrelationId corr;
    public final int field;

    Correlation(CorrelationId corr, int field, int uniqueKey) {
      this.corr = corr;
      this.field = field;
      this.uniqueKey = uniqueKey;
    }

    public int compareTo(Correlation o) {
      int res = corr.compareTo(o.corr);
      if (res != 0) {
        return res;
      }
      if (field != o.field) {
        return field - o.field;
      }
      return uniqueKey - o.uniqueKey;
    }
  }

  /** A map of the locations of
   * {@link org.apache.calcite.rel.logical.LogicalCorrelate}
   * in a tree of {@link RelNode}s.
   *
   * <p>It is used to drive the decorrelation process.
   * Treat it as immutable; rebuild if you modify the tree.
   *
   * <p>There are three maps:<ol>
   *
   * <li>mapRefRelToCorVars map a rel node to the correlated variables it
   * references;
   *
   * <li>mapCorVarToCorRel maps a correlated variable to the correlatorRel
   * providing it;
   *
   * <li>mapFieldAccessToCorVar maps a rex field access to
   * the cor var it represents. Because typeFlattener does not clone or
   * modify a correlated field access this map does not need to be
   * updated.
   *
   * </ol> */
  private static class CorelMap {
    private final Multimap<RelNode, Correlation> mapRefRelToCorVar;
    private final SortedMap<CorrelationId, LogicalCorrelate> mapCorVarToCorRel;
    private final Map<RexFieldAccess, Correlation> mapFieldAccessToCorVar;

    // TODO: create immutable copies of all maps
    private CorelMap(Multimap<RelNode, Correlation> mapRefRelToCorVar,
        SortedMap<CorrelationId, LogicalCorrelate> mapCorVarToCorRel,
        Map<RexFieldAccess, Correlation> mapFieldAccessToCorVar) {
      this.mapRefRelToCorVar = mapRefRelToCorVar;
      this.mapCorVarToCorRel = mapCorVarToCorRel;
      this.mapFieldAccessToCorVar = mapFieldAccessToCorVar;
    }

    @Override public String toString() {
      return "mapRefRelToCorVar=" + mapRefRelToCorVar
          + "\nmapCorVarToCorRel=" + mapCorVarToCorRel
          + "\nmapFieldAccessToCorVar=" + mapFieldAccessToCorVar
          + "\n";
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof CorelMap
          && mapRefRelToCorVar.equals(((CorelMap) obj).mapRefRelToCorVar)
          && mapCorVarToCorRel.equals(((CorelMap) obj).mapCorVarToCorRel)
          && mapFieldAccessToCorVar.equals(
              ((CorelMap) obj).mapFieldAccessToCorVar);
    }

    @Override public int hashCode() {
      return com.google.common.base.Objects.hashCode(mapRefRelToCorVar,
          mapCorVarToCorRel,
          mapFieldAccessToCorVar);
    }

    /** Creates a CorelMap with given contents. */
    public static CorelMap of(
        SortedSetMultimap<RelNode, Correlation> mapRefRelToCorVar,
        SortedMap<CorrelationId, LogicalCorrelate> mapCorVarToCorRel,
        Map<RexFieldAccess, Correlation> mapFieldAccessToCorVar) {
      return new CorelMap(mapRefRelToCorVar, mapCorVarToCorRel,
          mapFieldAccessToCorVar);
    }

    /** Creates a CorelMap by iterating over a {@link RelNode} tree. */
    public static CorelMap build(RelNode rel) {
      final SortedMap<CorrelationId, LogicalCorrelate> mapCorVarToCorRel =
          new TreeMap<CorrelationId, LogicalCorrelate>();

      final SortedSetMultimap<RelNode, Correlation> mapRefRelToCorVar =
          Multimaps.newSortedSetMultimap(
              Maps.<RelNode, Collection<Correlation>>newHashMap(),
              new Supplier<TreeSet<Correlation>>() {
                public TreeSet<Correlation> get() {
                  Bug.upgrade("use MultimapBuilder when we're on Guava-16");
                  return Sets.newTreeSet();
                }
              });

      final Map<RexFieldAccess, Correlation> mapFieldAccessToCorVar =
          new HashMap<RexFieldAccess, Correlation>();

      final Holder<Integer> offset = Holder.of(0);
      final int[] corrIdGenerator = new int[1];
      final RelShuttleImpl shuttle = new RelShuttleImpl() {
        @Override public RelNode visit(LogicalJoin join) {
          join.getCondition().accept(rexVisitor(join));
          return visitJoin(join);
        }

        @Override protected RelNode visitChild(RelNode parent, int i,
            RelNode child) {
          return super.visitChild(parent, i, stripHep(child));
        }

        @Override public RelNode visit(LogicalCorrelate correlate) {
          mapCorVarToCorRel.put(correlate.getCorrelationId(), correlate);
          return visitJoin(correlate);
        }

        private RelNode visitJoin(BiRel join) {
          final int x = offset.get();
          visitChild(join, 0, join.getLeft());
          offset.set(x + join.getLeft().getRowType().getFieldCount());
          visitChild(join, 1, join.getRight());
          offset.set(x);
          return join;
        }

        @Override public RelNode visit(final LogicalFilter filter) {
          filter.getCondition().accept(rexVisitor(filter));
          return super.visit(filter);
        }

        @Override public RelNode visit(LogicalProject project) {
          for (RexNode node : project.getProjects()) {
            node.accept(rexVisitor(project));
          }
          return super.visit(project);
        }

        private RexVisitorImpl<Void> rexVisitor(final RelNode rel) {
          return new RexVisitorImpl<Void>(true) {
            @Override public Void visitFieldAccess(RexFieldAccess fieldAccess) {
              final RexNode ref = fieldAccess.getReferenceExpr();
              if (ref instanceof RexCorrelVariable) {
                final RexCorrelVariable var = (RexCorrelVariable) ref;
                final Correlation correlation =
                    new Correlation(
                        new CorrelationId(var.getName()),
                        fieldAccess.getField().getIndex(),
                        corrIdGenerator[0]++);
                mapFieldAccessToCorVar.put(fieldAccess, correlation);
                mapRefRelToCorVar.put(rel, correlation);
              }
              return super.visitFieldAccess(fieldAccess);
            }
          };
        }
      };
      stripHep(rel).accept(shuttle);
      return new CorelMap(mapRefRelToCorVar, mapCorVarToCorRel,
          mapFieldAccessToCorVar);
    }

    private static RelNode stripHep(RelNode rel) {
      if (rel instanceof HepRelVertex) {
        HepRelVertex hepRelVertex = (HepRelVertex) rel;
        rel = hepRelVertex.getCurrentRel();
      }
      return rel;
    }

    /**
     * Returns whether there are any correlating variables in this statement.
     *
     * @return whether there are any correlating variables
     */
    public boolean hasCorrelation() {
      return !mapCorVarToCorRel.isEmpty();
    }
  }
}

// End RelDecorrelator.java
