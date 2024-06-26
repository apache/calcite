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
package org.apache.calcite.rel.rules.materialize;

import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexTableInputRef.RelTableRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.fun.SqlMinMaxAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

import static java.util.Objects.requireNonNull;

/** Materialized view rewriting for aggregate.
 *
 * @param <C> Configuration type
 */
public abstract class MaterializedViewAggregateRule<C extends MaterializedViewAggregateRule.Config>
    extends MaterializedViewRule<C> {

  protected static final ImmutableList<TimeUnitRange> SUPPORTED_DATE_TIME_ROLLUP_UNITS =
      ImmutableList.of(TimeUnitRange.YEAR, TimeUnitRange.QUARTER, TimeUnitRange.MONTH,
          TimeUnitRange.DAY, TimeUnitRange.HOUR, TimeUnitRange.MINUTE,
          TimeUnitRange.SECOND, TimeUnitRange.MILLISECOND, TimeUnitRange.MICROSECOND);

  /** Creates a MaterializedViewAggregateRule. */
  MaterializedViewAggregateRule(C config) {
    super(config);
  }

  @Override protected boolean isValidPlan(@Nullable Project topProject, RelNode node,
      RelMetadataQuery mq) {
    if (!(node instanceof Aggregate)) {
      return false;
    }
    Aggregate aggregate = (Aggregate) node;
    if (aggregate.getGroupType() != Aggregate.Group.SIMPLE) {
      // TODO: Rewriting with grouping sets not supported yet
      return false;
    }
    return isValidRelNodePlan(aggregate.getInput(), mq);
  }

  @Override protected @Nullable ViewPartialRewriting compensateViewPartial(
      RelBuilder relBuilder,
      RexBuilder rexBuilder,
      RelMetadataQuery mq,
      RelNode input,
      @Nullable Project topProject,
      RelNode node,
      Set<RelTableRef> queryTableRefs,
      EquivalenceClasses queryEC,
      @Nullable Project topViewProject,
      RelNode viewNode,
      Set<RelTableRef> viewTableRefs) {
    // Modify view to join with missing tables and add Project on top to reorder columns.
    // In turn, modify view plan to join with missing tables before Aggregate operator,
    // change Aggregate operator to group by previous grouping columns and columns in
    // attached tables, and add a final Project on top.
    // We only need to add the missing tables on top of the view and view plan using
    // a cartesian product.
    // Then the rest of the rewriting algorithm can be executed in the same
    // fashion, and if there are predicates between the existing and missing
    // tables, the rewriting algorithm will enforce them.
    final Set<RelTableRef> extraTableRefs = new HashSet<>();
    for (RelTableRef tRef : queryTableRefs) {
      if (!viewTableRefs.contains(tRef)) {
        // Add to extra tables if table is not part of the view
        extraTableRefs.add(tRef);
      }
    }
    Multimap<Class<? extends RelNode>, RelNode> nodeTypes = mq.getNodeTypes(node);
    if (nodeTypes == null) {
      return null;
    }
    Collection<RelNode> tableScanNodes = nodeTypes.get(TableScan.class);
    if (tableScanNodes == null) {
      return null;
    }
    List<RelNode> newRels = new ArrayList<>();
    for (RelTableRef tRef : extraTableRefs) {
      int i = 0;
      for (RelNode relNode : tableScanNodes) {
        TableScan scan = (TableScan) relNode;
        if (tRef.getQualifiedName().equals(scan.getTable().getQualifiedName())) {
          if (tRef.getEntityNumber() == i++) {
            newRels.add(relNode);
            break;
          }
        }
      }
    }
    assert extraTableRefs.size() == newRels.size();

    relBuilder.push(input);
    for (RelNode newRel : newRels) {
      // Add to the view
      relBuilder.push(newRel);
      relBuilder.join(JoinRelType.INNER, rexBuilder.makeLiteral(true));
    }
    final RelNode newView = relBuilder.build();

    final Aggregate aggregateViewNode = (Aggregate) viewNode;
    relBuilder.push(aggregateViewNode.getInput());
    int offset = 0;
    for (RelNode newRel : newRels) {
      // Add to the view plan
      relBuilder.push(newRel);
      relBuilder.join(JoinRelType.INNER, rexBuilder.makeLiteral(true));
      offset += newRel.getRowType().getFieldCount();
    }
    // Modify aggregate: add grouping columns
    ImmutableBitSet.Builder groupSet = ImmutableBitSet.builder();
    groupSet.addAll(aggregateViewNode.getGroupSet());
    groupSet.addAll(
        ImmutableBitSet.range(
            aggregateViewNode.getInput().getRowType().getFieldCount(),
            aggregateViewNode.getInput().getRowType().getFieldCount() + offset));
    final Aggregate newViewNode =
        aggregateViewNode.copy(aggregateViewNode.getTraitSet(),
            relBuilder.build(), groupSet.build(), null,
            aggregateViewNode.getAggCallList());

    relBuilder.push(newViewNode);
    List<RexNode> nodes = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>();
    if (topViewProject != null) {
      // Insert existing expressions (and shift aggregation arguments),
      // then append rest of columns
      Mappings.TargetMapping shiftMapping =
          Mappings.createShiftMapping(newViewNode.getRowType().getFieldCount(),
              0, 0, aggregateViewNode.getGroupCount(),
              newViewNode.getGroupCount(), aggregateViewNode.getGroupCount(),
              aggregateViewNode.getAggCallList().size());
      for (int i = 0; i < topViewProject.getProjects().size(); i++) {
        nodes.add(
            topViewProject.getProjects().get(i).accept(
                new RexPermuteInputsShuttle(shiftMapping, newViewNode)));
        fieldNames.add(topViewProject.getRowType().getFieldNames().get(i));
      }
      for (int i = aggregateViewNode.getRowType().getFieldCount();
           i < newViewNode.getRowType().getFieldCount(); i++) {
        int idx = i - aggregateViewNode.getAggCallList().size();
        nodes.add(rexBuilder.makeInputRef(newViewNode, idx));
        fieldNames.add(newViewNode.getRowType().getFieldNames().get(idx));
      }
    } else {
      // Original grouping columns, aggregation columns, then new grouping columns
      for (int i = 0; i < newViewNode.getRowType().getFieldCount(); i++) {
        int idx;
        if (i < aggregateViewNode.getGroupCount()) {
          idx = i;
        } else if (i < aggregateViewNode.getRowType().getFieldCount()) {
          idx = i + offset;
        } else {
          idx = i - aggregateViewNode.getAggCallList().size();
        }
        nodes.add(rexBuilder.makeInputRef(newViewNode, idx));
        fieldNames.add(newViewNode.getRowType().getFieldNames().get(idx));
      }
    }
    relBuilder.project(nodes, fieldNames, true);
    final Project newTopViewProject = (Project) relBuilder.build();

    return ViewPartialRewriting.of(newView, newTopViewProject, newViewNode);
  }

  @Override protected @Nullable RelNode rewriteQuery(
      RelBuilder relBuilder,
      RexBuilder rexBuilder,
      RexSimplify simplify,
      RelMetadataQuery mq,
      RexNode compensationColumnsEquiPred,
      RexNode otherCompensationPred,
      @Nullable Project topProject,
      RelNode node,
      BiMap<RelTableRef, RelTableRef> queryToViewTableMapping,
      EquivalenceClasses viewEC, EquivalenceClasses queryEC) {
    Aggregate aggregate = (Aggregate) node;

    // Our target node is the node below the root, which should have the maximum
    // number of available expressions in the tree in order to maximize our
    // number of rewritings.
    // If the program is available, we execute it to maximize rewriting opportunities.
    // For instance, a program might pull up all the expressions that are below the
    // aggregate so we can introduce compensation filters easily. This is important
    // depending on the planner strategy.
    RelNode newAggregateInput = aggregate.getInput(0);
    RelNode target = aggregate.getInput(0);
    HepProgram unionRewritingPullProgram = config.unionRewritingPullProgram();
    if (unionRewritingPullProgram != null) {
      final HepPlanner tmpPlanner = new HepPlanner(unionRewritingPullProgram);
      tmpPlanner.setRoot(newAggregateInput);
      newAggregateInput = tmpPlanner.findBestExp();
      target = newAggregateInput.getInput(0);
    }

    // We need to check that all columns required by compensating predicates
    // are contained in the query.
    List<RexNode> queryExprs = extractReferences(rexBuilder, target);
    if (!compensationColumnsEquiPred.isAlwaysTrue()) {
      RexNode newCompensationColumnsEquiPred =
          rewriteExpression(rexBuilder, mq, target, target, queryExprs,
              queryToViewTableMapping, queryEC, false,
              compensationColumnsEquiPred);
      if (newCompensationColumnsEquiPred == null) {
        // Skip it
        return null;
      }
      compensationColumnsEquiPred = newCompensationColumnsEquiPred;
    }
    // For the rest, we use the query equivalence classes
    if (!otherCompensationPred.isAlwaysTrue()) {
      RexNode newOtherCompensationPred =
          rewriteExpression(rexBuilder, mq, target, target, queryExprs,
              queryToViewTableMapping, viewEC, true,
              otherCompensationPred);
      if (newOtherCompensationPred == null) {
        // Skip it
        return null;
      }
      otherCompensationPred = newOtherCompensationPred;
    }
    final RexNode queryCompensationPred =
        RexUtil.not(
            RexUtil.composeConjunction(rexBuilder,
                ImmutableList.of(compensationColumnsEquiPred,
                    otherCompensationPred)));

    // Generate query rewriting.
    RelNode rewrittenPlan = relBuilder
        .push(target)
        .filter(simplify.simplifyUnknownAsFalse(queryCompensationPred))
        .build();
    if (config.unionRewritingPullProgram() != null) {
      return aggregate.copy(aggregate.getTraitSet(),
          ImmutableList.of(
              newAggregateInput.copy(newAggregateInput.getTraitSet(),
                  ImmutableList.of(rewrittenPlan))));
    }
    return aggregate.copy(aggregate.getTraitSet(), ImmutableList.of(rewrittenPlan));
  }

  @Override protected @Nullable RelNode createUnion(RelBuilder relBuilder, RexBuilder rexBuilder,
      @Nullable RelNode topProject, RelNode unionInputQuery, RelNode unionInputView) {
    // Union
    relBuilder.push(unionInputQuery);
    relBuilder.push(unionInputView);
    relBuilder.union(true);
    List<RexNode> exprList = new ArrayList<>(relBuilder.peek().getRowType().getFieldCount());
    List<String> nameList = new ArrayList<>(relBuilder.peek().getRowType().getFieldCount());
    for (int i = 0; i < relBuilder.peek().getRowType().getFieldCount(); i++) {
      // We can take unionInputQuery as it is query based.
      RelDataTypeField field = unionInputQuery.getRowType().getFieldList().get(i);
      exprList.add(
          rexBuilder.ensureType(
              field.getType(),
              rexBuilder.makeInputRef(relBuilder.peek(), i),
              true));
      nameList.add(field.getName());
    }
    relBuilder.project(exprList, nameList);
    // Rollup aggregate
    Aggregate aggregate = (Aggregate) unionInputQuery;
    final ImmutableBitSet groupSet = ImmutableBitSet.range(aggregate.getGroupCount());
    final List<AggCall> aggregateCalls = new ArrayList<>();
    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
      AggregateCall aggCall = aggregate.getAggCallList().get(i);
      if (aggCall.isDistinct()) {
        // Cannot ROLLUP distinct
        return null;
      }
      SqlAggFunction rollupAgg = aggCall.getAggregation().getRollup();
      if (rollupAgg == null) {
        // Cannot rollup this aggregate, bail out
        return null;
      }
      final RexInputRef operand =
          rexBuilder.makeInputRef(relBuilder.peek(),
              aggregate.getGroupCount() + i);
      aggregateCalls.add(
          relBuilder.aggregateCall(aggCall.getParserPosition(), rollupAgg, operand)
              .distinct(aggCall.isDistinct())
              .approximate(aggCall.isApproximate())
              .as(aggCall.name));
    }
    RelNode prevNode = relBuilder.peek();
    RelNode result = relBuilder
        .aggregate(relBuilder.groupKey(groupSet), aggregateCalls)
        .build();
    if (prevNode == result && groupSet.cardinality() != result.getRowType().getFieldCount()) {
      // Aggregate was not inserted but we need to prune columns
      result = relBuilder
          .push(result)
          .project(relBuilder.fields(groupSet))
          .build();
    }
    if (topProject != null) {
      // Top project
      return topProject.copy(topProject.getTraitSet(), ImmutableList.of(result));
    }
    // Result
    return result;
  }

  @Override protected @Nullable RelNode rewriteView(
      RelBuilder relBuilder,
      RexBuilder rexBuilder,
      RexSimplify simplify,
      RelMetadataQuery mq,
      MatchModality matchModality,
      boolean unionRewriting,
      RelNode input,
      @Nullable Project topProject,
      RelNode node,
      @Nullable Project topViewProject0,
      RelNode viewNode,
      BiMap<RelTableRef, RelTableRef> queryToViewTableMapping,
      EquivalenceClasses queryEC) {
    final Aggregate queryAggregate = (Aggregate) node;
    final Aggregate viewAggregate = (Aggregate) viewNode;
    // Get group by references and aggregate call input references needed
    final ImmutableBitSet.Builder indexes = ImmutableBitSet.builder();
    final ImmutableBitSet references;
    if (topProject != null && !unionRewriting) {
      // We have a Project on top, gather only what is needed
      final RelOptUtil.InputFinder inputFinder =
          new RelOptUtil.InputFinder(new LinkedHashSet<>());
      inputFinder.visitEach(topProject.getProjects());
      references = inputFinder.build();
      for (int i = 0; i < queryAggregate.getGroupCount(); i++) {
        indexes.set(queryAggregate.getGroupSet().nth(i));
      }
      for (int i = 0; i < queryAggregate.getAggCallList().size(); i++) {
        if (references.get(queryAggregate.getGroupCount() + i)) {
          for (int inputIdx : queryAggregate.getAggCallList().get(i).getArgList()) {
            indexes.set(inputIdx);
          }
        }
      }
    } else {
      // No project on top, all of them are needed
      for (int i = 0; i < queryAggregate.getGroupCount(); i++) {
        indexes.set(queryAggregate.getGroupSet().nth(i));
      }
      for (AggregateCall queryAggCall : queryAggregate.getAggCallList()) {
        for (int inputIdx : queryAggCall.getArgList()) {
          indexes.set(inputIdx);
        }
      }
      references = null;
    }

    // Create mapping from query columns to view columns
    final List<RexNode> rollupNodes = new ArrayList<>();
    final Multimap<Integer, Integer> m =
        generateMapping(rexBuilder, simplify, mq,
            queryAggregate.getInput(), viewAggregate.getInput(), indexes.build(),
            queryToViewTableMapping, queryEC, rollupNodes);
    if (m == null) {
      // Bail out
      return null;
    }

    // We could map all expressions. Create aggregate mapping.
    @SuppressWarnings("unused")
    int viewAggregateAdditionalFieldCount = rollupNodes.size();
    int viewInputFieldCount = viewAggregate.getInput().getRowType().getFieldCount();
    int viewInputDifferenceViewFieldCount =
        viewAggregate.getRowType().getFieldCount() - viewInputFieldCount;
    int viewAggregateTotalFieldCount =
        viewAggregate.getRowType().getFieldCount() + rollupNodes.size();
    boolean forceRollup = false;
    Mapping aggregateMapping =
        Mappings.create(MappingType.FUNCTION,
            queryAggregate.getRowType().getFieldCount(),
            viewAggregateTotalFieldCount);
    for (int i = 0; i < queryAggregate.getGroupCount(); i++) {
      Collection<Integer> c = m.get(queryAggregate.getGroupSet().nth(i));
      for (int j : c) {
        if (j >= viewAggregate.getInput().getRowType().getFieldCount()) {
          // This is one of the rollup columns
          aggregateMapping.set(i, j + viewInputDifferenceViewFieldCount);
          forceRollup = true;
        } else {
          int targetIdx = viewAggregate.getGroupSet().indexOf(j);
          if (targetIdx == -1) {
            continue;
          }
          aggregateMapping.set(i, targetIdx);
        }
        break;
      }
      if (aggregateMapping.getTargetOpt(i) == -1) {
        // It is not part of group by, we bail out
        return null;
      }
    }
    boolean containsDistinctAgg = false;
    for (Ord<AggregateCall> ord : Ord.zip(queryAggregate.getAggCallList())) {
      if (references != null
          && !references.get(queryAggregate.getGroupCount() + ord.i)) {
        // Ignore
        continue;
      }
      final AggregateCall queryAggCall = ord.e;
      if (queryAggCall.filterArg >= 0) {
        // Not supported currently
        return null;
      }
      List<Integer> queryAggCallIndexes = new ArrayList<>();
      for (int aggCallIdx : queryAggCall.getArgList()) {
        queryAggCallIndexes.add(m.get(aggCallIdx).iterator().next());
      }
      for (int j = 0; j < viewAggregate.getAggCallList().size(); j++) {
        AggregateCall viewAggCall = viewAggregate.getAggCallList().get(j);
        if (queryAggCall.getAggregation().getKind() != viewAggCall.getAggregation().getKind()
            || queryAggCall.isDistinct() != viewAggCall.isDistinct()
            || queryAggCall.getArgList().size() != viewAggCall.getArgList().size()
            || queryAggCall.getType() != viewAggCall.getType()
            || viewAggCall.filterArg >= 0) {
          // Continue
          continue;
        }
        if (!queryAggCallIndexes.equals(viewAggCall.getArgList())) {
          // Continue
          continue;
        }
        aggregateMapping.set(queryAggregate.getGroupCount() + ord.i,
            viewAggregate.getGroupCount() + j);
        if (queryAggCall.isDistinct()) {
          containsDistinctAgg = true;
        }
        break;
      }
    }

    // To simplify things, create an identity topViewProject if not present.
    final Project topViewProject = topViewProject0 != null
        ? topViewProject0
        : (Project) relBuilder.push(viewNode)
            .project(relBuilder.fields(), ImmutableList.of(), true)
            .build();

    // Generate result rewriting
    final List<RexNode> additionalViewExprs = new ArrayList<>();

    // Multimap is required since a column in the materialized view's project
    // could map to multiple columns in the target query
    final ImmutableMultimap<Integer, Integer> rewritingMapping;
    relBuilder.push(input);
    // We create view expressions that will be used in a Project on top of the
    // view in case we need to rollup the expression
    final List<RexNode> inputViewExprs = new ArrayList<>(relBuilder.fields());
    if (forceRollup
        || queryAggregate.getGroupCount() != viewAggregate.getGroupCount()
        || matchModality == MatchModality.VIEW_PARTIAL) {
      if (containsDistinctAgg) {
        // Cannot rollup DISTINCT aggregate
        return null;
      }
      // Target is coarser level of aggregation. Generate an aggregate.
      final ImmutableMultimap.Builder<Integer, Integer> rewritingMappingB =
          ImmutableMultimap.builder();
      final ImmutableBitSet.Builder groupSetB = ImmutableBitSet.builder();
      for (int i = 0; i < queryAggregate.getGroupCount(); i++) {
        final int targetIdx = aggregateMapping.getTargetOpt(i);
        if (targetIdx == -1) {
          // No matching group by column, we bail out
          return null;
        }
        if (targetIdx >= viewAggregate.getRowType().getFieldCount()) {
          RexNode targetNode =
              rollupNodes.get(targetIdx - viewInputFieldCount
                  - viewInputDifferenceViewFieldCount);
          // We need to rollup this expression
          final Multimap<RexNode, Integer> exprsLineage = ArrayListMultimap.create();
          for (int r : RelOptUtil.InputFinder.bits(targetNode)) {
            final int j = find(viewNode, r);
            final int k = find(topViewProject, j);
            if (k < 0) {
              // No matching column needed for computed expression, bail out
              return null;
            }
            final RexInputRef ref =
                relBuilder.with(viewNode.getInput(0), b -> b.field(r));
            exprsLineage.put(ref, k);
          }
          // We create the new node pointing to the index
          groupSetB.set(inputViewExprs.size());
          rewritingMappingB.put(inputViewExprs.size(), i);
          additionalViewExprs.add(
              new RexInputRef(targetIdx, targetNode.getType()));
          // We need to create the rollup expression
          RexNode rollupExpression =
              requireNonNull(shuttleReferences(rexBuilder, targetNode, exprsLineage),
                  () -> "shuttleReferences produced null for targetNode="
                      + targetNode + ", exprsLineage=" + exprsLineage);
          inputViewExprs.add(rollupExpression);
        } else {
          // This expression should be referenced directly
          final int k = find(topViewProject, targetIdx);
          if (k < 0) {
            // No matching group by column, we bail out
            return null;
          }
          groupSetB.set(k);
          rewritingMappingB.put(k, i);
        }
      }
      final ImmutableBitSet groupSet = groupSetB.build();
      final List<AggCall> aggregateCalls = new ArrayList<>();
      for (Ord<AggregateCall> ord : Ord.zip(queryAggregate.getAggCallList())) {
        final int sourceIdx = queryAggregate.getGroupCount() + ord.i;
        if (references != null && !references.get(sourceIdx)) {
          // Ignore
          continue;
        }
        final int targetIdx =
            aggregateMapping.getTargetOpt(sourceIdx);
        if (targetIdx < 0) {
          // No matching aggregation column, we bail out
          return null;
        }
        final int k = find(topViewProject, targetIdx);
        if (k < 0) {
          // No matching aggregation column, we bail out
          return null;
        }
        final AggregateCall queryAggCall = ord.e;
        SqlAggFunction rollupAgg = queryAggCall.getAggregation().getRollup();
        if (rollupAgg == null) {
          // Cannot rollup this aggregate, bail out
          return null;
        }
        rewritingMappingB.put(k,
            queryAggregate.getGroupCount() + aggregateCalls.size());
        final RexInputRef operand = rexBuilder.makeInputRef(input, k);
        aggregateCalls.add(
            relBuilder.aggregateCall(queryAggCall.getParserPosition(), rollupAgg, operand)
                .approximate(queryAggCall.isApproximate())
                .distinct(queryAggCall.isDistinct())
                .as(queryAggCall.name));
      }
      // Create aggregate on top of input
      final RelNode prevNode = relBuilder.peek();
      if (inputViewExprs.size() > prevNode.getRowType().getFieldCount()) {
        relBuilder.project(inputViewExprs);
      }
      relBuilder
          .aggregate(relBuilder.groupKey(groupSet), aggregateCalls);
      if (prevNode == relBuilder.peek()
          && groupSet.cardinality() != relBuilder.peek().getRowType().getFieldCount()) {
        // Aggregate was not inserted but we need to prune columns
        relBuilder.project(relBuilder.fields(groupSet));
      }
      // We introduce a project on top, as group by columns order is lost.
      // Multimap is required since a column in the materialized view's project
      // could map to multiple columns in the target query.
      rewritingMapping = rewritingMappingB.build();
      final ImmutableMultimap<Integer, Integer> inverseMapping = rewritingMapping.inverse();
      final List<RexNode> projects = new ArrayList<>();

      final ImmutableBitSet.Builder addedProjects = ImmutableBitSet.builder();
      for (int i = 0; i < queryAggregate.getGroupCount(); i++) {
        final int pos = groupSet.indexOf(inverseMapping.get(i).iterator().next());
        addedProjects.set(pos);
        projects.add(relBuilder.field(pos));
      }

      final ImmutableBitSet projectedCols = addedProjects.build();
      // We add aggregate functions that are present in result to projection list
      for (int i = 0; i < relBuilder.peek().getRowType().getFieldCount(); i++) {
        if (!projectedCols.get(i)) {
          projects.add(relBuilder.field(i));
        }
      }
      relBuilder.project(projects);
    } else {
      rewritingMapping = null;
    }

    // Add query expressions on top. We first map query expressions to view
    // expressions. Once we have done that, if the expression is contained
    // and we have introduced already an operator on top of the input node,
    // we use the mapping to resolve the position of the expression in the
    // node.
    final RelDataType topRowType;
    final List<RexNode> topExprs = new ArrayList<>();
    if (topProject != null && !unionRewriting) {
      topExprs.addAll(topProject.getProjects());
      topRowType = topProject.getRowType();
    } else {
      // Add all
      for (int pos = 0; pos < queryAggregate.getRowType().getFieldCount(); pos++) {
        topExprs.add(rexBuilder.makeInputRef(queryAggregate, pos));
      }
      topRowType = queryAggregate.getRowType();
    }
    // Available in view.
    final Multimap<RexNode, Integer> viewExprs = ArrayListMultimap.create();
    addAllIndexed(viewExprs, topViewProject.getProjects());
    addAllIndexed(viewExprs, additionalViewExprs);
    final List<RexNode> rewrittenExprs = new ArrayList<>(topExprs.size());
    for (RexNode expr : topExprs) {
      // First map through the aggregate
      final RexNode e2 = shuttleReferences(rexBuilder, expr, aggregateMapping);
      if (e2 == null) {
        // Cannot map expression
        return null;
      }
      // Next map through the last project
      final RexNode e3 =
          shuttleReferences(rexBuilder, e2, viewExprs,
              relBuilder.peek(), rewritingMapping);
      if (e3 == null) {
        // Cannot map expression
        return null;
      }
      rewrittenExprs.add(e3);
    }
    return relBuilder
        .project(rewrittenExprs)
        .convert(topRowType, false)
        .build();
  }

  private static <K> void addAllIndexed(Multimap<K, Integer> multimap,
      Iterable<? extends K> list) {
    for (K k : list) {
      multimap.put(k, multimap.size());
    }
  }

  /** Given a relational expression with a single input (such as a Project or
   * Aggregate) and the ordinal of an input field, returns the ordinal of the
   * output field that references the input field. Or -1 if the field is not
   * propagated.
   *
   * <p>For example, if {@code rel} is {@code Project(c0, c2)} (on input with
   * columns (c0, c1, c2)), then {@code find(rel, 2)} returns 1 (c2);
   * {@code find(rel, 1)} returns -1 (because c1 is not projected).
   *
   * <p>If {@code rel} is {@code Aggregate([0, 2], sum(1))}, then
   * {@code find(rel, 2)} returns 1, and {@code find(rel, 1)} returns -1.
   *
   * @param rel Relational expression
   * @param ref Ordinal of output field
   * @return Ordinal of input field, or -1
   */
  private static int find(RelNode rel, int ref) {
    if (rel instanceof Project) {
      Project project = (Project) rel;
      for (Ord<RexNode> p : Ord.zip(project.getProjects())) {
        if (p.e instanceof RexInputRef
            && ((RexInputRef) p.e).getIndex() == ref) {
          return p.i;
        }
      }
    }
    if (rel instanceof Aggregate) {
      Aggregate aggregate = (Aggregate) rel;
      int k = aggregate.getGroupSet().indexOf(ref);
      if (k >= 0) {
        return k;
      }
    }
    return -1;
  }

  /**
   * Mapping from node expressions to target expressions.
   *
   * <p>If any of the expressions cannot be mapped, we return null.
   */
  protected @Nullable Multimap<Integer, Integer> generateMapping(
      RexBuilder rexBuilder,
      RexSimplify simplify,
      RelMetadataQuery mq,
      RelNode node,
      RelNode target,
      ImmutableBitSet positions,
      BiMap<RelTableRef, RelTableRef> tableMapping,
      EquivalenceClasses sourceEC,
      List<RexNode> additionalExprs) {
    checkArgument(additionalExprs.isEmpty());
    Multimap<Integer, Integer> m = ArrayListMultimap.create();
    Map<RexTableInputRef, Set<RexTableInputRef>> equivalenceClassesMap =
        sourceEC.getEquivalenceClassesMap();
    Multimap<RexNode, Integer> exprsLineage = ArrayListMultimap.create();
    final List<RexNode> timestampExprs = new ArrayList<>();
    for (int i = 0; i < target.getRowType().getFieldCount(); i++) {
      Set<RexNode> s = mq.getExpressionLineage(target, rexBuilder.makeInputRef(target, i));
      if (s == null) {
        // Bail out
        continue;
      }
      // We only support project - filter - join, thus it should map to
      // a single expression
      final RexNode e = Iterables.getOnlyElement(s);
      // Rewrite expr to be expressed on query tables
      final RexNode simplified = simplify.simplifyUnknownAsFalse(e);
      final RexNode expr =
          RexUtil.swapTableColumnReferences(rexBuilder, simplified,
              tableMapping.inverse(), equivalenceClassesMap);
      exprsLineage.put(expr, i);
      SqlTypeName sqlTypeName = expr.getType().getSqlTypeName();
      if (sqlTypeName == SqlTypeName.TIMESTAMP
          || sqlTypeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
          || sqlTypeName == SqlTypeName.TIMESTAMP_TZ) {
        timestampExprs.add(expr);
      }
    }

    // If this is a column of TIMESTAMP (WITH LOCAL TIME ZONE)
    // type, we add the possible rollup columns too.
    // This way we will be able to match FLOOR(ts to HOUR) to
    // FLOOR(ts to DAY) via FLOOR(FLOOR(ts to HOUR) to DAY)
    for (RexNode timestampExpr : timestampExprs) {
      for (TimeUnitRange value : SUPPORTED_DATE_TIME_ROLLUP_UNITS) {
        final SqlFunction[] functions =
            {getCeilSqlFunction(value), getFloorSqlFunction(value)};
        for (SqlFunction function : functions) {
          final RexNode call =
              rexBuilder.makeCall(function,
                  timestampExpr, rexBuilder.makeFlag(value));
          // References self-row
          final RexNode rewrittenCall =
              shuttleReferences(rexBuilder, call, exprsLineage);
          if (rewrittenCall == null) {
            continue;
          }
          // We add the CEIL or FLOOR expression to the additional
          // expressions, replacing the child expression by the position that
          // it references
          additionalExprs.add(rewrittenCall);
          // Then we simplify the expression and we add it to the expressions
          // lineage so we can try to find a match.
          final RexNode simplified =
              simplify.simplifyUnknownAsFalse(call);
          exprsLineage.put(simplified,
              target.getRowType().getFieldCount() + additionalExprs.size() - 1);
        }
      }
    }

    for (int i : positions) {
      Set<RexNode> s = mq.getExpressionLineage(node, rexBuilder.makeInputRef(node, i));
      if (s == null) {
        // Bail out
        return null;
      }
      // We only support project - filter - join, thus it should map to
      // a single expression
      final RexNode e = Iterables.getOnlyElement(s);
      // Rewrite expr to be expressed on query tables
      final RexNode simplified = simplify.simplifyUnknownAsFalse(e);
      RexNode targetExpr =
          RexUtil.swapColumnReferences(rexBuilder, simplified,
              equivalenceClassesMap);
      final Collection<Integer> c = exprsLineage.get(targetExpr);
      if (!c.isEmpty()) {
        for (Integer j : c) {
          m.put(i, j);
        }
      } else {
        // If we did not find the expression, try to navigate it
        RexNode rewrittenTargetExpr =
            shuttleReferences(rexBuilder, targetExpr, exprsLineage);
        if (rewrittenTargetExpr == null) {
          // Some expressions were not present
          return null;
        }
        m.put(i, target.getRowType().getFieldCount() + additionalExprs.size());
        additionalExprs.add(rewrittenTargetExpr);
      }
    }
    return m;
  }

  /**
   * Get ceil function datetime.
   */
  protected SqlFunction getCeilSqlFunction(TimeUnitRange flag) {
    return SqlStdOperatorTable.CEIL;
  }

  /**
   * Get floor function datetime.
   */
  protected SqlFunction getFloorSqlFunction(TimeUnitRange flag) {
    return SqlStdOperatorTable.FLOOR;
  }

  /**
   * Get rollup aggregation function.
   */
  @Deprecated // to be removed before 2.0
  protected @Nullable SqlAggFunction getRollup(SqlAggFunction aggregation) {
    if (aggregation == SqlStdOperatorTable.SUM
        || aggregation == SqlStdOperatorTable.SUM0
        || aggregation instanceof SqlMinMaxAggFunction
        || aggregation == SqlStdOperatorTable.ANY_VALUE) {
      return aggregation;
    } else if (aggregation == SqlStdOperatorTable.COUNT) {
      return SqlStdOperatorTable.SUM0;
    } else {
      return null;
    }
  }

  @Override public Pair<@Nullable RelNode, RelNode> pushFilterToOriginalViewPlan(RelBuilder builder,
      @Nullable RelNode topViewProject, RelNode viewNode, RexNode cond) {
    // We add (and push) the filter to the view plan before triggering the rewriting.
    // This is useful in case some of the columns can be folded to same value after
    // filter is added.
    HepProgramBuilder pushFiltersProgram = new HepProgramBuilder();
    if (topViewProject != null) {
      pushFiltersProgram.addRuleInstance(config.filterProjectTransposeRule());
    }
    pushFiltersProgram
        .addRuleInstance(config.filterAggregateTransposeRule())
        .addRuleInstance(config.aggregateProjectPullUpConstantsRule())
        .addRuleInstance(config.projectMergeRule());
    final HepPlanner tmpPlanner = new HepPlanner(pushFiltersProgram.build());
    // Now that the planner is created, push the node
    RelNode topNode = builder
        .push(topViewProject != null ? topViewProject : viewNode)
        .filter(cond).build();
    tmpPlanner.setRoot(topNode);
    topNode = tmpPlanner.findBestExp();
    RelNode resultTopViewProject = null;
    RelNode resultViewNode = null;
    while (topNode != null) {
      if (topNode instanceof Project) {
        if (resultTopViewProject != null) {
          // Both projects could not be merged, we will bail out
          return Pair.of(topViewProject, viewNode);
        }
        resultTopViewProject = topNode;
        topNode = topNode.getInput(0);
      } else if (topNode instanceof Aggregate) {
        resultViewNode = topNode;
        topNode = null;
      } else {
        // We move to the child
        topNode = topNode.getInput(0);
      }
    }
    return Pair.of(resultTopViewProject, requireNonNull(resultViewNode, "resultViewNode"));
  }

  /**
   * Rule configuration.
   */
  public interface Config extends MaterializedViewRule.Config {

    /** Instance of rule to push filter through project. */
    @Value.Default default RelOptRule filterProjectTransposeRule() {
      return CoreRules.FILTER_PROJECT_TRANSPOSE.config
          .withRelBuilderFactory(relBuilderFactory())
          .as(FilterProjectTransposeRule.Config.class)
          .withOperandFor(Filter.class, filter ->
                  !RexUtil.containsCorrelation(filter.getCondition()),
              Project.class, project -> true)
          .withCopyFilter(true)
          .withCopyProject(true)
          .toRule();
    }

    /** Sets {@link #filterProjectTransposeRule()}. */
    Config withFilterProjectTransposeRule(RelOptRule rule);

    /** Instance of rule to push filter through aggregate. */
    @Value.Default default RelOptRule filterAggregateTransposeRule() {
      return CoreRules.FILTER_AGGREGATE_TRANSPOSE.config
          .withRelBuilderFactory(relBuilderFactory())
          .as(FilterAggregateTransposeRule.Config.class)
          .withOperandFor(Filter.class, Aggregate.class)
          .toRule();
    }

    /** Sets {@link #filterAggregateTransposeRule()}. */
    Config withFilterAggregateTransposeRule(RelOptRule rule);

    /** Instance of rule to pull up constants into aggregate. */
    @Value.Default default RelOptRule aggregateProjectPullUpConstantsRule() {
      return AggregateProjectPullUpConstantsRule.Config.DEFAULT
          .withRelBuilderFactory(relBuilderFactory())
          .withDescription("AggFilterPullUpConstants")
          .as(AggregateProjectPullUpConstantsRule.Config.class)
          .withOperandFor(Aggregate.class, Filter.class)
          .toRule();
    }

    /** Sets {@link #aggregateProjectPullUpConstantsRule()}. */
    Config withAggregateProjectPullUpConstantsRule(RelOptRule rule);

    /** Instance of rule to merge project operators. */
    @Value.Default default RelOptRule projectMergeRule() {
      return CoreRules.PROJECT_MERGE.config
          .withRelBuilderFactory(relBuilderFactory())
          .toRule();
    }

    /** Sets {@link #projectMergeRule()}. */
    Config withProjectMergeRule(RelOptRule rule);
  }
}
