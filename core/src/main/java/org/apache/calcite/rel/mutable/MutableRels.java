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
package org.apache.calcite.rel.mutable;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sample;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/** Utilities for dealing with {@link MutableRel}s. */
public abstract class MutableRels {

  public static boolean contains(MutableRel ancestor,
      final MutableRel target) {
    if (ancestor.equals(target)) {
      // Short-cut common case.
      return true;
    }
    try {
      new MutableRelVisitor() {
        @Override public void visit(@Nullable MutableRel node) {
          if (Objects.equals(node, target)) {
            throw Util.FoundOne.NULL;
          }
          super.visit(node);
        }
        // CHECKSTYLE: IGNORE 1
      }.go(ancestor);
      return false;
    } catch (Util.FoundOne e) {
      return true;
    }
  }

  public static @Nullable MutableRel preOrderTraverseNext(MutableRel node) {
    MutableRel parent = node.getParent();
    int ordinal = node.ordinalInParent + 1;
    while (parent != null) {
      if (parent.getInputs().size() > ordinal) {
        return parent.getInputs().get(ordinal);
      }
      node = parent;
      parent = node.getParent();
      ordinal = node.ordinalInParent + 1;
    }
    return null;
  }

  public static List<MutableRel> descendants(MutableRel query) {
    final List<MutableRel> list = new ArrayList<>();
    descendantsRecurse(list, query);
    return list;
  }

  private static void descendantsRecurse(List<MutableRel> list,
      MutableRel rel) {
    list.add(rel);
    for (MutableRel input : rel.getInputs()) {
      descendantsRecurse(list, input);
    }
  }

  /** Based on
   * {@link org.apache.calcite.rel.rules.ProjectRemoveRule#strip}. */
  public static MutableRel strip(MutableProject project) {
    return isTrivial(project) ? project.getInput() : project;
  }

  /** Based on
   * {@link org.apache.calcite.rel.rules.ProjectRemoveRule#isTrivial(org.apache.calcite.rel.core.Project)}. */
  public static boolean isTrivial(MutableProject project) {
    MutableRel child = project.getInput();
    return RexUtil.isIdentity(project.projects, child.rowType);
  }

  /** Equivalent to
   * {@link RelOptUtil#createProject(org.apache.calcite.rel.RelNode, java.util.List)}
   * for {@link MutableRel}. */
  public static MutableRel createProject(final MutableRel child,
      final List<Integer> posList) {
    final RelDataType rowType = child.rowType;
    if (Mappings.isIdentity(posList, rowType.getFieldCount())) {
      return child;
    }
    final Mapping mapping =
        Mappings.create(
            MappingType.INVERSE_SURJECTION,
            rowType.getFieldCount(),
            posList.size());
    for (int i = 0; i < posList.size(); i++) {
      mapping.set(posList.get(i), i);
    }
    return MutableProject.of(
        RelOptUtil.permute(child.cluster.getTypeFactory(), rowType, mapping),
        child,
        new AbstractList<RexNode>() {
          @Override public int size() {
            return posList.size();
          }

          @Override public RexNode get(int index) {
            final int pos = posList.get(index);
            return RexInputRef.of(pos, rowType);
          }
        });
  }

  /**
   * Construct expression list of Project by the given fields of the input.
   */
  public static List<RexNode> createProjectExprs(final MutableRel child,
      final List<Integer> posList) {
    return posList.stream().map(pos -> RexInputRef.of(pos, child.rowType))
        .collect(Collectors.toList());
  }

  /**
   * Construct expression list of Project by the given fields of the input.
   */
  public static List<RexNode> createProjects(final MutableRel child,
      final List<RexNode> projects) {
    List<RexNode> rexNodeList = new ArrayList<>(projects.size());
    for (RexNode project : projects) {
      if (project instanceof RexInputRef) {
        RexInputRef rexInputRef = (RexInputRef) project;
        rexNodeList.add(RexInputRef.of(rexInputRef.getIndex(), child.rowType));
      } else {
        rexNodeList.add(project);
      }
    }
    return rexNodeList;
  }

  /** Equivalence to {@link org.apache.calcite.plan.RelOptUtil#createCastRel}
   * for {@link MutableRel}. */
  public static MutableRel createCastRel(MutableRel rel,
      RelDataType castRowType, boolean rename) {
    RelDataType rowType = rel.rowType;
    if (RelOptUtil.areRowTypesEqual(rowType, castRowType, rename)) {
      // nothing to do
      return rel;
    }
    List<RexNode> castExps =
        RexUtil.generateCastExpressions(rel.cluster.getRexBuilder(),
            castRowType, rowType);
    final List<String> fieldNames =
        rename ? castRowType.getFieldNames() : rowType.getFieldNames();
    return MutableProject.of(rel, castExps, fieldNames);
  }

  public static RelNode fromMutable(MutableRel node) {
    return fromMutable(node, RelFactories.LOGICAL_BUILDER.create(node.cluster, null));
  }

  public static RelNode fromMutable(MutableRel node, RelBuilder relBuilder) {
    switch (node.type) {
    case TABLE_SCAN:
    case VALUES:
      return ((MutableLeafRel) node).rel;
    case PROJECT:
      final MutableProject project = (MutableProject) node;
      relBuilder.push(fromMutable(project.input, relBuilder));
      relBuilder.project(project.projects, project.rowType.getFieldNames(), true);
      return relBuilder.build();
    case FILTER:
      final MutableFilter filter = (MutableFilter) node;
      relBuilder.push(fromMutable(filter.input, relBuilder));
      relBuilder.filter(filter.condition);
      return relBuilder.build();
    case AGGREGATE:
      final MutableAggregate aggregate = (MutableAggregate) node;
      relBuilder.push(fromMutable(aggregate.input, relBuilder));
      relBuilder.aggregate(
          relBuilder.groupKey(aggregate.groupSet, aggregate.groupSets),
          aggregate.aggCalls);
      return relBuilder.build();
    case SORT:
      final MutableSort sort = (MutableSort) node;
      return LogicalSort.create(fromMutable(sort.input, relBuilder), sort.collation,
          sort.offset, sort.fetch);
    case CALC:
      final MutableCalc calc = (MutableCalc) node;
      return LogicalCalc.create(fromMutable(calc.input, relBuilder), calc.program);
    case EXCHANGE:
      final MutableExchange exchange = (MutableExchange) node;
      return LogicalExchange.create(
          fromMutable(exchange.getInput(), relBuilder), exchange.distribution);
    case COLLECT: {
      final MutableCollect collect = (MutableCollect) node;
      final RelNode child = fromMutable(collect.getInput(), relBuilder);
      return Collect.create(child, collect.rowType);
    }
    case UNCOLLECT: {
      final MutableUncollect uncollect = (MutableUncollect) node;
      final RelNode child = fromMutable(uncollect.getInput(), relBuilder);
      return Uncollect.create(child.getTraitSet(), child, uncollect.withOrdinality,
          Collections.emptyList());
    }
    case WINDOW: {
      final MutableWindow window = (MutableWindow) node;
      final RelNode child = fromMutable(window.getInput(), relBuilder);
      return LogicalWindow.create(child.getTraitSet(),
          child, window.constants, window.rowType, window.groups);
    }
    case MATCH: {
      final MutableMatch match = (MutableMatch) node;
      final RelNode child = fromMutable(match.getInput(), relBuilder);
      return LogicalMatch.create(child, match.rowType, match.pattern,
          match.strictStart, match.strictEnd, match.patternDefinitions,
          match.measures, match.after, match.subsets, match.allRows,
          match.partitionKeys, match.orderKeys, match.interval);
    }
    case TABLE_MODIFY:
      final MutableTableModify modify = (MutableTableModify) node;
      return LogicalTableModify.create(modify.table, modify.catalogReader,
          fromMutable(modify.getInput(), relBuilder), modify.operation, modify.updateColumnList,
          modify.sourceExpressionList, modify.flattened);
    case SAMPLE:
      final MutableSample sample = (MutableSample) node;
      return new Sample(sample.cluster, fromMutable(sample.getInput(), relBuilder), sample.params);
    case TABLE_FUNCTION_SCAN:
      final MutableTableFunctionScan tableFunctionScan = (MutableTableFunctionScan) node;
      return LogicalTableFunctionScan.create(tableFunctionScan.cluster,
          fromMutables(tableFunctionScan.getInputs(), relBuilder), tableFunctionScan.rexCall,
          tableFunctionScan.elementType, tableFunctionScan.rowType,
          tableFunctionScan.columnMappings);
    case JOIN:
      final MutableJoin join = (MutableJoin) node;
      relBuilder.push(fromMutable(join.getLeft(), relBuilder));
      relBuilder.push(fromMutable(join.getRight(), relBuilder));
      relBuilder.join(join.joinType, join.condition, join.variablesSet);
      return relBuilder.build();
    case CORRELATE:
      final MutableCorrelate correlate = (MutableCorrelate) node;
      return LogicalCorrelate.create(fromMutable(correlate.getLeft(), relBuilder),
          fromMutable(correlate.getRight(), relBuilder),
          ImmutableList.of(), correlate.correlationId,
          correlate.requiredColumns, correlate.joinType);
    case UNION:
      final MutableUnion union = (MutableUnion) node;
      relBuilder.pushAll(MutableRels.fromMutables(union.inputs, relBuilder));
      relBuilder.union(union.all, union.inputs.size());
      return relBuilder.build();
    case MINUS:
      final MutableMinus minus = (MutableMinus) node;
      relBuilder.pushAll(MutableRels.fromMutables(minus.inputs, relBuilder));
      relBuilder.minus(minus.all, minus.inputs.size());
      return relBuilder.build();
    case INTERSECT:
      final MutableIntersect intersect = (MutableIntersect) node;
      relBuilder.pushAll(MutableRels.fromMutables(intersect.inputs, relBuilder));
      relBuilder.intersect(intersect.all, intersect.inputs.size());
      return relBuilder.build();
    default:
      throw new AssertionError(node.deep());
    }
  }

  private static List<RelNode> fromMutables(List<MutableRel> nodes,
      final RelBuilder relBuilder) {
    return Util.transform(nodes,
        mutableRel -> fromMutable(mutableRel, relBuilder));
  }

  public static MutableRel toMutable(RelNode rel) {
    if (rel instanceof HepRelVertex) {
      return toMutable(((HepRelVertex) rel).getCurrentRel());
    }
    if (rel instanceof RelSubset) {
      RelSubset subset = (RelSubset) rel;
      RelNode best = subset.getBest();
      if (best == null) {
        best = requireNonNull(subset.getOriginal(),
            () -> "subset.getOriginal() is null for " + subset);
      }
      return toMutable(best);
    }
    if (rel instanceof TableScan) {
      return MutableScan.of((TableScan) rel);
    }
    if (rel instanceof Values) {
      return MutableValues.of((Values) rel);
    }
    if (rel instanceof Project) {
      final Project project = (Project) rel;
      final MutableRel input = toMutable(project.getInput());
      return MutableProject.of(input, project.getProjects(),
          project.getRowType().getFieldNames());
    }
    if (rel instanceof Filter) {
      final Filter filter = (Filter) rel;
      final MutableRel input = toMutable(filter.getInput());
      return MutableFilter.of(input, filter.getCondition());
    }
    if (rel instanceof Aggregate) {
      final Aggregate aggregate = (Aggregate) rel;
      final MutableRel input = toMutable(aggregate.getInput());
      return MutableAggregate.of(input, aggregate.getGroupSet(),
          aggregate.getGroupSets(), aggregate.getAggCallList());
    }
    if (rel instanceof Sort) {
      final Sort sort = (Sort) rel;
      final MutableRel input = toMutable(sort.getInput());
      return MutableSort.of(input, sort.getCollation(), sort.offset, sort.fetch);
    }
    if (rel instanceof Calc) {
      final Calc calc = (Calc) rel;
      final MutableRel input = toMutable(calc.getInput());
      return MutableCalc.of(input, calc.getProgram());
    }
    if (rel instanceof Exchange) {
      final Exchange exchange = (Exchange) rel;
      final MutableRel input = toMutable(exchange.getInput());
      return MutableExchange.of(input, exchange.getDistribution());
    }
    if (rel instanceof Collect) {
      final Collect collect = (Collect) rel;
      final MutableRel input = toMutable(collect.getInput());
      return MutableCollect.of(collect.getRowType(), input, collect.getFieldName());
    }
    if (rel instanceof Uncollect) {
      final Uncollect uncollect = (Uncollect) rel;
      final MutableRel input = toMutable(uncollect.getInput());
      return MutableUncollect.of(uncollect.getRowType(), input, uncollect.withOrdinality);
    }
    if (rel instanceof Window) {
      final Window window = (Window) rel;
      final MutableRel input = toMutable(window.getInput());
      return MutableWindow.of(window.getRowType(),
          input, window.groups, window.getConstants());
    }
    if (rel instanceof Match) {
      final Match match = (Match) rel;
      final MutableRel input = toMutable(match.getInput());
      return MutableMatch.of(match.getRowType(),
        input, match.getPattern(), match.isStrictStart(), match.isStrictEnd(),
        match.getPatternDefinitions(), match.getMeasures(), match.getAfter(),
        match.getSubsets(), match.isAllRows(), match.getPartitionKeys(),
        match.getOrderKeys(), match.getInterval());
    }
    if (rel instanceof TableModify) {
      final TableModify modify = (TableModify) rel;
      final MutableRel input = toMutable(modify.getInput());
      return MutableTableModify.of(modify.getRowType(), input, modify.getTable(),
          modify.getCatalogReader(), modify.getOperation(), modify.getUpdateColumnList(),
          modify.getSourceExpressionList(), modify.isFlattened());
    }
    if (rel instanceof Sample) {
      final Sample sample = (Sample) rel;
      final MutableRel input = toMutable(sample.getInput());
      return MutableSample.of(input, sample.getSamplingParameters());
    }
    if (rel instanceof TableFunctionScan) {
      final TableFunctionScan tableFunctionScan = (TableFunctionScan) rel;
      final List<MutableRel> inputs = toMutables(tableFunctionScan.getInputs());
      return MutableTableFunctionScan.of(tableFunctionScan.getCluster(),
          tableFunctionScan.getRowType(), inputs, tableFunctionScan.getCall(),
          tableFunctionScan.getElementType(), tableFunctionScan.getColumnMappings());
    }
    // It is necessary that SemiJoin is placed in front of Join here, since SemiJoin
    // is a sub-class of Join.
    if (rel instanceof Join) {
      final Join join = (Join) rel;
      final MutableRel left = toMutable(join.getLeft());
      final MutableRel right = toMutable(join.getRight());
      return MutableJoin.of(join.getRowType(), left, right,
          join.getCondition(), join.getJoinType(), join.getVariablesSet());
    }
    if (rel instanceof Correlate) {
      final Correlate correlate = (Correlate) rel;
      final MutableRel left = toMutable(correlate.getLeft());
      final MutableRel right = toMutable(correlate.getRight());
      return MutableCorrelate.of(correlate.getRowType(), left, right,
          correlate.getCorrelationId(), correlate.getRequiredColumns(),
          correlate.getJoinType());
    }
    if (rel instanceof Union) {
      final Union union = (Union) rel;
      final List<MutableRel> inputs = toMutables(union.getInputs());
      return MutableUnion.of(union.getRowType(), inputs, union.all);
    }
    if (rel instanceof Minus) {
      final Minus minus = (Minus) rel;
      final List<MutableRel> inputs = toMutables(minus.getInputs());
      return MutableMinus.of(minus.getRowType(), inputs, minus.all);
    }
    if (rel instanceof Intersect) {
      final Intersect intersect = (Intersect) rel;
      final List<MutableRel> inputs = toMutables(intersect.getInputs());
      return MutableIntersect.of(intersect.getRowType(), inputs, intersect.all);
    }
    throw new RuntimeException("cannot translate " + rel + " to MutableRel");
  }

  private static List<MutableRel> toMutables(List<RelNode> nodes) {
    return nodes.stream().map(MutableRels::toMutable)
        .collect(Collectors.toList());
  }
}
