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
package org.apache.calcite.tools;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCopier;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * We'll see what it does
 */
public class RelCopier implements RelShuttle {
  protected final RelOptCluster target;
  protected final RexCopier rexCopier;

  public RelCopier(RelOptCluster target) {
    this.target = target;
    this.rexCopier = new RexCopier(target.getRexBuilder());
  }

  protected RelDataType copyDataType(RelDataType dataType) {
    // TODO: transform correctly
    return dataType;
  }

  protected List<RelNode> visitInputs(List<RelNode> inputs) {
    return Lists.transform(inputs,
      new Function<RelNode, RelNode>() {
        public RelNode apply(RelNode rel) {
          return rel.accept(RelCopier.this);
        }
      });
  }

  protected Map<?, RexNode> applyRexCopierToMap(Map<?, RexNode> map) {
    return Maps.transformValues(map,
      new Function<RexNode, RexNode>() {
        public RexNode apply(RexNode expr) {
          return expr.accept(rexCopier);
        }
      });
  }

  public RelNode visit(LogicalAggregate aggregate) {
    RelNode input = aggregate.getInput().accept(this);
    return LogicalAggregate.create(
      input,
      aggregate.indicator,
      aggregate.getGroupSet(),
      aggregate.getGroupSets(),
      aggregate.getAggCallList());
  }

  public RelNode visit(LogicalMatch match) {
    RelNode input = match.getInput().accept(this);
    return LogicalMatch.create(input,
      rexCopier.apply(match.getPattern()),
      match.isStrictStart(),
      match.isStrictEnd(),
      (Map<String, RexNode>) applyRexCopierToMap(match.getPatternDefinitions()),
      copyDataType(match.getRowType()));
  }

  public RelNode visit(TableScan scan) {
    // TODO: TableScan is abstract
    // need specialized visitors or LogicalTableScan
    throw new AssertionError(
      "Not implemented RelCopier for [Class=" + scan.getClass() + "]");
  }

  public RelNode visit(TableFunctionScan scan) {
    // TODO: TableFunctionScan is abstract
    // need specialized visitors or visit(LogicalTableFunctionScan)
    throw new AssertionError(
      "Not implemented RelCopier for [Class=" + scan.getClass() + "]");
  }

  public RelNode visit(LogicalValues values) {
    ImmutableList<ImmutableList<RexLiteral>> tuples =
      ImmutableList.copyOf(
        Iterables.transform(values.tuples,
          new Function<ImmutableList<RexLiteral>, ImmutableList<RexLiteral>>() {
            public ImmutableList<RexLiteral> apply(ImmutableList<RexLiteral> list) {
              return ImmutableList.copyOf(
                Iterables.transform(list,
                  new Function<RexLiteral, RexLiteral>() {
                    public RexLiteral apply(RexLiteral rex) {
                      return (RexLiteral) rex.accept(rexCopier);
                    }
                  }));
            }
          }));
    return LogicalValues.create(target, copyDataType(values.getRowType()), tuples);
  }

  public RelNode visit(LogicalFilter filter) {
    RelNode input = filter.getInput().accept(this);
    RexNode condition = filter.getCondition().accept(rexCopier);
    ImmutableSet<CorrelationId> variablesSet = ImmutableSet.copyOf(
      filter.getVariablesSet());
    return LogicalFilter.create(input, condition, variablesSet);
  }

  public RelNode visit(LogicalProject project) {
    RelNode input = project.getInput().accept(this);
    List<RexNode> exprs = rexCopier.apply(project.getProjects());
    return LogicalProject.create(input, exprs, copyDataType(project.getRowType()));
  }

  public RelNode visit(LogicalJoin join) {
    RelNode leftNode = join.getLeft().accept(this);
    RelNode rightNode = join.getRight().accept(this);
    RexNode condition = join.getCondition().accept(rexCopier);
    // TODO: copy RelDataTypeField for getSystemFieldList
    return LogicalJoin.create(leftNode, rightNode, condition,
      join.getVariablesSet(), join.getJoinType(), join.isSemiJoinDone(),
      ImmutableList.copyOf(join.getSystemFieldList()));
  }

  public RelNode visit(LogicalCorrelate correlate) {
    RelNode leftNode = correlate.getLeft().accept(this);
    RelNode rightNode = correlate.getRight().accept(this);
    return LogicalCorrelate.create(leftNode, rightNode,
      correlate.getCorrelationId(), correlate.getRequiredColumns(),
      correlate.getJoinType());
  }

  public RelNode visit(LogicalUnion union) {
    List<RelNode> inputs = visitInputs(union.getInputs());
    return LogicalUnion.create(inputs, union.all);
  }

  public RelNode visit(LogicalIntersect intersect) {
    List<RelNode> inputs = visitInputs(intersect.getInputs());
    return LogicalIntersect.create(inputs, intersect.all);
  }

  public RelNode visit(LogicalMinus minus) {
    List<RelNode> inputs = visitInputs(minus.getInputs());
    return LogicalMinus.create(inputs, minus.all);
  }

  public RelNode visit(LogicalSort sort) {
    RelNode input = sort.getInput().accept(this);
    RexNode offset = sort.offset.accept(rexCopier);
    RexNode fetch = sort.fetch.accept(rexCopier);
    // TODO: is RelColaltion required to copy?
    return LogicalSort.create(input, sort.getCollation(), offset, fetch);
  }

  public RelNode visit(LogicalExchange exchange) {
    RelNode input = exchange.getInput().accept(this);
    // TODO: is RelDistribution required to copy?
    return LogicalExchange.create(input, exchange.getDistribution());
  }

  public RelNode visit(RelNode other) {
    throw new AssertionError(
      "Does not know how to duplicate RelNode [Class:" + other.getClass() + "]");
  }
}

// End RelCopier.java
