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

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Utility to extract Predicates that are present in the (sub)plan
 * starting at this node.
 *
 * <p>This should be used to infer whether same filters are applied on
 * a given plan by materialized view rewriting rules.
 *
 * <p>The output predicates might contain references to columns produced
 * by TableScan operators ({@link RexTableInputRef}). In turn, each TableScan
 * operator is identified uniquely by its qualified name and an identifier.
 *
 * <p>If the provider cannot infer the lineage for any of the expressions
 * contain in any of the predicates, it will return null. Observe that
 * this is different from the empty list of predicates, which means that
 * there are not predicates in the (sub)plan.
 *
 */
public class RelMdAllPredicates
    implements MetadataHandler<BuiltInMetadata.AllPredicates> {
  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider
      .reflectiveSource(BuiltInMethod.ALL_PREDICATES.method, new RelMdAllPredicates());

  public MetadataDef<BuiltInMetadata.AllPredicates> getDef() {
    return BuiltInMetadata.AllPredicates.DEF;
  }

  /** Catch-all implementation for
   * {@link BuiltInMetadata.AllPredicates#getAllPredicates()},
   * invoked using reflection.
   *
   * @see org.apache.calcite.rel.metadata.RelMetadataQuery#getAllPredicates(RelNode)
   */
  public RelOptPredicateList getAllPredicates(RelNode rel, RelMetadataQuery mq) {
    return null;
  }

  public RelOptPredicateList getAllPredicates(HepRelVertex rel, RelMetadataQuery mq) {
    return mq.getAllPredicates(rel.getCurrentRel());
  }

  public RelOptPredicateList getAllPredicates(RelSubset rel,
      RelMetadataQuery mq) {
    return mq.getAllPredicates(Util.first(rel.getBest(), rel.getOriginal()));
  }

  /**
   * Extract predicates for a table scan.
   */
  public RelOptPredicateList getAllPredicates(TableScan table, RelMetadataQuery mq) {
    return RelOptPredicateList.EMPTY;
  }

  /**
   * Extract predicates for a project.
   */
  public RelOptPredicateList getAllPredicates(Project project, RelMetadataQuery mq) {
    return mq.getAllPredicates(project.getInput());
  }

  /**
   * Add the Filter condition to the list obtained from the input.
   */
  public RelOptPredicateList getAllPredicates(Filter filter, RelMetadataQuery mq) {
    final RelNode input = filter.getInput();
    final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    final RexNode pred = filter.getCondition();

    final RelOptPredicateList predsBelow = mq.getAllPredicates(input);
    if (predsBelow == null) {
      // Safety check
      return null;
    }

    // Extract input fields referenced by Filter condition
    final Set<RelDataTypeField> inputExtraFields = new LinkedHashSet<>();
    final RelOptUtil.InputFinder inputFinder = new RelOptUtil.InputFinder(inputExtraFields);
    pred.accept(inputFinder);
    final ImmutableBitSet inputFieldsUsed = inputFinder.inputBitSet.build();

    // Infer column origin expressions for given references
    final Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
    for (int idx : inputFieldsUsed) {
      final RexInputRef ref = RexInputRef.of(idx, filter.getRowType().getFieldList());
      final Set<RexNode> originalExprs = mq.getExpressionLineage(filter, ref);
      if (originalExprs == null) {
        // Bail out
        return null;
      }
      mapping.put(ref, originalExprs);
    }

    // Replace with new expressions and return union of predicates
    return predsBelow.union(rexBuilder,
        RelOptPredicateList.of(rexBuilder,
            RelMdExpressionLineage.createAllPossibleExpressions(rexBuilder, pred, mapping)));
  }

  /**
   * Add the Join condition to the list obtained from the input.
   */
  public RelOptPredicateList getAllPredicates(Join join, RelMetadataQuery mq) {
    if (join.getJoinType() != JoinRelType.INNER) {
      // We cannot map origin of this expression.
      return null;
    }

    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    final RexNode pred = join.getCondition();
    final RelNode leftInput = join.getLeft();
    final RelNode rightInput = join.getRight();
    final int nLeftColumns = leftInput.getRowType().getFieldList().size();

    RelOptPredicateList newPreds = RelOptPredicateList.EMPTY;
    for (RelNode input : join.getInputs()) {
      final RelOptPredicateList inputPreds = mq.getAllPredicates(input);
      if (inputPreds == null) {
        // Bail out
        return null;
      }
      newPreds = newPreds.union(rexBuilder, inputPreds);
    }

    // Extract input fields referenced by Join condition
    final Set<RelDataTypeField> inputExtraFields = new LinkedHashSet<>();
    final RelOptUtil.InputFinder inputFinder = new RelOptUtil.InputFinder(inputExtraFields);
    pred.accept(inputFinder);
    final ImmutableBitSet inputFieldsUsed = inputFinder.inputBitSet.build();

    // Infer column origin expressions for given references
    final Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
    for (int idx : inputFieldsUsed) {
      if (idx < nLeftColumns) {
        final RexInputRef inputRef = RexInputRef.of(idx, leftInput.getRowType().getFieldList());
        final Set<RexNode> originalExprs = mq.getExpressionLineage(leftInput, inputRef);
        if (originalExprs == null) {
          // Bail out
          return null;
        }
        final RexInputRef ref = RexInputRef.of(idx, join.getRowType().getFieldList());
        mapping.put(ref, originalExprs);
      } else {
        // Right input.
        final RexInputRef inputRef = RexInputRef.of(idx - nLeftColumns,
                rightInput.getRowType().getFieldList());
        final Set<RexNode> originalExprs = mq.getExpressionLineage(rightInput, inputRef);
        if (originalExprs == null) {
          // Bail out
          return null;
        }
        final RexInputRef ref = RexInputRef.of(idx, join.getRowType().getFieldList());
        mapping.put(ref, originalExprs);
      }
    }

    // Replace with new expressions and return union of predicates
    return newPreds.union(rexBuilder,
        RelOptPredicateList.of(rexBuilder,
            RelMdExpressionLineage.createAllPossibleExpressions(rexBuilder, pred, mapping)));
  }

  /**
   * Extract predicates for an Aggregate.
   */
  public RelOptPredicateList getAllPredicates(Aggregate agg, RelMetadataQuery mq) {
    return mq.getAllPredicates(agg.getInput());
  }

  /**
   * Extract predicates for a Union.
   */
  public RelOptPredicateList getAllPredicates(Union union, RelMetadataQuery mq) {
    final RexBuilder rexBuilder = union.getCluster().getRexBuilder();

    RelOptPredicateList newPreds = RelOptPredicateList.EMPTY;
    for (RelNode input : union.getInputs()) {
      final RelOptPredicateList inputPreds = mq.getAllPredicates(input);
      if (inputPreds == null) {
        // Bail out
        return null;
      }
      newPreds = newPreds.union(rexBuilder, inputPreds);
    }
    return newPreds;
  }

  /**
   * Extract predicates for a Sort.
   */
  public RelOptPredicateList getAllPredicates(Sort sort, RelMetadataQuery mq) {
    return mq.getAllPredicates(sort.getInput());
  }

  /**
   * Extract predicates for an Exchange.
   */
  public RelOptPredicateList getAllPredicates(Exchange exchange,
      RelMetadataQuery mq) {
    return mq.getAllPredicates(exchange.getInput());
  }

}

// End RelMdAllPredicates.java
