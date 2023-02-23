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
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexTableInputRef.RelTableRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
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
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          new RelMdAllPredicates(), BuiltInMetadata.AllPredicates.Handler.class);

  @Override public MetadataDef<BuiltInMetadata.AllPredicates> getDef() {
    return BuiltInMetadata.AllPredicates.DEF;
  }

  /** Catch-all implementation for
   * {@link BuiltInMetadata.AllPredicates#getAllPredicates()},
   * invoked using reflection.
   *
   * @see org.apache.calcite.rel.metadata.RelMetadataQuery#getAllPredicates(RelNode)
   */
  public @Nullable RelOptPredicateList getAllPredicates(RelNode rel, RelMetadataQuery mq) {
    return null;
  }

  public @Nullable RelOptPredicateList getAllPredicates(HepRelVertex rel, RelMetadataQuery mq) {
    return mq.getAllPredicates(rel.stripped());
  }

  public @Nullable RelOptPredicateList getAllPredicates(RelSubset rel,
      RelMetadataQuery mq) {
    return mq.getAllPredicates(rel.stripped());
  }

  /**
   * Extracts predicates for a table scan.
   */
  public @Nullable RelOptPredicateList getAllPredicates(TableScan scan, RelMetadataQuery mq) {
    final BuiltInMetadata.AllPredicates.Handler handler =
        scan.getTable().unwrap(BuiltInMetadata.AllPredicates.Handler.class);
    if (handler != null) {
      return handler.getAllPredicates(scan, mq);
    }
    return RelOptPredicateList.EMPTY;
  }

  /**
   * Extracts predicates for a project.
   */
  public @Nullable RelOptPredicateList getAllPredicates(Project project, RelMetadataQuery mq) {
    return mq.getAllPredicates(project.getInput());
  }

  /**
   * Extracts predicates for a Filter.
   */
  public @Nullable RelOptPredicateList getAllPredicates(Filter filter, RelMetadataQuery mq) {
    return getAllFilterPredicates(filter.getInput(), mq, filter.getCondition());
  }

  /**
   * Extracts predicates for a Calc.
   */
  public @Nullable RelOptPredicateList getAllPredicates(Calc calc, RelMetadataQuery mq) {
    final RexProgram rexProgram = calc.getProgram();
    if (rexProgram.getCondition() != null) {
      final RexNode condition = rexProgram.expandLocalRef(rexProgram.getCondition());
      return getAllFilterPredicates(calc.getInput(), mq, condition);
    } else {
      return mq.getAllPredicates(calc.getInput());
    }
  }

  /**
   * Add the Filter condition to the list obtained from the input.
   * The pred comes from the parent of rel.
   */
  private static @Nullable RelOptPredicateList getAllFilterPredicates(RelNode rel,
      RelMetadataQuery mq, RexNode pred) {
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    final RelOptPredicateList predsBelow = mq.getAllPredicates(rel);
    if (predsBelow == null) {
      // Safety check
      return null;
    }

    // Extract input fields referenced by Filter condition
    final Set<RelDataTypeField> inputExtraFields = new LinkedHashSet<>();
    final RelOptUtil.InputFinder inputFinder = new RelOptUtil.InputFinder(inputExtraFields);
    pred.accept(inputFinder);
    final ImmutableBitSet inputFieldsUsed = inputFinder.build();

    // Infer column origin expressions for given references
    final Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
    for (int idx : inputFieldsUsed) {
      final RexInputRef ref = RexInputRef.of(idx, rel.getRowType().getFieldList());
      final Set<RexNode> originalExprs = mq.getExpressionLineage(rel, ref);
      if (originalExprs == null) {
        // Bail out
        return null;
      }
      mapping.put(ref, originalExprs);
    }

    // Replace with new expressions and return union of predicates
    final Set<RexNode> allExprs =
        RelMdExpressionLineage.createAllPossibleExpressions(rexBuilder, pred, mapping);
    if (allExprs == null) {
      return null;
    }
    return predsBelow.union(rexBuilder, RelOptPredicateList.of(rexBuilder, allExprs));
  }

  /**
   * Add the Join condition to the list obtained from the input.
   */
  public @Nullable RelOptPredicateList getAllPredicates(Join join, RelMetadataQuery mq) {
    if (join.getJoinType().isOuterJoin()) {
      // We cannot map origin of this expression.
      return null;
    }

    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    final RexNode pred = join.getCondition();

    final Multimap<List<String>, RelTableRef> qualifiedNamesToRefs = HashMultimap.create();
    RelOptPredicateList newPreds = RelOptPredicateList.EMPTY;
    for (RelNode input : join.getInputs()) {
      final RelOptPredicateList inputPreds = mq.getAllPredicates(input);
      if (inputPreds == null) {
        // Bail out
        return null;
      }
      // Gather table references
      final Set<RelTableRef> tableRefs = mq.getTableReferences(input);
      if (tableRefs == null) {
        return null;
      }
      if (input == join.getLeft()) {
        // Left input references remain unchanged
        for (RelTableRef leftRef : tableRefs) {
          qualifiedNamesToRefs.put(leftRef.getQualifiedName(), leftRef);
        }
        newPreds = newPreds.union(rexBuilder, inputPreds);
      } else {
        // Right input references might need to be updated if there are table name
        // clashes with left input
        final Map<RelTableRef, RelTableRef> currentTablesMapping = new HashMap<>();
        for (RelTableRef rightRef : tableRefs) {
          int shift = 0;
          Collection<RelTableRef> lRefs =
              qualifiedNamesToRefs.get(rightRef.getQualifiedName());
          if (lRefs != null) {
            shift = lRefs.size();
          }
          currentTablesMapping.put(rightRef,
              RelTableRef.of(rightRef.getTable(), shift + rightRef.getEntityNumber()));
        }
        final List<RexNode> updatedPreds =
            Util.transform(inputPreds.pulledUpPredicates,
                e -> RexUtil.swapTableReferences(rexBuilder, e,
                    currentTablesMapping));
        newPreds =
            newPreds.union(rexBuilder,
                RelOptPredicateList.of(rexBuilder, updatedPreds));
      }
    }

    // Extract input fields referenced by Join condition
    final Set<RelDataTypeField> inputExtraFields = new LinkedHashSet<>();
    final RelOptUtil.InputFinder inputFinder = new RelOptUtil.InputFinder(inputExtraFields);
    pred.accept(inputFinder);
    final ImmutableBitSet inputFieldsUsed = inputFinder.build();

    // Infer column origin expressions for given references
    final Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
    final RelDataType fullRowType =
        SqlValidatorUtil.createJoinType(rexBuilder.getTypeFactory(),
            join.getLeft().getRowType(),
            join.getRight().getRowType(),
            null,
            ImmutableList.of());
    for (int idx : inputFieldsUsed) {
      final RexInputRef inputRef = RexInputRef.of(idx, fullRowType.getFieldList());
      final Set<RexNode> originalExprs = mq.getExpressionLineage(join, inputRef);
      if (originalExprs == null) {
        // Bail out
        return null;
      }
      final RexInputRef ref = RexInputRef.of(idx, fullRowType.getFieldList());
      mapping.put(ref, originalExprs);
    }

    // Replace with new expressions and return union of predicates
    final Set<RexNode> allExprs =
        RelMdExpressionLineage.createAllPossibleExpressions(rexBuilder, pred, mapping);
    if (allExprs == null) {
      return null;
    }
    return newPreds.union(rexBuilder, RelOptPredicateList.of(rexBuilder, allExprs));
  }

  /**
   * Extracts predicates for an Aggregate.
   */
  public @Nullable RelOptPredicateList getAllPredicates(Aggregate agg, RelMetadataQuery mq) {
    return mq.getAllPredicates(agg.getInput());
  }

  /**
   * Extracts predicates for an TableModify.
   */
  public @Nullable RelOptPredicateList getAllPredicates(TableModify tableModify,
      RelMetadataQuery mq) {
    return mq.getAllPredicates(tableModify.getInput());
  }

  /**
   * Extracts predicates for a SetOp.
   */
  public @Nullable RelOptPredicateList getAllPredicates(SetOp setOp, RelMetadataQuery mq) {
    final RexBuilder rexBuilder = setOp.getCluster().getRexBuilder();

    final Multimap<List<String>, RelTableRef> qualifiedNamesToRefs = HashMultimap.create();
    RelOptPredicateList newPreds = RelOptPredicateList.EMPTY;
    for (int i = 0; i < setOp.getInputs().size(); i++) {
      final RelNode input = setOp.getInput(i);
      final RelOptPredicateList inputPreds = mq.getAllPredicates(input);
      if (inputPreds == null) {
        // Bail out
        return null;
      }
      // Gather table references
      final Set<RelTableRef> tableRefs = mq.getTableReferences(input);
      if (tableRefs == null) {
        return null;
      }
      if (i == 0) {
        // Left input references remain unchanged
        for (RelTableRef leftRef : tableRefs) {
          qualifiedNamesToRefs.put(leftRef.getQualifiedName(), leftRef);
        }
        newPreds = newPreds.union(rexBuilder, inputPreds);
      } else {
        // Right input references might need to be updated if there are table name
        // clashes with left input
        final Map<RelTableRef, RelTableRef> currentTablesMapping = new HashMap<>();
        for (RelTableRef rightRef : tableRefs) {
          int shift = 0;
          Collection<RelTableRef> lRefs =
              qualifiedNamesToRefs.get(rightRef.getQualifiedName());
          if (lRefs != null) {
            shift = lRefs.size();
          }
          currentTablesMapping.put(rightRef,
              RelTableRef.of(rightRef.getTable(),
                  shift + rightRef.getEntityNumber()));
        }
        // Add to existing qualified names
        for (RelTableRef newRef : currentTablesMapping.values()) {
          qualifiedNamesToRefs.put(newRef.getQualifiedName(), newRef);
        }
        // Update preds
        final List<RexNode> updatedPreds =
            Util.transform(inputPreds.pulledUpPredicates,
                e -> RexUtil.swapTableReferences(rexBuilder, e,
                    currentTablesMapping));
        newPreds =
            newPreds.union(rexBuilder,
                RelOptPredicateList.of(rexBuilder, updatedPreds));
      }
    }
    return newPreds;
  }

  /**
   * Extracts predicates for a Sort.
   */
  public @Nullable RelOptPredicateList getAllPredicates(Sort sort,
      RelMetadataQuery mq) {
    return mq.getAllPredicates(sort.getInput());
  }

  /**
   * Extracts predicates for an Exchange.
   */
  public @Nullable RelOptPredicateList getAllPredicates(Exchange exchange,
      RelMetadataQuery mq) {
    return mq.getAllPredicates(exchange.getInput());
  }

}
