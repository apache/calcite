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
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexTableInputRef.RelTableRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Default implementation of
 * {@link RelMetadataQuery#getExpressionLineage} for the standard logical
 * algebra.
 *
 * <p>The goal of this provider is to infer the lineage for the given expression.
 *
 * <p>The output expressions might contain references to columns produced by
 * {@link TableScan} operators ({@link RexTableInputRef}). In turn, each
 * TableScan operator is identified uniquely by a {@link RelTableRef} containing
 * its qualified name and an identifier.
 *
 * <p>If the lineage cannot be inferred, we return null.
 */
public class RelMdExpressionLineage
    implements MetadataHandler<BuiltInMetadata.ExpressionLineage> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.EXPRESSION_LINEAGE.method, new RelMdExpressionLineage());

  //~ Constructors -----------------------------------------------------------

  private RelMdExpressionLineage() {}

  //~ Methods ----------------------------------------------------------------

  public MetadataDef<BuiltInMetadata.ExpressionLineage> getDef() {
    return BuiltInMetadata.ExpressionLineage.DEF;
  }

  // Catch-all rule when none of the others apply.
  public Set<RexNode> getExpressionLineage(RelNode rel,
      RelMetadataQuery mq, RexNode outputExpression) {
    return null;
  }

  public Set<RexNode> getExpressionLineage(HepRelVertex rel, RelMetadataQuery mq,
      RexNode outputExpression) {
    return mq.getExpressionLineage(rel.getCurrentRel(), outputExpression);
  }

  public Set<RexNode> getExpressionLineage(RelSubset rel,
      RelMetadataQuery mq, RexNode outputExpression) {
    return mq.getExpressionLineage(Util.first(rel.getBest(), rel.getOriginal()),
        outputExpression);
  }

  /**
   * Expression lineage from {@link TableScan}.
   *
   * <p>We extract the fields referenced by the expression and we express them
   * using {@link RexTableInputRef}.
   */
  public Set<RexNode> getExpressionLineage(TableScan rel,
      RelMetadataQuery mq, RexNode outputExpression) {
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();

    // Extract input fields referenced by expression
    final Set<RelDataTypeField> inputExtraFields = new LinkedHashSet<>();
    final RelOptUtil.InputFinder inputFinder = new RelOptUtil.InputFinder(inputExtraFields);
    outputExpression.accept(inputFinder);
    final ImmutableBitSet inputFieldsUsed = inputFinder.inputBitSet.build();

    // Infer column origin expressions for given references
    final Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
    for (int idx : inputFieldsUsed) {
      final RexNode inputRef = RexTableInputRef.of(
          RelTableRef.of(rel.getTable(), 0),
          RexInputRef.of(idx, rel.getRowType().getFieldList()));
      final RexInputRef ref = RexInputRef.of(idx, rel.getRowType().getFieldList());
      mapping.put(ref, ImmutableSet.of(inputRef));
    }

    // Return result
    return createAllPossibleExpressions(rexBuilder, outputExpression, mapping);
  }

  /**
   * Expression lineage from {@link Aggregate}.
   *
   * <p>If the expression references grouping sets or aggregate function
   * results, we cannot extract the lineage and we return null.
   */
  public Set<RexNode> getExpressionLineage(Aggregate rel,
      RelMetadataQuery mq, RexNode outputExpression) {
    final RelNode input = rel.getInput();
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();

    // Extract input fields referenced by expression
    final Set<RelDataTypeField> inputExtraFields = new LinkedHashSet<>();
    final RelOptUtil.InputFinder inputFinder = new RelOptUtil.InputFinder(inputExtraFields);
    outputExpression.accept(inputFinder);
    final ImmutableBitSet inputFieldsUsed = inputFinder.inputBitSet.build();

    for (int idx : inputFieldsUsed) {
      if (idx >= rel.getGroupCount()) {
        // We cannot map origin of this expression.
        return null;
      }
    }

    // Infer column origin expressions for given references
    final Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
    for (int idx : inputFieldsUsed) {
      final RexInputRef inputRef = RexInputRef.of(rel.getGroupSet().nth(idx),
          input.getRowType().getFieldList());
      final Set<RexNode> originalExprs = mq.getExpressionLineage(input, inputRef);
      if (originalExprs == null) {
        // Bail out
        return null;
      }
      final RexInputRef ref = RexInputRef.of(idx, rel.getRowType().getFieldList());
      mapping.put(ref, originalExprs);
    }

    // Return result
    return createAllPossibleExpressions(rexBuilder, outputExpression, mapping);
  }

  /**
   * Expression lineage from {@link Join}.
   *
   * <p>We only extract the lineage for INNER joins.
   */
  public Set<RexNode> getExpressionLineage(Join rel, RelMetadataQuery mq,
      RexNode outputExpression) {
    if (rel.getJoinType() != JoinRelType.INNER) {
      // We cannot map origin of this expression.
      return null;
    }

    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    final RelNode leftInput = rel.getLeft();
    final RelNode rightInput = rel.getRight();
    final int nLeftColumns = leftInput.getRowType().getFieldList().size();

    // Infer column origin expressions for given references
    final Multimap<List<String>, RelTableRef> qualifiedNamesToRefs = HashMultimap.create();
    final Map<RelTableRef, RelTableRef> currentTablesMapping = new HashMap<>();
    final Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
    for (int idx = 0; idx < rel.getRowType().getFieldList().size(); idx++) {
      if (idx < nLeftColumns) {
        final RexInputRef inputRef = RexInputRef.of(idx, leftInput.getRowType().getFieldList());
        final Set<RexNode> originalExprs = mq.getExpressionLineage(leftInput, inputRef);
        if (originalExprs == null) {
          // Bail out
          return null;
        }
        // Gather table references, left input references remain unchanged
        final Set<RelTableRef> tableRefs =
            RexUtil.gatherTableReferences(Lists.newArrayList(originalExprs));
        for (RelTableRef leftRef : tableRefs) {
          qualifiedNamesToRefs.put(leftRef.getQualifiedName(), leftRef);
        }
        mapping.put(RexInputRef.of(idx, rel.getRowType().getFieldList()), originalExprs);
      } else {
        // Right input.
        final RexInputRef inputRef = RexInputRef.of(idx - nLeftColumns,
                rightInput.getRowType().getFieldList());
        final Set<RexNode> originalExprs = mq.getExpressionLineage(rightInput, inputRef);
        if (originalExprs == null) {
          // Bail out
          return null;
        }
        // Gather table references, right input references might need to be
        // updated if there are table names clashes with left input
        final Set<RelTableRef> tableRefs =
            RexUtil.gatherTableReferences(Lists.newArrayList(originalExprs));
        for (RelTableRef rightRef : tableRefs) {
          int shift = 0;
          Collection<RelTableRef> lRefs = qualifiedNamesToRefs.get(
              rightRef.getQualifiedName());
          if (lRefs != null) {
            shift = lRefs.size();
          }
          currentTablesMapping.put(rightRef,
              RelTableRef.of(rightRef.getTable(), shift + rightRef.getEntityNumber()));
        }
        final Set<RexNode> updatedExprs = ImmutableSet.copyOf(
            Iterables.transform(originalExprs, e ->
                RexUtil.swapTableReferences(rexBuilder, e,
                    currentTablesMapping)));
        mapping.put(RexInputRef.of(idx, rel.getRowType().getFieldList()), updatedExprs);
      }
    }

    // Return result
    return createAllPossibleExpressions(rexBuilder, outputExpression, mapping);
  }

  /**
   * Expression lineage from {@link Union}.
   *
   * <p>For Union operator, we might be able to extract multiple origins for the
   * references in the given expression.
   */
  public Set<RexNode> getExpressionLineage(Union rel, RelMetadataQuery mq,
      RexNode outputExpression) {
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();

    // Infer column origin expressions for given references
    final Multimap<List<String>, RelTableRef> qualifiedNamesToRefs = HashMultimap.create();
    final Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
    for (RelNode input : rel.getInputs()) {
      final Map<RelTableRef, RelTableRef> currentTablesMapping = new HashMap<>();
      for (int idx = 0; idx < input.getRowType().getFieldList().size(); idx++) {
        final RexInputRef inputRef = RexInputRef.of(idx, input.getRowType().getFieldList());
        final Set<RexNode> originalExprs = mq.getExpressionLineage(input, inputRef);
        if (originalExprs == null) {
          // Bail out
          return null;
        }

        final RexInputRef ref = RexInputRef.of(idx, rel.getRowType().getFieldList());
        // Gather table references, references might need to be
        // updated
        final Set<RelTableRef> tableRefs =
            RexUtil.gatherTableReferences(Lists.newArrayList(originalExprs));
        for (RelTableRef tableRef : tableRefs) {
          int shift = 0;
          Collection<RelTableRef> lRefs = qualifiedNamesToRefs.get(
              tableRef.getQualifiedName());
          if (lRefs != null) {
            shift = lRefs.size();
          }
          currentTablesMapping.put(tableRef,
              RelTableRef.of(tableRef.getTable(), shift + tableRef.getEntityNumber()));
        }
        final Set<RexNode> updatedExprs =
            originalExprs.stream()
                .map(e ->
                    RexUtil.swapTableReferences(rexBuilder, e,
                        currentTablesMapping))
                .collect(Collectors.toSet());
        final Set<RexNode> set = mapping.get(ref);
        if (set != null) {
          set.addAll(updatedExprs);
        } else {
          mapping.put(ref, updatedExprs);
        }
      }
      // Add to existing qualified names
      for (RelTableRef newRef : currentTablesMapping.values()) {
        qualifiedNamesToRefs.put(newRef.getQualifiedName(), newRef);
      }
    }

    // Return result
    return createAllPossibleExpressions(rexBuilder, outputExpression, mapping);
  }

  /**
   * Expression lineage from Project.
   */
  public Set<RexNode> getExpressionLineage(Project rel,
      final RelMetadataQuery mq, RexNode outputExpression) {
    final RelNode input = rel.getInput();
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();

    // Extract input fields referenced by expression
    final Set<RelDataTypeField> inputExtraFields = new LinkedHashSet<>();
    final RelOptUtil.InputFinder inputFinder = new RelOptUtil.InputFinder(inputExtraFields);
    outputExpression.accept(inputFinder);
    final ImmutableBitSet inputFieldsUsed = inputFinder.inputBitSet.build();

    // Infer column origin expressions for given references
    final Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
    for (int idx : inputFieldsUsed) {
      final RexNode inputExpr = rel.getChildExps().get(idx);
      final Set<RexNode> originalExprs = mq.getExpressionLineage(input, inputExpr);
      if (originalExprs == null) {
        // Bail out
        return null;
      }
      final RexInputRef ref = RexInputRef.of(idx, rel.getRowType().getFieldList());
      mapping.put(ref, originalExprs);
    }

    // Return result
    return createAllPossibleExpressions(rexBuilder, outputExpression, mapping);
  }

  /**
   * Expression lineage from Filter.
   */
  public Set<RexNode> getExpressionLineage(Filter rel,
      RelMetadataQuery mq, RexNode outputExpression) {
    return mq.getExpressionLineage(rel.getInput(), outputExpression);
  }

  /**
   * Expression lineage from Sort.
   */
  public Set<RexNode> getExpressionLineage(Sort rel, RelMetadataQuery mq,
      RexNode outputExpression) {
    return mq.getExpressionLineage(rel.getInput(), outputExpression);
  }

  /**
   * Expression lineage from Exchange.
   */
  public Set<RexNode> getExpressionLineage(Exchange rel,
      RelMetadataQuery mq, RexNode outputExpression) {
    return mq.getExpressionLineage(rel.getInput(), outputExpression);
  }

  /**
   * Given an expression, it will create all equivalent expressions resulting
   * from replacing all possible combinations of references in the mapping by
   * the corresponding expressions.
   *
   * @param rexBuilder rexBuilder
   * @param expr expression
   * @param mapping mapping
   * @return set of resulting expressions equivalent to the input expression
   */
  protected static Set<RexNode> createAllPossibleExpressions(RexBuilder rexBuilder,
      RexNode expr, Map<RexInputRef, Set<RexNode>> mapping) {
    // Extract input fields referenced by expression
    final Set<RelDataTypeField> inputExtraFields = new LinkedHashSet<>();
    final RelOptUtil.InputFinder inputFinder = new RelOptUtil.InputFinder(inputExtraFields);
    expr.accept(inputFinder);
    final ImmutableBitSet predFieldsUsed = inputFinder.inputBitSet.build();

    if (predFieldsUsed.isEmpty()) {
      // The unique expression is the input expression
      return ImmutableSet.of(expr);
    }

    return createAllPossibleExpressions(rexBuilder, expr, predFieldsUsed, mapping,
        new HashMap<RexInputRef, RexNode>());
  }

  private static Set<RexNode> createAllPossibleExpressions(RexBuilder rexBuilder,
      RexNode expr, ImmutableBitSet predFieldsUsed, Map<RexInputRef, Set<RexNode>> mapping,
      Map<RexInputRef, RexNode> singleMapping) {
    final RexInputRef inputRef = mapping.keySet().iterator().next();
    final Set<RexNode> replacements = mapping.remove(inputRef);
    Set<RexNode> result = new HashSet<>();
    assert !replacements.isEmpty();
    if (predFieldsUsed.indexOf(inputRef.getIndex()) != -1) {
      for (RexNode replacement : replacements) {
        singleMapping.put(inputRef, replacement);
        createExpressions(rexBuilder, expr, predFieldsUsed, mapping, singleMapping, result);
        singleMapping.remove(inputRef);
      }
    } else {
      createExpressions(rexBuilder, expr, predFieldsUsed, mapping, singleMapping, result);
    }
    mapping.put(inputRef, replacements);
    return result;
  }

  private static void createExpressions(RexBuilder rexBuilder,
      RexNode expr, ImmutableBitSet predFieldsUsed, Map<RexInputRef, Set<RexNode>> mapping,
      Map<RexInputRef, RexNode> singleMapping, Set<RexNode> result) {
    if (mapping.isEmpty()) {
      final RexReplacer replacer = new RexReplacer(singleMapping);
      final List<RexNode> updatedPreds = new ArrayList<>(
          RelOptUtil.conjunctions(
              rexBuilder.copy(expr)));
      replacer.mutate(updatedPreds);
      result.addAll(updatedPreds);
    } else {
      result.addAll(
          createAllPossibleExpressions(
              rexBuilder, expr, predFieldsUsed, mapping, singleMapping));
    }
  }

  /**
   * Replaces expressions with their equivalences. Note that we only have to
   * look for RexInputRef.
   */
  private static class RexReplacer extends RexShuttle {
    private final Map<RexInputRef, RexNode> replacementValues;

    RexReplacer(Map<RexInputRef, RexNode> replacementValues) {
      this.replacementValues = replacementValues;
    }

    @Override public RexNode visitInputRef(RexInputRef inputRef) {
      return replacementValues.get(inputRef);
    }
  }

}

// End RelMdExpressionLineage.java
