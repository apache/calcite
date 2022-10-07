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
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexTableInputRef.RelTableRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import org.checkerframework.checker.nullness.qual.KeyFor;
import org.checkerframework.checker.nullness.qual.Nullable;

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

import static java.util.Objects.requireNonNull;

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
          new RelMdExpressionLineage(), BuiltInMetadata.ExpressionLineage.Handler.class);

  //~ Constructors -----------------------------------------------------------

  protected RelMdExpressionLineage() {}

  //~ Methods ----------------------------------------------------------------

  @Override public MetadataDef<BuiltInMetadata.ExpressionLineage> getDef() {
    return BuiltInMetadata.ExpressionLineage.DEF;
  }

  // Catch-all rule when none of the others apply.
  public @Nullable Set<RexNode> getExpressionLineage(RelNode rel,
      RelMetadataQuery mq, RexNode outputExpression) {
    return null;
  }

  public @Nullable Set<RexNode> getExpressionLineage(RelSubset rel,
      RelMetadataQuery mq, RexNode outputExpression) {
    RelNode bestOrOriginal = Util.first(rel.getBest(), rel.getOriginal());
    if (bestOrOriginal == null) {
      return null;
    }
    return mq.getExpressionLineage(bestOrOriginal,
        outputExpression);
  }

  /**
   * Expression lineage from {@link TableScan}.
   *
   * <p>We extract the fields referenced by the expression and we express them
   * using {@link RexTableInputRef}.
   */
  public @Nullable Set<RexNode> getExpressionLineage(TableScan rel,
      RelMetadataQuery mq, RexNode outputExpression) {
    final BuiltInMetadata.ExpressionLineage.Handler handler =
        rel.getTable().unwrap(BuiltInMetadata.ExpressionLineage.Handler.class);
    if (handler != null) {
      return handler.getExpressionLineage(rel, mq, outputExpression);
    }

    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();

    // Extract input fields referenced by expression
    final ImmutableBitSet inputFieldsUsed = extractInputRefs(outputExpression);

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
  public @Nullable Set<RexNode> getExpressionLineage(Aggregate rel,
      RelMetadataQuery mq, RexNode outputExpression) {
    final RelNode input = rel.getInput();
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();

    // Extract input fields referenced by expression
    final ImmutableBitSet inputFieldsUsed = extractInputRefs(outputExpression);

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
  public @Nullable Set<RexNode> getExpressionLineage(Join rel, RelMetadataQuery mq,
      RexNode outputExpression) {
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    final RelNode leftInput = rel.getLeft();
    final RelNode rightInput = rel.getRight();
    final int nLeftColumns = leftInput.getRowType().getFieldList().size();

    // Extract input fields referenced by expression
    final ImmutableBitSet inputFieldsUsed = extractInputRefs(outputExpression);

    if (rel.getJoinType().isOuterJoin()) {
      // If we reference the inner side, we will bail out
      if (rel.getJoinType() == JoinRelType.LEFT) {
        ImmutableBitSet rightFields = ImmutableBitSet.range(
            nLeftColumns, rel.getRowType().getFieldCount());
        if (inputFieldsUsed.intersects(rightFields)) {
          // We cannot map origin of this expression.
          return null;
        }
      } else if (rel.getJoinType() == JoinRelType.RIGHT) {
        ImmutableBitSet leftFields = ImmutableBitSet.range(
            0, nLeftColumns);
        if (inputFieldsUsed.intersects(leftFields)) {
          // We cannot map origin of this expression.
          return null;
        }
      } else {
        // We cannot map origin of this expression.
        return null;
      }
    }

    // Gather table references
    final Set<RelTableRef> leftTableRefs = mq.getTableReferences(leftInput);
    if (leftTableRefs == null) {
      // Bail out
      return null;
    }
    final Set<RelTableRef> rightTableRefs = mq.getTableReferences(rightInput);
    if (rightTableRefs == null) {
      // Bail out
      return null;
    }
    final Multimap<List<String>, RelTableRef> qualifiedNamesToRefs = HashMultimap.create();
    final Map<RelTableRef, RelTableRef> currentTablesMapping = new HashMap<>();
    for (RelTableRef leftRef : leftTableRefs) {
      qualifiedNamesToRefs.put(leftRef.getQualifiedName(), leftRef);
    }
    for (RelTableRef rightRef : rightTableRefs) {
      int shift = 0;
      Collection<RelTableRef> lRefs = qualifiedNamesToRefs.get(
          rightRef.getQualifiedName());
      if (lRefs != null) {
        shift = lRefs.size();
      }
      currentTablesMapping.put(rightRef,
          RelTableRef.of(rightRef.getTable(), shift + rightRef.getEntityNumber()));
    }

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
        // Left input references remain unchanged
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
        // Right input references might need to be updated if there are
        // table names clashes with left input
        final RelDataType fullRowType = SqlValidatorUtil.createJoinType(
            rexBuilder.getTypeFactory(),
            rel.getLeft().getRowType(),
            rel.getRight().getRowType(),
            null,
            ImmutableList.of());
        final Set<RexNode> updatedExprs = ImmutableSet.copyOf(
            Util.transform(originalExprs, e ->
                RexUtil.swapTableReferences(rexBuilder, e,
                    currentTablesMapping)));
        mapping.put(RexInputRef.of(idx, fullRowType), updatedExprs);
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
  public @Nullable Set<RexNode> getExpressionLineage(Union rel, RelMetadataQuery mq,
      RexNode outputExpression) {
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();

    // Extract input fields referenced by expression
    final ImmutableBitSet inputFieldsUsed = extractInputRefs(outputExpression);

    // Infer column origin expressions for given references
    final Multimap<List<String>, RelTableRef> qualifiedNamesToRefs = HashMultimap.create();
    final Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
    for (RelNode input : rel.getInputs()) {
      // Gather table references
      final Map<RelTableRef, RelTableRef> currentTablesMapping = new HashMap<>();
      final Set<RelTableRef> tableRefs = mq.getTableReferences(input);
      if (tableRefs == null) {
        // Bail out
        return null;
      }
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
      // Map references
      for (int idx : inputFieldsUsed) {
        final RexInputRef inputRef = RexInputRef.of(idx, input.getRowType().getFieldList());
        final Set<RexNode> originalExprs = mq.getExpressionLineage(input, inputRef);
        if (originalExprs == null) {
          // Bail out
          return null;
        }
        // References might need to be updated
        final RexInputRef ref = RexInputRef.of(idx, rel.getRowType().getFieldList());
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
  public @Nullable Set<RexNode> getExpressionLineage(Project rel,
      final RelMetadataQuery mq, RexNode outputExpression) {
    final RelNode input = rel.getInput();
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();

    // Extract input fields referenced by expression
    final ImmutableBitSet inputFieldsUsed = extractInputRefs(outputExpression);

    // Infer column origin expressions for given references
    final Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
    for (int idx : inputFieldsUsed) {
      final RexNode inputExpr = rel.getProjects().get(idx);
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
  public @Nullable Set<RexNode> getExpressionLineage(Filter rel,
      RelMetadataQuery mq, RexNode outputExpression) {
    return mq.getExpressionLineage(rel.getInput(), outputExpression);
  }

  /**
   * Expression lineage from Sort.
   */
  public @Nullable Set<RexNode> getExpressionLineage(Sort rel, RelMetadataQuery mq,
      RexNode outputExpression) {
    return mq.getExpressionLineage(rel.getInput(), outputExpression);
  }

  /**
   * Expression lineage from TableModify.
   */
  public @Nullable Set<RexNode> getExpressionLineage(TableModify rel, RelMetadataQuery mq,
      RexNode outputExpression) {
    return mq.getExpressionLineage(rel.getInput(), outputExpression);
  }

  /**
   * Expression lineage from Exchange.
   */
  public @Nullable Set<RexNode> getExpressionLineage(Exchange rel,
      RelMetadataQuery mq, RexNode outputExpression) {
    return mq.getExpressionLineage(rel.getInput(), outputExpression);
  }

  /**
   * Expression lineage from Calc.
   */
  public @Nullable Set<RexNode> getExpressionLineage(Calc calc,
      RelMetadataQuery mq, RexNode outputExpression) {
    final RelNode input = calc.getInput();
    final RexBuilder rexBuilder = calc.getCluster().getRexBuilder();
    // Extract input fields referenced by expression
    final ImmutableBitSet inputFieldsUsed = extractInputRefs(outputExpression);

    // Infer column origin expressions for given references
    final Map<RexInputRef, Set<RexNode>> mapping = new LinkedHashMap<>();
    Pair<ImmutableList<RexNode>, ImmutableList<RexNode>> calcProjectsAndFilter =
        calc.getProgram().split();
    for (int idx : inputFieldsUsed) {
      final RexNode inputExpr = calcProjectsAndFilter.getKey().get(idx);
      final Set<RexNode> originalExprs = mq.getExpressionLineage(input, inputExpr);
      if (originalExprs == null) {
        // Bail out
        return null;
      }
      final RexInputRef ref = RexInputRef.of(idx, calc.getRowType().getFieldList());
      mapping.put(ref, originalExprs);
    }

    // Return result
    return createAllPossibleExpressions(rexBuilder, outputExpression, mapping);
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
  protected static @Nullable Set<RexNode> createAllPossibleExpressions(RexBuilder rexBuilder,
      RexNode expr, Map<RexInputRef, Set<RexNode>> mapping) {
    // Extract input fields referenced by expression
    final ImmutableBitSet predFieldsUsed = extractInputRefs(expr);

    if (predFieldsUsed.isEmpty()) {
      // The unique expression is the input expression
      return ImmutableSet.of(expr);
    }

    try {
      return createAllPossibleExpressions(rexBuilder, expr, predFieldsUsed, mapping,
          new HashMap<>());
    } catch (UnsupportedOperationException e) {
      // There may be a RexNode unsupported by RexCopier, just return null
      return null;
    }
  }

  private static Set<RexNode> createAllPossibleExpressions(RexBuilder rexBuilder,
      RexNode expr, ImmutableBitSet predFieldsUsed, Map<RexInputRef, Set<RexNode>> mapping,
      Map<RexInputRef, RexNode> singleMapping) {
    final @KeyFor("mapping") RexInputRef inputRef = mapping.keySet().iterator().next();
    final Set<RexNode> replacements = requireNonNull(mapping.remove(inputRef),
        () -> "mapping.remove(inputRef) is null for " + inputRef);
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
      final List<RexNode> updatedPreds = new ArrayList<>(1);
      updatedPreds.add(rexBuilder.copy(expr));
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
      return requireNonNull(
          replacementValues.get(inputRef),
          () -> "no replacement found for inputRef " + inputRef);
    }
  }

  private static ImmutableBitSet extractInputRefs(RexNode expr) {
    final Set<RelDataTypeField> inputExtraFields = new LinkedHashSet<>();
    final RelOptUtil.InputFinder inputFinder = new RelOptUtil.InputFinder(inputExtraFields);
    expr.accept(inputFinder);
    return inputFinder.build();
  }
}
