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

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of {@link BuiltInMetadata.FunctionalDependency} metadata handler
 * for the standard logical algebra.
 *
 * <p>Provides functional dependency analysis for relational operators using
 * {@link FunctionalDependencySet}.
 *
 * <p>Key capabilities:
 * <ul>
 * <li>Dependency detection via {@link #determines} and {@link #determinesSet}
 * <li>Attribute closure computation via {@link #closure}
 * <li>Candidate key discovery via {@link #candidateKeys}
 * <li>Cross-table dependency derivation for joins
 * </ul>
 *
 * @see FunctionalDependency
 * @see FunctionalDependencySet
 */
public class RelMdFunctionalDependency
    implements MetadataHandler<BuiltInMetadata.FunctionalDependency> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          new RelMdFunctionalDependency(), BuiltInMetadata.FunctionalDependency.Handler.class);

  //~ Constructors -----------------------------------------------------------

  protected RelMdFunctionalDependency() {}

  //~ Methods ----------------------------------------------------------------

  @Override public MetadataDef<BuiltInMetadata.FunctionalDependency> getDef() {
    return BuiltInMetadata.FunctionalDependency.DEF;
  }

  /**
   * Determines if column is functionally dependent on key for a given rel node.
   */
  public @Nullable Boolean determines(RelNode rel, RelMetadataQuery mq,
      int key, int column) {
    return determinesSet(rel, mq, ImmutableBitSet.of(key), ImmutableBitSet.of(column));
  }

  /**
   * Determines if a set of columns functionally determines another set of columns.
   */
  public Boolean determinesSet(RelNode rel, RelMetadataQuery mq,
      ImmutableBitSet determinants, ImmutableBitSet dependents) {
    FunctionalDependencySet fdSet = mq.getFunctionalDependencies(rel);
    return fdSet.implies(determinants, dependents);
  }

  /**
   * Returns the closure of a set of columns under all functional dependencies.
   */
  public ImmutableBitSet closure(RelNode rel, RelMetadataQuery mq, ImmutableBitSet attrs) {
    FunctionalDependencySet fdSet = mq.getFunctionalDependencies(rel);
    return fdSet.closure(attrs);
  }

  /**
   * Returns candidate keys for the relation within the specified set of attributes.
   */
  public Set<ImmutableBitSet> candidateKeys(
      RelNode rel, RelMetadataQuery mq, ImmutableBitSet attributes) {
    FunctionalDependencySet fdSet = mq.getFunctionalDependencies(rel);
    return fdSet.findCandidateKeys(attributes);
  }

  /**
   * Main dispatch method for getFunctionalDependencies.
   * Routes to appropriate handler based on RelNode type.
   */
  public FunctionalDependencySet getFunctionalDependencies(RelNode rel, RelMetadataQuery mq) {
    if (rel instanceof TableScan) {
      return getTableScanFD((TableScan) rel);
    } else if (rel instanceof Project) {
      return getProjectFD((Project) rel, mq);
    } else if (rel instanceof Aggregate) {
      return getAggregateFD((Aggregate) rel, mq);
    } else if (rel instanceof Join) {
      return getJoinFD((Join) rel, mq);
    } else if (rel instanceof Calc) {
      return getCalcFD((Calc) rel, mq);
    } else if (rel instanceof Filter) {
      return getFilterFD((Filter) rel, mq);
    } else if (rel instanceof SetOp) {
      // TODO: Handle UNION, INTERSECT, EXCEPT functional dependencies
      return new FunctionalDependencySet();
    } else if (rel instanceof Correlate) {
      // TODO: Handle CORRELATE functional dependencies
      return new FunctionalDependencySet();
    }
    return getFD(rel.getInputs(), mq);
  }

  private static FunctionalDependencySet getFD(List<RelNode> inputs, RelMetadataQuery mq) {
    if (inputs.size() > 1) {
      // Conservative approach for multi-input nodes without specific logic
      return new FunctionalDependencySet();
    }
    return mq.getFunctionalDependencies(inputs.get(0));
  }

  private static FunctionalDependencySet getTableScanFD(TableScan rel) {
    FunctionalDependencySet fdSet = new FunctionalDependencySet();

    RelOptTable table = rel.getTable();
    List<ImmutableBitSet> keys = table.getKeys();
    if (keys == null || keys.isEmpty()) {
      return fdSet;
    }

    for (ImmutableBitSet key : keys) {
      ImmutableBitSet allColumns = ImmutableBitSet.range(rel.getRowType().getFieldCount());
      ImmutableBitSet dependents = allColumns.except(key);
      if (!dependents.isEmpty()) {
        fdSet.addFD(key, dependents);
      }
    }

    return fdSet;
  }

  private static FunctionalDependencySet getProjectFD(Project rel, RelMetadataQuery mq) {
    return getProjectionFD(rel.getInput(), rel.getProjects(), mq);
  }

  /**
   * Common method to compute functional dependencies for projection operations.
   * Used by both Project and Calc nodes.
   *
   * @param input the input relation
   * @param projections the list of projection expressions
   * @param mq the metadata query
   * @return the functional dependency set for the projection
   */
  private static FunctionalDependencySet getProjectionFD(
      RelNode input, List<RexNode> projections, RelMetadataQuery mq) {
    FunctionalDependencySet inputFdSet = mq.getFunctionalDependencies(input);
    FunctionalDependencySet projectionFdSet = new FunctionalDependencySet();

    // Create mapping from input column indices to project column indices
    Mappings.TargetMapping inputToOutputMap =
        RelOptUtil.permutation(projections, input.getRowType()).inverse();

    // Map input functional dependencies to project dependencies
    mapInputFDs(inputFdSet, inputToOutputMap, projectionFdSet);

    int fieldCount = projections.size();
    final ImmutableBitSet[] inputBits = new ImmutableBitSet[fieldCount];
    final Map<RexNode, Integer> uniqueExprToIndex = new HashMap<>();
    final Map<RexLiteral, Integer> literalToIndex = new HashMap<>();
    final Map<RexInputRef, Integer> refToIndex = new HashMap<>();
    for (int i = 0; i < fieldCount; i++) {
      RexNode expr = projections.get(i);

      // Skip non-deterministic expressions
      if (!RexUtil.isDeterministic(expr)) {
        continue;
      }

      // Handle identical expressions in the projection list
      Integer prev = uniqueExprToIndex.putIfAbsent(expr, i);
      if (prev != null) {
        projectionFdSet.addBidirectionalFD(prev, i);
      }

      // Track literal constants to handle them specially
      if (expr instanceof RexLiteral) {
        literalToIndex.put((RexLiteral) expr, i);
        continue;
      }

      if (expr instanceof RexInputRef) {
        refToIndex.put((RexInputRef) expr, i);
        inputBits[i] = ImmutableBitSet.of(((RexInputRef) expr).getIndex());
      }

      inputBits[i] = RelOptUtil.InputFinder.bits(expr);
    }

    // Remove literals from uniqueExprToIndex to avoid redundant processing
    uniqueExprToIndex.keySet().removeIf(key -> key instanceof RexLiteral);

    for (Map.Entry<RexNode, Integer> entry : uniqueExprToIndex.entrySet()) {
      RexNode expr = entry.getKey();
      Integer i = entry.getValue();

      // All columns determine literals
      literalToIndex.values()
          .forEach(l -> projectionFdSet.addFD(ImmutableBitSet.of(i), ImmutableBitSet.of(l)));

      // Input columns determine identical expressions
      refToIndex.forEach((k, v) -> {
        ImmutableBitSet refIndex = ImmutableBitSet.of(k.getIndex());
        ImmutableBitSet bits = expr instanceof RexInputRef
            ? ImmutableBitSet.of(((RexInputRef) expr).getIndex())
            : inputBits[i];
        if (inputFdSet.implies(refIndex, bits)) {
          projectionFdSet.addFD(v, i);
        }
      });
    }

    return projectionFdSet;
  }

  /**
   * Maps input functional dependencies to output dependencies based on column mapping.
   */
  private static void mapInputFDs(FunctionalDependencySet inputFdSet,
      Mappings.TargetMapping mapping, FunctionalDependencySet outputFdSet) {
    for (FunctionalDependency inputFd : inputFdSet.getFDs()) {
      ImmutableBitSet determinants = inputFd.getDeterminants();
      ImmutableBitSet dependents = inputFd.getDependents();

      // Skip this FD if any determinant column is unmappable
      boolean allMappable =
          determinants.stream().allMatch(col -> mapping.getTargetOpt(col) >= 0);
      if (!allMappable) {
        continue;
      }

      // Map all determinant columns
      ImmutableBitSet mappedDeterminants = mapAllCols(determinants, mapping);
      if (mappedDeterminants.isEmpty()) {
        continue;
      }

      // Map only the dependent columns that can be mapped
      ImmutableBitSet mappedDependents = mapAvailableCols(dependents, mapping);
      if (!mappedDependents.isEmpty()) {
        outputFdSet.addFD(mappedDeterminants, mappedDependents);
      }
    }
  }

  /**
   * Maps all columns in the set. Returns empty set if any column cannot be mapped.
   */
  private static ImmutableBitSet mapAllCols(
      ImmutableBitSet columns, Mappings.TargetMapping mapping) {
    ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
    for (int col : columns) {
      int mappedCol = mapping.getTargetOpt(col);
      if (mappedCol < 0) {
        return ImmutableBitSet.of();
      }
      builder.set(mappedCol);
    }
    return builder.build();
  }

  /**
   * Maps only the columns that can be mapped, ignoring unmappable ones.
   */
  private static ImmutableBitSet mapAvailableCols(
      ImmutableBitSet columns, Mappings.TargetMapping mapping) {
    ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
    for (int col : columns) {
      int mappedCol = mapping.getTargetOpt(col);
      if (mappedCol >= 0) {
        builder.set(mappedCol);
      }
    }
    return builder.build();
  }

  private static FunctionalDependencySet getAggregateFD(Aggregate rel, RelMetadataQuery mq) {
    FunctionalDependencySet fdSet = new FunctionalDependencySet();
    FunctionalDependencySet inputFdSet = mq.getFunctionalDependencies(rel.getInput());

    // Group set columns in the output
    ImmutableBitSet groupSet = rel.getGroupSet();

    // 1. Preserve input FDs that only involve group columns
    if (Aggregate.isSimple(rel)) {
      for (FunctionalDependency inputFd : inputFdSet.getFDs()) {
        ImmutableBitSet determinants = inputFd.getDeterminants();
        ImmutableBitSet dependents = inputFd.getDependents();

        // Only preserve if both determinants and dependents are within group columns
        if (groupSet.contains(determinants) && groupSet.contains(dependents)) {
          fdSet.addFD(determinants, dependents);
        }
      }
    }

    // 2. Group keys determine all aggregate columns
    if (!groupSet.isEmpty() && !rel.getAggCallList().isEmpty()) {
      for (int i = rel.getGroupCount(); i < rel.getRowType().getFieldCount(); i++) {
        fdSet.addFD(groupSet, ImmutableBitSet.of(i));
      }
    }

    return fdSet;
  }

  private static FunctionalDependencySet getFilterFD(Filter rel, RelMetadataQuery mq) {
    FunctionalDependencySet inputSet = mq.getFunctionalDependencies(rel.getInput());
    FunctionalDependencySet filterFdSet = new FunctionalDependencySet(inputSet.getFDs());
    addFDsFromEquiCondition(rel.getCondition(), filterFdSet);
    return filterFdSet;
  }

  private static FunctionalDependencySet getJoinFD(Join rel, RelMetadataQuery mq) {
    FunctionalDependencySet leftFdSet = mq.getFunctionalDependencies(rel.getLeft());
    FunctionalDependencySet rightFdSet = mq.getFunctionalDependencies(rel.getRight());

    int leftFieldCount = rel.getLeft().getRowType().getFieldCount();
    JoinRelType joinType = rel.getJoinType();

    switch (joinType) {
    case INNER:
      // Inner join: preserve all FDs and derive cross-table dependencies
      FunctionalDependencySet innerJoinFdSet
          = leftFdSet.union(shiftFdSet(rightFdSet, leftFieldCount));
      addFDsFromEquiCondition(rel.getCondition(), innerJoinFdSet);
      return innerJoinFdSet.computeTransitiveClosure();
    case LEFT:
      // Left join: preserve left FDs, right FDs may be invalidated by NULLs
      FunctionalDependencySet leftJoinFdSet = new FunctionalDependencySet(leftFdSet.getFDs());
      addFDsFromEquiCondition(rel.getCondition(), leftJoinFdSet);
      return leftJoinFdSet.computeTransitiveClosure();
    case RIGHT:
      // Right join: preserve right FDs, left FDs may be invalidated by NULLs
      FunctionalDependencySet rightJoinFdSet = shiftFdSet(rightFdSet, leftFieldCount);
      addFDsFromEquiCondition(rel.getCondition(), rightJoinFdSet);
      return rightJoinFdSet.computeTransitiveClosure();
    case FULL:
      // Full join: both sides may have NULLs, very conservative approach
      return new FunctionalDependencySet();
    case SEMI:
      // Semi join: only left table columns in result, preserve left FDs only
      return new FunctionalDependencySet(leftFdSet.getFDs());
    case ANTI:
      // Anti join: only left table columns in result, preserve left FDs only
      return new FunctionalDependencySet(leftFdSet.getFDs());
    default:
      // Conservative fallback for unknown join types
      return new FunctionalDependencySet();
    }
  }

  private static FunctionalDependencySet getCalcFD(Calc rel, RelMetadataQuery mq) {
    List<RexNode> projections = rel.getProgram().expandList(rel.getProgram().getProjectList());
    return getProjectionFD(rel.getInput(), projections, mq);
  }

  private static FunctionalDependencySet shiftFdSet(FunctionalDependencySet fdSet, int offset) {
    FunctionalDependencySet shiftedFdSet = new FunctionalDependencySet();
    for (FunctionalDependency fd : fdSet.getFunctionalDependencies()) {
      ImmutableBitSet shiftedDeterminants = fd.getDeterminants().shift(offset);
      ImmutableBitSet shiftedDependents = fd.getDependents().shift(offset);
      shiftedFdSet.addFD(shiftedDeterminants, shiftedDependents);
    }
    return shiftedFdSet;
  }

  /**
   * Extracts FDs from equality and AND conditions.
   */
  private static void addFDsFromEquiCondition(RexNode condition, FunctionalDependencySet result) {
    if (!(condition instanceof RexCall)) {
      return;
    }

    RexCall call = (RexCall) condition;
    // Handle equality condition: col1 = col2 or col1 IS NOT DISTINCT FROM col2
    if (call.getOperator().getKind() == SqlKind.EQUALS
        || call.getOperator().getKind() == SqlKind.IS_NOT_DISTINCT_FROM) {
      List<RexNode> operands = call.getOperands();
      if (operands.size() == 2) {
        RexNode left = operands.get(0);
        RexNode right = operands.get(1);

        if (left instanceof RexInputRef && right instanceof RexInputRef) {
          int leftRef = ((RexInputRef) left).getIndex();
          int rightRef = ((RexInputRef) right).getIndex();

          result.addFD(ImmutableBitSet.of(leftRef), ImmutableBitSet.of(rightRef));
          result.addFD(ImmutableBitSet.of(rightRef), ImmutableBitSet.of(leftRef));
        }
      }
    } else if (call.getOperator().getKind() == SqlKind.AND) {
      // Handle compound AND conditions
      for (RexNode operand : call.getOperands()) {
        addFDsFromEquiCondition(operand, result);
      }
    }
  }
}
