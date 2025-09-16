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

import java.util.ArrayList;
import java.util.List;
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
    return fdSet.findKeys(attributes);
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
    FunctionalDependencySet result = new FunctionalDependencySet();
    for (RelNode input : inputs) {
      FunctionalDependencySet fdSet = mq.getFunctionalDependencies(input);
      result = result.union(fdSet);
    }
    return result;
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
    int fieldCount = projections.size();

    // Create mapping from input column indices to project column indices
    Mappings.TargetMapping inputToOutputMap =
        RelOptUtil.permutation(projections, input.getRowType());

    // Map input functional dependencies to project dependencies
    mapInputFDs(inputFdSet, inputToOutputMap, projectionFdSet);

    // For each pair of output columns, determine if one determines the other
    for (int i = 0; i < fieldCount; i++) {
      for (int j = i + 1; j < fieldCount; j++) {
        RexNode expr1 = projections.get(i);
        RexNode expr2 = projections.get(j);

        // Handle identical expressions, they determine each other
        if (expr1.equals(expr2) && RexUtil.isDeterministic(expr1)) {
          projectionFdSet.addFD(i, j);
          projectionFdSet.addFD(j, i);
          continue;
        }

        // Handle literal constants, all columns determine literals
        if (expr1 instanceof RexLiteral) {
          projectionFdSet.addFD(j, i);
        }
        if (expr2 instanceof RexLiteral) {
          projectionFdSet.addFD(i, j);
        }

        // For complex expressions, check if they have functional dependencies
        if (!(expr1 instanceof RexLiteral) && !(expr2 instanceof RexLiteral)
            && RexUtil.isDeterministic(expr1) && RexUtil.isDeterministic(expr2)) {
          ImmutableBitSet inputs1 = RelOptUtil.InputFinder.bits(expr1);
          ImmutableBitSet inputs2 = RelOptUtil.InputFinder.bits(expr2);

          if (!inputs1.isEmpty() && !inputs2.isEmpty()) {
            if (inputFdSet.implies(inputs1, inputs2)) {
              projectionFdSet.addFD(i, j);
            }
            if (inputFdSet.implies(inputs2, inputs1)) {
              projectionFdSet.addFD(j, i);
            }
          }
        }
      }
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
          determinants.stream().allMatch(col -> col >= 0
              && col < mapping.getSourceCount()
              && mapping.getTargetOpt(col) >= 0);
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
      if (col < 0 || col >= mapping.getSourceCount()) {
        return ImmutableBitSet.of();
      }
      int mappedCol = mapping.getTargetOpt(col);
      if (mappedCol >= 0) {
        builder.set(mappedCol);
      } else {
        return ImmutableBitSet.of();
      }
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
      if (col < 0 || col >= mapping.getSourceCount()) {
        continue;
      }
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
    for (FunctionalDependency inputFd : inputFdSet.getFDs()) {
      ImmutableBitSet determinants = inputFd.getDeterminants();
      ImmutableBitSet dependents = inputFd.getDependents();

      // Only preserve if both determinants and dependents are within group columns
      if (groupSet.contains(determinants) && groupSet.contains(dependents)) {
        fdSet.addFD(determinants, dependents);
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
      deriveTransitiveFDs(rel, innerJoinFdSet, leftFieldCount);
      return innerJoinFdSet;
    case LEFT:
      // Left join: preserve left FDs, right FDs may be invalidated by NULLs
      FunctionalDependencySet leftJoinFdSet = new FunctionalDependencySet(leftFdSet.getFDs());
      deriveTransitiveFDs(rel, leftJoinFdSet, leftFieldCount);
      return leftJoinFdSet;
    case RIGHT: {
      // Right join: preserve right FDs, left FDs may be invalidated by NULLs
      // 只保留右表字段决定右表字段的 FD，不做任何跨表 transitive 推导
      FunctionalDependencySet shiftedRightFdSet = shiftFdSet(rightFdSet, leftFieldCount);
      FunctionalDependencySet filteredFdSet = new FunctionalDependencySet();
      int totalFieldCount = rel.getRowType().getFieldCount();
      for (FunctionalDependency fd : shiftedRightFdSet.getFunctionalDependencies()) {
        boolean determinantsInRight = true;
        boolean dependentsInRight = true;
        for (int col : fd.getDeterminants()) {
          if (col < leftFieldCount || col >= totalFieldCount) {
            determinantsInRight = false;
            break;
          }
        }
        for (int col : fd.getDependents()) {
          if (col < leftFieldCount || col >= totalFieldCount) {
            dependentsInRight = false;
            break;
          }
        }
        if (determinantsInRight && dependentsInRight) {
          filteredFdSet.addFD(fd.getDeterminants(), fd.getDependents());
        }
      }
      // 禁用 deriveTransitiveFDs，彻底禁止跨表 FD
      return filteredFdSet;
    }
    case FULL:
      // Full join: both sides may have NULLs, very conservative approach
      return new FunctionalDependencySet();
    case SEMI:
      // Semi join: only left table columns in result, preserve left FDs only
      return leftFdSet;
    case ANTI:
      // Anti join: only left table columns in result, preserve left FDs only
      return leftFdSet;
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
   * Derives cross-table functional dependencies through join conditions.
   */
  private static void deriveTransitiveFDs(
      Join rel, FunctionalDependencySet result, int leftFieldCount) {
    deriveTransitiveFDsFromCondition(rel.getCondition(), result, leftFieldCount);
    deriveAdditionalTransitiveFDs(result);
  }

  /**
   * Iteratively derives additional transitive dependencies until closure.
   */
  private static void deriveAdditionalTransitiveFDs(FunctionalDependencySet result) {
    boolean changed = true;
    // Limit iterations to avoid infinite loops
    int iteration = 10;

    while (changed && iteration > 0) {
      changed = false;
      iteration--;
      List<FunctionalDependency> currentFDs = new ArrayList<>(result.getFunctionalDependencies());

      for (FunctionalDependency fd1 : currentFDs) {
        for (FunctionalDependency fd2 : currentFDs) {
          // Only apply transitivity when fd1's dependents completely contain fd2's determinants
          // This ensures: if X → Y and Y → Z, then X → Z
          if (fd1.getDependents().contains(fd2.getDeterminants())) {
            ImmutableBitSet newDeterminants = fd1.getDeterminants();
            ImmutableBitSet newDependents = fd2.getDependents();
            result.addFD(newDeterminants, newDependents);
            changed = true;
          }
        }
      }
    }
  }

  /**
   * Processes join conditions to derive cross-table dependencies.
   */
  private static void deriveTransitiveFDsFromCondition(
      RexNode condition, FunctionalDependencySet result, int leftFieldCount) {
    if (!(condition instanceof RexCall)) {
      return;
    }

    RexCall call = (RexCall) condition;
    if (call.getOperator().getKind() == SqlKind.EQUALS) {
      // Handle equality condition: col1 = col2
      List<RexNode> operands = call.getOperands();
      if (operands.size() == 2) {
        RexNode left = operands.get(0);
        RexNode right = operands.get(1);

        if (left instanceof RexInputRef && right instanceof RexInputRef) {
          int leftCol = ((RexInputRef) left).getIndex();
          int rightCol = ((RexInputRef) right).getIndex();

          // Ensure one column is from left table and other from right table
          if (leftCol < leftFieldCount && rightCol >= leftFieldCount) {
            addTransitiveFDs(result, leftCol, rightCol);
          } else if (rightCol < leftFieldCount && leftCol >= leftFieldCount) {
            addTransitiveFDs(result, rightCol, leftCol);
          }
        }
      }
    } else if (call.getOperator().getKind() == SqlKind.AND) {
      // Handle compound AND conditions
      for (RexNode operand : call.getOperands()) {
        deriveTransitiveFDsFromCondition(operand, result, leftFieldCount);
      }
    }
  }

  /**
   * Adds transitive dependencies between equivalent columns from equi-join.
   */
  private static void addTransitiveFDs(FunctionalDependencySet result,
      int leftCol, int rightCol) {
    List<FunctionalDependency> dependencies = new ArrayList<>(result.getFunctionalDependencies());

    for (FunctionalDependency fd : dependencies) {
      // If leftCol is determined by some columns, then rightCol is also determined
      if (fd.getDependents().get(leftCol)) {
        ImmutableBitSet newDependents = fd.getDependents().union(ImmutableBitSet.of(rightCol));
        result.addFD(fd.getDeterminants(), newDependents);
      }

      // If rightCol is determined by some columns, then leftCol is also determined
      if (fd.getDependents().get(rightCol)) {
        ImmutableBitSet newDependents = fd.getDependents().union(ImmutableBitSet.of(leftCol));
        result.addFD(fd.getDeterminants(), newDependents);
      }

      // If leftCol is a determinant in an FD, create equivalent FD with rightCol as determinant
      if (fd.getDeterminants().get(leftCol)) {
        ImmutableBitSet newDeterminants =
            fd.getDeterminants().rebuild().clear(leftCol).set(rightCol).build();
        result.addFD(newDeterminants, fd.getDependents());
      }

      // If rightCol is a determinant in an FD, create equivalent FD with leftCol as determinant
      if (fd.getDeterminants().get(rightCol)) {
        ImmutableBitSet newDeterminants =
            fd.getDeterminants().rebuild().clear(rightCol).set(leftCol).build();
        result.addFD(newDeterminants, fd.getDependents());
      }
    }
  }
}
