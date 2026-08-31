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
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Arrow;
import org.apache.calcite.util.ArrowSet;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of {@link BuiltInMetadata.FunctionalDependency} metadata handler
 * for relational algebra nodes. Uses {@link ArrowSet} to represent functional dependencies.
 *
 * <p>Core functionalities:
 * <ul>
 *   <li>Detects functional dependencies ({@link #determines}, {@link #determinesSet})</li>
 *   <li>Computes all columns functionally determined by a given set ({@link #dependents})</li>
 *   <li>Finds minimal determinant sets ({@link #determinants})</li>
 * </ul>
 *
 * @see Arrow
 * @see ArrowSet
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
   * Returns whether one column functionally determines another.
   *
   * @param rel Relational node
   * @param mq Metadata query
   * @param determinant Determinant column index
   * @param dependent Dependent column index
   * @return true if determinant determines dependent
   */
  public @Nullable Boolean determines(RelNode rel, RelMetadataQuery mq,
      int determinant, int dependent) {
    return determinesSet(rel, mq, ImmutableBitSet.of(determinant), ImmutableBitSet.of(dependent));
  }

  /**
   * Returns whether a set of columns functionally determines another set.
   *
   * @param rel Relational node
   * @param mq Metadata query
   * @param determinants Indices of determinant columns
   * @param dependents Indices of dependent columns
   * @return true if determinants determine dependents
   */
  public Boolean determinesSet(RelNode rel, RelMetadataQuery mq,
      ImmutableBitSet determinants, ImmutableBitSet dependents) {
    ArrowSet fdSet = mq.getFDs(rel);
    return fdSet.implies(determinants, dependents);
  }

  /**
   * Returns all columns functionally determined by the given columns.
   *
   * @param rel Relational node
   * @param mq Metadata query
   * @param ordinals Indices of input columns
   * @return Indices of determined columns
   */
  public ImmutableBitSet dependents(RelNode rel, RelMetadataQuery mq,
      ImmutableBitSet ordinals) {
    ArrowSet fdSet = mq.getFDs(rel);
    return fdSet.dependents(ordinals);
  }

  /**
   * Returns all minimal determinant sets for the given columns.
   *
   * @param rel Relational node
   * @param mq Metadata query
   * @param ordinals Indices of columns
   * @return Minimal determinant sets
   */
  public Set<ImmutableBitSet> determinants(
      RelNode rel, RelMetadataQuery mq, ImmutableBitSet ordinals) {
    ArrowSet fdSet = mq.getFDs(rel);
    return fdSet.determinants(ordinals);
  }

  /**
   * Returns all functional dependencies for the given relational node.
   */
  public ArrowSet getFDs(RelNode rel, RelMetadataQuery mq) {
    rel = rel.stripped();
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
      return ArrowSet.EMPTY;
    } else if (rel instanceof Correlate) {
      // TODO: Handle CORRELATE functional dependencies
      return ArrowSet.EMPTY;
    }
    return getFD(rel.getInputs(), mq);
  }

  /**
   * Returns functional dependencies for input nodes;
   * returns empty if multiple inputs.
   */
  private ArrowSet getFD(List<RelNode> inputs, RelMetadataQuery mq) {
    if (inputs.size() != 1) {
      // Conservative approach for multi-input nodes without specific logic
      return ArrowSet.EMPTY;
    }
    return mq.getFDs(inputs.get(0));
  }

  /**
   * Returns functional dependencies for a TableScan node.
   */
  private static ArrowSet getTableScanFD(TableScan rel) {
    ArrowSet.Builder fdBuilder = new ArrowSet.Builder();

    RelOptTable table = rel.getTable();
    List<ImmutableBitSet> keys = table.getKeys();
    if (keys == null || keys.isEmpty()) {
      return fdBuilder.build();
    }

    for (ImmutableBitSet key : keys) {
      ImmutableBitSet allColumns = ImmutableBitSet.range(rel.getRowType().getFieldCount());
      ImmutableBitSet dependents = allColumns.except(key);
      if (!dependents.isEmpty()) {
        fdBuilder.addArrow(key, dependents);
      }
    }

    return fdBuilder.build();
  }

  /**
   * Returns functional dependencies for a Project node.
   */
  private ArrowSet getProjectFD(Project rel, RelMetadataQuery mq) {
    return getProjectionFD(rel.getInput(), rel.getProjects(), mq);
  }

  /**
   * Returns functional dependencies after projection.
   *
   * @param input Input relation
   * @param projections Projection expressions
   * @param mq Metadata query
   * @return Functional dependencies after projection
   */
  private ArrowSet getProjectionFD(
      RelNode input, List<RexNode> projections, RelMetadataQuery mq) {
    ArrowSet inputFdSet = mq.getFDs(input);
    ArrowSet.Builder fdBuilder = new ArrowSet.Builder();

    // Create mapping from input column indices to project column indices
    Mappings.TargetMapping inputToOutputMap =
        RelOptUtil.permutation(projections, input.getRowType()).inverse();

    // Map input functional dependencies to project dependencies
    mapInputFDs(inputFdSet, inputToOutputMap, fdBuilder);

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
        fdBuilder.addBidirectionalArrow(prev, i);
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
          .forEach(l -> fdBuilder.addArrow(i, l));

      // Input columns determine identical expressions
      refToIndex.forEach((k, v) -> {
        ImmutableBitSet refIndex = ImmutableBitSet.of(k.getIndex());
        ImmutableBitSet bitSet = expr instanceof RexInputRef
            ? ImmutableBitSet.of(((RexInputRef) expr).getIndex())
            : inputBits[i];
        if (typeSupportsGroupKeyInference(k.getType())
            && inputFdSet.implies(refIndex, bitSet)) {
          fdBuilder.addArrow(v, i);
        }
      });
    }

    return fdBuilder.build();
  }

  /**
   * Maps input dependencies to output dependencies using column mapping.
   */
  private static void mapInputFDs(ArrowSet inputFdSet,
      Mappings.TargetMapping mapping, ArrowSet.Builder outputFdBuilder) {
    for (Arrow inputFd : inputFdSet.getArrows()) {
      ImmutableBitSet determinants = inputFd.getDeterminants();
      ImmutableBitSet dependents = inputFd.getDependents();

      // Map all determinant columns
      ImmutableBitSet mappedDeterminants = mapAllCols(determinants, mapping);
      if (mappedDeterminants.isEmpty() && !determinants.isEmpty()) {
        continue;
      }

      // Map only the dependent columns that can be mapped
      ImmutableBitSet mappedDependents = mapAvailableCols(dependents, mapping);
      if (!mappedDependents.isEmpty()) {
        outputFdBuilder.addArrow(mappedDeterminants, mappedDependents);
      }
    }
  }

  /**
   * Maps all columns;
   * returns empty if any cannot be mapped.
   */
  private static ImmutableBitSet mapAllCols(
      ImmutableBitSet ordinals, Mappings.TargetMapping mapping) {
    ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
    for (int ord : ordinals) {
      int mappedOrd = mapping.getTargetOpt(ord);
      if (mappedOrd < 0) {
        return ImmutableBitSet.of();
      }
      builder.set(mappedOrd);
    }
    return builder.build();
  }

  /**
   * Maps only columns that can be mapped;
   * ignores others.
   */
  private static ImmutableBitSet mapAvailableCols(
      ImmutableBitSet ordinals, Mappings.TargetMapping mapping) {
    ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
    for (int ord : ordinals) {
      int mappedOrd = mapping.getTargetOpt(ord);
      if (mappedOrd >= 0) {
        builder.set(mappedOrd);
      }
    }
    return builder.build();
  }

  /**
   * Returns functional dependencies for Aggregate.
   */
  private ArrowSet getAggregateFD(Aggregate rel, RelMetadataQuery mq) {
    ArrowSet.Builder fdBuilder = new ArrowSet.Builder();
    ArrowSet inputFdSet = mq.getFDs(rel.getInput());

    ImmutableBitSet groupSet = rel.getGroupSet();
    Mappings.TargetMapping inputToOutputMap =
        Mappings.target(groupSet::indexOf,
            rel.getInput().getRowType().getFieldCount(), rel.getGroupCount());

    // Preserve input FDs that only involve group columns
    if (Aggregate.isSimple(rel)) {
      mapInputFDs(inputFdSet, inputToOutputMap, fdBuilder);

      // Add transitive dependencies within group columns
      for (int groupCol : groupSet) {
        ImmutableBitSet singleton = ImmutableBitSet.of(groupCol);
        ImmutableBitSet closure = inputFdSet.dependents(singleton);
        ImmutableBitSet groupDependents = closure.intersect(groupSet).except(singleton);
        if (!groupDependents.isEmpty()) {
          fdBuilder.addArrow(mapAllCols(singleton, inputToOutputMap),
              mapAllCols(groupDependents, inputToOutputMap));
        }
      }
    }

    // Group keys determine all aggregate columns
    if (!groupSet.isEmpty() && !rel.getAggCallList().isEmpty()) {
      ImmutableBitSet aggCols =
          ImmutableBitSet.range(rel.getGroupCount(), rel.getRowType().getFieldCount());
      fdBuilder.addArrow(ImmutableBitSet.range(rel.getGroupCount()), aggCols);
    }

    return fdBuilder.build();
  }

  /**
   * Returns functional dependencies for Filter.
   */
  private ArrowSet getFilterFD(Filter rel, RelMetadataQuery mq) {
    ArrowSet inputSet = mq.getFDs(rel.getInput());
    ArrowSet.Builder fdBuilder = new ArrowSet.Builder();

    // Adds equality dependencies from filter conditions.
    addBidirectionalFDsFromEqualityCondition(rel.getCondition(), fdBuilder);

    return fdBuilder.build().union(inputSet);
  }

  /**
   * Returns functional dependencies for Join.
   * Preserves and combines dependencies based on join type.
   */
  private ArrowSet getJoinFD(Join rel, RelMetadataQuery mq) {
    ArrowSet leftFdSet = mq.getFDs(rel.getLeft());
    ArrowSet rightFdSet = mq.getFDs(rel.getRight());

    int leftFieldCount = rel.getLeft().getRowType().getFieldCount();
    JoinRelType joinType = rel.getJoinType();

    switch (joinType) {
    case INNER:
    case LEFT:
    case RIGHT:
      ArrowSet.Builder joinFdBuilder = new ArrowSet.Builder();
      addJoinInputFDs(leftFdSet, rel.getLeft(), 0,
          joinType.generatesNullsOnLeft(), joinFdBuilder);
      addJoinInputFDs(rightFdSet, rel.getRight(), leftFieldCount,
          joinType.generatesNullsOnRight(), joinFdBuilder);
      addFDsFromJoinCondition(rel, leftFieldCount, joinFdBuilder);
      return joinFdBuilder.build();
    case SEMI:
    case ANTI:
      return leftFdSet.clone();
    default:
      return ArrowSet.EMPTY;
    }
  }

  /**
   * Copies input dependencies into a join, optionally filtering dependencies
   * that can be invalidated when the input is null-generated.
   */
  private static void addJoinInputFDs(ArrowSet inputFdSet, RelNode input,
      int offset, boolean nullGenerated, ArrowSet.Builder fdBuilder) {
    for (Arrow inputFd : inputFdSet.getArrows()) {
      if (nullGenerated && !hasNonNullableDeterminant(inputFd, input)) {
        continue;
      }
      fdBuilder.addArrow(inputFd.getDeterminants().shift(offset),
          inputFd.getDependents().shift(offset));
    }
  }

  /**
   * Returns whether a dependency's determinant contains a non-nullable input
   * field. Such a determinant cannot collide with the all-NULL determinant of
   * a padded outer-join row.
   */
  private static boolean hasNonNullableDeterminant(Arrow fd, RelNode input) {
    final int fieldCount = input.getRowType().getFieldCount();
    boolean hasNonNullableField = false;
    for (int determinant : fd.getDeterminants()) {
      if (determinant >= fieldCount) {
        return false;
      }
      if (!input.getRowType().getFieldList().get(determinant)
          .getType().isNullable()) {
        hasNonNullableField = true;
      }
    }
    return hasNonNullableField;
  }

  /**
   * Returns functional dependencies for Calc.
   */
  private ArrowSet getCalcFD(Calc rel, RelMetadataQuery mq) {
    List<RexNode> projections = rel.getProgram().expandList(rel.getProgram().getProjectList());
    return getProjectionFD(rel.getInput(), projections, mq);
  }

  /**
   * Adds functional dependencies implied by a join condition.
   */
  private static void addFDsFromJoinCondition(Join rel, int leftFieldCount,
      ArrowSet.Builder builder) {
    final JoinRelType joinType = rel.getJoinType();
    if (joinType == JoinRelType.INNER) {
      addBidirectionalFDsFromEqualityCondition(rel.getCondition(), builder);
      return;
    }

    if (joinType == JoinRelType.LEFT || joinType == JoinRelType.RIGHT) {
      final JoinInfo joinInfo = rel.analyzeCondition();
      if (!joinInfo.isEqui() || joinInfo.leftKeys.isEmpty()
          || !fieldsSupportEqualityInference(rel.getLeft(), joinInfo.leftSet())
          || !fieldsSupportEqualityInference(rel.getRight(), joinInfo.rightSet())) {
        return;
      }

      final ImmutableBitSet leftKeys = joinInfo.leftSet();
      final ImmutableBitSet rightKeys = joinInfo.rightSet().shift(leftFieldCount);
      if (joinType == JoinRelType.LEFT) {
        builder.addArrow(leftKeys, rightKeys);
      } else {
        builder.addArrow(rightKeys, leftKeys);
      }
      return;
    }

    throw new AssertionError("unsupported join type: " + joinType);
  }

  /**
   * Adds bidirectional dependencies for input-reference equalities in a
   * condition. Callers are responsible for ensuring that every output row
   * satisfies the condition, as is true for Filters and inner joins.
   */
  private static void addBidirectionalFDsFromEqualityCondition(
      RexNode condition, ArrowSet.Builder builder) {
    for (RexNode conjunct : RelOptUtil.conjunctions(condition)) {
      if (!(conjunct instanceof RexCall)) {
        continue;
      }

      RexCall call = (RexCall) conjunct;
      if (call.getOperator().getKind() != SqlKind.EQUALS
          && call.getOperator().getKind() != SqlKind.IS_NOT_DISTINCT_FROM) {
        continue;
      }
      List<RexNode> operands = call.getOperands();
      if (operands.size() == 2) {
        RexNode left = operands.get(0);
        RexNode right = operands.get(1);

        if (left instanceof RexInputRef && right instanceof RexInputRef
            && typeSupportsGroupKeyInference(left.getType())
            && typeSupportsGroupKeyInference(right.getType())) {
          int leftRef = ((RexInputRef) left).getIndex();
          int rightRef = ((RexInputRef) right).getIndex();

          builder.addBidirectionalArrow(leftRef, rightRef);
        }
      }
    }
  }

  /**
   * Returns whether equality on the given fields can safely imply a functional
   * dependency for grouping purposes.
   */
  private static boolean fieldsSupportEqualityInference(RelNode input,
      ImmutableBitSet fields) {
    for (int field : fields) {
      if (!typeSupportsGroupKeyInference(
          input.getRowType().getFieldList().get(field).getType())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns whether a type can safely be used to infer that one grouping key
   * determines another. Approximate numerics and intervals are unsafe,
   * including when nested in rows, collections, or maps.
   */
  private static boolean typeSupportsGroupKeyInference(RelDataType type) {
    if (SqlTypeUtil.isApproximateNumeric(type) || SqlTypeUtil.isInterval(type)) {
      return false;
    }
    if (type.isStruct() && type.getFieldList().stream()
        .anyMatch(field -> !typeSupportsGroupKeyInference(field.getType()))) {
      return false;
    }
    final RelDataType componentType = type.getComponentType();
    if (componentType != null && !typeSupportsGroupKeyInference(componentType)) {
      return false;
    }
    final RelDataType keyType = type.getKeyType();
    if (keyType != null && !typeSupportsGroupKeyInference(keyType)) {
      return false;
    }
    final RelDataType valueType = type.getValueType();
    return valueType == null || typeSupportsGroupKeyInference(valueType);
  }
}
