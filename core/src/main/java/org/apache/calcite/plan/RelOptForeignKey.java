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
package org.apache.calcite.plan;

import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.InferredConstraintKey;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ForeignKey represents the foreign and unique key constraint relationship
 * on the current {@link org.apache.calcite.rel.RelNode}.
 *
 * <p><b>constraints</b> field {@link #constraints} are
 * constraints that foreign key and unique key relationships in bottom-up derivation.
 *
 * <p><b>foreignColumns</b> field {@link #foreignColumns} indicates the
 * position of the foreign key on the current {@link org.apache.calcite.rel.RelNode}
 * if not or not be confirmed, it is an empty set.
 *
 * <p><b>uniqueColumns</b> field {@link #uniqueColumns} indicates the position of
 * the unique key on the current {@link org.apache.calcite.rel.RelNode},
 * if not or not be confirmed, it is an empty set, the position in uniqueColumns
 * corresponds to foreignColumns.
 *
 * <p>The element positions in {@code uniqueColumns} and {@code foreignColumns}
 * correspond to each other. The order of elements in {@code uniqueColumns}
 * and {@code foreignColumns} is consistent with the order of constraints in
 * the constraints list.
 *
 * <p>For instance,
 * <blockquote>
 * <pre>select e.deptno, e.ename, d.deptno
 * from emp as e
 * inner join dept as d
 * on e.deptno = d.deptno</pre></blockquote>
 *
 * <p>the foreign key is the DEPTNO column of CATALOG.SALES.EMP table,
 * reference the DEPTNO unique column of CATALOG.SALES.DEPT table.
 *
 * <p>Invoke the {@link RelMetadataQuery#getConfirmedForeignKeys} method which
 * input param is the top {@link org.apache.calcite.rel.core.Project},
 * the following results can be obtained.
 *
 * <p>{@code constraints} is
 * [{left: [CATALOG, SALES, EMP].$7.#true, right: [CATALOG, SALES, DEPT].$0.#true}]
 * {@code foreignColumns} is {0}
 * {@code uniqueColumns} is {2}
 *
 * <p>For instance,
 * <blockquote>
 * <pre>select name, deptno
 * from dept</pre></blockquote>
 *
 * <p>{@code constraints} is
 * [{left: null.null.#false, right: [CATALOG, SALES, DEPT].$0.#false}]
 * {@code foreignColumns} is {}
 * {@code uniqueColumns} is {1}
 *
 * <p>For instance,
 * <blockquote>
 * <pre>select ename, sal, deptno
 * from emp</pre></blockquote>
 *
 * <p>{@code constraints} is
 * [{left: [CATALOG, SALES, EMP].$7.#false, right: [CATALOG, SALES, DEPT].$0.#false}]
 * {@code foreignColumns} is {2}
 * {@code uniqueColumns} is {}
 *
 * @see InferredConstraintKey
 * @see org.apache.calcite.plan.RelOptForeignKey
 */
public class RelOptForeignKey {

  /** Foreign key and unique key relationships in bottom-up derivation. */
  private final List<Pair<InferredConstraintKey, InferredConstraintKey>> constraints;
  /** Position of the foreign key on the current {@link org.apache.calcite.rel.RelNode}. */
  private final ImmutableBitSet foreignColumns;
  /** Position of the unique key on the current {@link org.apache.calcite.rel.RelNode}. */
  private final ImmutableBitSet uniqueColumns;

  private RelOptForeignKey(
      List<Pair<InferredConstraintKey, InferredConstraintKey>> constraints,
      ImmutableBitSet foreignColumns,
      ImmutableBitSet uniqueColumns) {
    this.constraints = constraints;
    this.foreignColumns = foreignColumns;
    this.uniqueColumns = uniqueColumns;
  }

  public static RelOptForeignKey of(List<Pair<InferredConstraintKey,
      InferredConstraintKey>> constraints,
      ImmutableBitSet foreignColumns,
      ImmutableBitSet uniqueColumns) {
    return new RelOptForeignKey(constraints, foreignColumns, uniqueColumns);
  }

  public ImmutableBitSet getForeignColumns() {
    return foreignColumns;
  }

  public ImmutableBitSet getUniqueColumns() {
    return uniqueColumns;
  }

  public List<Pair<InferredConstraintKey, InferredConstraintKey>> getConstraints() {
    return constraints;
  }

  /** Returns the left side of a list of constraints. */
  public static List<InferredConstraintKey> constraintsLeft(
      List<Pair<InferredConstraintKey, InferredConstraintKey>> constraints) {
    if (constraints.isEmpty()) {
      return new ArrayList<>();
    }
    return constraints.stream()
        .map(Pair::getKey)
        .collect(Collectors.toList());
  }

  /** Returns the right side of a list of constraints. */
  public static List<InferredConstraintKey> constraintsRight(
      List<Pair<InferredConstraintKey, InferredConstraintKey>> constraints) {
    if (constraints.isEmpty()) {
      return new ArrayList<>();
    }
    return constraints.stream()
        .map(Pair::getValue)
        .collect(Collectors.toList());
  }

  /** Permutes relOptForeignKey set according to given mappings.
   *
   * <p>Example as follows:
   * Simplified representation as foreignColumns and uniqueColumns
   *
   * <p>current relOptForeignKey:
   * foreignColumns: {1, 3}
   * uniqueColumns: {4}
   *
   * <p>permute params:
   * foreignMapping: {1: [2, 6], 3: [7]}
   * uniqueMapping: {4: [5, 8]}
   *
   * <p>result:
   * [
   *   {
   *     foreignColumns: {2, 7}
   *     uniqueColumns: {5}
   *   },
   *   {
   *     foreignColumns: {2, 7}
   *     uniqueColumns: {8}
   *   },
   *   {
   *     foreignColumns: {6, 7}
   *     uniqueColumns: {5}
   *   },
   *   {
   *     foreignColumns: {6, 7}
   *     uniqueColumns: {8}
   *   }
   * ]
   *
   * @param foreignMapping foreignKey mapping relationship
   * @param uniqueMapping uniqueKey mapping relationship
   * @return mapped relOptForeignKey set
   */
  public Set<RelOptForeignKey> permute(Map<Integer, List<Integer>> foreignMapping,
      Map<Integer, List<Integer>> uniqueMapping) {
    if (foreignMapping.isEmpty() && uniqueMapping.isEmpty()) {
      return Sets.newHashSet(this);
    }
    final List<ImmutableBitSet> mappedForeignColumns = new ArrayList<>();
    if (!foreignMapping.isEmpty()
        && foreignMapping.keySet().containsAll(this.foreignColumns.asSet())) {
      flatMappings(this.foreignColumns.toList(), foreignMapping)
          .forEach(each -> mappedForeignColumns.add(this.foreignColumns.permute(each)));
    }
    if (mappedForeignColumns.isEmpty()) {
      mappedForeignColumns.add(this.foreignColumns);
    }
    final List<ImmutableBitSet> mappedUniqueColumns = new ArrayList<>();
    if (!uniqueMapping.isEmpty()
        && uniqueMapping.keySet().containsAll(this.uniqueColumns.asSet())) {
      flatMappings(this.uniqueColumns.toList(), uniqueMapping)
          .forEach(each -> mappedUniqueColumns.add(this.uniqueColumns.permute(each)));
    }
    if (mappedUniqueColumns.isEmpty()) {
      mappedUniqueColumns.add(this.uniqueColumns);
    }
    return Lists.newArrayList(
            Linq4j.product(
                Lists.newArrayList(mappedForeignColumns, mappedUniqueColumns))).stream()
        .map(pair -> copy(pair.get(0), pair.get(1)))
        .collect(Collectors.toSet());
  }

  /**
   * Flatten the mapping based on the sources, which mapping can be one-to-many.
   *
   * <p>Example as follows:
   * sources: [1, 2]
   * mapping: {1: [3, 4], 2: [5, 6]}
   * result: [{1:3, 2:5}, {1:3, 2:6}, {1:4, 2:5}, {1:4, 2:6}]
   *
   * @param sources the sources which will be mapped
   * @param mapping the field mapping relationship which can potentially be one-to-many
   * @return mapped sources
   */
  private static Set<Map<Integer, Integer>> flatMappings(List<Integer> sources,
      Map<Integer, List<Integer>> mapping) {
    List<List<Integer>> sourceMappings = new ArrayList<>();
    for (int source : sources) {
      List<Integer> sourceMapping = mapping.get(source);
      if (sourceMapping == null || sourceMapping.isEmpty()) {
        return new HashSet<>();
      }
      sourceMappings.add(sourceMapping);
    }
    Set<Map<Integer, Integer>> sourceTargetMappings = new HashSet<>();
    Iterable<List<Integer>> targetMappingProducts = Linq4j.product(sourceMappings);
    for (List<Integer> target : targetMappingProducts) {
      // build map, key -> sources, value -> mapped targets
      Iterator<Integer> sourceIterator = sources.iterator();
      Iterator<Integer> targetIterator = target.iterator();
      Map<Integer, Integer> sourceTargetMapping = new HashMap<>();
      while (sourceIterator.hasNext() && targetIterator.hasNext()) {
        sourceTargetMapping.put(sourceIterator.next(), targetIterator.next());
      }
      sourceTargetMappings.add(sourceTargetMapping);
    }
    return sourceTargetMappings;
  }

  /** Returns relOptForeignKey with every bit moved up {@code offset} positions.
   * Offset may be negative, but throws if any bit ends up negative.
   * Can control the shift side.
   *
   * <p>For instance,
   * {@code constraints} is
   * [{left: null.null.#false, right: [CATALOG, SALES, DEPT].$0.#false}]
   * {@code foreignColumns} is {}
   * {@code uniqueColumns} is {1}
   *
   * <p>It's inferred unique key, it will return as following when request params are
   * offset = 2, shiftSides = [INFERRED_UNIQUE_TARGET]
   * results:
   * [{left: null.null.#false, right: [CATALOG, SALES, DEPT].$0.#false}]
   * {@code foreignColumns} is {}
   * {@code uniqueColumns} is {3}
   *
   * <p>It will return as following when request params are
   * offset = 2,
   * shiftSides = [INFERRED_UNIQUE_SOURCE, INFERRED_FOREIGN_SOURCE, INFERRED_FOREIGN_TARGET]
   * results:
   * [{left: null.null.#false, right: [CATALOG, SALES, DEPT].$0.#false}]
   * {@code foreignColumns} is {}
   * {@code uniqueColumns} is {1}
   *
   * @see org.apache.calcite.plan.RelOptForeignKey.ShiftSide */
  public RelOptForeignKey shift(int offset, ShiftSide... shiftSides) {
    if (offset == 0) {
      return this;
    }
    ImmutableBitSet shiftedForeignColumns = ImmutableBitSet.of(this.foreignColumns);
    ImmutableBitSet shiftedUniqueColumns = ImmutableBitSet.of(this.uniqueColumns);
    for (ShiftSide shiftSide : shiftSides) {
      if (ShiftSide.INFERRED_FOREIGN_SOURCE == shiftSide
          && isInferredForeignKey()) {
        shiftedForeignColumns = shiftedForeignColumns.shift(offset);
      }
      if (ShiftSide.INFERRED_FOREIGN_TARGET == shiftSide
          && isInferredForeignKey()) {
        shiftedUniqueColumns = shiftedUniqueColumns.shift(offset);
      }
      if (ShiftSide.INFERRED_UNIQUE_SOURCE == shiftSide
          && isInferredUniqueKey()) {
        shiftedForeignColumns = shiftedForeignColumns.shift(offset);
      }
      if (ShiftSide.INFERRED_UNIQUE_TARGET == shiftSide
          && isInferredUniqueKey()) {
        shiftedUniqueColumns = shiftedUniqueColumns.shift(offset);
      }
    }
    return copy(shiftedForeignColumns, shiftedUniqueColumns);
  }

  /**
   * The current constraint relationships consist only of foreign keys.
   * These constraints need to be propagated to the top of the
   * {@link org.apache.calcite.rel.RelNode} in order to be merged and confirmed
   * in higher-level {@link org.apache.calcite.rel.RelNode} in the future.
   */
  public boolean isInferredForeignKey() {
    return this.constraints.stream()
        .allMatch(constraint -> !constraint.left.isNull()
            && !constraint.right.isNull()
            && !constraint.left.isConfirmed()
            && !constraint.right.isConfirmed());
  }

  /**
   * The current constraint relationships consist only of unique keys.
   * These constraints need to be propagated to the top of the
   * {@link org.apache.calcite.rel.RelNode} in order to be merged and confirmed
   * in higher-level {@link org.apache.calcite.rel.RelNode} in the future.
   */
  public boolean isInferredUniqueKey() {
    return this.constraints.stream()
        .allMatch(constraint -> constraint.left.isNull()
            && !constraint.right.isNull()
            && !constraint.left.isConfirmed()
            && !constraint.right.isConfirmed());
  }

  /** The inferred foreign key and unique key relationships have been determined,
   * which typically occurs after a join operation. */
  public boolean isConfirmed() {
    return this.constraints.stream()
        .allMatch(constraint -> !constraint.left.isNull()
            && !constraint.right.isNull()
            && constraint.left.isConfirmed()
            && constraint.right.isConfirmed());
  }

  /** Deep copy based on foreignColumns and uniqueColumns. */
  public RelOptForeignKey copy(ImmutableBitSet foreignColumns,
      ImmutableBitSet uniqueColumns) {
    List<Pair<InferredConstraintKey, InferredConstraintKey>> copiedConstraints =
        this.constraints.stream()
            .map(constraint -> Pair.of(constraint.left.copy(), constraint.right.copy()))
            .collect(Collectors.toList());
    return RelOptForeignKey.of(copiedConstraints, foreignColumns, uniqueColumns);
  }

  @Override public boolean equals(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    RelOptForeignKey that = (RelOptForeignKey) obj;
    return constraints.equals(that.constraints)
        && uniqueColumns.equals(that.uniqueColumns)
        && foreignColumns.equals(that.foreignColumns);
  }

  @Override public int hashCode() {
    return Objects.hash(constraints, uniqueColumns, foreignColumns);
  }

  /** Represents the target bit set position to be shifted. */
  public enum ShiftSide {
    /**
     * Shift constraint pair left when relOptForeignKey is inferred foreignKey.
     *
     * @see RelOptForeignKey#isInferredForeignKey()
     */
    INFERRED_FOREIGN_SOURCE,
    /**
     * Shift constraint pair right when relOptForeignKey is inferred foreignKey.
     *
     * @see RelOptForeignKey#isInferredForeignKey()
     */
    INFERRED_FOREIGN_TARGET,
    /**
     * Shift constraint pair left when relOptForeignKey is inferred uniqueKey.
     *
     * @see RelOptForeignKey#isInferredUniqueKey()
     */
    INFERRED_UNIQUE_SOURCE,
    /**
     * Shift constraint pair left when relOptForeignKey is inferred uniqueKey.
     *
     * @see RelOptForeignKey#isInferredUniqueKey()
     */
    INFERRED_UNIQUE_TARGET
  }
}
