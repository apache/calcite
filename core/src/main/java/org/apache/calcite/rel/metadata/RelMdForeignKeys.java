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

import org.apache.calcite.plan.RelOptForeignKey;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.InferredRexTableInputRef;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.IntPair;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * RelMdForeignKeys supplies a default implementation of
 * {@link RelMetadataQuery#getForeignKeys} for the standard logical algebra.
 * The relNodes supported are same to {@link RelMetadataQuery#getUniqueKeys(RelNode)}
 */
public class RelMdForeignKeys
    implements MetadataHandler<BuiltInMetadata.ForeignKeys> {
  protected static final Set<RelOptForeignKey> EMPTY_BIT_SET = new HashSet<>();
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          new RelMdForeignKeys(), BuiltInMetadata.ForeignKeys.Handler.class);

//~ Constructors -----------------------------------------------------------

  private RelMdForeignKeys() {}

//~ Methods ----------------------------------------------------------------

  @Override public MetadataDef<BuiltInMetadata.ForeignKeys> getDef() {
    return BuiltInMetadata.ForeignKeys.DEF;
  }

  public Set<RelOptForeignKey> getForeignKeys(Filter rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
    return mq.getForeignKeys(rel.getInput(), ignoreNulls);
  }

  public Set<RelOptForeignKey> getForeignKeys(Sort rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
    return mq.getForeignKeys(rel.getInput(), ignoreNulls);
  }

  public Set<RelOptForeignKey> getForeignKeys(Correlate rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
    return mq.getForeignKeys(rel.getLeft(), ignoreNulls);
  }

  public Set<RelOptForeignKey> getForeignKeys(TableModify rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
    return mq.getForeignKeys(rel.getInput(), ignoreNulls);
  }

  public Set<RelOptForeignKey> getForeignKeys(Join rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
    final RelNode left = rel.getLeft();
    final RelNode right = rel.getRight();
    if (!rel.getJoinType().projectsRight()) {
      // only return the foreign keys from the LHS since a semi or anti join only
      // returns the LHS
      return mq.getForeignKeys(left, ignoreNulls);
    }
    int nLeftColumns = rel.getLeft().getRowType().getFieldList().size();
    final Set<RelOptForeignKey> foreignKeys = new HashSet<>();
    final Set<RelOptForeignKey> leftInputForeignKeys =
        mq.getForeignKeys(left, ignoreNulls);
    final Set<RelOptForeignKey> rightInputForeignKeys =
        mq.getForeignKeys(right, ignoreNulls);

    if (leftInputForeignKeys.isEmpty() && rightInputForeignKeys.isEmpty()) {
      return EMPTY_BIT_SET;
    }
    // shift right index
    Set<RelOptForeignKey> shiftedRightInputForeignKeys = rightInputForeignKeys.stream()
        .map(
            foreignKey -> foreignKey.shift(nLeftColumns,
            RelOptForeignKey.ShiftSide.INFERRED_FOREIGN_SOURCE,
            RelOptForeignKey.ShiftSide.INFERRED_UNIQUE_TARGET))
        .collect(Collectors.toSet());
    if (!rel.getJoinType().generatesNullsOnLeft() || ignoreNulls) {
      foreignKeys.addAll(
          tryMerge(leftInputForeignKeys, shiftedRightInputForeignKeys));
    }
    if (!rel.getJoinType().generatesNullsOnRight() || ignoreNulls) {
      foreignKeys.addAll(
          tryMerge(shiftedRightInputForeignKeys, leftInputForeignKeys));
    }
    return foreignKeys;
  }

  private Set<RelOptForeignKey> tryMerge(Set<RelOptForeignKey> foreignKeys,
      Set<RelOptForeignKey> uniqueKeys) {
    ImmutableMap.Builder<Set<InferredRexTableInputRef>, RelOptForeignKey>
        inferredUniqueKeyMapBuilder = ImmutableMap.builder();
    // The upper-level relational algebra may use constraints,
    // so keep the unmerged foreign keys
    final Set<RelOptForeignKey> mixedForeignKeys = new HashSet<>(foreignKeys);
    mixedForeignKeys.addAll(uniqueKeys);
    if (foreignKeys.isEmpty() || uniqueKeys.isEmpty()) {
      return mixedForeignKeys;
    }
    // Build unique keys map, key -> unique table set, value -> unique relOptForeignKey
    for (RelOptForeignKey uniqueKey : uniqueKeys) {
      if (!uniqueKey.getConstraints().isEmpty()
          && uniqueKey.isInferredUniqueKey()) {
        inferredUniqueKeyMapBuilder.put(
            Sets.newHashSet(RelOptForeignKey.constraintsRight(uniqueKey.getConstraints())),
            uniqueKey);
      }
    }
    ImmutableMap<Set<InferredRexTableInputRef>, RelOptForeignKey> inferredUniqueKeyMap =
        inferredUniqueKeyMapBuilder.build();
    for (RelOptForeignKey foreignKey : foreignKeys) {
      if (foreignKey.getConstraints().isEmpty()
          || !foreignKey.isInferredForeignKey()) {
        continue;
      }
      // try merge
      Set<InferredRexTableInputRef> foreignSideNeededUniqueSet =
          Sets.newHashSet(RelOptForeignKey.constraintsRight(foreignKey.getConstraints()));
      RelOptForeignKey inferredUniqueKey =
          inferredUniqueKeyMap.get(foreignSideNeededUniqueSet);
      if (inferredUniqueKey != null) {
        mixedForeignKeys.add(
            RelOptForeignKey.of(
                foreignKey.getConstraints().stream()
                    .map(
                        constraint -> Pair.of(
                        constraint.left.copy(true),
                        constraint.right.copy(true)))
                    .collect(Collectors.toList()),
                ImmutableBitSet.of(foreignKey.getForeignColumns()),
                ImmutableBitSet.of(inferredUniqueKey.getUniqueColumns())));
      }
    }
    return mixedForeignKeys;
  }

  public Set<RelOptForeignKey> getForeignKeys(Aggregate rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
    final ImmutableBitSet groupSet = rel.getGroupSet();
    if (groupSet.isEmpty()) {
      return EMPTY_BIT_SET;
    }
    final Set<RelOptForeignKey> inputForeignKeys =
        mq.getForeignKeys(rel.getInput(), ignoreNulls);
    return inputForeignKeys.stream()
        .filter(foreignKey -> filterValidateColumns(groupSet, foreignKey))
        .collect(Collectors.toSet());
  }

  public Set<RelOptForeignKey> getForeignKeys(Project rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
    return getProjectForeignKeys(rel, mq, ignoreNulls, rel.getProjects());
  }

  public Set<RelOptForeignKey> getForeignKeys(Calc rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
    RexProgram program = rel.getProgram();
    return getProjectForeignKeys(rel, mq, ignoreNulls,
        Util.transform(program.getProjectList(), program::expandLocalRef));
  }

  private static Set<RelOptForeignKey> getProjectForeignKeys(SingleRel rel,
      RelMetadataQuery mq,
      boolean ignoreNulls,
      List<RexNode> projExprs) {

    // Single input can be mapped to multiple outputs
    final ImmutableListMultimap.Builder<Integer, Integer> inToOutIndexBuilder =
        ImmutableListMultimap.builder();
    final ImmutableBitSet.Builder inColumnsBuilder = ImmutableBitSet.builder();
    for (int i = 0; i < projExprs.size(); i++) {
      RexNode projExpr = projExprs.get(i);
      if (projExpr instanceof RexInputRef) {
        int inputIndex = ((RexInputRef) projExpr).getIndex();
        inToOutIndexBuilder.put(inputIndex, i);
        inColumnsBuilder.set(inputIndex);
      }
    }
    final ImmutableBitSet inColumnsUsed = inColumnsBuilder.build();
    if (inColumnsUsed.isEmpty()) {
      return EMPTY_BIT_SET;
    }
    final Map<Integer, List<Integer>> mapInToOutPos =
        Maps.transformValues(inToOutIndexBuilder.build().asMap(), Lists::newArrayList);
    final Set<RelOptForeignKey> inputForeignKeys =
        mq.getForeignKeys(rel.getInput(), ignoreNulls);
    if (inputForeignKeys.isEmpty()) {
      return EMPTY_BIT_SET;
    }
    return inputForeignKeys.stream()
        .filter(foreignKey -> filterValidateColumns(inColumnsUsed, foreignKey))
        .flatMap(foreignKey -> foreignKey.permute(mapInToOutPos, mapInToOutPos).stream())
        .collect(Collectors.toSet());
  }

  private static boolean filterValidateColumns(ImmutableBitSet columnsUsed,
      RelOptForeignKey foreignKey) {
    return (!foreignKey.getForeignColumns().isEmpty()
        && columnsUsed.contains(foreignKey.getForeignColumns()))
        || (!foreignKey.getUniqueColumns().isEmpty()
        && columnsUsed.contains(foreignKey.getUniqueColumns()));
  }

  public Set<RelOptForeignKey> getForeignKeys(TableScan rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
    final RelOptTable table = rel.getTable();
    final BuiltInMetadata.ForeignKeys.Handler handler =
        table.unwrap(BuiltInMetadata.ForeignKeys.Handler.class);
    if (handler != null) {
      return handler.getForeignKeys(rel, mq, ignoreNulls);
    }

    final List<RelReferentialConstraint> referentialConstraints =
        table.getReferentialConstraints();
    final List<ImmutableBitSet> keys = table.getKeys();

    final Set<RelOptForeignKey> foreignKeys = new HashSet<>();
    if (referentialConstraints != null) {
      foreignKeys.addAll(referentialConstraints.stream()
          .map(constraint -> {
            List<Pair<InferredRexTableInputRef, InferredRexTableInputRef>> constraints =
                constraint.getColumnPairs().stream()
                    .map(
                        intPair -> Pair.of(
                        InferredRexTableInputRef.of(constraint.getSourceQualifiedName(),
                            intPair.source,
                            false),
                        InferredRexTableInputRef.of(constraint.getTargetQualifiedName(),
                            intPair.target,
                            false)))
                    .collect(Collectors.toList());
            return RelOptForeignKey.of(constraints,
                ImmutableBitSet.of(IntPair.left(constraint.getColumnPairs())),
                ImmutableBitSet.of());
          })
          .collect(Collectors.toSet()));
    }
    if (keys != null) {
      foreignKeys.addAll(
          keys.stream()
              .map(keyBitSet -> {
                List<Pair<InferredRexTableInputRef, InferredRexTableInputRef>> constraints =
                    keyBitSet.asList().stream()
                        .map(
                            index -> Pair.of(
                            InferredRexTableInputRef.of(),
                            InferredRexTableInputRef.of(
                                table.getQualifiedName(),
                                index,
                                false)))
                        .collect(Collectors.toList());
                return RelOptForeignKey.of(constraints, ImmutableBitSet.of(), keyBitSet);
              })
              .collect(Collectors.toList()));
    }
    if (!ignoreNulls) {
      final List<RelDataTypeField> fieldList = rel.getRowType().getFieldList();
      return foreignKeys.stream()
          .filter(foreignKey -> foreignKey.getForeignColumns().asSet().stream()
              .noneMatch(index -> fieldList.get(index).getType().isNullable())
              && foreignKey.getUniqueColumns().asSet().stream()
              .noneMatch(index -> fieldList.get(index).getType().isNullable()))
          .collect(Collectors.toSet());
    }
    return foreignKeys;
  }

  /**
   * The foreign keys of SetOp are precisely the intersection of its every
   * input foreign keys.
   */
  public Set<RelOptForeignKey> getForeignKeys(SetOp rel, RelMetadataQuery mq,
      boolean ignoreNulls) {

    Set<RelOptForeignKey> foreignKeys = new HashSet<>();
    for (RelNode input : rel.getInputs()) {
      Set<RelOptForeignKey> inputForeignKeys = mq.getForeignKeys(input, ignoreNulls);
      if (inputForeignKeys.isEmpty()) {
        return EMPTY_BIT_SET;
      }
      foreignKeys = foreignKeys.isEmpty()
          ? inputForeignKeys : Sets.intersection(foreignKeys, inputForeignKeys);
    }
    return foreignKeys;
  }

  /** Catch-all rule when none of the others apply. */
  public Set<RelOptForeignKey> getForeignKeys(RelNode rel, RelMetadataQuery mq,
      boolean ignoreNulls) {
    // no information available
    return EMPTY_BIT_SET;
  }
}
