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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

import org.apiguardian.api.API;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Utilities for traits propagation.
 */
@API(since = "1.24", status = API.Status.INTERNAL)
class EnumerableTraitsUtils {

  private EnumerableTraitsUtils() {}

  /**
   * Determine whether there is mapping between project input and output fields.
   * Bail out if sort relies on non-trivial expressions.
   */
  private static boolean isCollationOnTrivialExpr(
      List<RexNode> projects, RelDataTypeFactory typeFactory,
      Mappings.TargetMapping map, RelFieldCollation fc, boolean passDown) {
    final int index = fc.getFieldIndex();
    int target = map.getTargetOpt(index);
    if (target < 0) {
      return false;
    }

    final RexNode node = passDown ? projects.get(index) : projects.get(target);
    if (node.isA(SqlKind.CAST)) {
      // Check whether it is a monotonic preserving cast
      final RexCall cast = (RexCall) node;
      RelFieldCollation newFieldCollation =
          requireNonNull(RexUtil.apply(map, fc));
      final RexCallBinding binding =
          RexCallBinding.create(typeFactory, cast,
              ImmutableList.of(RelCollations.of(newFieldCollation)));
      return cast.getOperator().getMonotonicity(binding)
          != SqlMonotonicity.NOT_MONOTONIC;
    }

    return true;
  }

  static @Nullable Pair<RelTraitSet, List<RelTraitSet>> passThroughTraitsForProject(
      RelTraitSet required,
      List<RexNode> exps,
      RelDataType inputRowType,
      RelDataTypeFactory typeFactory,
      RelTraitSet currentTraits) {
    final RelCollation collation = required.getCollation();
    if (collation == null || collation == RelCollations.EMPTY) {
      return null;
    }

    final Mappings.TargetMapping map =
        RelOptUtil.permutationIgnoreCast(
            exps, inputRowType);

    if (collation.getFieldCollations().stream().anyMatch(
        rc -> !isCollationOnTrivialExpr(exps, typeFactory,
            map, rc, true))) {
      return null;
    }

    final RelCollation newCollation = collation.apply(map);
    return Pair.of(currentTraits.replace(collation),
        ImmutableList.of(currentTraits.replace(newCollation)));
  }

  static @Nullable Pair<RelTraitSet, List<RelTraitSet>> deriveTraitsForProject(
      RelTraitSet childTraits, int childId, List<RexNode> exps,
      RelDataType inputRowType, RelDataTypeFactory typeFactory, RelTraitSet currentTraits) {
    final RelCollation collation = childTraits.getCollation();
    if (collation == null || collation == RelCollations.EMPTY) {
      return null;
    }

    final int maxField = Math.max(exps.size(), inputRowType.getFieldCount());
    Mappings.TargetMapping mapping = Mappings
        .create(MappingType.FUNCTION, maxField, maxField);
    for (Ord<RexNode> node : Ord.zip(exps)) {
      if (node.e instanceof RexInputRef) {
        mapping.set(((RexInputRef) node.e).getIndex(), node.i);
      } else if (node.e.isA(SqlKind.CAST)) {
        final RexNode operand = ((RexCall) node.e).getOperands().get(0);
        if (operand instanceof RexInputRef) {
          mapping.set(((RexInputRef) operand).getIndex(), node.i);
        }
      }
    }

    List<RelFieldCollation> collationFieldsToDerive = new ArrayList<>();
    for (RelFieldCollation rc : collation.getFieldCollations()) {
      if (isCollationOnTrivialExpr(exps, typeFactory, mapping, rc, false)) {
        collationFieldsToDerive.add(rc);
      } else {
        break;
      }
    }

    if (!collationFieldsToDerive.isEmpty()) {
      final RelCollation newCollation = RelCollations
          .of(collationFieldsToDerive).apply(mapping);
      return Pair.of(currentTraits.replace(newCollation),
          ImmutableList.of(currentTraits.replace(collation)));
    } else {
      return null;
    }
  }

  /**
   * This function can be reused when a Join's traits pass-down shall only
   * pass through collation to left input.
   *
   * @param required required trait set for the join
   * @param joinType the join type
   * @param leftInputFieldCount number of field count of left join input
   * @param joinTraitSet trait set of the join
   */
  static @Nullable Pair<RelTraitSet, List<RelTraitSet>> passThroughTraitsForJoin(
      RelTraitSet required, JoinRelType joinType,
      int leftInputFieldCount, RelTraitSet joinTraitSet) {
    RelCollation collation = required.getCollation();
    if (collation == null
        || collation == RelCollations.EMPTY
        || joinType == JoinRelType.FULL
        || joinType == JoinRelType.RIGHT) {
      return null;
    }

    for (RelFieldCollation fc : collation.getFieldCollations()) {
      // If field collation belongs to right input: cannot push down collation.
      if (fc.getFieldIndex() >= leftInputFieldCount) {
        return null;
      }
    }

    RelTraitSet passthroughTraitSet = joinTraitSet.replace(collation);
    return Pair.of(passthroughTraitSet,
        ImmutableList.of(
            passthroughTraitSet,
            passthroughTraitSet.replace(RelCollations.EMPTY)));
  }

  /**
   * This function can be reused when a Join's traits derivation shall only
   * derive collation from left input.
   *
   * @param childTraits trait set of the child
   * @param childId id of the child (0 is left join input)
   * @param joinType the join type
   * @param joinTraitSet trait set of the join
   * @param rightTraitSet trait set of the right join input
   */
  static @Nullable Pair<RelTraitSet, List<RelTraitSet>> deriveTraitsForJoin(
      RelTraitSet childTraits, int childId, JoinRelType joinType,
      RelTraitSet joinTraitSet, RelTraitSet rightTraitSet) {
    // should only derive traits (limited to collation for now) from left join input.
    assert childId == 0;

    RelCollation collation = childTraits.getCollation();
    if (collation == null
        || collation == RelCollations.EMPTY
        || joinType == JoinRelType.FULL
        || joinType == JoinRelType.RIGHT) {
      return null;
    }

    RelTraitSet derivedTraits = joinTraitSet.replace(collation);
    return Pair.of(
        derivedTraits,
        ImmutableList.of(derivedTraits, rightTraitSet));
  }
}
