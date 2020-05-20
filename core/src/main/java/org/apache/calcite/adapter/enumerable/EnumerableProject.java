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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


/** Implementation of {@link org.apache.calcite.rel.core.Project} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableProject extends Project implements EnumerableRel {
  /**
   * Creates an EnumerableProject.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   *
   * @param cluster  Cluster this relational expression belongs to
   * @param traitSet Traits of this relational expression
   * @param input    Input relational expression
   * @param projects List of expressions for the input columns
   * @param rowType  Output row type
   */
  public EnumerableProject(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      List<? extends RexNode> projects,
      RelDataType rowType) {
    super(cluster, traitSet, ImmutableList.of(), input, projects, rowType);
    assert getConvention() instanceof EnumerableConvention;
  }

  @Deprecated // to be removed before 2.0
  public EnumerableProject(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, List<? extends RexNode> projects, RelDataType rowType,
      int flags) {
    this(cluster, traitSet, input, projects, rowType);
    Util.discard(flags);
  }

  /** Creates an EnumerableProject, specifying row type rather than field
   * names. */
  public static EnumerableProject create(final RelNode input,
      final List<? extends RexNode> projects, RelDataType rowType) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet =
        cluster.traitSet().replace(EnumerableConvention.INSTANCE)
            .replaceIfs(RelCollationTraitDef.INSTANCE,
                () -> RelMdCollation.project(mq, input, projects));
    return new EnumerableProject(cluster, traitSet, input, projects, rowType);
  }

  public EnumerableProject copy(RelTraitSet traitSet, RelNode input,
      List<RexNode> projects, RelDataType rowType) {
    return new EnumerableProject(getCluster(), traitSet, input,
        projects, rowType);
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    // EnumerableCalcRel is always better
    throw new UnsupportedOperationException();
  }

  @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(
      RelTraitSet required) {
    RelCollation collation = required.getCollation();
    if (collation == null || required.isDefaultSansConvention()) {
      return null;
    }

    final Mappings.TargetMapping map =
        RelOptUtil.permutationIgnoreCast(
            getProjects(), getInput().getRowType());

    for (RelFieldCollation rc : collation.getFieldCollations()) {
      if (!isCollationOnTrivialExpr(map, rc, true)) {
        return null;
      }
    }

    final RelCollation newCollation = collation.apply(map);
    return Pair.of(traitSet.replace(collation),
        ImmutableList.of(traitSet.replace(newCollation)));
  }

  @Override public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
      final RelTraitSet childTraits, final int childId) {
    RelCollation collation = childTraits.getCollation();
    if (collation == null || childTraits.isDefaultSansConvention()) {
      return null;
    }

    int maxField = Math.max(getInput().getRowType().getFieldCount(), getProjects().size());
    Mappings.TargetMapping mapping = Mappings
        .create(MappingType.FUNCTION, maxField, maxField);
    for (Ord<RexNode> node : Ord.zip(getProjects())) {
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
      if (isCollationOnTrivialExpr(mapping, rc, false)) {
        collationFieldsToDerive.add(rc);
      } else {
        break;
      }
    }

    if (collationFieldsToDerive.size() > 0) {
      final RelCollation newCollation = RelCollations.of(collationFieldsToDerive).apply(mapping);
      return Pair.of(traitSet.replace(newCollation), ImmutableList.of(childTraits));
    } else {
      return null;
    }
  }

  // Determine mapping between project input and output fields. If sort
  // relies on non-trivial expressions, we can't push.
  private boolean isCollationOnTrivialExpr(
      Mappings.TargetMapping map, RelFieldCollation fc, boolean passdown) {
    if (map.getTargetOpt(fc.getFieldIndex()) < 0) {
      return false;
    }

    final RexNode node;
    if (passdown) {
      node = getProjects().get(fc.getFieldIndex());
    } else {
      node = getProjects().get(map.getTarget(fc.getFieldIndex()));
    }

    if (node.isA(SqlKind.CAST)) {
      // Check whether it is a monotonic preserving cast, otherwise we cannot push
      final RexCall cast = (RexCall) node;
      RelFieldCollation newFc = Objects.requireNonNull(RexUtil.apply(map, fc));
      final RexCallBinding binding =
          RexCallBinding.create(getCluster().getTypeFactory(), cast,
              ImmutableList.of(RelCollations.of(newFc)));
      if (cast.getOperator().getMonotonicity(binding) == SqlMonotonicity.NOT_MONOTONIC) {
        return false;
      }
    }

    return true;
  }
}
