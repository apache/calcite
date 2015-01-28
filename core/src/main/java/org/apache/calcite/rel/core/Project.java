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
package org.apache.calcite.rel.core;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Functions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexChecker;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Permutation;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Relational expression that computes a set of
 * 'select expressions' from its input relational expression.
 *
 * <p>The result is usually 'boxed' as a record with one named field for each
 * column; if there is precisely one expression, the result may be 'unboxed',
 * and consist of the raw value type.
 *
 * @see org.apache.calcite.rel.logical.LogicalProject
 */
public abstract class Project extends SingleRel {
  //~ Instance fields --------------------------------------------------------

  protected final ImmutableList<RexNode> exps;

  protected final ImmutableList<RelCollation> collationList;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a Project.
   *
   * @param cluster Cluster that this relational expression belongs to
   * @param traits  traits of this rel
   * @param input   input relational expression
   * @param exps    List of expressions for the input columns
   * @param rowType output row type
   */
  protected Project(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      List<? extends RexNode> exps,
      RelDataType rowType) {
    super(cluster, traits, input);
    assert rowType != null;
    this.exps = ImmutableList.copyOf(exps);
    this.rowType = rowType;
    final RelCollation collation =
        traits.getTrait(RelCollationTraitDef.INSTANCE);
    this.collationList =
        collation == null
            ? ImmutableList.<RelCollation>of()
            : ImmutableList.of(collation);
    assert isValid(true);
  }

  /**
   * Creates a Project by parsing serialized output.
   */
  protected Project(RelInput input) {
    this(input.getCluster(), input.getTraitSet(), input.getInput(),
        input.getExpressionList("exprs"),
        input.getRowType("exprs", "fields"));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public final RelNode copy(RelTraitSet traitSet,
      List<RelNode> inputs) {
    return copy(traitSet, sole(inputs), exps, rowType);
  }

  /**
   * Copies a project.
   *
   * @param traitSet Traits
   * @param input Input
   * @param exps Project expressions
   * @param rowType Output row type
   * @return New {@code Project} if any parameter differs from the value of this
   *   {@code Project}, or just {@code this} if all the parameters are
   *   the same
   *
   * @see #copy(RelTraitSet, List)
   */
  public abstract Project copy(RelTraitSet traitSet, RelNode input,
      List<RexNode> exps, RelDataType rowType);

  public List<RelCollation> getCollationList() {
    return collationList;
  }

  @Override public List<RexNode> getChildExps() {
    return exps;
  }

  public RelNode accept(RexShuttle shuttle) {
    List<RexNode> exps = shuttle.apply(this.exps);
    if (this.exps == exps) {
      return this;
    }
    return copy(traitSet, getInput(), exps, rowType);
  }

  /**
   * Returns the project expressions.
   *
   * @return Project expressions
   */
  public List<RexNode> getProjects() {
    return exps;
  }

  /**
   * Returns a list of (expression, name) pairs. Convenient for various
   * transformations.
   *
   * @return List of (expression, name) pairs
   */
  public final List<Pair<RexNode, String>> getNamedProjects() {
    return Pair.zip(getProjects(), getRowType().getFieldNames());
  }

  public boolean isValid(boolean fail) {
    if (!super.isValid(fail)) {
      assert !fail;
      return false;
    }
    if (!RexUtil.compatibleTypes(
        exps,
        getRowType(),
        true)) {
      assert !fail;
      return false;
    }
    RexChecker checker =
        new RexChecker(
            getInput().getRowType(), fail);
    for (RexNode exp : exps) {
      exp.accept(checker);
    }
    if (checker.getFailureCount() > 0) {
      assert !fail;
      return false;
    }
    if (collationList == null) {
      assert !fail;
      return false;
    }
    if (!collationList.isEmpty()
        && collationList.get(0)
        != traitSet.getTrait(RelCollationTraitDef.INSTANCE)) {
      assert !fail;
      return false;
    }
    if (!Util.isDistinct(rowType.getFieldNames())) {
      assert !fail : rowType;
      return false;
    }
    //CHECKSTYLE: IGNORE 1
    if (false && !Util.isDistinct(
        Functions.adapt(
            exps,
            new Function1<RexNode, Object>() {
              public Object apply(RexNode a0) {
                return a0.toString();
              }
            }))) {
      // Projecting the same expression twice is usually a bad idea,
      // because it may create expressions downstream which are equivalent
      // but which look different. We can't ban duplicate projects,
      // because we need to allow
      //
      //  SELECT a, b FROM c UNION SELECT x, x FROM z
      assert !fail : exps;
      return false;
    }
    return true;
  }

  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    double dRows = RelMetadataQuery.getRowCount(getInput());
    double dCpu = dRows * exps.size();
    double dIo = 0;
    return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
  }

  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    if (pw.nest()) {
      pw.item("fields", rowType.getFieldNames());
      pw.item("exprs", exps);
    } else {
      for (Ord<RelDataTypeField> field : Ord.zip(rowType.getFieldList())) {
        String fieldName = field.e.getName();
        if (fieldName == null) {
          fieldName = "field#" + field.i;
        }
        pw.item(fieldName, exps.get(field.i));
      }
    }

    // If we're generating a digest, include the rowtype. If two projects
    // differ in return type, we don't want to regard them as equivalent,
    // otherwise we will try to put rels of different types into the same
    // planner equivalence set.
    //CHECKSTYLE: IGNORE 2
    if ((pw.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES)
        && false) {
      pw.item("type", rowType);
    }

    return pw;
  }

  /**
   * Returns a mapping, or null if this projection is not a mapping.
   *
   * @return Mapping, or null if this projection is not a mapping
   */
  public Mappings.TargetMapping getMapping() {
    return getMapping(getInput().getRowType().getFieldCount(), exps);
  }

  /**
   * Returns a mapping of a set of project expressions.
   *
   * <p>The mapping is an inverse surjection.
   * Every target has a source field, but
   * a source field may appear as zero, one, or more target fields.
   * Thus you can safely call
   * {@link Mappings.TargetMapping#getTarget(int)}.
   *
   * @param inputFieldCount Number of input fields
   * @param projects Project expressions
   * @return Mapping of a set of project expressions
   */
  public static Mappings.TargetMapping getMapping(int inputFieldCount,
      List<RexNode> projects) {
    Mappings.TargetMapping mapping =
        Mappings.create(MappingType.INVERSE_SURJECTION, inputFieldCount,
            projects.size());
    for (Ord<RexNode> exp : Ord.zip(projects)) {
      if (!(exp.e instanceof RexInputRef)) {
        return null;
      }
      mapping.set(((RexInputRef) exp.e).getIndex(), exp.i);
    }
    return mapping;
  }

  /**
   * Returns a permutation, if this projection is merely a permutation of its
   * input fields, otherwise null.
   *
   * @return Permutation, if this projection is merely a permutation of its
   *   input fields, otherwise null
   */
  public Permutation getPermutation() {
    final int fieldCount = rowType.getFieldList().size();
    if (fieldCount != getInput().getRowType().getFieldList().size()) {
      return null;
    }
    Permutation permutation = new Permutation(fieldCount);
    for (int i = 0; i < fieldCount; ++i) {
      final RexNode exp = exps.get(i);
      if (exp instanceof RexInputRef) {
        permutation.set(i, ((RexInputRef) exp).getIndex());
      } else {
        return null;
      }
    }
    return permutation;
  }

  /**
   * Checks whether this is a functional mapping.
   * Every output is a source field, but
   * a source field may appear as zero, one, or more output fields.
   */
  public boolean isMapping() {
    for (RexNode exp : exps) {
      if (!(exp instanceof RexInputRef)) {
        return false;
      }
    }
    return true;
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Visitor which walks over a program and checks validity.
   */
  private static class Checker extends RexVisitorImpl<Boolean> {
    private final boolean fail;
    private final RelDataType inputRowType;
    int failCount = 0;

    /**
     * Creates a Checker.
     *
     * @param inputRowType Input row type to expressions
     * @param fail         Whether to throw if checker finds an error
     */
    private Checker(RelDataType inputRowType, boolean fail) {
      super(true);
      this.fail = fail;
      this.inputRowType = inputRowType;
    }

    public Boolean visitInputRef(RexInputRef inputRef) {
      final int index = inputRef.getIndex();
      final List<RelDataTypeField> fields = inputRowType.getFieldList();
      if ((index < 0) || (index >= fields.size())) {
        assert !fail;
        ++failCount;
        return false;
      }
      if (!RelOptUtil.eq("inputRef",
          inputRef.getType(),
          "underlying field",
          fields.get(index).getType(),
          fail)) {
        assert !fail;
        ++failCount;
        return false;
      }
      return true;
    }

    public Boolean visitLocalRef(RexLocalRef localRef) {
      assert !fail : "localRef invalid in project";
      ++failCount;
      return false;
    }

    public Boolean visitFieldAccess(RexFieldAccess fieldAccess) {
      super.visitFieldAccess(fieldAccess);
      final RelDataType refType =
          fieldAccess.getReferenceExpr().getType();
      assert refType.isStruct();
      final RelDataTypeField field = fieldAccess.getField();
      final int index = field.getIndex();
      if ((index < 0) || (index > refType.getFieldList().size())) {
        assert !fail;
        ++failCount;
        return false;
      }
      final RelDataTypeField typeField =
          refType.getFieldList().get(index);
      if (!RelOptUtil.eq(
          "type1",
          typeField.getType(),
          "type2",
          fieldAccess.getType(),
          fail)) {
        assert !fail;
        ++failCount;
        return false;
      }
      return true;
    }
  }
}

// End Project.java
