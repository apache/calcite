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
package org.eigenbase.rel;

import java.util.*;

import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.Pair;
import org.eigenbase.util.Permutation;
import org.eigenbase.util.Util;
import org.eigenbase.util.mapping.MappingType;
import org.eigenbase.util.mapping.Mappings;

import net.hydromatic.linq4j.Ord;
import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.linq4j.function.Functions;

import com.google.common.collect.ImmutableList;

/**
 * <code>ProjectRelBase</code> is an abstract base class for implementations of
 * {@link ProjectRel}.
 */
public abstract class ProjectRelBase extends SingleRel {
  //~ Instance fields --------------------------------------------------------

  protected final ImmutableList<RexNode> exps;

  /**
   * Values defined in {@link Flags}.
   */
  protected int flags;

  protected final ImmutableList<RelCollation> collationList;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a Project.
   *
   * @param cluster Cluster this relational expression belongs to
   * @param traits  traits of this rel
   * @param child   input relational expression
   * @param exps    List of expressions for the input columns
   * @param rowType output row type
   * @param flags   values as in {@link Flags}
   */
  protected ProjectRelBase(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      List<? extends RexNode> exps,
      RelDataType rowType,
      int flags) {
    super(cluster, traits, child);
    assert rowType != null;
    this.exps = ImmutableList.copyOf(exps);
    this.rowType = rowType;
    this.flags = flags;
    final RelCollation collation =
        traits.getTrait(RelCollationTraitDef.INSTANCE);
    this.collationList =
        collation == null
            ? ImmutableList.<RelCollation>of()
            : ImmutableList.of(collation);
    assert isValid(true);
  }

  /**
   * Creates a ProjectRelBase by parsing serialized output.
   */
  protected ProjectRelBase(RelInput input) {
    this(
        input.getCluster(), input.getTraitSet(), input.getInput(),
        input.getExpressionList("exprs"),
        input.getRowType("exprs", "fields"), Flags.BOXED);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public final RelNode copy(RelTraitSet traitSet,
      List<RelNode> inputs) {
    return copy(traitSet, sole(inputs), exps, rowType);
  }

  /** Copies a project.
   *
   * @see #copy(RelTraitSet, List) */
  public abstract ProjectRelBase copy(RelTraitSet traitSet, RelNode input,
      List<RexNode> exps, RelDataType rowType);

  public List<RelCollation> getCollationList() {
    return collationList;
  }

  public boolean isBoxed() {
    return (flags & Flags.BOXED) == Flags.BOXED;
  }

  @Override
  public List<RexNode> getChildExps() {
    return exps;
  }

  /**
   * Returns the project expressions.
   */
  public List<RexNode> getProjects() {
    return exps;
  }

  /**
   * Returns a list of (expression, name) pairs. Convenient for various
   * transformations.
   */
  public final List<Pair<RexNode, String>> getNamedProjects() {
    return Pair.zip(getProjects(), getRowType().getFieldNames());
  }

  public int getFlags() {
    return flags;
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
            getChild().getRowType(), fail);
    for (RexNode exp : exps) {
      exp.accept(checker);
    }
    if (checker.getFailureCount() > 0) {
      assert !fail;
      return false;
    }
    if (!isBoxed()) {
      if (exps.size() != 1) {
        assert !fail;
        return false;
      }
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
    double dRows = RelMetadataQuery.getRowCount(getChild());
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
   */
  public Mappings.TargetMapping getMapping() {
    return getMapping(getChild().getRowType().getFieldCount(), exps);
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
   */
  public Permutation getPermutation() {
    final int fieldCount = rowType.getFieldList().size();
    if (fieldCount != getChild().getRowType().getFieldList().size()) {
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

  /** A collection of integer constants that describe the kind of project. */
  public static class Flags {
    public static final int ANON_FIELDS = 2;

    /**
     * Whether the resulting row is to be a synthetic class whose fields are
     * the aliases of the fields. <code>boxed</code> must be true unless
     * there is only one field: <code>select {dept.deptno} from dept</code>
     * is boxed, <code>select dept.deptno from dept</code> is not.
     */
    public static final int BOXED = 1;
    public static final int NONE = 0;
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

// End ProjectRelBase.java
