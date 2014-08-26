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

import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.validate.SqlValidatorUtil;
import org.eigenbase.util.*;
import org.eigenbase.util.mapping.*;

/**
 * A relational expression which computes project expressions and also filters.
 *
 * <p>This relational expression combines the functionality of {@link
 * ProjectRel} and {@link FilterRel}. It should be created in the latter stages
 * of optimization, by merging consecutive {@link ProjectRel} and {@link
 * FilterRel} nodes together.
 *
 * <p>The following rules relate to <code>CalcRel</code>:</p>
 *
 * <ul>
 * <li>{@link FilterToCalcRule} creates this from a {@link FilterRel}</li>
 * <li>{@link ProjectToCalcRule} creates this from a {@link FilterRel}</li>
 * <li>{@link MergeFilterOntoCalcRule} merges this with a {@link FilterRel}</li>
 * <li>{@link MergeProjectOntoCalcRule} merges this with a {@link
 * ProjectRel}</li>
 * <li>{@link MergeCalcRule} merges two CalcRels</li>
 * </ul>
 */
public final class CalcRel extends CalcRelBase {
  //~ Static fields/initializers ---------------------------------------------

  public static final boolean DEPRECATE_PROJECT_AND_FILTER = false;

  //~ Constructors -----------------------------------------------------------

  public CalcRel(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      RelDataType rowType,
      RexProgram program,
      List<RelCollation> collationList) {
    super(cluster, traits, child, rowType, program, collationList);
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public CalcRelBase copy(
      RelTraitSet traitSet, RelNode child,
      RexProgram program, List<RelCollation> collationList) {
    return new CalcRel(
        getCluster(), traitSet, child, program.getOutputRowType(), program,
        collationList);
  }

  /**
   * Creates a relational expression which projects an array of expressions.
   *
   * @param child         input relational expression
   * @param exprList      list of expressions for the input columns
   * @param fieldNameList aliases of the expressions, or null to generate
   */
  public static RelNode createProject(
      RelNode child,
      List<RexNode> exprList,
      List<String> fieldNameList) {
    return createProject(child, exprList, fieldNameList, false);
  }

  /**
   * Creates a relational expression which projects an array of expressions.
   *
   * @param child       input relational expression
   * @param projectList list of (expression, name) pairs
   * @param optimize    Whether to optimize
   */
  public static RelNode createProject(
      RelNode child,
      List<Pair<RexNode, String>> projectList,
      boolean optimize) {
    return createProject(
        child, Pair.left(projectList), Pair.right(projectList), optimize);
  }

  /**
   * Creates a relational expression that projects the given fields of the
   * input.
   *
   * <p>Optimizes if the fields are the identity projection.</p>
   *
   * @param child   Input relational expression
   * @param posList Source of each projected field
   * @return Relational expression that projects given fields
   */
  public static RelNode createProject(final RelNode child,
      final List<Integer> posList) {
    return RelFactories.createProject(RelFactories.DEFAULT_PROJECT_FACTORY,
        child, posList);
  }

  /**
   * Creates a relational expression which projects an array of expressions,
   * and optionally optimizes.
   *
   * <p>The result may not be a {@link ProjectRel}. If the projection is
   * trivial, <code>child</code> is returned directly; and future versions may
   * return other formulations of expressions, such as {@link CalcRel}.
   *
   * @param child      input relational expression
   * @param exprs      list of expressions for the input columns
   * @param fieldNames aliases of the expressions, or null to generate
   * @param optimize   Whether to return <code>child</code> unchanged if the
   *                   projections are trivial.
   */
  public static RelNode createProject(
      RelNode child,
      List<RexNode> exprs,
      List<String> fieldNames,
      boolean optimize) {
    final RelOptCluster cluster = child.getCluster();
    final RexProgram program =
        RexProgram.create(
            child.getRowType(), exprs, null, fieldNames,
            cluster.getRexBuilder());
    final List<RelCollation> collationList =
        program.getCollations(child.getCollationList());
    if (DEPRECATE_PROJECT_AND_FILTER) {
      return new CalcRel(
          cluster,
          child.getTraitSet(),
          child,
          program.getOutputRowType(),
          program,
          collationList);
    } else {
      final RelDataType rowType =
          RexUtil.createStructType(
              cluster.getTypeFactory(),
              exprs,
              fieldNames == null
                  ? null
                  : SqlValidatorUtil.uniquify(
                      fieldNames, SqlValidatorUtil.F_SUGGESTER));
      if (optimize
          && RemoveTrivialProjectRule.isIdentity(exprs, rowType,
              child.getRowType())) {
        return child;
      }
      return
          new ProjectRel(
              cluster,
              cluster.traitSetOf(
                  collationList.isEmpty()
                      ? RelCollationImpl.EMPTY
                      : collationList.get(0)),
              child,
              exprs,
              rowType,
              ProjectRelBase.Flags.BOXED);
    }
  }

  /**
   * Creates a relational expression which filters according to a given
   * condition, returning the same fields as its input.
   *
   * @param child     Child relational expression
   * @param condition Condition
   * @return Relational expression
   */
  public static RelNode createFilter(
      RelNode child,
      RexNode condition) {
    if (DEPRECATE_PROJECT_AND_FILTER) {
      final RelOptCluster cluster = child.getCluster();
      RexProgramBuilder builder =
          new RexProgramBuilder(
              child.getRowType(),
              cluster.getRexBuilder());
      builder.addIdentity();
      builder.addCondition(condition);
      final RexProgram program = builder.getProgram();
      return new CalcRel(
          cluster,
          child.getTraitSet(),
          child,
          program.getOutputRowType(),
          program,
          Collections.<RelCollation>emptyList());
    } else {
      return new FilterRel(
          child.getCluster(),
          child,
          condition);
    }
  }

  /**
   * Returns a relational expression which has the same fields as the
   * underlying expression, but the fields have different names.
   *
   * @param rel        Relational expression
   * @param fieldNames Field names
   * @return Renamed relational expression
   */
  public static RelNode createRename(
      RelNode rel,
      List<String> fieldNames) {
    final List<RelDataTypeField> fields = rel.getRowType().getFieldList();
    assert fieldNames.size() == fields.size();
    final List<Pair<RexNode, String>> refs =
        new AbstractList<Pair<RexNode, String>>() {
          public int size() {
            return fields.size();
          }

          public Pair<RexNode, String> get(int index) {
            return RexInputRef.of2(index, fields);
          }
        };
    return createProject(rel, refs, true);
  }

  public void collectVariablesUsed(Set<String> variableSet) {
    final RelOptUtil.VariableUsedVisitor vuv =
        new RelOptUtil.VariableUsedVisitor();
    for (RexNode expr : program.getExprList()) {
      expr.accept(vuv);
    }
    variableSet.addAll(vuv.variables);
  }

  /**
   * Creates a relational expression which permutes the output fields of a
   * relational expression according to a permutation.
   *
   * <p>Optimizations:</p>
   *
   * <ul>
   * <li>If the relational expression is a {@link CalcRel} or {@link
   * ProjectRel} which is already acting as a permutation, combines the new
   * permutation with the old;</li>
   * <li>If the permutation is the identity, returns the original relational
   * expression.</li>
   * </ul>
   *
   * <p>If a permutation is combined with its inverse, these optimizations
   * would combine to remove them both.
   *
   * @param rel         Relational expression
   * @param permutation Permutation to apply to fields
   * @param fieldNames  Field names; if null, or if a particular entry is null,
   *                    the name of the permuted field is used
   * @return relational expression which permutes its input fields
   */
  public static RelNode permute(
      RelNode rel,
      Permutation permutation,
      List<String> fieldNames) {
    if (permutation.isIdentity()) {
      return rel;
    }
    if (rel instanceof CalcRel) {
      CalcRel calcRel = (CalcRel) rel;
      Permutation permutation1 = calcRel.getProgram().getPermutation();
      if (permutation1 != null) {
        Permutation permutation2 = permutation.product(permutation1);
        return permute(rel, permutation2, null);
      }
    }
    if (rel instanceof ProjectRel) {
      Permutation permutation1 = ((ProjectRel) rel).getPermutation();
      if (permutation1 != null) {
        Permutation permutation2 = permutation.product(permutation1);
        return permute(rel, permutation2, null);
      }
    }
    final List<RelDataType> outputTypeList = new ArrayList<RelDataType>();
    final List<String> outputNameList = new ArrayList<String>();
    final List<RexNode> exprList = new ArrayList<RexNode>();
    final List<RexLocalRef> projectRefList = new ArrayList<RexLocalRef>();
    final List<RelDataTypeField> fields = rel.getRowType().getFieldList();
    for (int i = 0; i < permutation.getTargetCount(); i++) {
      int target = permutation.getTarget(i);
      final RelDataTypeField targetField = fields.get(target);
      outputTypeList.add(targetField.getType());
      outputNameList.add(
          ((fieldNames == null)
              || (fieldNames.size() <= i)
              || (fieldNames.get(i) == null)) ? targetField.getName()
              : fieldNames.get(i));
      exprList.add(
          rel.getCluster().getRexBuilder().makeInputRef(
              fields.get(i).getType(),
              i));
      final int source = permutation.getSource(i);
      projectRefList.add(
          new RexLocalRef(
              source,
              fields.get(source).getType()));
    }
    final RexProgram program =
        new RexProgram(
            rel.getRowType(),
            exprList,
            projectRefList,
            null,
            rel.getCluster().getTypeFactory().createStructType(
                outputTypeList,
                outputNameList));
    return new CalcRel(
        rel.getCluster(),
        rel.getTraitSet(),
        rel,
        program.getOutputRowType(),
        program,
        Collections.<RelCollation>emptyList());
  }

  /**
   * Creates a relational expression which projects the output fields of a
   * relational expression according to a partial mapping.
   *
   * <p>A partial mapping is weaker than a permutation: every target has one
   * source, but a source may have 0, 1 or more than one targets. Usually the
   * result will have fewer fields than the source, unless some source fields
   * are projected multiple times.
   *
   * <p>This method could optimize the result as {@link #permute} does, but
   * does not at present.
   *
   * @param rel        Relational expression
   * @param mapping    Mapping from source fields to target fields. The mapping
   *                   type must obey the constraints
   *                   {@link MappingType#isMandatorySource()} and
   *                   {@link MappingType#isSingleSource()}, as does
   *                   {@link MappingType#INVERSE_FUNCTION}.
   * @param fieldNames Field names; if null, or if a particular entry is null,
   *                   the name of the permuted field is used
   * @return relational expression which projects a subset of the input fields
   */
  public static RelNode projectMapping(
      RelNode rel,
      Mapping mapping,
      List<String> fieldNames) {
    assert mapping.getMappingType().isSingleSource();
    assert mapping.getMappingType().isMandatorySource();
    if (mapping.isIdentity()) {
      return rel;
    }
    final List<RelDataType> outputTypeList = new ArrayList<RelDataType>();
    final List<String> outputNameList = new ArrayList<String>();
    final List<RexNode> exprList = new ArrayList<RexNode>();
    final List<RexLocalRef> projectRefList = new ArrayList<RexLocalRef>();
    final List<RelDataTypeField> fields = rel.getRowType().getFieldList();
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    for (int i = 0; i < fields.size(); i++) {
      exprList.add(rexBuilder.makeInputRef(rel, i));
    }
    for (int i = 0; i < mapping.getTargetCount(); i++) {
      int source = mapping.getSource(i);
      final RelDataTypeField sourceField = fields.get(source);
      outputTypeList.add(sourceField.getType());
      outputNameList.add(
          ((fieldNames == null)
              || (fieldNames.size() <= i)
              || (fieldNames.get(i) == null)) ? sourceField.getName()
              : fieldNames.get(i));
      projectRefList.add(
          new RexLocalRef(
              source,
              sourceField.getType()));
    }
    final RexProgram program =
        new RexProgram(
            rel.getRowType(),
            exprList,
            projectRefList,
            null,
            rel.getCluster().getTypeFactory().createStructType(
                outputTypeList,
                outputNameList));
    return new CalcRel(
        rel.getCluster(),
        rel.getTraitSet(),
        rel,
        program.getOutputRowType(),
        program,
        Collections.<RelCollation>emptyList());
  }
}

// End CalcRel.java
