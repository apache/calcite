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
package org.apache.calcite.sql2rel;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelOptUtil.RexCorrelVariableMapShuttle;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.AsofJoin;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sample;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Snapshot;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.SortExchange;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Transformer that walks over a tree of relational expressions, replacing each
 * {@link RelNode} with a 'slimmed down' relational expression that projects
 * only the columns required by its consumer.
 *
 * <p>Uses multi-methods to fire the right rule for each type of relational
 * expression. This allows the transformer to be extended without having to
 * add a new method to RelNode, and without requiring a collection of rule
 * classes scattered to the four winds.
 *
 * <p>REVIEW: jhyde, 2009/7/28: Is sql2rel the correct package for this class?
 * Trimming fields is not an essential part of SQL-to-Rel translation, and
 * arguably belongs in the optimization phase. But this transformer does not
 * obey the usual pattern for planner rules; it is difficult to do so, because
 * each {@link RelNode} needs to return a different set of fields after
 * trimming.
 *
 * <p>TODO: Change 2nd arg of the {@link #trimFields} method from BitSet to
 * Mapping. Sometimes it helps the consumer if you return the columns in a
 * particular order. For instance, it may avoid a project at the top of the
 * tree just for reordering. Could ease the transition by writing methods that
 * convert BitSet to Mapping and vice versa.
 */
public class RelFieldTrimmer implements ReflectiveVisitor {
  //~ Static fields/initializers ---------------------------------------------

  //~ Instance fields --------------------------------------------------------

  private final ReflectUtil.MethodDispatcher<TrimResult> trimFieldsDispatcher;
  private final RelBuilder relBuilder;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a RelFieldTrimmer.
   *
   * @param validator Validator
   */
  public RelFieldTrimmer(@Nullable SqlValidator validator, RelBuilder relBuilder) {
    Util.discard(validator); // may be useful one day
    this.relBuilder = relBuilder;
    @SuppressWarnings("argument.type.incompatible")
    ReflectUtil.MethodDispatcher<TrimResult> dispatcher =
        ReflectUtil.createMethodDispatcher(
            TrimResult.class,
            this,
            "trimFields",
            RelNode.class,
            ImmutableBitSet.class,
            Set.class);
    this.trimFieldsDispatcher = dispatcher;
  }

  @Deprecated // to be removed before 2.0
  public RelFieldTrimmer(@Nullable SqlValidator validator,
      RelOptCluster cluster,
      RelFactories.ProjectFactory projectFactory,
      RelFactories.FilterFactory filterFactory,
      RelFactories.JoinFactory joinFactory,
      RelFactories.SortFactory sortFactory,
      RelFactories.AggregateFactory aggregateFactory,
      RelFactories.SetOpFactory setOpFactory) {
    this(validator,
        RelBuilder.proto(projectFactory, filterFactory, joinFactory,
            sortFactory, aggregateFactory, setOpFactory)
        .create(cluster, null));
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Trims unused fields from a relational expression.
   *
   * <p>We presume that all fields of the relational expression are wanted by
   * its consumer, so only trim fields that are not used within the tree.
   *
   * @param root Root node of relational expression
   * @return Trimmed relational expression
   */
  public RelNode trim(RelNode root) {
    final int fieldCount = root.getRowType().getFieldCount();
    final ImmutableBitSet fieldsUsed = ImmutableBitSet.range(fieldCount);
    final Set<RelDataTypeField> extraFields = Collections.emptySet();
    final TrimResult trimResult =
        dispatchTrimFields(root, fieldsUsed, extraFields);
    if (!trimResult.right.isIdentity()) {
      throw new IllegalArgumentException();
    }
    if (SqlToRelConverter.SQL2REL_LOGGER.isDebugEnabled()) {
      SqlToRelConverter.SQL2REL_LOGGER.debug(
          RelOptUtil.dumpPlan("Plan after trimming unused fields",
              trimResult.left, SqlExplainFormat.TEXT,
              SqlExplainLevel.EXPPLAN_ATTRIBUTES));
    }
    return trimResult.left;
  }

  /**
   * Trims the fields of an input relational expression.
   *
   * @param rel        Relational expression
   * @param input      Input relational expression, whose fields to trim
   * @param fieldsUsed Bitmap of fields needed by the consumer
   * @return New relational expression and its field mapping
   */
  protected TrimResult trimChild(
      RelNode rel,
      RelNode input,
      final ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final ImmutableBitSet.Builder fieldsUsedBuilder = fieldsUsed.rebuild();

    // Fields that define the collation cannot be discarded.
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
    final ImmutableList<RelCollation> collations = mq.collations(input);
    if (collations != null) {
      for (RelCollation collation : collations) {
        for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
          fieldsUsedBuilder.set(fieldCollation.getFieldIndex());
        }
      }
    }

    // Correlating variables are a means for other relational expressions to use
    // fields.
    for (final CorrelationId correlation : rel.getVariablesSet()) {
      rel.accept(
          new CorrelationReferenceFinder() {
            @Override protected RexNode handle(RexFieldAccess fieldAccess) {
              final RexCorrelVariable v =
                  (RexCorrelVariable) fieldAccess.getReferenceExpr();
              if (v.id.equals(correlation)) {
                fieldsUsedBuilder.set(fieldAccess.getField().getIndex());
              }
              return fieldAccess;
            }
          });
    }

    return dispatchTrimFields(input, fieldsUsedBuilder.build(), extraFields);
  }

  /**
   * Trims a child relational expression, then adds back a dummy project to
   * restore the fields that were removed.
   *
   * <p>Sounds pointless? It causes unused fields to be removed
   * further down the tree (towards the leaves), but it ensure that the
   * consuming relational expression continues to see the same fields.
   *
   * @param rel        Relational expression
   * @param input      Input relational expression, whose fields to trim
   * @param fieldsUsed Bitmap of fields needed by the consumer
   * @return New relational expression and its field mapping
   */
  protected TrimResult trimChildRestore(
      RelNode rel,
      RelNode input,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    TrimResult trimResult = trimChild(rel, input, fieldsUsed, extraFields);
    if (trimResult.right.isIdentity()) {
      return trimResult;
    }
    final RelDataType rowType = input.getRowType();
    List<RelDataTypeField> fieldList = rowType.getFieldList();
    final List<RexNode> exprList = new ArrayList<>();
    final List<String> nameList = rowType.getFieldNames();
    RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    assert trimResult.right.getSourceCount() == fieldList.size();
    for (int i = 0; i < fieldList.size(); i++) {
      int source = trimResult.right.getTargetOpt(i);
      RelDataTypeField field = fieldList.get(i);
      exprList.add(
          source < 0
              ? rexBuilder.makeZeroLiteral(field.getType())
              : rexBuilder.makeInputRef(field.getType(), source));
    }
    relBuilder.push(trimResult.left)
        .project(exprList, nameList);
    return result(relBuilder.build(),
        Mappings.createIdentity(fieldList.size()), rel);
  }

  /**
   * Invokes {@link #trimFields}, or the appropriate method for the type
   * of the rel parameter, using multi-method dispatch.
   *
   * @param rel        Relational expression
   * @param fieldsUsed Bitmap of fields needed by the consumer
   * @return New relational expression and its field mapping
   */
  protected final TrimResult dispatchTrimFields(
      RelNode rel,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final TrimResult trimResult =
        trimFieldsDispatcher.invoke(rel, fieldsUsed, extraFields);
    final RelNode newRel = trimResult.left;
    final Mapping mapping = trimResult.right;
    final int fieldCount = rel.getRowType().getFieldCount();
    assert mapping.getSourceCount() == fieldCount
        : "source: " + mapping.getSourceCount() + " != " + fieldCount;
    final int newFieldCount = newRel.getRowType().getFieldCount();
    assert mapping.getTargetCount() + extraFields.size() == newFieldCount
        || Bug.TODO_FIXED
        : "target: " + mapping.getTargetCount()
        + " + " + extraFields.size()
        + " != " + newFieldCount;
    if (Bug.TODO_FIXED) {
      assert newFieldCount > 0 : "rel has no fields after trim: " + rel;
    }
    if (newRel.equals(rel)) {
      return result(rel, mapping);
    }
    return trimResult;
  }

  protected TrimResult result(RelNode rel, final Mapping mapping, RelNode oldRel) {
    return result(RelOptUtil.copyRelHints(oldRel, rel), mapping);
  }

  protected TrimResult result(RelNode r, final Mapping mapping) {
    final RexBuilder rexBuilder = relBuilder.getRexBuilder();
    for (final CorrelationId correlation : r.getVariablesSet()) {
      r =
          r.accept(new CorrelationReferenceFinder() {
            @Override protected RexNode handle(RexFieldAccess fieldAccess) {
              final RexCorrelVariable v =
                  (RexCorrelVariable) fieldAccess.getReferenceExpr();
              if (v.id.equals(correlation)
                  && v.getType().getFieldCount() == mapping.getSourceCount()) {
                final int old = fieldAccess.getField().getIndex();
                final int new_ = mapping.getTarget(old);
                final RelDataTypeFactory.Builder typeBuilder =
                    relBuilder.getTypeFactory().builder();
                for (int target : Util.range(mapping.getTargetCount())) {
                  typeBuilder.add(
                      v.getType().getFieldList().get(mapping.getSource(target)));
                }
                final RexNode newV =
                    rexBuilder.makeCorrel(typeBuilder.build(), v.id);
                if (old != new_) {
                  return rexBuilder.makeFieldAccess(newV, new_);
                }
              }
              return fieldAccess;
            }
          });
    }
    return new TrimResult(r, mapping);
  }

  /**
   * Visit method, per {@link org.apache.calcite.util.ReflectiveVisitor}.
   *
   * <p>This method is invoked reflectively, so there may not be any apparent
   * calls to it. The class (or derived classes) may contain overloads of
   * this method with more specific types for the {@code rel} parameter.
   *
   * <p>Returns a pair: the relational expression created, and the mapping
   * between the original fields and the fields of the newly created
   * relational expression.
   *
   * @param rel        Relational expression
   * @param fieldsUsed Fields needed by the consumer
   * @return relational expression and mapping
   */
  public TrimResult trimFields(
      RelNode rel,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    // We don't know how to trim this kind of relational expression
    Util.discard(fieldsUsed);
    if (rel.getInputs().isEmpty()) {
      return result(rel, Mappings.createIdentity(rel.getRowType().getFieldCount()));
    }

    // We don't know how to trim this RelNode, but we can try to trim inside its inputs
    List<RelNode> newInputs = new ArrayList<>(rel.getInputs().size());
    for (RelNode input : rel.getInputs()) {
      ImmutableBitSet inputFieldsUsed = ImmutableBitSet.range(input.getRowType().getFieldCount());
      TrimResult trimResult = dispatchTrimFields(input, inputFieldsUsed, extraFields);
      if (!trimResult.right.isIdentity()) {
        throw new IllegalArgumentException("Expected identity mapping after processing RelNode "
            + input + "; but got " + trimResult.right);
      }
      newInputs.add(trimResult.left);
    }
    RelNode newRel = rel.copy(rel.getTraitSet(), newInputs);
    return result(newRel, Mappings.createIdentity(newRel.getRowType().getFieldCount()), rel);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.logical.LogicalCalc}.
   */
  public TrimResult trimFields(
      Calc calc,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final RexProgram rexProgram = calc.getProgram();
    final List<RexNode> projs =
        Util.transform(rexProgram.getProjectList(), rexProgram::expandLocalRef);

    final RexNode conditionExpr =
        rexProgram.getCondition() == null ? null
            : rexProgram.expandLocalRef(rexProgram.getCondition());

    final RelDataType rowType = calc.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final RelNode input = calc.getInput();

    final Set<RelDataTypeField> inputExtraFields =
        new HashSet<>(extraFields);
    RelOptUtil.InputFinder inputFinder =
        new RelOptUtil.InputFinder(inputExtraFields);
    for (Ord<RexNode> ord : Ord.zip(projs)) {
      if (fieldsUsed.get(ord.i)) {
        ord.e.accept(inputFinder);
      }
    }
    if (conditionExpr != null) {
      conditionExpr.accept(inputFinder);
    }
    ImmutableBitSet inputFieldsUsed = inputFinder.build();

    // Create input with trimmed columns.
    TrimResult trimResult =
        trimChild(calc, input, inputFieldsUsed, inputExtraFields);
    RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;

    // If the input is unchanged, and we need to project all columns,
    // there's nothing we can do.
    if (newInput == input
        && fieldsUsed.cardinality() == fieldCount) {
      return result(calc, Mappings.createIdentity(fieldCount));
    }

    // Some parts of the system can't handle rows with zero fields, so
    // pretend that one field is used.
    if (fieldsUsed.cardinality() == 0 && rexProgram.getCondition() == null) {
      return dummyProject(fieldCount, newInput);
    }

    // Build new project expressions, and populate the mapping.
    final List<RexNode> newProjects = new ArrayList<>();
    final RexVisitor<RexNode> shuttle =
        new RexPermuteInputsShuttle(
            inputMapping, newInput);
    final Mapping mapping =
        Mappings.create(
            MappingType.INVERSE_SURJECTION,
            fieldCount,
            fieldsUsed.cardinality());
    for (Ord<RexNode> ord : Ord.zip(projs)) {
      if (fieldsUsed.get(ord.i)) {
        mapping.set(ord.i, newProjects.size());
        RexNode newProjectExpr = ord.e.accept(shuttle);
        newProjects.add(newProjectExpr);
      }
    }

    final RelDataType newRowType =
        RelOptUtil.permute(calc.getCluster().getTypeFactory(), rowType,
            mapping);

    final RelNode newInputRelNode = relBuilder.push(newInput).build();
    RexNode newConditionExpr = null;
    if (conditionExpr != null) {
      newConditionExpr = conditionExpr.accept(shuttle);
    }
    final RexProgram newRexProgram =
        RexProgram.create(newInputRelNode.getRowType(), newProjects,
            newConditionExpr, newRowType.getFieldNames(),
            newInputRelNode.getCluster().getRexBuilder());
    final Calc newCalc =
        calc.copy(calc.getTraitSet(), newInputRelNode, newRexProgram);
    return result(newCalc, mapping, calc);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.logical.LogicalProject}.
   */
  public TrimResult trimFields(
      Project project,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = project.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final RelNode input = project.getInput();

    // Which fields are required from the input?
    final Set<RelDataTypeField> inputExtraFields =
        new LinkedHashSet<>(extraFields);
    RelOptUtil.InputFinder inputFinder =
        new RelOptUtil.InputFinder(inputExtraFields);
    for (Ord<RexNode> ord : Ord.zip(project.getProjects())) {
      if (fieldsUsed.get(ord.i)) {
        ord.e.accept(inputFinder);
      }
    }

    // Collect all the SubQueries in the projection list.
    List<RexSubQuery> subQueries = RexUtil.SubQueryCollector.collect(project);
    // Get all the correlationIds present in the SubQueries
    Set<CorrelationId> correlationIds = RelOptUtil.getVariablesUsed(subQueries);
    ImmutableBitSet requiredColumns = ImmutableBitSet.of();
    if (!correlationIds.isEmpty()) {
      assert correlationIds.size() == 1;
      // Correlation columns are also needed by SubQueries, so add them to inputFieldsUsed.
      requiredColumns = RelOptUtil.correlationColumns(correlationIds.iterator().next(), project);
    }

    ImmutableBitSet finderFields = inputFinder.build();

    ImmutableBitSet inputFieldsUsed = ImmutableBitSet.builder()
        .addAll(requiredColumns)
        .addAll(finderFields)
        .build();

    // Create input with trimmed columns.
    TrimResult trimResult =
        trimChild(project, input, inputFieldsUsed, inputExtraFields);
    RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;

    // If the input is unchanged, and we need to project all columns,
    // there's nothing we can do.
    if (newInput == input
        && fieldsUsed.cardinality() == fieldCount) {
      return result(project, Mappings.createIdentity(fieldCount));
    }

    // Some parts of the system can't handle rows with zero fields, so
    // pretend that one field is used.
    if (fieldsUsed.cardinality() == 0) {
      return dummyProject(fieldCount, newInput, project);
    }

    // Build new project expressions, and populate the mapping.
    final List<RexNode> newProjects = new ArrayList<>();
    final RexVisitor<RexNode> shuttle;

    if (!correlationIds.isEmpty()) {
      assert correlationIds.size() == 1;
      shuttle = new RexPermuteInputsShuttle(inputMapping, newInput) {
        @Override public RexNode visitSubQuery(RexSubQuery subQuery) {
          subQuery = (RexSubQuery) super.visitSubQuery(subQuery);

          return RelOptUtil.remapCorrelatesInSuqQuery(relBuilder.getRexBuilder(),
            subQuery, correlationIds.iterator().next(), newInput.getRowType(), inputMapping);
        }
      };
    } else {
      shuttle = new RexPermuteInputsShuttle(inputMapping, newInput);
    }

    final Mapping mapping =
        Mappings.create(
            MappingType.INVERSE_SURJECTION,
            fieldCount,
            fieldsUsed.cardinality());
    for (Ord<RexNode> ord : Ord.zip(project.getProjects())) {
      if (fieldsUsed.get(ord.i)) {
        mapping.set(ord.i, newProjects.size());
        RexNode newProjectExpr = ord.e.accept(shuttle);
        newProjects.add(newProjectExpr);
      }
    }

    final RelDataType newRowType =
        RelOptUtil.permute(project.getCluster().getTypeFactory(), rowType,
            mapping);

    relBuilder.push(newInput);
    relBuilder.project(newProjects, newRowType.getFieldNames(), false, correlationIds);
    final RelNode newProject = relBuilder.build();
    return result(newProject, mapping, project);
  }

  /** Creates a project with a dummy column, to protect the parts of the system
   * that cannot handle a relational expression with no columns.
   *
   * @param fieldCount Number of fields in the original relational expression
   * @param input Trimmed input
   * @return Dummy project
   */
  protected TrimResult dummyProject(int fieldCount, RelNode input) {
    return dummyProject(fieldCount, input, null);
  }

  /** Creates a project with a dummy column, to protect the parts of the system
   * that cannot handle a relational expression with no columns.
   *
   * @param fieldCount Number of fields in the original relational expression
   * @param input Trimmed input
   * @param originalRelNode Source RelNode for hint propagation (or null if no propagation needed)
   * @return Dummy project
   */
  protected TrimResult dummyProject(int fieldCount, RelNode input,
      @Nullable RelNode originalRelNode) {
    final RelOptCluster cluster = input.getCluster();
    final Mapping mapping =
        Mappings.create(MappingType.INVERSE_SURJECTION, fieldCount, 1);
    if (input.getRowType().getFieldCount() == 1) {
      // Input already has one field (and may in fact be a dummy project we
      // created for the child). We can't do better.
      return result(input, mapping);
    }
    final RexLiteral expr =
        cluster.getRexBuilder().makeExactLiteral(BigDecimal.ZERO);
    relBuilder.push(input);
    relBuilder.project(ImmutableList.of(expr), ImmutableList.of("DUMMY"));
    RelNode newProject = relBuilder.build();
    if (originalRelNode != null) {
      newProject = RelOptUtil.propagateRelHints(originalRelNode, newProject);
    }
    return result(newProject, mapping);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.logical.LogicalFilter}.
   */
  public TrimResult trimFields(
      Filter filter,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = filter.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final RexNode conditionExpr = filter.getCondition();
    final RelNode input = filter.getInput();

    // We use the fields used by the consumer, plus any fields used in the
    // filter.
    final Set<RelDataTypeField> inputExtraFields =
        new LinkedHashSet<>(extraFields);
    RelOptUtil.InputFinder inputFinder =
        new RelOptUtil.InputFinder(inputExtraFields, fieldsUsed);
    conditionExpr.accept(inputFinder);
    final ImmutableBitSet inputFieldsUsed = inputFinder.build();

    // Create input with trimmed columns.
    TrimResult trimResult =
        trimChild(filter, input, inputFieldsUsed, inputExtraFields);
    RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;

    // If the input is unchanged, and we need to project all columns,
    // there's nothing we can do.
    if (newInput == input
        && fieldsUsed.cardinality() == fieldCount) {
      return result(filter, Mappings.createIdentity(fieldCount));
    }

    // Build new project expressions, and populate the mapping.
    final RexVisitor<RexNode> shuttle =
        new RexPermuteInputsShuttle(inputMapping, newInput);
    RexNode newConditionExpr =
        conditionExpr.accept(shuttle);

    // Build new filter with trimmed input and condition.
    relBuilder.push(newInput)
        .filter(filter.getVariablesSet(), newConditionExpr);

    // The result has the same mapping as the input gave us. Sometimes we
    // return fields that the consumer didn't ask for, because the filter
    // needs them for its condition.
    return result(relBuilder.build(), inputMapping, filter);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.core.Sort}.
   */
  public TrimResult trimFields(
      Sort sort,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = sort.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final RelCollation collation = sort.getCollation();
    final RelNode input = sort.getInput();

    // We use the fields used by the consumer, plus any fields used as sort
    // keys.
    final ImmutableBitSet.Builder inputFieldsUsed = fieldsUsed.rebuild();
    for (RelFieldCollation field : collation.getFieldCollations()) {
      inputFieldsUsed.set(field.getFieldIndex());
    }

    // Create input with trimmed columns.
    final Set<RelDataTypeField> inputExtraFields = Collections.emptySet();
    TrimResult trimResult =
        trimChild(sort, input, inputFieldsUsed.build(), inputExtraFields);
    RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;

    // If the input is unchanged, and we need to project all columns,
    // there's nothing we can do.
    if (newInput == input
        && inputMapping.isIdentity()
        && fieldsUsed.cardinality() == fieldCount) {
      return result(sort, Mappings.createIdentity(fieldCount));
    }

    relBuilder.push(newInput);
    final ImmutableList<RexNode> fields =
        relBuilder.fields(RexUtil.apply(inputMapping, collation));
    relBuilder.sortLimit(sort.offset, sort.fetch, fields);

    // The result has the same mapping as the input gave us. Sometimes we
    // return fields that the consumer didn't ask for, because the filter
    // needs them for its condition.
    return result(relBuilder.build(), inputMapping, sort);
  }

  public TrimResult trimFields(
      Exchange exchange,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = exchange.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final RelDistribution distribution = exchange.getDistribution();
    final RelNode input = exchange.getInput();

    // We use the fields used by the consumer, plus any fields used as exchange
    // keys.
    final ImmutableBitSet.Builder inputFieldsUsed = fieldsUsed.rebuild();
    for (int keyIndex : distribution.getKeys()) {
      inputFieldsUsed.set(keyIndex);
    }

    // Create input with trimmed columns.
    final Set<RelDataTypeField> inputExtraFields = Collections.emptySet();
    final TrimResult trimResult =
        trimChild(exchange, input, inputFieldsUsed.build(), inputExtraFields);
    final RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;

    // If the input is unchanged, and we need to project all columns,
    // there's nothing we can do.
    if (newInput == input
        && inputMapping.isIdentity()
        && fieldsUsed.cardinality() == fieldCount) {
      return result(exchange, Mappings.createIdentity(fieldCount));
    }

    relBuilder.push(newInput);
    final RelDistribution newDistribution = distribution.apply(inputMapping);
    relBuilder.exchange(newDistribution);

    return result(relBuilder.build(), inputMapping, exchange);
  }

  public TrimResult trimFields(
      SortExchange sortExchange,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = sortExchange.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final RelCollation collation = sortExchange.getCollation();
    final RelDistribution distribution = sortExchange.getDistribution();
    final RelNode input = sortExchange.getInput();

    // We use the fields used by the consumer, plus any fields used as sortExchange
    // keys.
    final ImmutableBitSet.Builder inputFieldsUsed = fieldsUsed.rebuild();
    for (RelFieldCollation field : collation.getFieldCollations()) {
      inputFieldsUsed.set(field.getFieldIndex());
    }
    for (int keyIndex : distribution.getKeys()) {
      inputFieldsUsed.set(keyIndex);
    }

    // Create input with trimmed columns.
    final Set<RelDataTypeField> inputExtraFields = Collections.emptySet();
    TrimResult trimResult =
        trimChild(sortExchange, input, inputFieldsUsed.build(), inputExtraFields);
    RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;

    // If the input is unchanged, and we need to project all columns,
    // there's nothing we can do.
    if (newInput == input
        && inputMapping.isIdentity()
        && fieldsUsed.cardinality() == fieldCount) {
      return result(sortExchange, Mappings.createIdentity(fieldCount));
    }

    relBuilder.push(newInput);
    RelCollation newCollation = RexUtil.apply(inputMapping, collation);
    RelDistribution newDistribution = distribution.apply(inputMapping);
    relBuilder.sortExchange(newDistribution, newCollation);

    return result(relBuilder.build(), inputMapping, sortExchange);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.logical.LogicalJoin}.
   */
  public TrimResult trimFields(
      Join join,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final int fieldCount = join.getSystemFieldList().size()
        + join.getLeft().getRowType().getFieldCount()
        + join.getRight().getRowType().getFieldCount();
    final RexNode conditionExpr = join.getCondition();
    final RexNode matchConditionExpr = (join instanceof AsofJoin)
        ? ((AsofJoin) join).getMatchCondition()
        : null;
    final int systemFieldCount = join.getSystemFieldList().size();

    // Add in fields used in the condition.
    final Set<RelDataTypeField> combinedInputExtraFields =
        new LinkedHashSet<>(extraFields);
    RelOptUtil.InputFinder inputFinder =
        new RelOptUtil.InputFinder(combinedInputExtraFields, fieldsUsed);
    conditionExpr.accept(inputFinder);
    if (matchConditionExpr != null) {
      matchConditionExpr.accept(inputFinder);
    }
    final ImmutableBitSet fieldsUsedPlus = inputFinder.build();

    // If no system fields are used, we can remove them.
    int systemFieldUsedCount = 0;
    for (int i = 0; i < systemFieldCount; ++i) {
      if (fieldsUsed.get(i)) {
        ++systemFieldUsedCount;
      }
    }
    final int newSystemFieldCount;
    if (systemFieldUsedCount == 0) {
      newSystemFieldCount = 0;
    } else {
      newSystemFieldCount = systemFieldCount;
    }

    int offset = systemFieldCount;
    int changeCount = 0;
    int newFieldCount = newSystemFieldCount;
    final List<RelNode> newInputs = new ArrayList<>(2);
    final List<Mapping> inputMappings = new ArrayList<>();
    final List<Integer> inputExtraFieldCounts = new ArrayList<>();
    for (RelNode input : join.getInputs()) {
      final RelDataType inputRowType = input.getRowType();
      final int inputFieldCount = inputRowType.getFieldCount();

      // Compute required mapping.
      ImmutableBitSet.Builder inputFieldsUsed = ImmutableBitSet.builder();
      for (int bit : fieldsUsedPlus) {
        if (bit >= offset && bit < offset + inputFieldCount) {
          inputFieldsUsed.set(bit - offset);
        }
      }

      // If there are system fields, we automatically use the
      // corresponding field in each input.
      inputFieldsUsed.set(0, newSystemFieldCount);

      // FIXME: We ought to collect extra fields for each input
      // individually. For now, we assume that just one input has
      // on-demand fields.
      Set<RelDataTypeField> inputExtraFields =
          RelDataTypeImpl.extra(inputRowType) == null
              ? Collections.emptySet()
              : combinedInputExtraFields;
      inputExtraFieldCounts.add(inputExtraFields.size());
      TrimResult trimResult =
          trimChild(join, input, inputFieldsUsed.build(), inputExtraFields);
      newInputs.add(trimResult.left);
      if (trimResult.left != input) {
        ++changeCount;
      }

      final Mapping inputMapping = trimResult.right;
      inputMappings.add(inputMapping);

      // Move offset to point to start of next input.
      offset += inputFieldCount;
      newFieldCount +=
          inputMapping.getTargetCount() + inputExtraFields.size();
    }

    Mapping mapping =
        Mappings.create(
            MappingType.INVERSE_SURJECTION,
            fieldCount,
            newFieldCount);
    for (int i = 0; i < newSystemFieldCount; ++i) {
      mapping.set(i, i);
    }
    offset = systemFieldCount;
    int newOffset = newSystemFieldCount;
    for (int i = 0; i < inputMappings.size(); i++) {
      Mapping inputMapping = inputMappings.get(i);
      for (IntPair pair : inputMapping) {
        mapping.set(pair.source + offset, pair.target + newOffset);
      }
      offset += inputMapping.getSourceCount();
      newOffset += inputMapping.getTargetCount()
          + inputExtraFieldCounts.get(i);
    }

    if (changeCount == 0
        && mapping.isIdentity()) {
      return result(join, Mappings.createIdentity(join.getRowType().getFieldCount()));
    }

    // Build new join.
    final RexVisitor<RexNode> shuttle =
        new RexPermuteInputsShuttle(
            mapping, newInputs.get(0), newInputs.get(1));
    RexNode newConditionExpr =
        conditionExpr.accept(shuttle);
    RexNode newMatchConditionExpr =
        matchConditionExpr != null ? matchConditionExpr.accept(shuttle) : null;

    relBuilder.push(newInputs.get(0));
    relBuilder.push(newInputs.get(1));

    switch (join.getJoinType()) {
    case SEMI:
    case ANTI:
      // For SemiJoins and AntiJoins only map fields from the left-side
      if (join.getJoinType() == JoinRelType.SEMI) {
        relBuilder.semiJoin(newConditionExpr);
      } else {
        relBuilder.antiJoin(newConditionExpr);
      }
      Mapping inputMapping = inputMappings.get(0);
      mapping =
          Mappings.create(MappingType.INVERSE_SURJECTION,
              join.getRowType().getFieldCount(),
              newSystemFieldCount + inputMapping.getTargetCount());
      for (int i = 0; i < newSystemFieldCount; ++i) {
        mapping.set(i, i);
      }
      offset = systemFieldCount;
      newOffset = newSystemFieldCount;
      for (IntPair pair : inputMapping) {
        mapping.set(pair.source + offset, pair.target + newOffset);
      }
      break;
    case ASOF:
    case LEFT_ASOF:
      relBuilder.asofJoin(join.getJoinType(), newConditionExpr,
          requireNonNull(newMatchConditionExpr, "newMatchConditionExpr"));
      break;
    default:
      relBuilder.join(join.getJoinType(), newConditionExpr);
      break;
    }
    return result(relBuilder.build(), mapping, join);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.core.SetOp} (Only UNION ALL is supported).
   */
  public TrimResult trimFields(
      SetOp setOp,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = setOp.getRowType();
    final int fieldCount = rowType.getFieldCount();

    // Trim fields only for UNION ALL.
    //
    // UNION | INTERSECT | INTERSECT ALL | EXCEPT | EXCEPT ALL
    // all have comparison between branches.
    // They can not be trimmed because the comparison needs
    // complete fields.
    if (!(setOp.kind == SqlKind.UNION && setOp.all)) {
      return trimFields((RelNode) setOp, fieldsUsed, extraFields);
    }

    int changeCount = 0;

    // Fennel abhors an empty row type, so pretend that the parent rel
    // wants the last field. (The last field is the least likely to be a
    // system field.)
    if (fieldsUsed.isEmpty()) {
      fieldsUsed = ImmutableBitSet.of(rowType.getFieldCount() - 1);
    }

    // Compute the desired field mapping. Give the consumer the fields they
    // want, in the order that they appear in the bitset.
    final Mapping mapping = createMapping(fieldsUsed, fieldCount);

    // Create input with trimmed columns.
    for (RelNode input : setOp.getInputs()) {
      TrimResult trimResult =
          trimChild(setOp, input, fieldsUsed, extraFields);

      // We want "mapping", the input gave us "inputMapping", compute
      // "remaining" mapping.
      //    |                   |                |
      //    |---------------- mapping ---------->|
      //    |-- inputMapping -->|                |
      //    |                   |-- remaining -->|
      //
      // For instance, suppose we have columns [a, b, c, d],
      // the consumer asked for mapping = [b, d],
      // and the transformed input has columns inputMapping = [d, a, b].
      // remaining will permute [b, d] to [d, a, b].
      Mapping remaining = Mappings.divide(mapping, trimResult.right);

      // Create a projection; does nothing if remaining is identity.
      relBuilder.push(trimResult.left);
      relBuilder.permute(remaining);

      if (input != relBuilder.peek()) {
        ++changeCount;
      }
    }

    // If the input is unchanged, and we need to project all columns,
    // there's to do.
    if (changeCount == 0
        && mapping.isIdentity()) {
      for (@SuppressWarnings("unused") RelNode input : setOp.getInputs()) {
        relBuilder.build();
      }
      return result(setOp, mapping);
    }

    switch (setOp.kind) {
    case UNION:
      relBuilder.union(setOp.all, setOp.getInputs().size());
      break;
    case INTERSECT:
      relBuilder.intersect(setOp.all, setOp.getInputs().size());
      break;
    case EXCEPT:
      assert setOp.getInputs().size() == 2;
      relBuilder.minus(setOp.all);
      break;
    default:
      throw new AssertionError("unknown setOp " + setOp);
    }
    return result(relBuilder.build(), mapping, setOp);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.logical.LogicalAggregate}.
   */
  public TrimResult trimFields(
      Aggregate aggregate,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    // Fields:
    //
    // | sys fields | group fields | indicator fields | agg functions |
    //
    // Two kinds of trimming:
    //
    // 1. If agg rel has system fields but none of these are used, create an
    // agg rel with no system fields.
    //
    // 2. If aggregate functions are not used, remove them.
    //
    // But group and indicator fields stay, even if they are not used.

    final RelDataType rowType = aggregate.getRowType();

    // Compute which input fields are used.
    // 1. group fields are always used
    final ImmutableBitSet.Builder inputFieldsUsed =
        aggregate.getGroupSet().rebuild();
    // 2. agg functions: consider only the ones that are needed according to fieldsUsed
    int aggCallIndex = aggregate.getGroupCount();
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      if (fieldsUsed.get(aggCallIndex)) {
        inputFieldsUsed.addAll(aggCall.getArgList());
        if (aggCall.filterArg >= 0) {
          inputFieldsUsed.set(aggCall.filterArg);
        }
        if (aggCall.distinctKeys != null) {
          inputFieldsUsed.addAll(aggCall.distinctKeys);
        }
        inputFieldsUsed.addAll(RelCollations.ordinals(aggCall.collation));
      }
      aggCallIndex++;
    }

    // Create input with trimmed columns.
    final RelNode input = aggregate.getInput();
    final Set<RelDataTypeField> inputExtraFields = Collections.emptySet();
    final TrimResult trimResult =
        trimChild(aggregate, input, inputFieldsUsed.build(), inputExtraFields);
    final RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;
    // We have to return group keys and (if present) indicators.
    // So, pretend that the consumer asked for them.
    final int groupCount = aggregate.getGroupSet().cardinality();
    fieldsUsed =
        fieldsUsed.union(ImmutableBitSet.range(groupCount));

    // If the input is unchanged, and we need to project all columns,
    // there's nothing to do.
    if (input == newInput
        && fieldsUsed.equals(ImmutableBitSet.range(rowType.getFieldCount()))) {
      return result(aggregate,
          Mappings.createIdentity(rowType.getFieldCount()));
    }

    // Which agg calls are used by our consumer?
    int j = groupCount;
    int usedAggCallCount = 0;
    for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
      if (fieldsUsed.get(j++)) {
        ++usedAggCallCount;
      }
    }

    // Offset due to the number of system fields having changed.
    Mapping mapping =
        Mappings.create(
            MappingType.INVERSE_SURJECTION,
            rowType.getFieldCount(),
            groupCount + usedAggCallCount);

    final ImmutableBitSet newGroupSet =
        Mappings.apply(inputMapping, aggregate.getGroupSet());

    final ImmutableList<ImmutableBitSet> newGroupSets =
        ImmutableList.copyOf(
            Util.transform(aggregate.getGroupSets(),
                input1 -> Mappings.apply(inputMapping, input1)));

    // Populate mapping of where to find the fields. System, group key and
    // indicator fields first.
    for (j = 0; j < groupCount; j++) {
      mapping.set(j, j);
    }

    // Now create new agg calls, and populate mapping for them.
    relBuilder.push(newInput);
    final List<RelBuilder.AggCall> newAggCallList = new ArrayList<>();
    j = groupCount;
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      if (fieldsUsed.get(j)) {
        mapping.set(j, groupCount + newAggCallList.size());
        newAggCallList.add(relBuilder.aggregateCall(aggCall, inputMapping));
      }
      ++j;
    }

    if (newAggCallList.isEmpty() && newGroupSet.isEmpty()) {
      // Add a dummy call if all the column fields have been trimmed
      mapping =
          Mappings.create(MappingType.INVERSE_SURJECTION,
              mapping.getSourceCount(), 1);
      newAggCallList.add(relBuilder.count(false, "DUMMY"));
    }

    final RelBuilder.GroupKey groupKey = relBuilder.groupKey(newGroupSet, newGroupSets);
    relBuilder.aggregate(groupKey, newAggCallList);

    final RelNode newAggregate = relBuilder.build();
    return result(newAggregate, mapping, aggregate);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.logical.LogicalTableModify}.
   */
  public TrimResult trimFields(
      LogicalTableModify modifier,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    // Ignore what consumer wants. We always project all columns.
    Util.discard(fieldsUsed);

    final RelDataType rowType = modifier.getRowType();
    final int fieldCount = rowType.getFieldCount();
    RelNode input = modifier.getInput();

    // We want all fields from the child.
    final int inputFieldCount = input.getRowType().getFieldCount();
    final ImmutableBitSet inputFieldsUsed =
        ImmutableBitSet.range(inputFieldCount);

    // Create input with trimmed columns.
    final Set<RelDataTypeField> inputExtraFields = Collections.emptySet();
    TrimResult trimResult =
        trimChild(modifier, input, inputFieldsUsed, inputExtraFields);
    RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;
    if (!inputMapping.isIdentity()) {
      // We asked for all fields. Can't believe that the child decided
      // to permute them!
      throw new AssertionError(
          "Expected identity mapping, got " + inputMapping);
    }

    LogicalTableModify newModifier = modifier;
    if (newInput != input) {
      newModifier =
          modifier.copy(
              modifier.getTraitSet(),
              Collections.singletonList(newInput));
    }
    assert newModifier.getClass() == modifier.getClass();

    // Always project all fields.
    Mapping mapping = Mappings.createIdentity(fieldCount);
    return result(newModifier, mapping, modifier);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.logical.LogicalTableFunctionScan}.
   */
  public TrimResult trimFields(
      LogicalTableFunctionScan tabFun,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = tabFun.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final List<RelNode> newInputs = new ArrayList<>();

    for (RelNode input : tabFun.getInputs()) {
      final int inputFieldCount = input.getRowType().getFieldCount();
      ImmutableBitSet inputFieldsUsed = ImmutableBitSet.range(inputFieldCount);

      // Create input with trimmed columns.
      final Set<RelDataTypeField> inputExtraFields =
          Collections.emptySet();
      TrimResult trimResult =
          trimChildRestore(
              tabFun, input, inputFieldsUsed, inputExtraFields);
      assert trimResult.right.isIdentity();
      newInputs.add(trimResult.left);
    }

    LogicalTableFunctionScan newTabFun = tabFun;
    if (!tabFun.getInputs().equals(newInputs)) {
      newTabFun =
          tabFun.copy(tabFun.getTraitSet(), newInputs, tabFun.getCall(),
              tabFun.getElementType(), tabFun.getRowType(),
              tabFun.getColumnMappings());
    }
    assert newTabFun.getClass() == tabFun.getClass();

    // Always project all fields.
    Mapping mapping = Mappings.createIdentity(fieldCount);
    return result(newTabFun, mapping, tabFun);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.logical.LogicalValues}.
   */
  public TrimResult trimFields(
      LogicalValues values,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = values.getRowType();
    final int fieldCount = rowType.getFieldCount();

    // If they are asking for no fields, we can't give them what they want,
    // because zero-column records are illegal. Give them the last field,
    // which is unlikely to be a system field.
    if (fieldsUsed.isEmpty()) {
      fieldsUsed = ImmutableBitSet.range(fieldCount - 1, fieldCount);
    }

    // If all fields are used, return unchanged.
    if (fieldsUsed.equals(ImmutableBitSet.range(fieldCount))) {
      Mapping mapping = Mappings.createIdentity(fieldCount);
      return result(values, mapping);
    }

    final ImmutableList.Builder<ImmutableList<RexLiteral>> newTuples =
        ImmutableList.builder();
    for (ImmutableList<RexLiteral> tuple : values.getTuples()) {
      ImmutableList.Builder<RexLiteral> newTuple = ImmutableList.builder();
      for (int field : fieldsUsed) {
        newTuple.add(tuple.get(field));
      }
      newTuples.add(newTuple.build());
    }

    final Mapping mapping = createMapping(fieldsUsed, fieldCount);
    final RelDataType newRowType =
        RelOptUtil.permute(values.getCluster().getTypeFactory(), rowType,
            mapping);
    final LogicalValues newValues =
        LogicalValues.create(values.getCluster(), newRowType,
            newTuples.build());
    return result(newValues, mapping, values);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.core.Sample}.
   */
  public TrimResult trimFields(
      final Sample sample,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = sample.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final RelNode input = sample.getInput();

    // Create input with trimmed columns.
    final Set<RelDataTypeField> inputExtraFields = Collections.emptySet();
    TrimResult trimResult =
        trimChild(sample, input, fieldsUsed, inputExtraFields);
    final RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;

    // If the input is unchanged, and we need to project all columns,
    // there's nothing we can do.
    if (newInput == input
        && fieldsUsed.cardinality() == fieldCount) {
      return result(sample, Mappings.createIdentity(fieldCount));
    }

    final RelNode newSample =
        sample.copy(sample.getTraitSet(), ImmutableList.of(newInput));
    return result(newSample, inputMapping, sample);
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.core.Snapshot}.
   */
  public TrimResult trimFields(
      final Snapshot snapshot,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final RelDataType rowType = snapshot.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final RelNode input = snapshot.getInput();

    // Create input with trimmed columns.
    final Set<RelDataTypeField> inputExtraFields = Collections.emptySet();
    TrimResult trimResult =
        trimChild(snapshot, input, fieldsUsed, inputExtraFields);
    final RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;

    // If the input is unchanged, and we need to project all columns,
    // there's nothing we can do.
    if (newInput == input
        && fieldsUsed.cardinality() == fieldCount) {
      return result(snapshot, Mappings.createIdentity(fieldCount));
    }

    final Snapshot newSnapshot =
        snapshot.copy(snapshot.getTraitSet(), newInput,
        snapshot.getPeriod());
    return result(newSnapshot, inputMapping, snapshot);
  }

  /**
   * Trims {@link LogicalCorrelate} nodes.
   */
  public TrimResult trimFields(LogicalCorrelate correlate,
      ImmutableBitSet fieldsUsed, Set<RelDataTypeField> extraFields) {
    if (!extraFields.isEmpty()) {
      // bail out with generic trim
      return trimFields((RelNode) correlate, fieldsUsed, extraFields);
    }

    fieldsUsed = fieldsUsed.union(correlate.getRequiredColumns());

    List<RelNode> newInputs = new ArrayList<>();
    List<Mapping> inputMappings = new ArrayList<>();
    int changeCount = 0;
    int offset = 0;
    for (RelNode input : correlate.getInputs()) {
      final RelDataType inputRowType = input.getRowType();
      final int inputFieldCount = inputRowType.getFieldCount();

      ImmutableBitSet currentInputFieldsUsed = fieldsUsed
          .intersect(ImmutableBitSet.range(offset, offset + inputFieldCount))
          .shift(-offset);

      TrimResult trimResult =
          dispatchTrimFields(input, currentInputFieldsUsed, extraFields);

      newInputs.add(trimResult.left);
      inputMappings.add(trimResult.right);

      offset += inputFieldCount;

      if (trimResult.left != input) {
        changeCount++;
      }
    }

    if (changeCount == 0) {
      return result(correlate,
          Mappings.createIdentity(correlate.getRowType().getFieldCount()));
    }

    Mapping mapping = Mappings.concatenateMappings(inputMappings);
    RexBuilder rexBuilder = relBuilder.getRexBuilder();

    RelNode newLeft = newInputs.get(0);
    RexCorrelVariableMapShuttle rexVisitor =
        new RexCorrelVariableMapShuttle(correlate.getCorrelationId(),
            newLeft.getRowType(), mapping, rexBuilder);
    RelNode newRight =
        newInputs.get(1).accept(new RexRewritingRelShuttle(rexVisitor));
    final LogicalCorrelate newCorrelate =
        correlate
            .copy(correlate.getTraitSet(),
                newLeft,
                newRight,
                correlate.getCorrelationId(),
                correlate.getRequiredColumns().permute(mapping),
                correlate.getJoinType());

    return result(newCorrelate, mapping);
  }

  protected Mapping createMapping(ImmutableBitSet fieldsUsed, int fieldCount) {
    final Mapping mapping =
        Mappings.create(
            MappingType.INVERSE_SURJECTION,
            fieldCount,
            fieldsUsed.cardinality());
    int i = 0;
    for (int field : fieldsUsed) {
      mapping.set(field, i++);
    }
    return mapping;
  }

  /**
   * Variant of {@link #trimFields(RelNode, ImmutableBitSet, Set)} for
   * {@link org.apache.calcite.rel.logical.LogicalTableScan}.
   */
  public TrimResult trimFields(
      final TableScan tableAccessRel,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields) {
    final int fieldCount = tableAccessRel.getRowType().getFieldCount();
    if (fieldsUsed.equals(ImmutableBitSet.range(fieldCount))
        && extraFields.isEmpty()) {
      // if there is nothing to project or if we are projecting everything
      // then no need to introduce another RelNode
      return trimFields(
          (RelNode) tableAccessRel, fieldsUsed, extraFields);
    }
    final RelNode newTableAccessRel =
        tableAccessRel.project(fieldsUsed, extraFields, relBuilder);

    // Some parts of the system can't handle rows with zero fields, so
    // pretend that one field is used.
    if (fieldsUsed.cardinality() == 0) {
      RelNode input = newTableAccessRel;
      if (input instanceof Project) {
        // The table has implemented the project in the obvious way - by
        // creating project with 0 fields. Strip it away, and create our own
        // project with one field.
        Project project = (Project) input;
        if (project.getRowType().getFieldCount() == 0) {
          input = project.getInput();
        }
      }
      return dummyProject(fieldCount, input);
    }

    final Mapping mapping = createMapping(fieldsUsed, fieldCount);
    return result(newTableAccessRel, mapping, tableAccessRel);
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Result of an attempt to trim columns from a relational expression.
   *
   * <p>The mapping describes where to find the columns wanted by the parent
   * of the current relational expression.
   *
   * <p>The mapping is a
   * {@link org.apache.calcite.util.mapping.Mappings.SourceMapping}, which means
   * that no column can be used more than once, and some columns are not used.
   * {@code columnsUsed.getSource(i)} returns the source of the i'th output
   * field.
   *
   * <p>For example, consider the mapping for a relational expression that
   * has 4 output columns but only two are being used. The mapping
   * {2 &rarr; 1, 3 &rarr; 0} would give the following behavior:
   *
   * <ul>
   * <li>columnsUsed.getSourceCount() returns 4
   * <li>columnsUsed.getTargetCount() returns 2
   * <li>columnsUsed.getSource(0) returns 3
   * <li>columnsUsed.getSource(1) returns 2
   * <li>columnsUsed.getSource(2) throws IndexOutOfBounds
   * <li>columnsUsed.getTargetOpt(3) returns 0
   * <li>columnsUsed.getTargetOpt(0) returns -1
   * </ul>
   */
  protected static class TrimResult extends Pair<RelNode, Mapping> {
    /**
     * Creates a TrimResult.
     *
     * @param left  New relational expression
     * @param right Mapping of fields onto original fields
     */
    public TrimResult(RelNode left, Mapping right) {
      super(left, right);
      assert right.getTargetCount() == left.getRowType().getFieldCount()
          : "rowType: " + left.getRowType() + ", mapping: " + right;
    }
  }
}
