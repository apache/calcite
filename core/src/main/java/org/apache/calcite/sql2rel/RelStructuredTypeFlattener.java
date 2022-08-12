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
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sample;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSnapshot;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalSortExchange;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.stream.LogicalChi;
import org.apache.calcite.rel.stream.LogicalDelta;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitDispatcher;
import org.apache.calcite.util.ReflectiveVisitor;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.Lists;
import com.google.common.collect.SortedSetMultimap;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.common.value.qual.MinLen;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.SortedSet;

import static java.util.Objects.requireNonNull;

// TODO jvs 10-Feb-2005:  factor out generic rewrite helper, with the
// ability to map between old and new rels and field ordinals.  Also,
// for now need to prohibit queries which return UDT instances.

/**
 * RelStructuredTypeFlattener removes all structured types from a tree of
 * relational expressions. Because it must operate globally on the tree, it is
 * implemented as an explicit self-contained rewrite operation instead of via
 * normal optimizer rules. This approach has the benefit that real optimizer and
 * codegen rules never have to deal with structured types.
 *
 * <p>As an example, suppose we have a structured type <code>ST(A1 smallint, A2
 * bigint)</code>, a table <code>T(c1 ST, c2 double)</code>, and a query <code>
 * select t.c2, t.c1.a2 from t</code>. After SqlToRelConverter executes, the
 * unflattened tree looks like:
 *
 * <blockquote><pre><code>
 * LogicalProject(C2=[$1], A2=[$0.A2])
 *   LogicalTableScan(table=[T])
 * </code></pre></blockquote>
 *
 * <p>After flattening, the resulting tree looks like
 *
 * <blockquote><pre><code>
 * LogicalProject(C2=[$3], A2=[$2])
 *   FtrsIndexScanRel(table=[T], index=[clustered])
 * </code></pre></blockquote>
 *
 * <p>The index scan produces a flattened row type <code>(boolean, smallint,
 * bigint, double)</code> (the boolean is a null indicator for c1), and the
 * projection picks out the desired attributes (omitting <code>$0</code> and
 * <code>$1</code> altogether). After optimization, the projection might be
 * pushed down into the index scan, resulting in a final tree like
 *
 * <blockquote><pre><code>
 * FtrsIndexScanRel(table=[T], index=[clustered], projection=[3, 2])
 * </code></pre></blockquote>
 */
public class RelStructuredTypeFlattener implements ReflectiveVisitor {
  //~ Instance fields --------------------------------------------------------

  private final RelBuilder relBuilder;
  private final RexBuilder rexBuilder;
  private final boolean restructure;

  private final Map<RelNode, RelNode> oldToNewRelMap = new HashMap<>();
  private @Nullable RelNode currentRel;
  private int iRestructureInput;
  @SuppressWarnings("unused")
  private @Nullable RelDataType flattenedRootType;
  boolean restructured;
  private final RelOptTable.ToRelContext toRelContext;

  //~ Constructors -----------------------------------------------------------

  @Deprecated // to be removed before 2.0
  public RelStructuredTypeFlattener(
      RexBuilder rexBuilder,
      RelOptTable.ToRelContext toRelContext,
      boolean restructure) {
    this(RelFactories.LOGICAL_BUILDER.create(toRelContext.getCluster(), null),
        rexBuilder, toRelContext, restructure);
  }

  public RelStructuredTypeFlattener(
      RelBuilder relBuilder,
      RexBuilder rexBuilder,
      RelOptTable.ToRelContext toRelContext,
      boolean restructure) {
    this.relBuilder = relBuilder;
    this.rexBuilder = rexBuilder;
    this.toRelContext = toRelContext;
    this.restructure = restructure;
  }

  //~ Methods ----------------------------------------------------------------

  private RelNode getCurrentRelOrThrow() {
    return requireNonNull(currentRel, "currentRel");
  }

  public void updateRelInMap(
      SortedSetMultimap<RelNode, CorrelationId> mapRefRelToCorVar) {
    for (RelNode rel : Lists.newArrayList(mapRefRelToCorVar.keySet())) {
      if (oldToNewRelMap.containsKey(rel)) {
        SortedSet<CorrelationId> corVarSet =
            mapRefRelToCorVar.removeAll(rel);
        mapRefRelToCorVar.putAll(oldToNewRelMap.get(rel), corVarSet);
      }
    }
  }

  @SuppressWarnings({"JdkObsolete", "ModifyCollectionInEnhancedForLoop"})
  public void updateRelInMap(
      SortedMap<CorrelationId, LogicalCorrelate> mapCorVarToCorRel) {
    for (CorrelationId corVar : mapCorVarToCorRel.keySet()) {
      LogicalCorrelate oldRel = mapCorVarToCorRel.get(corVar);
      if (oldToNewRelMap.containsKey(oldRel)) {
        RelNode newRel = oldToNewRelMap.get(oldRel);
        assert newRel instanceof LogicalCorrelate;
        mapCorVarToCorRel.put(corVar, (LogicalCorrelate) newRel);
      }
    }
  }

  public RelNode rewrite(RelNode root) {
    // Perform flattening.
    final RewriteRelVisitor visitor = new RewriteRelVisitor();
    visitor.visit(root, 0, null);
    RelNode flattened = getNewForOldRel(root);
    flattenedRootType = flattened.getRowType();
    if (restructure) {
      return tryRestructure(root, flattened);
    }
    return flattened;
  }

  private RelNode tryRestructure(RelNode root, RelNode flattened) {
    iRestructureInput = 0;
    restructured = false;
    final List<RexNode> structuringExps = restructureFields(root.getRowType());
    if (restructured) {
      List<String> resultFieldNames = root.getRowType().getFieldNames();
      // If requested, add an additional projection which puts
      // everything back into structured form for return to the
      // client.
      RelNode restructured = relBuilder.push(flattened)
          .projectNamed(structuringExps, resultFieldNames, true)
          .build();
      restructured = RelOptUtil.copyRelHints(flattened, restructured);
      // REVIEW jvs 23-Mar-2005:  How do we make sure that this
      // implementation stays in Java?  Fennel can't handle
      // structured types.
      return restructured;
    } else {
      return flattened;
    }
  }

  /**
   * When called with old root rowType it's known that flattened root (which may become input)
   * returns flat fields, so it simply refers flat fields by increasing index and collects them
   * back into struct constructor expressions if necessary.
   *
   * @param structuredType old root rowType or it's nested struct
   * @return list of rex nodes, some of them may collect flattened struct's fields back
   *         into original structure to return correct type for client
   */
  private List<RexNode> restructureFields(RelDataType structuredType) {
    final List<RexNode> structuringExps = new ArrayList<>();
    for (RelDataTypeField field : structuredType.getFieldList()) {
      final RelDataType fieldType = field.getType();
      RexNode expr;
      if (fieldType.isStruct()) {
        restructured = true;
        expr = restructure(fieldType);
      } else {
        expr = new RexInputRef(iRestructureInput++, fieldType);
      }
      structuringExps.add(expr);
    }
    return structuringExps;
  }

  private RexNode restructure(RelDataType structuredType) {
    // Use ROW(f1,f2,...,fn) to put flattened data back together into a structure.
    List<RexNode> structFields = restructureFields(structuredType);
    RexNode rowConstructor = rexBuilder.makeCall(
        structuredType,
        SqlStdOperatorTable.ROW,
        structFields);
    return rowConstructor;
  }

  protected void setNewForOldRel(RelNode oldRel, RelNode newRel) {
    newRel = RelOptUtil.copyRelHints(oldRel, newRel);
    oldToNewRelMap.put(oldRel, newRel);
  }

  protected RelNode getNewForOldRel(RelNode oldRel) {
    return requireNonNull(
        oldToNewRelMap.get(oldRel),
        () -> "newRel not found for " + oldRel);
  }

  /**
   * Maps the ordinal of a field pre-flattening to the ordinal of the
   * corresponding field post-flattening.
   *
   * @param oldOrdinal Pre-flattening ordinal
   * @return Post-flattening ordinal
   */
  protected int getNewForOldInput(int oldOrdinal) {
    return getNewFieldForOldInput(oldOrdinal).i;
  }

  /**
   * Finds type and new ordinal relative to new inputs by oldOrdinal and
   * innerOrdinal indexes.
   *
   * @param oldOrdinal   ordinal of the field relative to old inputs
   * @param innerOrdinal when oldOrdinal points to struct and target field
   *                     is inner field of struct, this argument should contain
   *                     calculated field's ordinal within struct after flattening.
   *                     Otherwise when oldOrdinal points to primitive field, this
   *                     argument should be zero.
   * @return flat type with new ordinal relative to new inputs
   */
  private Ord<RelDataType> getNewFieldForOldInput(int oldOrdinal, int innerOrdinal) {
    // sum of predecessors post flatten sizes points to new ordinal
    // of flat field or first field of flattened struct
    final int postFlatteningOrdinal = getCurrentRelOrThrow().getInputs().stream()
        .flatMap(node -> node.getRowType().getFieldList().stream())
        .limit(oldOrdinal)
        .map(RelDataTypeField::getType)
        .mapToInt(this::postFlattenSize)
        .sum();
    final int newOrdinal = postFlatteningOrdinal + innerOrdinal;
    // NoSuchElementException may be thrown because of two reasons:
    // 1. postFlattenSize() didn't predict well
    // 2. innerOrdinal has wrong value
    RelDataTypeField newField = getNewInputFieldByNewOrdinal(newOrdinal);
    return Ord.of(newOrdinal, newField.getType());
  }

  private RelDataTypeField getNewInputFieldByNewOrdinal(int newOrdinal) {
    return getCurrentRelOrThrow().getInputs().stream()
          .map(this::getNewForOldRel)
          .flatMap(node -> node.getRowType().getFieldList().stream())
          .skip(newOrdinal)
          .findFirst()
          .orElseThrow(NoSuchElementException::new);
  }

  /** Returns whether the old field at index {@code fieldIdx} was not flattened. */
  private boolean noFlatteningForInput(int fieldIdx) {
    final List<RelNode> inputs = getCurrentRelOrThrow().getInputs();
    int fieldCnt = 0;
    for (RelNode input : inputs) {
      fieldCnt += input.getRowType().getFieldCount();
      if (fieldCnt > fieldIdx) {
        return getNewForOldRel(input).getRowType().getFieldList().size()
            == input.getRowType().getFieldList().size();
      }
    }
    return false;
  }

  /**
   * Maps the ordinal of a field pre-flattening to the ordinal of the
   * corresponding field post-flattening, and also returns its type.
   *
   * @param oldOrdinal Pre-flattening ordinal
   * @return Post-flattening ordinal and type
   */
  protected Ord<RelDataType> getNewFieldForOldInput(int oldOrdinal) {
    return getNewFieldForOldInput(oldOrdinal, 0);
  }

  /**
   * Returns a mapping between old and new fields.
   *
   * @param oldRel Old relational expression
   * @return Mapping between fields of old and new
   */
  private Mappings.TargetMapping getNewForOldInputMapping(RelNode oldRel) {
    final RelNode newRel = getNewForOldRel(oldRel);
    return Mappings.target(
        this::getNewForOldInput,
        oldRel.getRowType().getFieldCount(),
        newRel.getRowType().getFieldCount());
  }

  private int getPostFlatteningOrdinal(RelDataType preFlattenRowType, int preFlattenOrdinal) {
    return preFlattenRowType.getFieldList().stream()
        .limit(preFlattenOrdinal)
        .map(RelDataTypeField::getType)
        .mapToInt(this::postFlattenSize)
        .sum();
  }

  private int postFlattenSize(RelDataType type) {
    if (type.isStruct()) {
      return type.getFieldList().stream()
          .map(RelDataTypeField::getType)
          .mapToInt(this::postFlattenSize)
          .sum();
    } else {
      return 1;
    }
  }

  public void rewriteRel(LogicalTableModify rel) {
    LogicalTableModify newRel =
        LogicalTableModify.create(
            rel.getTable(),
            rel.getCatalogReader(),
            getNewForOldRel(rel.getInput()),
            rel.getOperation(),
            rel.getUpdateColumnList(),
            rel.getSourceExpressionList(),
            true);
    setNewForOldRel(rel, newRel);
  }

  public void rewriteRel(LogicalAggregate rel) {
    RelNode oldInput = rel.getInput();
    RelDataType inputType = oldInput.getRowType();
    List<RelDataTypeField> inputTypeFields = inputType.getFieldList();
    if (SqlTypeUtil.isFlat(inputType) || rel.getAggCallList().stream().allMatch(
        call -> call.getArgList().isEmpty()
            || call.getArgList().stream().noneMatch(idx -> inputTypeFields.get(idx)
            .getType().isStruct()))) {
      rewriteGeneric(rel);
    } else {
      // one of aggregate calls definitely refers to field with struct type from oldInput,
      // let's restructure new input back and use restructured one as new input for aggregate node
      RelNode restructuredInput = tryRestructure(oldInput, getNewForOldRel(oldInput));
      // expected that after restructuring indexes in AggregateCalls again became relevant,
      // leave it as is but with new input
      RelNode newRel = rel.copy(rel.getTraitSet(), restructuredInput, rel.getGroupSet(),
          rel.getGroupSets(), rel.getAggCallList());
      if (!SqlTypeUtil.isFlat(rel.getRowType())) {
        newRel = coverNewRelByFlatteningProjection(rel, newRel);
      }
      setNewForOldRel(rel, newRel);
    }
  }

  public void rewriteRel(Sort rel) {
    RelCollation oldCollation = rel.getCollation();
    final RelNode oldChild = rel.getInput();
    final RelNode newChild = getNewForOldRel(oldChild);
    final Mappings.TargetMapping mapping =
        getNewForOldInputMapping(oldChild);

    // validate
    for (RelFieldCollation field : oldCollation.getFieldCollations()) {
      int oldInput = field.getFieldIndex();
      RelDataType sortFieldType =
          oldChild.getRowType().getFieldList().get(oldInput).getType();
      if (sortFieldType.isStruct()) {
        // TODO jvs 10-Feb-2005
        throw Util.needToImplement("sorting on structured types");
      }
    }
    RelCollation newCollation = RexUtil.apply(mapping, oldCollation);
    Sort newRel =
        LogicalSort.create(newChild, newCollation, rel.offset, rel.fetch);
    setNewForOldRel(rel, newRel);
  }

  public void rewriteRel(LogicalFilter rel) {
    RelTraitSet traits = rel.getTraitSet();
    RewriteRexShuttle rewriteRexShuttle = new RewriteRexShuttle();
    RexNode oldCondition = rel.getCondition();
    RelNode newInput = getNewForOldRel(rel.getInput());
    RexNode newCondition = oldCondition.accept(rewriteRexShuttle);
    LogicalFilter newRel = rel.copy(traits, newInput, newCondition);
    setNewForOldRel(rel, newRel);
  }

  public void rewriteRel(LogicalJoin rel) {
    final LogicalJoin newRel =
        LogicalJoin.create(getNewForOldRel(rel.getLeft()),
            getNewForOldRel(rel.getRight()),
            rel.getHints(),
            rel.getCondition().accept(new RewriteRexShuttle()),
            rel.getVariablesSet(), rel.getJoinType());
    setNewForOldRel(rel, newRel);
  }

  public void rewriteRel(LogicalCorrelate rel) {
    ImmutableBitSet.Builder newPos = ImmutableBitSet.builder();
    for (int pos : rel.getRequiredColumns()) {
      RelDataType corrFieldType =
          rel.getLeft().getRowType().getFieldList().get(pos)
              .getType();
      if (corrFieldType.isStruct()) {
        throw Util.needToImplement("correlation on structured type");
      }
      newPos.set(getNewForOldInput(pos));
    }
    LogicalCorrelate newRel =
        LogicalCorrelate.create(getNewForOldRel(rel.getLeft()),
            getNewForOldRel(rel.getRight()),
            rel.getHints(),
            rel.getCorrelationId(),
            newPos.build(),
            rel.getJoinType());
    setNewForOldRel(rel, newRel);
  }

  public void rewriteRel(Collect rel) {
    rewriteGeneric(rel);
  }

  public void rewriteRel(Uncollect rel) {
    rewriteGeneric(rel);
  }

  public void rewriteRel(LogicalIntersect rel) {
    rewriteGeneric(rel);
  }

  public void rewriteRel(LogicalMinus rel) {
    rewriteGeneric(rel);
  }

  public void rewriteRel(LogicalUnion rel) {
    rewriteGeneric(rel);
  }

  public void rewriteRel(LogicalValues rel) {
    // NOTE jvs 30-Apr-2006:  UDT instances require invocation
    // of a constructor method, which can't be represented
    // by the tuples stored in a LogicalValues, so we don't have
    // to worry about them here.
    rewriteGeneric(rel);
  }

  public void rewriteRel(LogicalTableFunctionScan rel) {
    rewriteGeneric(rel);
  }

  public void rewriteRel(Sample rel) {
    rewriteGeneric(rel);
  }

  public void rewriteRel(LogicalProject rel) {
    RewriteRexShuttle shuttle = new RewriteRexShuttle();
    List<RexNode> oldProjects = rel.getProjects();
    List<String> oldNames = rel.getRowType().getFieldNames();
    List<Pair<RexNode, String>> flattenedExpList = new ArrayList<>();
    flattenProjections(shuttle, oldProjects, oldNames, "", flattenedExpList);
    RelNode newInput = getNewForOldRel(rel.getInput());
    List<RexNode> newProjects = Pair.left(flattenedExpList);
    List<String> newNames = Pair.right(flattenedExpList);
    final RelNode newRel = relBuilder.push(newInput)
        .projectNamed(newProjects, newNames, true)
        .hints(rel.getHints())
        .build();
    setNewForOldRel(rel, newRel);
  }

  public void rewriteRel(LogicalCalc rel) {
    // Translate the child.
    final RelNode newInput = getNewForOldRel(rel.getInput());

    final RelOptCluster cluster = rel.getCluster();
    RexProgramBuilder programBuilder =
        new RexProgramBuilder(
            newInput.getRowType(),
            cluster.getRexBuilder());

    // Convert the common expressions.
    final RexProgram program = rel.getProgram();
    final RewriteRexShuttle shuttle = new RewriteRexShuttle();
    for (RexNode expr : program.getExprList()) {
      programBuilder.registerInput(expr.accept(shuttle));
    }

    // Convert the projections.
    final List<Pair<RexNode, String>> flattenedExpList = new ArrayList<>();
    List<String> fieldNames = rel.getRowType().getFieldNames();
    flattenProjections(new RewriteRexShuttle(),
        program.getProjectList(),
        fieldNames,
        "",
        flattenedExpList);

    // Register each of the new projections.
    for (Pair<RexNode, String> flattenedExp : flattenedExpList) {
      programBuilder.addProject(flattenedExp.left, flattenedExp.right);
    }

    // Translate the condition.
    final RexLocalRef conditionRef = program.getCondition();
    if (conditionRef != null) {
      final Ord<RelDataType> newField =
          getNewFieldForOldInput(conditionRef.getIndex());
      programBuilder.addCondition(new RexLocalRef(newField.i, newField.e));
    }

    RexProgram newProgram = programBuilder.getProgram();

    // Create a new calc relational expression.
    LogicalCalc newRel = LogicalCalc.create(newInput, newProgram);
    setNewForOldRel(rel, newRel);
  }

  public void rewriteRel(SelfFlatteningRel rel) {
    rel.flattenRel(this);
  }

  public void rewriteRel(LogicalExchange rel) {
    rewriteGeneric(rel);
  }

  public void rewriteRel(LogicalSortExchange rel) {
    rewriteGeneric(rel);
  }

  public void rewriteGeneric(RelNode rel) {
    RelNode newRel = rel.copy(rel.getTraitSet(), rel.getInputs());
    List<RelNode> oldInputs = rel.getInputs();
    for (int i = 0; i < oldInputs.size(); ++i) {
      newRel.replaceInput(
          i,
          getNewForOldRel(oldInputs.get(i)));
    }
    setNewForOldRel(rel, newRel);
  }

  private void flattenProjections(RewriteRexShuttle shuttle,
      List<? extends RexNode> exps,
      @Nullable List<? extends @Nullable String> fieldNames,
      String prefix,
      List<Pair<RexNode, String>> flattenedExps) {
    for (int i = 0; i < exps.size(); ++i) {
      RexNode exp = exps.get(i);
      String fieldName = extractName(fieldNames, prefix, i);
      flattenProjection(shuttle, exp, fieldName, flattenedExps);
    }
  }

  private static String extractName(@Nullable List<? extends @Nullable String> fieldNames,
      String prefix, int i) {
    String fieldName = (fieldNames == null || fieldNames.get(i) == null)
        ? ("$" + i)
        : fieldNames.get(i);
    if (!prefix.equals("")) {
      fieldName = prefix + "$" + fieldName;
    }
    return fieldName;
  }

  private void flattenProjection(RewriteRexShuttle shuttle,
      RexNode exp,
      String fieldName,
      List<Pair<RexNode, String>> flattenedExps) {
    if (exp.getType().isStruct()) {
      if (exp instanceof RexInputRef) {
        final int oldOrdinal = ((RexInputRef) exp).getIndex();
        final int flattenFieldsCount = postFlattenSize(exp.getType());
        for (int innerOrdinal = 0; innerOrdinal < flattenFieldsCount; innerOrdinal++) {
          Ord<RelDataType> newField = getNewFieldForOldInput(oldOrdinal, innerOrdinal);
          RexInputRef newRef = new RexInputRef(newField.i, newField.e);
          flattenedExps.add(Pair.of(newRef, fieldName));
        }
      } else if (isConstructor(exp) || exp.isA(SqlKind.CAST)) {
        // REVIEW jvs 27-Feb-2005:  for cast, see corresponding note
        // in RewriteRexShuttle
        RexCall call = (RexCall) exp;
        if (exp.isA(SqlKind.CAST)
            && RexLiteral.isNullLiteral(call.operands.get(0))) {
          // Translate CAST(NULL AS UDT) into
          // the correct number of null fields.
          flattenNullLiteral(exp.getType(), flattenedExps);
          return;
        }
        flattenProjections(shuttle,
            call.getOperands(),
            Collections.nCopies(call.getOperands().size(), null),
            fieldName,
            flattenedExps);
      } else if (exp instanceof RexCall) {
        // NOTE jvs 10-Feb-2005:  This is a lame hack to keep special
        // functions which return row types working.
        RexNode newExp = exp;
        List<RexNode> operands = ((RexCall) exp).getOperands();
        SqlOperator operator = ((RexCall) exp).getOperator();

        if (operator == SqlStdOperatorTable.ITEM
            && operands.get(0).getType().isStruct()
            && operands.get(1).isA(SqlKind.LITERAL)
            && SqlTypeUtil.inCharFamily(operands.get(1).getType())) {
          String literalString = ((RexLiteral) operands.get(1)).getValueAs(String.class);
          RexNode firstOp = operands.get(0);

          if (firstOp instanceof RexInputRef) {
            // when performed getting field from struct exp by field name
            // and new input is flattened it's enough to refer target field by index.
            // But it's possible that requested field is also of type struct, that's
            // why we're trying to get range from to. For primitive just one field will be in range.
            int from = 0;
            for (RelDataTypeField field : firstOp.getType().getFieldList()) {
              if (field.getName().equalsIgnoreCase(literalString)) {
                int oldOrdinal = ((RexInputRef) firstOp).getIndex();
                int to = from + postFlattenSize(field.getType());
                for (int newInnerOrdinal = from; newInnerOrdinal < to; newInnerOrdinal++) {
                  Ord<RelDataType> newField = getNewFieldForOldInput(oldOrdinal, newInnerOrdinal);
                  RexInputRef newRef = rexBuilder.makeInputRef(newField.e, newField.i);
                  flattenedExps.add(Pair.of(newRef, fieldName));
                }
                break;
              } else {
                from += postFlattenSize(field.getType());
              }
            }
          } else if (firstOp instanceof RexCall) {
            // to get nested struct from return type of firstOp rex call,
            // we need to flatten firstOp and get range of expressions which
            // corresponding to desirable nested struct flattened fields
            List<Pair<RexNode, String>> firstOpFlattenedExps = new ArrayList<>();
            flattenProjection(shuttle, firstOp, fieldName + "$0", firstOpFlattenedExps);
            int newInnerOrdinal = getNewInnerOrdinal(firstOp, literalString);
            int endOfRange = newInnerOrdinal + postFlattenSize(newExp.getType());
            for (int i = newInnerOrdinal; i < endOfRange; i++) {
              flattenedExps.add(firstOpFlattenedExps.get(i));
            }
          }
        } else {
          newExp = rexBuilder.makeCall(exp.getType(), operator,
              shuttle.visitList(operands));
          // flatten call result type
          flattenResultTypeOfRexCall(newExp, fieldName, flattenedExps);
        }
      } else {
        throw Util.needToImplement(exp);
      }
    } else {
      flattenedExps.add(
          Pair.of(exp.accept(shuttle), fieldName));
    }
  }

  private void flattenResultTypeOfRexCall(RexNode newExp,
      String fieldName,
      List<Pair<RexNode, String>> flattenedExps) {
    int nameIdx = 0;
    for (RelDataTypeField field : newExp.getType().getFieldList()) {
      RexNode fieldRef = rexBuilder.makeFieldAccess(newExp, field.getIndex());
      String fieldRefName = fieldName + "$" + nameIdx++;
      if (fieldRef.getType().isStruct()) {
        flattenResultTypeOfRexCall(fieldRef, fieldRefName, flattenedExps);
      } else {
        flattenedExps.add(Pair.of(fieldRef, fieldRefName));
      }
    }
  }

  private void flattenNullLiteral(
      RelDataType type,
      List<Pair<RexNode, String>> flattenedExps) {
    RelDataType flattenedType =
        SqlTypeUtil.flattenRecordType(rexBuilder.getTypeFactory(), type, null);
    for (RelDataTypeField field : flattenedType.getFieldList()) {
      flattenedExps.add(
          Pair.of(
              rexBuilder.makeNullLiteral(field.getType()),
              field.getName()));
    }
  }

  private static boolean isConstructor(RexNode rexNode) {
    // TODO jvs 11-Feb-2005:  share code with SqlToRelConverter
    if (!(rexNode instanceof RexCall)) {
      return false;
    }
    RexCall call = (RexCall) rexNode;
    return call.getOperator().getName().equalsIgnoreCase("row")
        || call.isA(SqlKind.NEW_SPECIFICATION);
  }

  public void rewriteRel(TableScan rel) {
    RelNode newRel = rel.getTable().toRel(toRelContext);
    if (!SqlTypeUtil.isFlat(rel.getRowType())) {
      newRel = coverNewRelByFlatteningProjection(rel, newRel);
    }
    setNewForOldRel(rel, newRel);
  }

  private RelNode coverNewRelByFlatteningProjection(RelNode rel, RelNode newRel) {
    final List<Pair<RexNode, String>> flattenedExpList = new ArrayList<>();
    RexNode newRowRef = rexBuilder.makeRangeReference(newRel);
    List<RelDataTypeField> inputRowFields = rel.getRowType().getFieldList();
    flattenInputs(inputRowFields, newRowRef, flattenedExpList);
    // cover new scan with flattening projection
    List<RexNode> projects = Pair.left(flattenedExpList);
    List<String> fieldNames = Pair.right(flattenedExpList);
    newRel = relBuilder.push(newRel)
        .projectNamed(projects, fieldNames, true)
        .build();
    return newRel;
  }

  public void rewriteRel(LogicalSnapshot rel) {
    RelNode newRel =
        rel.copy(rel.getTraitSet(),
            getNewForOldRel(rel.getInput()),
            rel.getPeriod().accept(new RewriteRexShuttle()));
    setNewForOldRel(rel, newRel);
  }

  public void rewriteRel(LogicalDelta rel) {
    rewriteGeneric(rel);
  }

  public void rewriteRel(LogicalChi rel) {
    rewriteGeneric(rel);
  }

  public void rewriteRel(LogicalMatch rel) {
    rewriteGeneric(rel);
  }

  /** Generates expressions that reference the flattened input fields from
   * a given row type. */
  private void flattenInputs(List<RelDataTypeField> fieldList, RexNode prefix,
      List<Pair<RexNode, String>> flattenedExpList) {
    for (RelDataTypeField field : fieldList) {
      final RexNode ref =
          rexBuilder.makeFieldAccess(prefix, field.getIndex());
      if (field.getType().isStruct()) {
        final List<RelDataTypeField> structFields = field.getType().getFieldList();
        flattenInputs(structFields, ref, flattenedExpList);
      } else {
        flattenedExpList.add(Pair.of(ref, field.getName()));
      }
    }
  }

  //~ Inner Interfaces -------------------------------------------------------

  /** Mix-in interface for relational expressions that know how to
   * flatten themselves. */
  public interface SelfFlatteningRel extends RelNode {
    void flattenRel(RelStructuredTypeFlattener flattener);
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Visitor that flattens each relational expression in a tree. */
  private class RewriteRelVisitor extends RelVisitor {
    private final ReflectiveVisitDispatcher<RelStructuredTypeFlattener,
            RelNode> dispatcher =
        ReflectUtil.createDispatcher(
            RelStructuredTypeFlattener.class,
            RelNode.class);

    @Override public void visit(RelNode p, int ordinal, @Nullable RelNode parent) {
      // rewrite children first
      super.visit(p, ordinal, parent);

      currentRel = p;
      final String visitMethodName = "rewriteRel";
      boolean found =
          dispatcher.invokeVisitor(
              RelStructuredTypeFlattener.this,
              currentRel,
              visitMethodName);
      currentRel = null;
      if (!found) {
        if (p.getInputs().size() == 0) {
          // for leaves, it's usually safe to assume that
          // no transformation is required
          rewriteGeneric(p);
        } else {
          throw new AssertionError("no '" + visitMethodName
              + "' method found for class " + p.getClass().getName());
        }
      }
    }
  }

  /** Shuttle that rewrites scalar expressions. */
  private class RewriteRexShuttle extends RexShuttle {
    @Override public RexNode visitInputRef(RexInputRef input) {
      final int oldIndex = input.getIndex();
      final Ord<RelDataType> field = getNewFieldForOldInput(oldIndex);
      RelDataTypeField inputFieldByOldIndex = getCurrentRelOrThrow().getInputs().stream()
          .flatMap(relInput -> relInput.getRowType().getFieldList().stream())
          .skip(oldIndex)
          .findFirst()
          .orElseThrow(() ->
              new AssertionError("Found input ref with index not found in old inputs"));
      if (inputFieldByOldIndex.getType().isStruct()) {
        iRestructureInput = field.i;
        List<RexNode> rexNodes = restructureFields(inputFieldByOldIndex.getType());
        return rexBuilder.makeCall(
            inputFieldByOldIndex.getType(),
            SqlStdOperatorTable.ROW,
            rexNodes);
      }

      // Use the actual flattened type, which may be different from the current
      // type.
      RelDataType fieldType = removeDistinct(field.e);
      return new RexInputRef(field.i, fieldType);
    }

    private RelDataType removeDistinct(RelDataType type) {
      if (type.getSqlTypeName() != SqlTypeName.DISTINCT) {
        return type;
      }
      return type.getFieldList().get(0).getType();
    }

    @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      // walk down the field access path expression, calculating
      // the desired input number
      int iInput = 0;
      Deque<Integer> accessOrdinals = new ArrayDeque<>();

      for (;;) {
        RexNode refExp = fieldAccess.getReferenceExpr();
        int ordinal = fieldAccess.getField().getIndex();
        accessOrdinals.push(ordinal);
        iInput +=
            getPostFlatteningOrdinal(
                refExp.getType(),
                ordinal);
        if (refExp instanceof RexInputRef) {
          // Consecutive field accesses over some input can be removed since by now the input
          // is flattened (no struct types). We just have to create a new RexInputRef with the
          // correct ordinal and type.
          RexInputRef inputRef = (RexInputRef) refExp;
          if (noFlatteningForInput(inputRef.getIndex())) {
            // Sanity check, the input must not have struct type fields.
            // We better have a record for each old input field
            // whether it is flattened.
            return fieldAccess;
          }
          final Ord<RelDataType> newField =
              getNewFieldForOldInput(inputRef.getIndex(), iInput);
          return new RexInputRef(newField.getKey(), removeDistinct(newField.getValue()));
        } else if (refExp instanceof RexCorrelVariable) {
          RelDataType refType =
              SqlTypeUtil.flattenRecordType(
                  rexBuilder.getTypeFactory(), refExp.getType(), null);
          refExp = rexBuilder.makeCorrel(refType, ((RexCorrelVariable) refExp).id);
          return rexBuilder.makeFieldAccess(refExp, iInput);
        } else if (refExp instanceof RexCall) {
          // Field accesses over calls cannot be simplified since the result of the call may be
          // a struct type.
          RexCall call = (RexCall) refExp;
          RexNode newRefExp = visitCall(call);
          for (Integer ord : accessOrdinals) {
            newRefExp = rexBuilder.makeFieldAccess(newRefExp, ord);
          }
          return newRefExp;
        } else if (refExp instanceof RexFieldAccess) {
          fieldAccess = (RexFieldAccess) refExp;
        } else {
          throw Util.needToImplement(refExp);
        }
      }
    }

    @Override public RexNode visitCall(RexCall rexCall) {
      if (rexCall.isA(SqlKind.CAST)) {
        RexNode input = rexCall.getOperands().get(0).accept(this);
        RelDataType targetType = removeDistinct(rexCall.getType());
        return rexBuilder.makeCast(
            targetType,
            input);
      }

      if (rexCall.op == SqlStdOperatorTable.ITEM
          && rexCall.operands.get(0).getType().isStruct()
          && rexCall.operands.get(1).isA(SqlKind.LITERAL)
          && SqlTypeUtil.inCharFamily(rexCall.operands.get(1).getType())) {
        RexNode firstOp = rexCall.operands.get(0);
        final String literalString = ((RexLiteral) rexCall.operands.get(1))
            .getValueAs(String.class);
        if (firstOp instanceof RexInputRef) {
          int oldOrdinal = ((RexInputRef) firstOp).getIndex();
          int newInnerOrdinal = getNewInnerOrdinal(firstOp, literalString);
          Ord<RelDataType> newField = getNewFieldForOldInput(oldOrdinal, newInnerOrdinal);
          RexInputRef newRef = rexBuilder.makeInputRef(newField.e, newField.i);
          return newRef;
        } else {
          RexNode newFirstOp = firstOp.accept(this);
          if (newFirstOp instanceof RexInputRef) {
            int newRefOrdinal = ((RexInputRef) newFirstOp).getIndex()
                + getNewInnerOrdinal(firstOp, literalString);
            RelDataTypeField newField = getNewInputFieldByNewOrdinal(newRefOrdinal);
            RexInputRef newRef = rexBuilder.makeInputRef(newField.getType(), newRefOrdinal);
            return newRef;
          }
        }
      }
      if (!rexCall.isA(SqlKind.COMPARISON)) {
        return super.visitCall(rexCall);
      }
      RexNode lhs = rexCall.getOperands().get(0);
      if (!lhs.getType().isStruct()) {
        // NOTE jvs 9-Mar-2005:  Calls like IS NULL operate
        // on the representative null indicator.  Since it comes
        // first, we don't have to do any special translation.
        return super.visitCall(rexCall);
      }

      // NOTE jvs 22-Mar-2005:  Likewise, the null indicator takes
      // care of comparison null semantics without any special casing.
      return flattenComparison(
          rexBuilder,
          rexCall.getOperator(),
          rexCall.getOperands());
    }

    @Override public RexNode visitSubQuery(RexSubQuery subQuery) {
      subQuery = (RexSubQuery) super.visitSubQuery(subQuery);
      RelStructuredTypeFlattener flattener =
          new RelStructuredTypeFlattener(relBuilder, rexBuilder,
              toRelContext, restructure);
      RelNode rel = flattener.rewrite(subQuery.rel);
      return subQuery.clone(rel);
    }

    private RexNode flattenComparison(
        RexBuilder rexBuilder,
        SqlOperator op,
        @MinLen(1) List<RexNode> exprs) {
      final List<Pair<RexNode, String>> flattenedExps = new ArrayList<>();
      flattenProjections(this, exprs, null, "", flattenedExps);
      int n = flattenedExps.size() / 2;
      if (n == 0) {
        throw new IllegalArgumentException("exprs must be non-empty");
      }
      boolean negate = false;
      if (op.getKind() == SqlKind.NOT_EQUALS) {
        negate = true;
        op = SqlStdOperatorTable.EQUALS;
      }
      if ((n > 1) && op.getKind() != SqlKind.EQUALS) {
        throw Util.needToImplement(
            "inequality comparison for row types");
      }
      RexNode conjunction = null;
      for (int i = 0; i < n; ++i) {
        RexNode comparison =
            rexBuilder.makeCall(
                op,
                flattenedExps.get(i).left,
                flattenedExps.get(i + n).left);
        if (conjunction == null) {
          conjunction = comparison;
        } else {
          conjunction =
              rexBuilder.makeCall(
                  SqlStdOperatorTable.AND,
                  conjunction,
                  comparison);
        }
      }
      requireNonNull(conjunction, "conjunction must be non-null");
      if (negate) {
        return rexBuilder.makeCall(
            SqlStdOperatorTable.NOT,
            conjunction);
      } else {
        return conjunction;
      }
    }

  }

  private int getNewInnerOrdinal(RexNode firstOp, @Nullable String literalString) {
    int newInnerOrdinal = 0;
    for (RelDataTypeField field : firstOp.getType().getFieldList()) {
      if (field.getName().equalsIgnoreCase(literalString)) {
        break;
      } else {
        newInnerOrdinal += postFlattenSize(field.getType());
      }
    }
    return newInnerOrdinal;
  }

}
