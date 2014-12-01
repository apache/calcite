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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Sample;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
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
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitDispatcher;
import org.apache.calcite.util.ReflectiveVisitor;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SortedSetMultimap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;

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
 * <pre><code>
 * LogicalProject(C2=[$1], A2=[$0.A2])
 *   LogicalTableScan(table=[T])
 * </code></pre>
 *
 * After flattening, the resulting tree looks like
 *
 * <pre><code>
 * LogicalProject(C2=[$3], A2=[$2])
 *   FtrsIndexScanRel(table=[T], index=[clustered])
 * </code></pre>
 *
 * The index scan produces a flattened row type <code>(boolean, smallint,
 * bigint, double)</code> (the boolean is a null indicator for c1), and the
 * projection picks out the desired attributes (omitting <code>$0</code> and
 * <code>$1</code> altogether). After optimization, the projection might be
 * pushed down into the index scan, resulting in a final tree like
 *
 * <pre><code>
 * FtrsIndexScanRel(table=[T], index=[clustered], projection=[3, 2])
 * </code></pre>
 */
public class RelStructuredTypeFlattener implements ReflectiveVisitor {
  //~ Instance fields --------------------------------------------------------

  private final RexBuilder rexBuilder;
  private final RewriteRelVisitor visitor;

  private final Map<RelNode, RelNode> oldToNewRelMap = Maps.newHashMap();
  private RelNode currentRel;
  private int iRestructureInput;
  private RelDataType flattenedRootType;
  boolean restructured;
  private final RelOptTable.ToRelContext toRelContext;

  //~ Constructors -----------------------------------------------------------

  public RelStructuredTypeFlattener(
      RexBuilder rexBuilder,
      RelOptTable.ToRelContext toRelContext) {
    this.rexBuilder = rexBuilder;
    this.visitor = new RewriteRelVisitor();
    this.toRelContext = toRelContext;
  }

  //~ Methods ----------------------------------------------------------------

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

  public RelNode rewrite(RelNode root, boolean restructure) {
    // Perform flattening.
    visitor.visit(root, 0, null);
    RelNode flattened = getNewForOldRel(root);
    flattenedRootType = flattened.getRowType();

    // If requested, add an additional projection which puts
    // everything back into structured form for return to the
    // client.
    restructured = false;
    List<RexNode> structuringExps = null;
    if (restructure) {
      iRestructureInput = 0;
      structuringExps = restructureFields(root.getRowType());
    }
    if (restructured) {
      // REVIEW jvs 23-Mar-2005:  How do we make sure that this
      // implementation stays in Java?  Fennel can't handle
      // structured types.
      return RelOptUtil.createProject(
          flattened,
          structuringExps,
          root.getRowType().getFieldNames());
    } else {
      return flattened;
    }
  }

  private List<RexNode> restructureFields(RelDataType structuredType) {
    List<RexNode> structuringExps = new ArrayList<RexNode>();
    for (RelDataTypeField field : structuredType.getFieldList()) {
      // TODO:  row
      if (field.getType().getSqlTypeName() == SqlTypeName.STRUCTURED) {
        restructured = true;
        structuringExps.add(restructure(field.getType()));
      } else {
        structuringExps.add(
            new RexInputRef(
                iRestructureInput,
                field.getType()));
        ++iRestructureInput;
      }
    }
    return structuringExps;
  }

  private RexNode restructure(
      RelDataType structuredType) {
    // Access null indicator for entire structure.
    RexInputRef nullIndicator =
        RexInputRef.of(
            iRestructureInput++, flattenedRootType.getFieldList());

    // Use NEW to put flattened data back together into a structure.
    List<RexNode> inputExprs = restructureFields(structuredType);
    RexNode newInvocation =
        rexBuilder.makeNewInvocation(
            structuredType,
            inputExprs);

    if (!structuredType.isNullable()) {
      // Optimize away the null test.
      return newInvocation;
    }

    // Construct a CASE expression to handle the structure-level null
    // indicator.
    RexNode[] caseOperands = new RexNode[3];

    // WHEN StructuredType.Indicator IS NULL
    caseOperands[0] =
        rexBuilder.makeCall(
            SqlStdOperatorTable.IS_NULL,
            nullIndicator);

    // THEN CAST(NULL AS StructuredType)
    caseOperands[1] =
        rexBuilder.makeCast(
            structuredType,
            rexBuilder.constantNull());

    // ELSE NEW StructuredType(inputs...) END
    caseOperands[2] = newInvocation;

    return rexBuilder.makeCall(
        SqlStdOperatorTable.CASE,
        caseOperands);
  }

  protected void setNewForOldRel(RelNode oldRel, RelNode newRel) {
    oldToNewRelMap.put(oldRel, newRel);
  }

  protected RelNode getNewForOldRel(RelNode oldRel) {
    return oldToNewRelMap.get(oldRel);
  }

  /**
   * Maps the ordinal of a field pre-flattening to the ordinal of the
   * corresponding field post-flattening, and optionally returns its type.
   *
   * @param oldOrdinal Pre-flattening ordinal
   * @return Post-flattening ordinal
   */
  protected int getNewForOldInput(int oldOrdinal) {
    assert currentRel != null;
    int newOrdinal = 0;

    // determine which input rel oldOrdinal references, and adjust
    // oldOrdinal to be relative to that input rel
    RelNode oldInput = null;
    for (RelNode oldInput1 : currentRel.getInputs()) {
      RelDataType oldInputType = oldInput1.getRowType();
      int n = oldInputType.getFieldCount();
      if (oldOrdinal < n) {
        oldInput = oldInput1;
        break;
      }
      RelNode newInput = getNewForOldRel(oldInput1);
      newOrdinal += newInput.getRowType().getFieldCount();
      oldOrdinal -= n;
    }
    assert oldInput != null;

    RelDataType oldInputType = oldInput.getRowType();
    newOrdinal += calculateFlattenedOffset(oldInputType, oldOrdinal);
    return newOrdinal;
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
        new Function<Integer, Integer>() {
          public Integer apply(Integer oldInput) {
            return getNewForOldInput(oldInput);
          }
        },
        oldRel.getRowType().getFieldCount(),
        newRel.getRowType().getFieldCount());
  }

  private int calculateFlattenedOffset(RelDataType rowType, int ordinal) {
    int offset = 0;
    if (SqlTypeUtil.needsNullIndicator(rowType)) {
      // skip null indicator
      ++offset;
    }
    List<RelDataTypeField> oldFields = rowType.getFieldList();
    for (int i = 0; i < ordinal; ++i) {
      RelDataType oldFieldType = oldFields.get(i).getType();
      if (oldFieldType.isStruct()) {
        // TODO jvs 10-Feb-2005:  this isn't terribly efficient;
        // keep a mapping somewhere
        RelDataType flattened =
            SqlTypeUtil.flattenRecordType(
                rexBuilder.getTypeFactory(),
                oldFieldType,
                null);
        final List<RelDataTypeField> fields = flattened.getFieldList();
        offset += fields.size();
      } else {
        ++offset;
      }
    }
    return offset;
  }

  protected RexNode flattenFieldAccesses(RexNode exp) {
    RewriteRexShuttle shuttle = new RewriteRexShuttle();
    return exp.accept(shuttle);
  }

  public void rewriteRel(LogicalTableModify rel) {
    LogicalTableModify newRel =
        new LogicalTableModify(
            rel.getCluster(),
            rel.getTable(),
            rel.getCatalogReader(),
            getNewForOldRel(rel.getInput()),
            rel.getOperation(),
            rel.getUpdateColumnList(),
            true);
    setNewForOldRel(rel, newRel);
  }

  public void rewriteRel(LogicalAggregate rel) {
    RelDataType inputType = rel.getInput().getRowType();
    for (RelDataTypeField field : inputType.getFieldList()) {
      if (field.getType().isStruct()) {
        // TODO jvs 10-Feb-2005
        throw Util.needToImplement("aggregation on structured types");
      }
    }

    rewriteGeneric(rel);
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
        new Sort(
            rel.getCluster(),
            rel.getCluster().traitSetOf(Convention.NONE).plus(newCollation),
            newChild,
            newCollation,
            rel.offset,
            rel.fetch);
    setNewForOldRel(rel, newRel);
  }

  public void rewriteRel(LogicalFilter rel) {
    RelNode newRel =
        RelOptUtil.createFilter(
            getNewForOldRel(rel.getInput()),
            flattenFieldAccesses(rel.getCondition()));
    setNewForOldRel(rel, newRel);
  }

  public void rewriteRel(LogicalJoin rel) {
    LogicalJoin newRel =
        new LogicalJoin(
            rel.getCluster(),
            getNewForOldRel(rel.getLeft()),
            getNewForOldRel(rel.getRight()),
            flattenFieldAccesses(rel.getCondition()),
            rel.getJoinType(),
            rel.getVariablesStopped());
    setNewForOldRel(rel, newRel);
  }

  public void rewriteRel(LogicalCorrelate rel) {
    ImmutableBitSet.Builder newPos = ImmutableBitSet.builder();
    for (Integer pos : rel.getRequiredColumns()) {
      RelDataType corrFieldType =
          rel.getLeft().getRowType().getFieldList().get(pos)
              .getType();
      if (corrFieldType.isStruct()) {
        throw Util.needToImplement("correlation on structured type");
      }
      newPos.set(getNewForOldInput(pos));
    }
    LogicalCorrelate newRel =
        new LogicalCorrelate(
            rel.getCluster(),
            getNewForOldRel(rel.getLeft()),
            getNewForOldRel(rel.getRight()),
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
    final List<RexNode> flattenedExpList = new ArrayList<RexNode>();
    final List<String> flattenedFieldNameList = new ArrayList<String>();
    List<String> fieldNames = rel.getRowType().getFieldNames();
    flattenProjections(
        rel.getProjects(),
        fieldNames,
        "",
        flattenedExpList,
        flattenedFieldNameList);
    RelNode newRel =
        RelOptUtil.createProject(
            getNewForOldRel(rel.getInput()),
            flattenedExpList,
            flattenedFieldNameList);
    setNewForOldRel(rel, newRel);
  }

  public void rewriteRel(LogicalCalc rel) {
    // Translate the child.
    final RelNode newChild = getNewForOldRel(rel.getInput());

    final RelOptCluster cluster = rel.getCluster();
    RexProgramBuilder programBuilder =
        new RexProgramBuilder(
            newChild.getRowType(),
            cluster.getRexBuilder());

    // Convert the common expressions.
    final RexProgram program = rel.getProgram();
    for (RexNode expr : program.getExprList()) {
      programBuilder.registerInput(
          flattenFieldAccesses(expr));
    }

    // Convert the projections.
    final List<RexNode> flattenedExpList = new ArrayList<RexNode>();
    final List<String> flattenedFieldNameList = new ArrayList<String>();
    List<String> fieldNames = rel.getRowType().getFieldNames();
    flattenProjections(
        program.getProjectList(),
        fieldNames,
        "",
        flattenedExpList,
        flattenedFieldNameList);

    // Register each of the new projections.
    int i = -1;
    for (RexNode flattenedExp : flattenedExpList) {
      ++i;
      programBuilder.addProject(
          flattenedExp,
          flattenedFieldNameList.get(i));
    }

    // Translate the condition.
    final RexLocalRef conditionRef = program.getCondition();
    if (conditionRef != null) {
      programBuilder.addCondition(
          new RexLocalRef(
              getNewForOldInput(conditionRef.getIndex()),
              conditionRef.getType()));
    }

    RexProgram newProgram = programBuilder.getProgram();

    // Create a new calc relational expression.
    LogicalCalc newRel =
        new LogicalCalc(
            cluster,
            rel.getTraitSet(),
            newChild,
            newProgram.getOutputRowType(),
            newProgram,
            Collections.<RelCollation>emptyList());
    setNewForOldRel(rel, newRel);
  }

  public void rewriteRel(SelfFlatteningRel rel) {
    rel.flattenRel(this);
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

  private void flattenProjections(
      List<? extends RexNode> exps,
      List<String> fieldNames,
      String prefix,
      List<RexNode> flattenedExps,
      List<String> flattenedFieldNames) {
    for (int i = 0; i < exps.size(); ++i) {
      RexNode exp = exps.get(i);
      String fieldName =
          (fieldNames == null || fieldNames.get(i) == null)
              ? ("$" + i)
              : fieldNames.get(i);
      if (!prefix.equals("")) {
        fieldName = prefix + "$" + fieldName;
      }
      flattenProjection(
          exp,
          fieldName,
          flattenedExps,
          flattenedFieldNames);
    }
  }

  private void flattenProjection(
      RexNode exp,
      String fieldName,
      List<RexNode> flattenedExps,
      List<String> flattenedFieldNames) {
    if (exp.getType().isStruct()) {
      if (exp instanceof RexInputRef) {
        RexInputRef inputRef = (RexInputRef) exp;
        int newOffset = getNewForOldInput(inputRef.getIndex());

        // expand to range
        RelDataType flattenedType =
            SqlTypeUtil.flattenRecordType(
                rexBuilder.getTypeFactory(),
                exp.getType(),
                null);
        List<RelDataTypeField> fieldList = flattenedType.getFieldList();
        int n = fieldList.size();
        for (int j = 0; j < n; ++j) {
          RelDataTypeField field = fieldList.get(j);
          flattenedExps.add(
              new RexInputRef(
                  newOffset + j,
                  field.getType()));
          flattenedFieldNames.add(fieldName);
        }
      } else if (isConstructor(exp) || exp.isA(SqlKind.CAST)) {
        // REVIEW jvs 27-Feb-2005:  for cast, see corresponding note
        // in RewriteRexShuttle
        RexCall call = (RexCall) exp;
        if (exp.isA(SqlKind.NEW_SPECIFICATION)) {
          // For object constructors, prepend a FALSE null
          // indicator.
          flattenedExps.add(
              rexBuilder.makeLiteral(false));
          flattenedFieldNames.add(fieldName);
        } else if (exp.isA(SqlKind.CAST)) {
          if (RexLiteral.isNullLiteral(
              ((RexCall) exp).operands.get(0))) {
            // Translate CAST(NULL AS UDT) into
            // the correct number of null fields.
            flattenNullLiteral(
                exp.getType(),
                flattenedExps,
                flattenedFieldNames);
            return;
          }
        }
        flattenProjections(
            call.getOperands(),
            Collections.<String>nCopies(
                call.getOperands().size(), null),
            fieldName,
            flattenedExps,
            flattenedFieldNames);
      } else if (exp instanceof RexCall) {
        // NOTE jvs 10-Feb-2005:  This is a lame hack to keep special
        // functions which return row types working.

        int j = 0;
        for (RelDataTypeField field : exp.getType().getFieldList()) {
          flattenedExps.add(rexBuilder.makeFieldAccess(exp, field.getIndex()));
          flattenedFieldNames.add(fieldName + "$" + (j++));
        }
      } else {
        throw Util.needToImplement(exp);
      }
    } else {
      exp = flattenFieldAccesses(exp);
      flattenedExps.add(exp);
      flattenedFieldNames.add(fieldName);
    }
  }

  private void flattenNullLiteral(
      RelDataType type,
      List<RexNode> flattenedExps,
      List<String> flattenedFieldNames) {
    RelDataType flattenedType =
        SqlTypeUtil.flattenRecordType(
            rexBuilder.getTypeFactory(),
            type,
            null);
    for (RelDataTypeField field : flattenedType.getFieldList()) {
      flattenedExps.add(
          rexBuilder.makeCast(
              field.getType(),
              rexBuilder.constantNull()));
      flattenedFieldNames.add(field.getName());
    }
  }

  private boolean isConstructor(RexNode rexNode) {
    // TODO jvs 11-Feb-2005:  share code with SqlToRelConverter
    if (!(rexNode instanceof RexCall)) {
      return false;
    }
    RexCall call = (RexCall) rexNode;
    return call.getOperator().getName().equalsIgnoreCase("row")
        || (call.isA(SqlKind.NEW_SPECIFICATION));
  }

  public void rewriteRel(LogicalTableScan rel) {
    RelNode newRel =
        rel.getTable().toRel(toRelContext);
    setNewForOldRel(rel, newRel);
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

    // implement RelVisitor
    public void visit(RelNode p, int ordinal, RelNode parent) {
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
        }
      }
      if (!found) {
        throw Util.newInternal(
            "no '" + visitMethodName + "' method found for class "
            + p.getClass().getName());
      }
    }
  }

  /** Shuttle that rewrites scalar expressions. */
  private class RewriteRexShuttle extends RexShuttle {
    // override RexShuttle
    public RexNode visitInputRef(RexInputRef input) {
      final int oldIndex = input.getIndex();
      final int newIndex = getNewForOldInput(oldIndex);

      // FIXME: jhyde, 2005/12/3: Once indicator fields have been
      //  introduced, the new field type may be very different to the
      //  old field type. We should look at the actual flattened types,
      //  rather than trying to deduce the type from the current type.
      RelDataType fieldType = removeDistinct(input.getType());
      RexInputRef newInput = new RexInputRef(newIndex, fieldType);
      return newInput;
    }

    private RelDataType removeDistinct(RelDataType type) {
      if (type.getSqlTypeName() != SqlTypeName.DISTINCT) {
        return type;
      }
      return type.getFieldList().get(0).getType();
    }

    // override RexShuttle
    public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      // walk down the field access path expression, calculating
      // the desired input number
      int iInput = 0;
      RelDataType fieldType = removeDistinct(fieldAccess.getType());

      for (;;) {
        RexNode refExp = fieldAccess.getReferenceExpr();
        int ordinal = fieldAccess.getField().getIndex();
        iInput +=
            calculateFlattenedOffset(
                refExp.getType(),
                ordinal);
        if (refExp instanceof RexInputRef) {
          RexInputRef inputRef = (RexInputRef) refExp;
          iInput += getNewForOldInput(inputRef.getIndex());
          return new RexInputRef(iInput, fieldType);
        } else if (refExp instanceof RexCorrelVariable) {
          return fieldAccess;
        } else if (refExp.isA(SqlKind.CAST)) {
          // REVIEW jvs 27-Feb-2005:  what about a cast between
          // different user-defined types (once supported)?
          RexCall cast = (RexCall) refExp;
          refExp = cast.getOperands().get(0);
        }
        if (refExp.isA(SqlKind.NEW_SPECIFICATION)) {
          return ((RexCall) refExp).operands
              .get(fieldAccess.getField().getIndex());
        }
        if (!(refExp instanceof RexFieldAccess)) {
          throw Util.needToImplement(refExp);
        }
        fieldAccess = (RexFieldAccess) refExp;
      }
    }

    // override RexShuttle
    public RexNode visitCall(RexCall rexCall) {
      if (rexCall.isA(SqlKind.CAST)) {
        RexNode input = rexCall.getOperands().get(0).accept(this);
        RelDataType targetType = removeDistinct(rexCall.getType());
        return rexBuilder.makeCast(
            targetType,
            input);
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

    private RexNode flattenComparison(
        RexBuilder rexBuilder,
        SqlOperator op,
        List<RexNode> exprs) {
      List<RexNode> flattenedExps = new ArrayList<RexNode>();
      flattenProjections(
          exprs,
          null,
          "",
          flattenedExps,
          new ArrayList<String>());
      int n = flattenedExps.size() / 2;
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
                flattenedExps.get(i),
                flattenedExps.get(i + n));
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
      if (negate) {
        return rexBuilder.makeCall(
            SqlStdOperatorTable.NOT,
            conjunction);
      } else {
        return conjunction;
      }
    }
  }
}

// End RelStructuredTypeFlattener.java
