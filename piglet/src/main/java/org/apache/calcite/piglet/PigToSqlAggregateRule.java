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
package org.apache.calcite.piglet;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.piglet.PigTypes.TYPE_FACTORY;

/**
 * Planner rule that converts Pig aggregate UDF calls to built-in SQL
 * aggregates.
 *
 * <p>This rule is applied for logical relational algebra plan that is
 * the result of Pig translation. In Pig, aggregate calls are separate
 * from grouping where we create a bag of all tuples in each group
 * first then apply the Pig aggregate UDF later.  It is inefficient to
 * do that in SQL.
 */
public class PigToSqlAggregateRule extends RelOptRule {
  private static final String MULTISET_PROJECTION = "MULTISET_PROJECTION";

  public static final PigToSqlAggregateRule INSTANCE =
      new PigToSqlAggregateRule(RelFactories.LOGICAL_BUILDER);

  private PigToSqlAggregateRule(RelBuilderFactory relBuilderFactory) {
    super(
        operand(Project.class,
            operand(Project.class,
                operand(Aggregate.class,
                    operand(Project.class, any())))),
        relBuilderFactory,
        "PigToSqlAggregateRule");
  }

  /**
   * Visitor that finds all Pig aggregate UDFs or multiset
   * projection called in an expression and also whether a column is
   * referred in that expression.
   */
  private class PigAggUdfFinder extends RexVisitorImpl<Void> {
    // Index of the column
    private final int projCol;
    // List of all Pig aggregate UDFs found in the expression
    private final List<RexCall> pigAggCalls;
    // True iff the column is referred in the expression
    private boolean projColReferred;
    // True to ignore multiset projection inside a PigUDF
    private boolean ignoreMultisetProj = false;

    PigAggUdfFinder(int projCol) {
      super(true);
      this.projCol = projCol;
      pigAggCalls = new ArrayList<>();
      projColReferred = false;
    }

    @Override public Void visitCall(RexCall call) {
      if (PigRelUdfConverter.getSqlAggFuncForPigUdf(call) != null) {
        pigAggCalls.add(call);
        ignoreMultisetProj = true;
      } else if (isMultisetProjection(call) && !ignoreMultisetProj) {
        pigAggCalls.add(call);
      }
      for (RexNode operand : call.operands) {
        operand.accept(this);
      }
      return null;
    }

    @Override public Void visitInputRef(RexInputRef inputRef) {
      if (inputRef.getIndex() == projCol) {
        projColReferred = true;
      }
      return null;
    }
  }

  /**
   * Helper class to replace each {@link RexCall} by a corresponding
   * {@link RexNode}, defined in a given map, for an expression.
   *
   * <p>It also replaces a projection by a new projection.
   */
  private class RexCallReplacer extends RexShuttle {
    private final Map<RexNode, RexNode> replacementMap;
    private final RexBuilder builder;
    private final int oldProjCol;
    private final RexNode newProjectCol;

    RexCallReplacer(RexBuilder builder, Map<RexNode, RexNode> replacementMap,
        int oldProjCol, RexNode newProjectCol) {
      this.replacementMap = replacementMap;
      this.builder = builder;
      this.oldProjCol = oldProjCol;
      this.newProjectCol = newProjectCol;
    }

    RexCallReplacer(RexBuilder builder, Map<RexNode, RexNode> replacementMap) {
      this(builder, replacementMap, -1, null);
    }

    @Override public RexNode visitCall(RexCall call) {
      if (replacementMap.containsKey(call)) {
        return replacementMap.get(call);
      }

      List<RexNode> newOperands = new ArrayList<>();
      for (RexNode operand : call.operands) {
        if (replacementMap.containsKey(operand)) {
          newOperands.add(replacementMap.get(operand));
        } else {
          newOperands.add(operand.accept(this));
        }
      }
      return builder.makeCall(call.type, call.op, newOperands);
    }

    @Override public RexNode visitInputRef(RexInputRef inputRef) {
      if (inputRef.getIndex() == oldProjCol
          && newProjectCol != null
          && inputRef.getType() == newProjectCol.getType()) {
        return newProjectCol;
      }
      return inputRef;
    }
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Project oldTopProject = call.rel(0);
    final Project oldMiddleProject = call.rel(1);
    final Aggregate oldAgg = call.rel(2);
    final Project oldBottomProject = call.rel(3);
    final RelBuilder relBuilder = call.builder();

    if (oldAgg.getAggCallList().size() != 1
        || oldAgg.getAggCallList().get(0).getAggregation().getKind() != SqlKind.COLLECT) {
      // Prevent the rule to be re-applied. Nothing to do here
      return;
    }

    // Step 0: Find all target Pig aggregate UDFs to rewrite
    final List<RexCall> pigAggUdfs = new ArrayList<>();
    // Whether we need to keep the grouping aggregate call in the new aggregate
    boolean needGoupingCol = false;
    for (RexNode rex : oldTopProject.getProjects()) {
      PigAggUdfFinder udfVisitor = new PigAggUdfFinder(1);
      rex.accept(udfVisitor);
      if (!udfVisitor.pigAggCalls.isEmpty()) {
        for (RexCall pigAgg : udfVisitor.pigAggCalls) {
          if (!pigAggUdfs.contains(pigAgg)) {
            pigAggUdfs.add(pigAgg);
          }
        }
      } else if (udfVisitor.projColReferred) {
        needGoupingCol = true;
      }
    }


    // Step 1 Build new bottom project
    final List<RexNode> newBottomProjects = new ArrayList<>();
    relBuilder.push(oldBottomProject.getInput());
    // First project all group keys, just copy from old one
    for (int i = 0; i < oldAgg.getGroupCount(); i++) {
      newBottomProjects.add(oldBottomProject.getChildExps().get(i));
    }
    // If grouping aggregate is needed, project the whole ROW
    if (needGoupingCol) {
      final RexNode row = relBuilder.getRexBuilder().makeCall(relBuilder.peek().getRowType(),
          SqlStdOperatorTable.ROW, relBuilder.fields());
      newBottomProjects.add(row);
    }
    final int groupCount = oldAgg.getGroupCount() + (needGoupingCol ? 1 : 0);

    // Now figure out which columns need to be projected for Pig UDF aggregate calls
    // We need to project these columns for the new aggregate

    // This is a map from old index to new index
    final Map<Integer, Integer> projectedAggColumns = new HashMap<>();
    for (int i = 0; i < newBottomProjects.size(); i++) {
      if (newBottomProjects.get(i) instanceof RexInputRef) {
        projectedAggColumns.put(((RexInputRef) newBottomProjects.get(i)).getIndex(), i);
      }
    }
    // Build a map of each agg call to a list of columns in the new projection for later use
    final Map<RexCall, List<Integer>> aggCallColumns = new HashMap<>();
    for (RexCall rexCall : pigAggUdfs) {
      // Get columns in old projection required for the agg call
      final List<Integer> requiredColumns = getAggColumns(rexCall);
      // And map it to columns of new projection
      final List<Integer> newColIndexes = new ArrayList<>();
      for (int col : requiredColumns) {
        Integer newCol = projectedAggColumns.get(col);
        if (newCol != null) {
          // The column has been projected before
          newColIndexes.add(newCol);
        } else {
          // Add it to the projection list if we never project it before
          // First get the ROW operator call
          final RexCall rowCall = (RexCall) oldBottomProject.getChildExps()
                                                .get(oldAgg.getGroupCount());
          // Get the corresponding column index in parent rel through the call operand list
          final RexInputRef columRef = (RexInputRef) rowCall.getOperands().get(col);
          final int newIndex = newBottomProjects.size();
          newBottomProjects.add(columRef);
          projectedAggColumns.put(columRef.getIndex(), newIndex);
          newColIndexes.add(newIndex);

        }
      }
      aggCallColumns.put(rexCall, newColIndexes);
    }
    // Now do the projection
    relBuilder.project(newBottomProjects);

    // Step 2 build new Aggregate
    // Copy the group key
    final RelBuilder.GroupKey groupKey = relBuilder.groupKey(oldAgg.getGroupSet(),
        oldAgg.groupSets);
    // The construct the agg call list
    final List<RelBuilder.AggCall> aggCalls = new ArrayList<>();
    if (needGoupingCol) {
      aggCalls.add(
          relBuilder.aggregateCall(SqlStdOperatorTable.COLLECT,
              relBuilder.field(groupCount - 1)));
    }
    for (RexCall rexCall : pigAggUdfs) {
      final List<RexNode> aggOperands = new ArrayList<>();
      for (int i : aggCallColumns.get(rexCall)) {
        aggOperands.add(relBuilder.field(i));
      }
      if (isMultisetProjection(rexCall)) {
        if (aggOperands.size() == 1) {
          // Project single column
          aggCalls.add(
              relBuilder.aggregateCall(SqlStdOperatorTable.COLLECT,
                  aggOperands));
        } else {
          // Project more than one column, need to construct a record (ROW)
          // from them
          final RelDataType rowType =
              createRecordType(relBuilder, aggCallColumns.get(rexCall));
          final RexNode row = relBuilder.getRexBuilder()
              .makeCall(rowType, SqlStdOperatorTable.ROW, aggOperands);
          aggCalls.add(
              relBuilder.aggregateCall(SqlStdOperatorTable.COLLECT, row));
        }
      } else {
        final SqlAggFunction udf =
            PigRelUdfConverter.getSqlAggFuncForPigUdf(rexCall);
        aggCalls.add(relBuilder.aggregateCall(udf, aggOperands));
      }
    }
    relBuilder.aggregate(groupKey, aggCalls);

    // Step 3 build new top projection
    final RelDataType aggType = relBuilder.peek().getRowType();
    // First construct a map from old Pig agg UDF call to a projection
    // on new aggregate.
    final Map<RexNode, RexNode> pigCallToNewProjections = new HashMap<>();
    for (int i = 0; i < pigAggUdfs.size(); i++) {
      final RexCall pigAgg = pigAggUdfs.get(i);
      final int colIndex = i + groupCount;
      final RelDataType fieldType = aggType.getFieldList().get(colIndex).getType();
      final RelDataType oldFieldType = pigAgg.getType();
      // If the data type is different, we need to do a type CAST
      if (fieldType.equals(oldFieldType)) {
        pigCallToNewProjections.put(pigAgg, relBuilder.field(colIndex));
      } else {
        pigCallToNewProjections.put(pigAgg,
            relBuilder.getRexBuilder().makeCast(oldFieldType,
                relBuilder.field(colIndex)));
      }
    }
    // Now build all expression for the new top project
    final List<RexNode> newTopProjects = new ArrayList<>();
    final List<RexNode> oldUpperProjects = oldTopProject.getProjects();
    for (RexNode rexNode : oldUpperProjects) {
      int groupRefIndex = getGroupRefIndex(rexNode);
      if (groupRefIndex >= 0) {
        // project a field of the group
        newTopProjects.add(relBuilder.field(groupRefIndex));
      } else if (rexNode instanceof RexInputRef && ((RexInputRef) rexNode).getIndex() == 0) {
        // project the whole group (as a record)
        newTopProjects.add(oldMiddleProject.getProjects().get(0));
      } else {
        // aggregate funcs
        RexCallReplacer replacer =
            needGoupingCol ? new RexCallReplacer(relBuilder.getRexBuilder(),
                pigCallToNewProjections, 1,
                relBuilder.field(groupCount - 1))
                : new RexCallReplacer(relBuilder.getRexBuilder(), pigCallToNewProjections);
        newTopProjects.add(rexNode.accept(replacer));
      }
    }
    // Finally make the top projection
    relBuilder.project(newTopProjects, oldTopProject.getRowType().getFieldNames());

    call.transformTo(relBuilder.build());
  }

  private static RelDataType createRecordType(RelBuilder relBuilder, List<Integer> fields) {
    final List<String> destNames = new ArrayList<>();
    final List<RelDataType> destTypes = new ArrayList<>();
    for (Integer index : fields) {
      final RelDataTypeField field = relBuilder.peek().getRowType().getFieldList().get(index);
      destNames.add(field.getName());
      destTypes.add(field.getType());
    }
    return TYPE_FACTORY.createStructType(destTypes, destNames);
  }

  private int getGroupRefIndex(RexNode rex) {
    if (rex instanceof RexFieldAccess) {
      final RexFieldAccess fieldAccess = (RexFieldAccess) rex;
      if (fieldAccess.getReferenceExpr() instanceof RexInputRef) {
        final RexInputRef inputRef = (RexInputRef) fieldAccess.getReferenceExpr();
        if (inputRef.getIndex() == 0) {
          // Project from 'group' column
          return fieldAccess.getField().getIndex();
        }
      }
    }
    return -1;
  }

  /**
   * Returns a list of columns accessed in a Pig aggregate UDF call.
   *
   * @param pigAggCall Pig aggregate UDF call
   */
  private List<Integer> getAggColumns(RexCall pigAggCall) {
    if (isMultisetProjection(pigAggCall)) {
      return getColsFromMultisetProjection(pigAggCall);
    }

    // The only operand should be PIG_BAG
    assert pigAggCall.getOperands().size() == 1
        && pigAggCall.getOperands().get(0) instanceof RexCall;
    final RexCall pigBag = (RexCall) pigAggCall.getOperands().get(0);
    assert pigBag.getOperands().size() == 1;
    final RexNode pigBagInput = pigBag.getOperands().get(0);

    if (pigBagInput instanceof RexCall) {
      // Multiset-projection call
      final RexCall multisetProjection = (RexCall) pigBagInput;
      assert isMultisetProjection(multisetProjection);
      return getColsFromMultisetProjection(multisetProjection);
    }
    return new ArrayList<>();
  }

  private List<Integer> getColsFromMultisetProjection(RexCall multisetProjection) {
    final List<Integer> columns = new ArrayList<>();
    assert multisetProjection.getOperands().size() >= 1;
    for (int i = 1; i < multisetProjection.getOperands().size(); i++) {
      final RexLiteral indexLiteral =
          (RexLiteral) multisetProjection.getOperands().get(i);
      columns.add(((BigDecimal) indexLiteral.getValue()).intValue());
    }
    return columns;
  }

  private static boolean isMultisetProjection(RexCall rexCall) {
    return rexCall.getOperator().getName().equals(MULTISET_PROJECTION);
  }
}

// End PigToSqlAggregateRule.java
