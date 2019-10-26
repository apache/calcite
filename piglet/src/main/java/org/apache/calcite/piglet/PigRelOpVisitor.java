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

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import org.apache.pig.builtin.CubeDimensions;
import org.apache.pig.builtin.RollupDimensions;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.LinkedMultiMap;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.relational.LOCogroup;
import org.apache.pig.newplan.logical.relational.LOCross;
import org.apache.pig.newplan.logical.relational.LOCube;
import org.apache.pig.newplan.logical.relational.LODistinct;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOForEach;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LOJoin;
import org.apache.pig.newplan.logical.relational.LOLimit;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.newplan.logical.relational.LONative;
import org.apache.pig.newplan.logical.relational.LORank;
import org.apache.pig.newplan.logical.relational.LOSort;
import org.apache.pig.newplan.logical.relational.LOSplit;
import org.apache.pig.newplan.logical.relational.LOSplitOutput;
import org.apache.pig.newplan.logical.relational.LOStore;
import org.apache.pig.newplan.logical.relational.LOStream;
import org.apache.pig.newplan.logical.relational.LOUnion;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;

import com.google.common.collect.ImmutableList;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Visits Pig logical operators and converts them into corresponding relational
 * algebra plans.
 */
class PigRelOpVisitor extends PigRelOpWalker.PlanPreVisitor {
  private static final String RANK_PREFIX = "rank_";

  // The relational algebra builder customized for Pig
  protected final PigRelBuilder builder;
  private Operator currentRoot;

  /**
   * Type of Pig groups
   */
  private enum GroupType {
    CUBE,
    ROLLUP,
    REGULAR
  }

  /**
   * Creates a PigRelOpVisitor.
   *
   * @param plan    Pig logical plan
   * @param walker  The walker over Pig logical plan
   * @param builder Relational algebra builder
   * @throws FrontendException Exception during processing Pig operators
   */
  PigRelOpVisitor(OperatorPlan plan, PlanWalker walker, PigRelBuilder builder)
      throws FrontendException {
    super(plan, walker);
    if (!(walker instanceof PigRelOpWalker)) {
      throw new FrontendException("Expected PigRelOpWalker", 2223);
    }
    this.builder = builder;
    this.currentRoot = null;
  }

  Operator getCurrentRoot() {
    return currentRoot;
  }

  /**
   * Translates the given pig logical plan into a list of relational algebra plans.
   *
   * @return The list of roots of translated plans, each corresponding to a sink
   * operator in the Pig plan
   * @throws FrontendException Exception during processing Pig operators
   */
  List<RelNode> translate() throws FrontendException {
    List<RelNode> relNodes = new ArrayList<>();
    for (Operator pigOp : plan.getSinks()) {
      currentRoot = pigOp;
      currentWalker.walk(this);
      if (!(pigOp instanceof LOStore)) {
        relNodes.add(builder.build());
      }
    }
    return relNodes;
  }

  @Override public void visit(LOLoad load) throws FrontendException {
    // Two types of tables to load:
    // 1. LOAD '[schemaName.]tableName': load table from database catalog
    // 2. LOAD '/path/to/tableName': load from a file
    String fullName = load.getSchemaFile();
    if (fullName.contains("file://")) {
      // load from database catalog. Pig will see it as a file in the working directory
      fullName = Paths.get(load.getSchemaFile()).getFileName().toString();
    }
    String[] tableNames;
    if (fullName.startsWith("/")) {
      // load from file
      tableNames = new String[1];
      tableNames[0] = fullName;
    } else {
      // load from catalog
      tableNames = fullName.split("\\.");
    }
    final LogicalSchema pigSchema = load.getSchema();
    final RelOptTable pigRelOptTable;
    if (pigSchema == null) {
      pigRelOptTable = null;
    } else {
      // If Pig schema is provided in the load command, convert it into
      // relational row type
      final RelDataType rowType = PigTypes.convertSchema(pigSchema);
      pigRelOptTable = PigTable.createRelOptTable(builder.getRelOptSchema(),
          rowType, Arrays.asList(tableNames));
    }
    builder.scan(pigRelOptTable, tableNames);
    builder.register(load);
  }

  @Override public void visit(LOFilter filter) throws FrontendException {
    final RexNode relExFilter = PigRelExVisitor.translatePigEx(builder, filter.getFilterPlan());
    builder.filter(relExFilter);
    builder.register(filter);
  }

  @Override public void visit(LOForEach foreach) throws FrontendException {
    // Use an inner visitor to translate Pig inner plan into a relational plan
    // See @PigRelOpInnerVisitor for details.
    PigRelOpWalker innerWalker = new PigRelOpWalker(foreach.getInnerPlan());
    PigRelOpInnerVisitor innerVisitor =
        new PigRelOpInnerVisitor(foreach.getInnerPlan(), innerWalker, builder);
    RelNode root = innerVisitor.translate().get(0);
    builder.push(root);
    builder.register(foreach);
  }

  @Override public void visit(LOCogroup loCogroup) throws FrontendException {
    // Pig parser already converted CUBE operator into a set of operators, including LOCogroup.
    // Thus this method handles all GROUP/COGROUP/CUBE commands
    final GroupType groupType = getGroupType(loCogroup);
    if (groupType == GroupType.REGULAR) {
      processRegularGroup(loCogroup);
    } else { // for CUBE and ROLLUP
      processCube(groupType, loCogroup);
    }

    // Finally project the group and aggregate fields. Note that if group consists of multiple
    // group keys, we do grouping using multiple keys and then convert these group keys into
    // a single composite group key (with tuple/struct type) in this step.
    // The other option is to create the composite group key first and do grouping on this
    // composite key. But this option is less friendly the relational algebra, which flat
    // types are more common.
    projectGroup(loCogroup.getExpressionPlans().get(0).size());
    builder.register(loCogroup);
  }

  /**
   * Projects group key with 'group' alias so that upstream operator can refer to, along
   * with other aggregate columns. If group consists of multiple group keys, construct
   * a composite tuple/struct type to make it compatible with PIG group semantic.
   *
   * @param groupCount Number of group keys.
   */
  private void projectGroup(int groupCount) {
    final List<RelDataTypeField> inputFields = builder.peek().getRowType().getFieldList();
    RexNode groupRex;
    // First construct the group field
    if (groupCount == 1) {
      // Single group key, just project it out directly
      groupRex = builder.field(0);
    } else {
      // Otherwise, build a struct for all group keys use SQL ROW operator
      List<String> fieldNames = new ArrayList<>();
      List<RelDataType> fieldTypes = new ArrayList<>();
      List<RexNode> fieldRexes = new ArrayList<>();
      for (int j = 0; j < groupCount; j++) {
        fieldTypes.add(inputFields.get(j).getType());
        fieldNames.add(inputFields.get(j).getName());
        fieldRexes.add(builder.field(j));
      }
      RelDataType groupDataType =
          PigTypes.TYPE_FACTORY.createStructType(fieldTypes, fieldNames);
      groupRex = builder.getRexBuilder().makeCall(
          groupDataType, SqlStdOperatorTable.ROW, fieldRexes);
    }
    List<RexNode> outputFields = new ArrayList<>();
    List<String> outputNames = new ArrayList<>();
    // Project group field first
    outputFields.add(groupRex);
    outputNames.add("group");
    // Then all other aggregate fields
    for (int i = groupCount; i < inputFields.size(); i++) {
      outputFields.add(builder.field(i));
      outputNames.add(inputFields.get(i).getName());
    }
    builder.project(outputFields, outputNames, true);
  }

  /**
   * Processes regular a group/group.
   *
   * @param loCogroup Pig logical group operator
   * @throws FrontendException Exception during processing Pig operators
   */
  private void processRegularGroup(LOCogroup loCogroup) throws FrontendException {
    final List<RelBuilder.GroupKey> groupKeys = new ArrayList<>();
    final int numRels = loCogroup.getExpressionPlans().size();
    // Project out the group keys and the whole row, which will be aggregated with
    // COLLECT operator later.
    preprocessCogroup(loCogroup, false);

    // Build the group key
    for (Integer key : loCogroup.getExpressionPlans().keySet()) {
      final int groupCount = loCogroup.getExpressionPlans().get(key).size();
      final List<RexNode> relKeys = new ArrayList<>();
      for (int i = 0; i < groupCount; i++) {
        relKeys.add(builder.field(numRels - key, 0, i));
      }
      groupKeys.add(builder.groupKey(relKeys));
    }

    // The do COLLECT aggregate.
    builder.cogroup(groupKeys);
  }

  /**
   * Processes a CUBE/ROLLUP group type.
   *
   * @param groupType type of the group, either ROLLUP or CUBE
   * @param loCogroup Pig logical group operator
   * @throws FrontendException Exception during processing Pig operator
   */
  private void processCube(GroupType groupType, LOCogroup loCogroup)
      throws FrontendException {
    assert loCogroup.getExpressionPlans().size() == 1;
    // First adjust the top rel in the builder, which will be served as input rel for
    // the CUBE COGROUP operator because Pig already convert LOCube into
    // a ForEach (to project out the group set using @CubeDimensions or @RollupDimension UDFs)
    // and a @LOCogroup. We dont need to use these UDFs to generate the groupset.
    // So we need to undo the effect of translate this ForEach int relational
    // algebra nodes before.
    adjustCubeInput();

    // Project out the group keys and the whole row, which will be aggregated with
    // COLLECT operator later.
    preprocessCogroup(loCogroup, true);

    // Generate the group set for the corresponding group type.
    ImmutableList.Builder<ImmutableBitSet> groupsetBuilder =
        new ImmutableList.Builder<>();
    List<Integer> keyIndexs = new ArrayList<>();
    groupsetBuilder.add(ImmutableBitSet.of(keyIndexs));
    int groupCount = loCogroup.getExpressionPlans().get(0).size();
    for (int i = groupCount - 1; i >= 0; i--) {
      keyIndexs.add(i);
      groupsetBuilder.add(ImmutableBitSet.of(keyIndexs));
    }
    final ImmutableBitSet groupSet = ImmutableBitSet.of(keyIndexs);
    final ImmutableList<ImmutableBitSet> groupSets =
        (groupType == GroupType.CUBE)
            ? ImmutableList.copyOf(groupSet.powerSet()) : groupsetBuilder.build();
    RelBuilder.GroupKey groupKey = builder.groupKey(groupSet, groupSets);

    // Finally, do COLLECT aggregate.
    builder.cogroup(ImmutableList.of(groupKey));
  }

  /**
   * Adjusts the rel input for Pig Cube operator.
   */
  private void adjustCubeInput() {
    RelNode project1 = builder.peek();
    assert project1 instanceof LogicalProject;
    RelNode correl = ((LogicalProject) project1).getInput();
    assert correl instanceof LogicalCorrelate;
    RelNode project2 = ((LogicalCorrelate) correl).getLeft();
    assert project2 instanceof LogicalProject;
    builder.replaceTop(((LogicalProject) project2).getInput());
  }

  /**
   * Projects out group key and the row for each relation
   *
   * @param loCogroup Pig logical group operator
   * @throws FrontendException Exception during processing Pig operator
   */
  private void preprocessCogroup(LOCogroup loCogroup, boolean isCubeRollup)
      throws FrontendException {
    final int numRels = loCogroup.getExpressionPlans().size();

    // Pull out all cogrouped relations from the builder
    List<RelNode> inputRels = new ArrayList<>();
    for (int i = 0; i < numRels; i++) {
      inputRels.add(0, builder.build());
    }

    // Then adding back with the corresponding projection
    for (int i = 0; i < numRels; i++) {
      final RelNode originalRel = inputRels.get(i);
      builder.push(originalRel);
      final Collection<LogicalExpressionPlan> pigGroupKeys =
          loCogroup.getExpressionPlans().get(i);
      List<RexNode> fieldRels = new ArrayList<>();
      for (LogicalExpressionPlan pigKey : pigGroupKeys) {
        fieldRels.add(PigRelExVisitor.translatePigEx(builder, pigKey));
      }
      final RexNode row = builder.getRexBuilder().makeCall(getGroupRowType(fieldRels, isCubeRollup),
          SqlStdOperatorTable.ROW, getGroupRowOperands(fieldRels, isCubeRollup));
      fieldRels.add(row);
      builder.project(fieldRels);
      builder.updateAlias(builder.getPig(originalRel), builder.getAlias(originalRel), false);
    }
  }

  // Gets row type for the group column
  private RelDataType getGroupRowType(List<RexNode> groupFields, boolean isCubeRollup) {
    if (isCubeRollup) {
      final List<RelDataTypeField> rowFields = builder.peek().getRowType().getFieldList();
      final List<String> fieldNames = new ArrayList<>();
      final List<RelDataType> fieldTypes = new ArrayList<>();
      final List<Integer> groupColIndexes = new ArrayList<>();

      // First copy fields of grouping columns
      for (RexNode rex : groupFields) {
        assert rex instanceof RexInputRef;
        int colIndex = ((RexInputRef) rex).getIndex();
        groupColIndexes.add(colIndex);
        fieldNames.add(rowFields.get(colIndex).getName());
        fieldTypes.add(rowFields.get(colIndex).getType());
      }

      // Then copy the remaining fields from the parent rel
      for (int i = 0; i < rowFields.size(); i++) {
        if (!groupColIndexes.contains(i)) {
          fieldNames.add(rowFields.get(i).getName());
          fieldTypes.add(rowFields.get(i).getType());
        }
      }
      return PigTypes.TYPE_FACTORY.createStructType(fieldTypes, fieldNames);
    }
    return builder.peek().getRowType();
  }

  /** Gets the operands for the ROW operator to construct the group column. */
  private List<RexNode> getGroupRowOperands(List<RexNode> fieldRels,
      boolean isCubeRollup) {
    final List<RexNode> rowFields = builder.fields();
    if (isCubeRollup) {
      // Add group by columns first
      List<RexNode> cubeRowFields = new ArrayList<>(fieldRels);

      // Then and remaining columns
      for (RexNode field : rowFields) {
        if (!cubeRowFields.contains(field)) {
          cubeRowFields.add(field);
        }
      }
      return cubeRowFields;
    }
    return rowFields;
  }

  /**
   * Checks the group type of a group.
   *
   * @param pigGroup Pig logical group operator
   * @return The group type, either CUBE, ROLLUP, or REGULAR
   */
  private static GroupType getGroupType(LOCogroup pigGroup) {
    if (pigGroup.getInputs((LogicalPlan) pigGroup.getPlan()).size() == 1) {
      final Operator input = pigGroup.getInputs((LogicalPlan) pigGroup.getPlan()).get(0);
      if (input instanceof LOForEach) {
        final LOForEach foreach = (LOForEach) input;
        if (foreach.getInnerPlan().getSinks().size() == 1) {
          final LOGenerate generate = (LOGenerate) foreach.getInnerPlan().getSinks().get(0);
          final List<LogicalExpressionPlan> projectList = generate.getOutputPlans();
          if (projectList.size() > 1) {
            final LogicalExpressionPlan exPlan = projectList.get(0);
            if (exPlan.getSources().size() == 1
                    && exPlan.getSources().get(0) instanceof UserFuncExpression) {
              final UserFuncExpression func = (UserFuncExpression) exPlan.getSources().get(0);
              if (func.getFuncSpec().getClassName().equals(CubeDimensions.class.getName())) {
                return GroupType.CUBE;
              }
              if (func.getFuncSpec().getClassName().equals(RollupDimensions.class.getName())) {
                return GroupType.ROLLUP;
              }
            }
          }
        }
      }
    }
    return GroupType.REGULAR;
  }

  @Override public void visit(LOLimit loLimit) throws FrontendException {
    builder.limit(0, (int) loLimit.getLimit());
    builder.register(loLimit);
  }

  @Override public void visit(LOSort loSort) throws FrontendException {
    // TODO Hanlde custom sortFunc from Pig???
    final int limit = (int) loSort.getLimit();
    List<RexNode> relSortCols = new ArrayList<>();
    if (loSort.isStar()) {
      // Sort using all columns
      RelNode top = builder.peek();
      for (RelDataTypeField field : top.getRowType().getFieldList()) {
        relSortCols.add(builder.field(field.getIndex()));
      }
    } else {
      // Sort using specific columns
      assert loSort.getSortColPlans().size() == loSort.getAscendingCols().size();
      for (int i = 0; i < loSort.getSortColPlans().size(); i++) {
        RexNode sortColsNoDirection =
            PigRelExVisitor.translatePigEx(builder, loSort.getSortColPlans().get(i));
        // Add sort directions
        if (!loSort.getAscendingCols().get(i)) {
          relSortCols.add(builder.desc(sortColsNoDirection));
        } else {
          relSortCols.add(sortColsNoDirection);
        }
      }
    }
    builder.sortLimit(-1, limit, relSortCols);
    builder.register(loSort);
  }

  @Override public void visit(LOJoin join) throws FrontendException {
    joinInternal(join.getExpressionPlans(), join.getInnerFlags());
    LogicalJoin joinRel = (LogicalJoin) builder.peek();
    Set<String> duplicateNames = new HashSet<>(joinRel.getLeft().getRowType().getFieldNames());
    duplicateNames.retainAll(joinRel.getRight().getRowType().getFieldNames());
    if (!duplicateNames.isEmpty()) {
      final List<String> fieldNames = new ArrayList<>();
      final List<RexNode> fields = new ArrayList<>();
      for (RelDataTypeField leftField : joinRel.getLeft().getRowType().getFieldList()) {
        fieldNames.add(builder.getAlias(joinRel.getLeft()) + "::" + leftField.getName());
        fields.add(builder.field(leftField.getIndex()));
      }
      int leftCount = joinRel.getLeft().getRowType().getFieldList().size();
      for (RelDataTypeField rightField : joinRel.getRight().getRowType().getFieldList()) {
        fieldNames.add(builder.getAlias(joinRel.getRight()) + "::" + rightField.getName());
        fields.add(builder.field(rightField.getIndex() + leftCount));
      }
      builder.project(fields, fieldNames);
    }
    builder.register(join);
  }

  @Override public void visit(LOCross loCross) throws FrontendException {
    final int numInputs = loCross.getInputs().size();
    MultiMap<Integer, LogicalExpressionPlan> joinPlans = new LinkedMultiMap<>();
    boolean[] innerFlags = new boolean[numInputs];
    for (int i = 0; i < numInputs; i++) {
      // Adding empty join keys
      joinPlans.put(i, new LinkedList<>());
      innerFlags[i] = true;
    }
    joinInternal(joinPlans, innerFlags);
    builder.register(loCross);
  }

  /**
   * Joins a list of relations (previously pushed into the builder).
   *
   * @param joinPlans  Join keys
   * @param innerFlags Join type
   * @throws FrontendException Exception during processing Pig operator
   */
  private void joinInternal(MultiMap<Integer, LogicalExpressionPlan> joinPlans,
                            boolean[] innerFlags) throws FrontendException {
    final int numRels = joinPlans.size();

    // Pull out all joined relations from the builder
    List<RelNode> joinRels = new ArrayList<>();
    for (int i = 0; i < numRels; i++) {
      joinRels.add(0, builder.build());
    }

    // Then join each pair from left to right
    for (int i = 0; i < numRels; i++) {
      builder.push(joinRels.get(i));
      if (i == 0) {
        continue;
      }
      List<RexNode> predicates = new ArrayList<>();
      List<LogicalExpressionPlan> leftJoinExprs = joinPlans.get(i - 1);
      List<LogicalExpressionPlan> rightJoinExprs = joinPlans.get(i);
      assert leftJoinExprs.size() == rightJoinExprs.size();
      for (int j = 0; j < leftJoinExprs.size(); j++) {
        RexNode leftRelExpr =
            PigRelExVisitor.translatePigEx(builder, leftJoinExprs.get(j), 2, 0);
        RexNode rightRelExpr =
            PigRelExVisitor.translatePigEx(builder, rightJoinExprs.get(j), 2, 1);
        predicates.add(builder.equals(leftRelExpr, rightRelExpr));
      }
      builder.join(getJoinType(innerFlags[i - 1], innerFlags[i]), builder.and(predicates));
    }
  }

  /**
   * Decides the join type from the inner types of both relation.
   *
   * @param leftInner  true if the left requires inner
   * @param rightInner true if the right requires inner
   * @return The join type, either INNER, LEFT, RIGHT, or FULL
   */
  private static JoinRelType getJoinType(boolean leftInner, boolean rightInner) {
    if (leftInner && rightInner) {
      return JoinRelType.INNER;
    } else if (leftInner) {
      return JoinRelType.LEFT;
    } else if (rightInner) {
      return JoinRelType.RIGHT;
    } else {
      return JoinRelType.FULL;
    }
  }

  @Override public void visit(LOUnion loUnion) throws FrontendException {
    // The tricky thing to translate union are the input schemas. Relational algebra does not
    // support UNION of input with different schemas, so we need to make sure to have inputs
    // with same schema first.
    LogicalSchema unionSchema = loUnion.getSchema();
    if (unionSchema == null) {
      throw new IllegalArgumentException("UNION on incompatible types is not supported. "
          + "Please consider using ONSCHEMA option");
    }
    // First get the shared schema
    int numInputs = loUnion.getInputs().size();
    RelDataType unionRelType = PigTypes.convertSchema(unionSchema);

    // Then using projections to adjust input relations with the shared schema
    List<RelNode> adjustedInputs = new ArrayList<>();
    for (int i = 0; i < numInputs; i++) {
      adjustedInputs.add(builder.project(builder.build(), unionRelType));
    }

    // Push the adjusted input back to the builder to do union
    for (int i = numInputs - 1; i >= 0; i--) {
      builder.push(adjustedInputs.get(i));
    }

    // Finally do union
    builder.union(true, numInputs);
    builder.register(loUnion);
  }

  @Override public void visit(LODistinct loDistinct) throws FrontendException {
    // Straightforward, just build distinct on the top relation
    builder.distinct();
    builder.register(loDistinct);
  }

  @Override public void visit(LOCube cube) throws FrontendException {
    // Invalid to get here
    throw new FrontendException("Cube should be translated into group by Pig parser", 10000);
  }

  @Override public void visit(LOInnerLoad load) throws FrontendException {
    // InnerLoad should be handled by @PigRelOpInnerVisitor
    throw new FrontendException("Not implemented", 10000);
  }

  @Override public void visit(LOSplit loSplit) throws FrontendException {
    builder.register(loSplit);
  }

  @Override public void visit(LOSplitOutput loSplitOutput) throws FrontendException {
    final RexNode relExFilter =
        PigRelExVisitor.translatePigEx(builder, loSplitOutput.getFilterPlan());
    builder.filter(relExFilter);
    builder.register(loSplitOutput);
  }

  @Override public void visit(LOStore store) throws FrontendException {
    builder.store(store.getAlias());
  }

  @Override public void visit(LOGenerate gen) throws FrontendException {
    // LOGenerate should be handled by @PigRelOpInnerVisitor
    throw new FrontendException("Not implemented", 10000);
  }

  @Override public void visit(LORank loRank) throws FrontendException {
    // First build the rank field using window function with information from loRank
    final RexNode rankField = buildRankField(loRank);

    // Then project out the rank field along with all other fields
    final RelDataType inputRowType = builder.peek().getRowType();
    List<RexNode> projectedFields = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>();

    projectedFields.add(rankField);
    fieldNames.add(RANK_PREFIX + loRank.getAlias()); // alias of the rank field
    for (int i = 0; i < inputRowType.getFieldCount(); i++) {
      projectedFields.add(builder.field(i));
      fieldNames.add(inputRowType.getFieldNames().get(i));
    }

    // Finally do project
    builder.project(projectedFields, fieldNames);
    builder.register(loRank);
  }

  /**
   * Builds a window function for {@link LORank}.
   *
   * @param loRank Pig logical rank operator
   * @return The window function
   * @throws FrontendException Exception during processing Pig operator
   */
  private RexNode buildRankField(LORank loRank) throws FrontendException {
    // Aggregate function is either RANK or DENSE_RANK
    SqlAggFunction rank =
        loRank.isDenseRank() ? SqlStdOperatorTable.DENSE_RANK : SqlStdOperatorTable.RANK;

    // Build the order keys
    List<RexFieldCollation> orderNodes = new ArrayList<>();
    for (int i = 0; i < loRank.getRankColPlans().size(); i++) {
      RexNode orderNode =
          PigRelExVisitor.translatePigEx(builder, loRank.getRankColPlans().get(i));
      Set<SqlKind> flags = new HashSet<>();
      if (!loRank.getAscendingCol().get(i)) {
        flags.add(SqlKind.DESCENDING);
      }
      orderNodes.add(new RexFieldCollation(orderNode, flags));
    }

    return builder.getRexBuilder().makeOver(
        PigTypes.TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), // Return type
        rank, // Aggregate function
        Collections.emptyList(), // Operands for the aggregate function, empty here
        Collections.emptyList(), // No partition keys
        ImmutableList.copyOf(orderNodes), // order keys
        RexWindowBound.create(
            SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO),
            null), // window with unbounded lower
        RexWindowBound.create(
            SqlWindow.createCurrentRow(SqlParserPos.ZERO),
            null), // till current
        false, // Range-based
        true, // allow partial
        false, // not return null when count is zero
        false, // no distinct
        false);
  }

  @Override public void visit(LOStream loStream) throws FrontendException {
    throw new FrontendException("Not implemented", 10000);
  }

  @Override public void visit(LONative nativeMR) throws FrontendException {
    throw new FrontendException("Not implemented", 10000);
  }

  @Override public boolean preVisit(LogicalRelationalOperator root) {
    return builder.checkMap(root);
  }
}

// End PigRelOpVisitor.java
