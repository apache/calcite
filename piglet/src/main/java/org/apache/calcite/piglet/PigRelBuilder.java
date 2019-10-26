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

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.MultisetSqlType;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Static;
import org.apache.calcite.util.Util;

import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.scripting.jython.JythonFunction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Extension to {@link RelBuilder} for Pig logical operators.
 */
public class PigRelBuilder extends RelBuilder {
  private final Map<RelNode, String> reverseAliasMap = new HashMap<>();
  private final Map<String, RelNode> aliasMap = new HashMap<>();
  private final Map<Operator, RelNode> pigRelMap = new HashMap<>();
  private final Map<RelNode, Operator> relPigMap = new HashMap<>();
  private final Map<String, RelNode> storeMap = new HashMap<>();
  private int nextCorrelId = 0;
  private final PigRelTranslationContext pigRelContext =
      new PigRelTranslationContext();

  private PigRelBuilder(Context context, RelOptCluster cluster,
      RelOptSchema relOptSchema) {
    super(context, cluster, relOptSchema);
  }

  /** Creates a PigRelBuilder. */
  public static PigRelBuilder create(FrameworkConfig config) {
    final RelBuilder relBuilder = RelBuilder.create(config);
    Hook.REL_BUILDER_SIMPLIFY.add(Hook.propertyJ(false));
    return new PigRelBuilder(config.getContext(), relBuilder.getCluster(),
        relBuilder.getRelOptSchema());
  }

  public RelNode getRel(String alias) {
    return aliasMap.get(alias);
  }

  public RelNode getRel(Operator pig) {
    return pigRelMap.get(pig);
  }

  Operator getPig(RelNode rel)  {
    return relPigMap.get(rel);
  }

  String getAlias(RelNode rel)  {
    return reverseAliasMap.get(rel);
  }

  /**
   * Gets the next correlation id.
   *
   * @return The correlation id
   */
  CorrelationId nextCorrelId() {
    return new CorrelationId(nextCorrelId++);
  }

  @Override protected boolean shouldMergeProject() {
    return false;
  }

  public String getAlias() {
    final RelNode input = peek();
    if (reverseAliasMap.containsKey(input)) {
      return reverseAliasMap.get(input);
    }
    return null;
  }

  @Override public void clear() {
    super.clear();
    reverseAliasMap.clear();
    aliasMap.clear();
    pigRelMap.clear();
    relPigMap.clear();
    storeMap.clear();
    nextCorrelId = 0;
  }

  /**
   * Checks if a Pig logical operator has been translated before. If it has,
   * push the corresponding relational algebra operator on top instead of
   * doing the translation work again.
   *
   * @param pigOp The Pig logical operator to check.
   * @return true iff the pigOp has been processed before.
   */
  public boolean checkMap(LogicalRelationalOperator pigOp) {
    if (pigRelMap.containsKey(pigOp)) {
      push(pigRelMap.get(pigOp));
      return true;
    }
    return false;
  }

  /**
   * Updates the Pig logical operator and its alias with the top
   * relational algebra node.
   *
   * @param pigOp the Pig logical operator
   * @param alias the alias
   * @param updatePigRelMap whether to update the PigRelMap
   */
  public void updateAlias(Operator pigOp, String alias, boolean updatePigRelMap) {
    final RelNode rel = peek();
    if (updatePigRelMap) {
      pigRelMap.put(pigOp, rel);
    }
    relPigMap.put(rel, pigOp);
    aliasMap.put(alias, rel);
    reverseAliasMap.put(rel, alias);
  }

  /**
   * Registers the Pig logical operator with the top relational algebra node.
   *
   * @param pigOp the Pig logical operator
   */
  void register(LogicalRelationalOperator pigOp) {
    updateAlias(pigOp, pigOp.getAlias(), true);
  }

  void registerPigUDF(String className, FuncSpec pigFunc) {
    Class udfClass = pigFunc.getClass();
    String key = className;
    if (udfClass == JythonFunction.class) {
      final String[] args = pigFunc.getCtorArgs();
      assert args != null && args.length == 2;
      final String fileName = args[0].substring(args[0].lastIndexOf("/") + 1,
          args[0].lastIndexOf(".py"));
      // key = [clas name]_[file name]_[function name]
      key = udfClass.getName() + "_" + fileName + "_" + args[1];
    }
    pigRelContext.pigUdfs.put(key, pigFunc);
  }

  /**
   * Replaces the relational algebra operator at the top of the stack with
   * a new one.
   *
   * @param newRel the new relational algebra operator to replace
   */
  void replaceTop(RelNode newRel) {
    final RelNode topRel = peek();
    if (topRel instanceof SingleRel) {
      String alias = reverseAliasMap.get(topRel);
      if (alias != null) {
        reverseAliasMap.remove(topRel);
        reverseAliasMap.put(newRel, alias);
        aliasMap.put(alias, newRel);
      }
      Operator pig = getPig(topRel);
      if (pig != null) {
        relPigMap.remove(topRel);
        relPigMap.put(newRel, pig);
        pigRelMap.put(pig, newRel);
      }
      build();
      push(newRel);
    }
  }

  /**
   * Scans a table with its given schema and names.
   *
   * @param userSchema The schema of the table to scan
   * @param tableNames The names of the table to scan
   * @return This builder
   */
  public RelBuilder scan(RelOptTable userSchema, String... tableNames) {
    // First, look up the database schema to find the table schema with the given names
    final List<String> names = ImmutableList.copyOf(tableNames);
    final RelOptTable systemSchema = relOptSchema.getTableForMember(names);

    // Now we may end up with two different schemas.
    if (systemSchema != null) {
      if (userSchema != null && !compatibleType(userSchema.getRowType(),
          systemSchema.getRowType())) {
        // If both schemas are valid, they must be compatible
        throw new IllegalArgumentException(
            "Pig script schema does not match database schema for table " + names + ".\n"
                + "\t Scrip schema: " + userSchema.getRowType().getFullTypeString() + "\n"
                + "\t Database schema: " + systemSchema.getRowType().getFullTypeString());
      }
      // We choose to use systemSchema if it is valid
      return scan(systemSchema);
    } else if (userSchema != null) {
      // If systemSchema is not valid, use userSchema if it is valid
      return scan(userSchema);
    } else {
      // At least one of them needs to be valid
      throw Static.RESOURCE.tableNotFound(String.join(".", names)).ex();
    }
  }

  /**
   * Scans a table with a given schema.
   *
   * @param tableSchema The table schema
   * @return This builder
   */
  private RelBuilder scan(RelOptTable tableSchema) {
    final RelNode scan = getScanFactory().createScan(cluster, tableSchema);
    push(scan);
    return this;
  }

  /**
   * Makes a table scan operator for a given row type and names
   *
   * @param rowType Row type
   * @param tableNames Table names
   * @return This builder
   */
  public RelBuilder scan(RelDataType rowType, String... tableNames) {
    return scan(rowType, Arrays.asList(tableNames));
  }

  /**
   * Makes a table scan operator for a given row type and names
   *
   * @param rowType Row type
   * @param tableNames Table names
   * @return This builder
   */
  public RelBuilder scan(RelDataType rowType, List<String> tableNames) {
    final RelOptTable relOptTable =
        PigTable.createRelOptTable(getRelOptSchema(), rowType, tableNames);
    return scan(relOptTable);
  }

  /**
   * Projects a specific row type out of a relation algebra operator.
   * For any field in output type, if there is no matching input field, we project
   * null value of the corresponding output field type.
   *
   * <p>For example, given:
   * <ul>
   * <li>Input rel {@code A} with {@code A_type(X: int, Y: varchar)}
   * <li>Output type {@code B_type(X: int, Y: varchar, Z: boolean, W: double)}
   * </ul>
   *
   * <p>{@code project(A, B_type)} gives new relation
   * {@code C(X: int, Y: varchar, null, null)}.
   *
   * @param input The relation algebra operator to be projected
   * @param outputType The data type for the projected relation algebra operator
   * @return The projected relation algebra operator
   */
  public RelNode project(RelNode input, RelDataType outputType) {
    final RelDataType inputType = input.getRowType();
    if (compatibleType(inputType, outputType)
        && inputType.getFieldNames().equals(outputType.getFieldNames())) {
      // Same data type, simply returns the input rel
      return input;
    }

    // Now build the projection expressions on top of the input rel.
    push(input);
    project(projects(inputType, outputType), outputType.getFieldNames(), true);
    return build();
  }

  /**
   * Builds the projection expressions for a data type on top of an input data type.
   * For any field in output type, if there is no matching input field, we build
   * the literal null expression with the corresponding output field type.
   *
   * @param inputType The input data type
   * @param outputType The output data type that defines the types of projection expressions
   * @return List of projection expressions
   */
  private List<RexNode> projects(RelDataType inputType, RelDataType outputType) {
    final List<RelDataTypeField> outputFields = outputType.getFieldList();
    final List<RelDataTypeField> inputFields = inputType.getFieldList();
    final List<RexNode> projectionExprs = new ArrayList<>();

    for (RelDataTypeField outputField : outputFields) {
      RelDataTypeField matchInputField = null;
      // First find the matching input field
      for (RelDataTypeField inputField : inputFields) {
        if (inputField.getName().equals(outputField.getName())) {
          // Matched if same name
          matchInputField = inputField;
          break;
        }
      }
      if (matchInputField != null) {
        RexNode fieldProject = field(matchInputField.getIndex());
        if (matchInputField.getType().equals(outputField.getType())) {
          // If found and on same type, just project the field
          projectionExprs.add(fieldProject);
        } else {
          // Different types, CAST is required
          projectionExprs.add(getRexBuilder().makeCast(outputField.getType(), fieldProject));
        }
      } else {
        final RelDataType columnType = outputField.getType();
        if (!columnType.isStruct() && columnType.getComponentType() == null) {
          // If not, project the null Literal with the same basic type
          projectionExprs.add(getRexBuilder().makeNullLiteral(outputField.getType()));
        } else {
          // If Record or Multiset just project a constant null
          projectionExprs.add(literal(null));
        }
      }
    }
    return projectionExprs;
  }

  public AggCall aggregateCall(SqlAggFunction aggFunction, String alias, RexNode... operands) {
    return aggregateCall(aggFunction, false, false, false, null,
        ImmutableList.of(), alias, ImmutableList.copyOf(operands));
  }

  /**
   * Cogroups relations on top of the stack. The number of relations and the
   * group key are specified in groupKeys
   *
   * @param groupKeys Lists of group keys of relations to be cogrouped.
   * @return This builder
   */
  public RelBuilder cogroup(Iterable<? extends GroupKey> groupKeys) {
    @SuppressWarnings("unchecked") final List<GroupKeyImpl> groupKeyList =
        ImmutableList.copyOf((Iterable) groupKeys);
    final int groupCount = groupKeyList.get(0).nodes.size();

    // Pull out all relations needed for the group
    final int numRels = groupKeyList.size();
    List<RelNode> cogroupRels = new ArrayList<>();
    for (int i = 0; i < numRels; i++) {
      cogroupRels.add(0, build());
    }

    // Group and join relations from left to right
    for (int i = 0; i < numRels; i++) {
      // 1. Group each rel first by using COLLECT operator
      push(cogroupRels.get(i));
      // Create a ROW to pass to COLLECT.
      final RexNode row = field(groupCount);
      aggregate(groupKeyList.get(i),
          aggregateCall(SqlStdOperatorTable.COLLECT, row).as(getAlias()));
      if (i == 0) {
        continue;
      }

      // 2. Then join with the previous group relation
      List<RexNode> predicates = new ArrayList<>();
      for (int key : Util.range(groupCount)) {
        predicates.add(equals(field(2, 0, key), field(2, 1, key)));
      }
      join(JoinRelType.FULL, and(predicates));

      // 3. Project group keys from one of these two joined relations, whichever
      // is not null and the remaining payload columns
      RexNode[] projectFields = new RexNode[groupCount + i + 1];
      String[] fieldNames = new String [groupCount + i + 1];
      LogicalJoin join = (LogicalJoin) peek();
      for (int j = 0; j < groupCount; j++) {
        RexNode[] caseOperands = new RexNode[3];
        // WHEN groupKey[i] of leftRel IS NOT NULL
        caseOperands[0] = call(SqlStdOperatorTable.IS_NOT_NULL, field(j));
        // THEN choose groupKey[i] of leftRel
        caseOperands[1] = field(j);
        // ELSE choose groupKey[i] of rightRel
        caseOperands[2] = field(j + groupCount + i);
        projectFields[j] = call(SqlStdOperatorTable.CASE, caseOperands);
        String leftName = join.getLeft().getRowType().getFieldNames().get(j);
        String rightName = join.getRight().getRowType().getFieldNames().get(j);
        fieldNames[j] = leftName.equals(rightName) ? leftName : rightName;
      }

      // Project the group fields of the leftRel
      for (int j = groupCount; j < groupCount + i + 1; j++) {
        projectFields[j] = field(j);
        fieldNames[j] = peek().getRowType().getFieldNames().get(j);
      }

      // Project the group fields of the rightRel
      projectFields[groupCount + i] = field(2 * groupCount + i);
      fieldNames[groupCount + i] = peek().getRowType().getFieldNames().get(2 * groupCount + i);

      project(ImmutableList.copyOf(projectFields), ImmutableList.copyOf(fieldNames));
    }
    return this;
  }

  /**
   * Flattens the top relation on provided columns.
   *
   * @param flattenCols Indexes of columns to be flattened. These columns should have multiset type.
   * @return This builder
   */
  public RelBuilder multiSetFlatten(List<Integer> flattenCols, List<String> flattenOutputAliases) {
    final int colCount = peek().getRowType().getFieldCount();
    final List<RelDataTypeField> inputFields = peek().getRowType().getFieldList();
    final CorrelationId correlId = nextCorrelId();

    // First build a correlated expression from the input row
    final RexNode cor = correl(inputFields, correlId);

    // Then project out flatten columns from the correlated expression
    List<RexNode> flattenNodes = new ArrayList<>();
    for (int i : flattenCols) {
      assert inputFields.get(i).getType().getFamily() instanceof MultisetSqlType;
      flattenNodes.add(getRexBuilder().makeFieldAccess(cor, i));
    }
    push(LogicalValues.createOneRow(getCluster()));
    project(flattenNodes);

    // Now do flatten on input rel that contains only multiset columns
    multiSetFlatten();

    // And rejoin the result -> output: original columns + new flattened columns
    join(JoinRelType.INNER, literal(true), ImmutableSet.of(correlId));

    // Finally project out only required columns. The original multiset columns are replaced
    // by the new corresponding flattened columns
    final List<RexNode> finnalCols = new ArrayList<>();
    final List<String> finnalColNames = new ArrayList<>();
    int flattenCount = 0;
    for (int i = 0; i < colCount; i++) {
      if (flattenCols.indexOf(i) >= 0) {
        // The original multiset columns to be flattened, select new flattened columns instead
        RelDataType componentType = inputFields.get(i).getType().getComponentType();
        final int numSubFields = componentType.isStruct() ? componentType.getFieldCount() : 1;
        for (int j = 0; j < numSubFields; j++) {
          finnalCols.add(field(colCount + flattenCount));
          finnalColNames.add(flattenOutputAliases.get(flattenCount));
          flattenCount++;
        }
      } else {
        // Otherwise, just copy the original column
        finnalCols.add(field(i));
        finnalColNames.add(inputFields.get(i).getName());
      }
    }
    project(finnalCols, finnalColNames);
    return this;
  }

  /**
   * Flattens the top relation will all multiset columns. Call this method only if
   * the top relation contains multiset columns only.
   *
   * @return This builder.
   */
  public RelBuilder multiSetFlatten() {
    // [CALCITE-3193] Add RelBuilder.uncollect method, and interface
    // UncollectFactory, to instantiate Uncollect
    Uncollect uncollect = new Uncollect(cluster,
        cluster.traitSetOf(Convention.NONE), build(), false);
    push(uncollect);
    return this;
  }

  /**
   * Makes the correlated expression from rel input fields and correlation id.
   *
   * @param inputFields Rel input field list
   * @param correlId Correlation id
   *
   * @return This builder
   */
  public RexNode correl(List<RelDataTypeField> inputFields,
      CorrelationId correlId) {
    final RelDataTypeFactory.Builder fieldBuilder =
        PigTypes.TYPE_FACTORY.builder();
    for (RelDataTypeField field : inputFields) {
      fieldBuilder.add(field.getName(), field.getType());
    }
    return getRexBuilder().makeCorrel(fieldBuilder.uniquify().build(), correlId);
  }

  /**
   * Collects all rows of the top rel into a single multiset value.
   *
   * @return This builder
   */
  public RelBuilder collect() {
    final  RelNode inputRel = peek();

    // First project out a combined column which is a of all other columns
    final RexNode row = getRexBuilder().makeCall(inputRel.getRowType(),
        SqlStdOperatorTable.ROW, fields());
    project(ImmutableList.of(literal("all"), row));

    // Update the alias map for the new projected rel.
    updateAlias(getPig(inputRel), getAlias(inputRel), false);

    // Build a single group for all rows
    cogroup(ImmutableList.of(groupKey(ImmutableList.<RexNode>of(field(0)))));

    // Finally project out the final multiset value
    project(field(1));

    return this;
  }

  public RexNode dot(RexNode node, Object field) {
    if (field instanceof Integer) {
      int fieldIndex = (Integer) field;
      final RelDataType type = node.getType();
      if (type instanceof DynamicTupleRecordType) {
        ((DynamicTupleRecordType) type).resize(fieldIndex + 1);
      }
      return super.dot(node, fieldIndex);
    }
    return super.dot(node, (String) field);
  }

  public RexNode literal(Object value, RelDataType type) {
    if (value instanceof Tuple) {
      assert type.isStruct();
      return getRexBuilder().makeLiteral(((Tuple) value).getAll(), type, false);
    }

    if (value instanceof DataBag) {
      assert type.getComponentType() != null && type.getComponentType().isStruct();
      final List<List<Object>> multisetObj = new ArrayList<>();
      for (Tuple tuple : (DataBag) value) {
        multisetObj.add(tuple.getAll());
      }
      return getRexBuilder().makeLiteral(multisetObj, type, false);
    }
    return getRexBuilder().makeLiteral(value, type, false);
  }

  /**
   * Save the store alias with the corresponding relational algebra node
   *
   * @param storeAlias alias of the Pig store operator
   * @return This builder
   */
  RelBuilder store(String storeAlias) {
    storeMap.put(storeAlias, build());
    return this;
  }

  /**
   * Gets all relational plans corresponding to Pig Store operators.
   *
   */
  public List<RelNode> getRelsForStores() {
    if (storeMap.isEmpty()) {
      return null;
    }
    return ImmutableList.copyOf(storeMap.values());
  }

  public ImmutableList<RexNode> getFields(int inputCount, int inputOrdinal, int fieldOrdinal) {
    if (fieldOrdinal == -1) {
      return fields(inputCount, inputOrdinal);
    }
    return ImmutableList.of(field(inputCount, inputOrdinal, fieldOrdinal));
  }

  /**
   * Checks if two relational data types are compatible.
   *
   * @param t1 first type
   * @param t2 second type
   * @return true if t1 is compatible with t2
   */
  public static boolean compatibleType(RelDataType t1, RelDataType t2) {
    if (t1.isStruct() || t2.isStruct()) {
      if (!t1.isStruct() || !t2.isStruct()) {
        return false;
      }
      if (t1.getFieldCount() != t2.getFieldCount()) {
        return false;
      }
      List<RelDataTypeField> fields1 = t1.getFieldList();
      List<RelDataTypeField> fields2 = t2.getFieldList();
      for (int i = 0; i < fields1.size(); ++i) {
        if (!compatibleType(
            fields1.get(i).getType(),
            fields2.get(i).getType())) {
          return false;
        }
      }
      return true;
    }
    RelDataType comp1 = t1.getComponentType();
    RelDataType comp2 = t2.getComponentType();
    if ((comp1 != null) || (comp2 != null)) {
      if ((comp1 == null) || (comp2 == null)) {
        return false;
      }
      if (!compatibleType(comp1, comp2)) {
        return false;
      }
    }
    return t1.getSqlTypeName().getFamily() == t2.getSqlTypeName().getFamily();
  }

  /**
   * Context constructed during Pig-to-{@link RelNode} translation process.
   */
  public class PigRelTranslationContext {
    final Map<String, FuncSpec> pigUdfs = new HashMap<>();
  }
}

// End PigRelBuilder.java
