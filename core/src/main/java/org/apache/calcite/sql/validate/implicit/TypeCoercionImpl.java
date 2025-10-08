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
package org.apache.calcite.sql.validate.implicit;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandMetadata;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeMappingRule;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Util;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.validate.SqlNonNullableAccessors.getScope;
import static org.apache.calcite.sql.validate.SqlNonNullableAccessors.getSelectList;

import static java.util.Objects.requireNonNull;

/**
 * Default implementation of Calcite implicit type cast.
 */
public class TypeCoercionImpl extends AbstractTypeCoercion {

  public TypeCoercionImpl(RelDataTypeFactory typeFactory, SqlValidator validator) {
    super(typeFactory, validator);
  }

  /**
   * Widen a SqlNode's field type to common type,
   * mainly used for set operations like UNION, INTERSECT and EXCEPT.
   *
   * <p>Rules:
   * <blockquote><pre>
   *       type1, type2  type3       select a, b, c from t1
   *          \      \      \
   *         type4  type5  type6              UNION
   *          /      /      /
   *       type7  type8  type9       select d, e, f from t2
   * </pre></blockquote>
   *
   * <p>For struct type (type1, type2, type3) union type (type4, type5, type6),
   * infer the first result column type type7 as the wider type of type1 and type4,
   * the second column type as the wider type of type2 and type5 and so on.
   *
   * @param scope       Validator scope
   * @param query       Query node to update the field type for
   * @param columnIndex Target column index
   * @param targetType  Target type to cast to
   */
  @Override public boolean rowTypeCoercion(
      @Nullable SqlValidatorScope scope,
      SqlNode query,
      int columnIndex,
      RelDataType targetType) {
    final SqlKind kind = query.getKind();
    switch (kind) {
    case SELECT:
      SqlSelect selectNode = (SqlSelect) query;
      SqlValidatorScope scope1 = validator.getSelectScope(selectNode);
      if (!coerceColumnType(scope1, getSelectList(selectNode), columnIndex, targetType)) {
        return false;
      }
      updateInferredColumnType(scope1, query, columnIndex, targetType);
      return true;
    case VALUES:
      boolean coerceValues = false;
      for (SqlNode rowConstructor : ((SqlCall) query).getOperandList()) {
        if (coerceOperandType(scope, (SqlCall) rowConstructor, columnIndex, targetType)) {
          coerceValues = true;
        }
      }
      if (coerceValues) {
        updateInferredColumnType(
            requireNonNull(scope, "scope"), query, columnIndex, targetType);
      }
      return coerceValues;
    case WITH:
      SqlNode body = ((SqlWith) query).body;
      return rowTypeCoercion(validator.getOverScope(query), body, columnIndex, targetType);
    case UNION:
    case INTERSECT:
    case EXCEPT:
      // Set operations are binary for now.
      final SqlCall operand0 = ((SqlCall) query).operand(0);
      final SqlCall operand1 = ((SqlCall) query).operand(1);
      // Operand1 should be coerced even if operand0 not need to be coerced.
      // For example, we have one table named t:
      // INSERT INTO t -- only one column is c(int).
      // SELECT 1 UNION   -- operand0 not need to be coerced.
      // SELECT 1.0  -- operand1 should be coerced.
      boolean coerced = rowTypeCoercion(scope, operand0, columnIndex, targetType);
      coerced = rowTypeCoercion(scope, operand1, columnIndex, targetType) || coerced;
      // Update the nested SET operator node type.
      if (coerced) {
        updateInferredColumnType(
            requireNonNull(scope, "scope"), query, columnIndex, targetType);
      }
      return coerced;
    default:
      return false;
    }
  }

  /**
   * Coerces operands in binary arithmetic expressions to NUMERIC types.
   *
   * <p>For binary arithmetic operators like [+, -, *, /, %]:
   * If the operand is VARCHAR,
   * coerce it to data type of the other operand if its data type is NUMERIC;
   * If the other operand is DECIMAL,
   * coerce the STRING operand to max precision/scale DECIMAL.
   */
  @Override public boolean binaryArithmeticCoercion(SqlCallBinding binding) {
    // Assume the operator has NUMERIC family operand type checker.
    SqlOperator operator = binding.getOperator();
    SqlKind kind = operator.getKind();
    boolean coerced = false;
    // Binary operator
    if (binding.getOperandCount() == 2) {
      final RelDataType type1 = binding.getOperandType(0);
      final RelDataType type2 = binding.getOperandType(1);
      // Special case for datetime + interval or datetime - interval
      if (kind == SqlKind.PLUS || kind == SqlKind.MINUS) {
        if (SqlTypeUtil.isInterval(type1) || SqlTypeUtil.isInterval(type2)) {
          return false;
        }
      }
      // Binary arithmetic operator like: + - * / %
      if (kind.belongsTo(SqlKind.BINARY_ARITHMETIC)) {
        coerced = binaryArithmeticWithStrings(binding, type1, type2);
      }
    }
    return coerced;
  }

  /** For NUMERIC and STRING operands, cast STRING to data type of the other
   * operand. */
  protected boolean binaryArithmeticWithStrings(
      SqlCallBinding binding,
      RelDataType left,
      RelDataType right) {
    // For expression "NUMERIC <OP> CHARACTER",
    // PostgreSQL and MS-SQL coerce the CHARACTER operand to NUMERIC,
    // i.e. for '9':VARCHAR(1) / 2: INT, '9' would be coerced to INTEGER,
    // while for '9':VARCHAR(1) / 3.3: DOUBLE, '9' would be coerced to DOUBLE.
    // They do not allow both CHARACTER operands for binary arithmetic operators.

    // MySQL and Oracle would coerce all the string operands to DOUBLE.

    // Keep sync with PostgreSQL and MS-SQL because their behaviors are more in
    // line with the SQL standard.
    if (SqlTypeUtil.isString(left) && SqlTypeUtil.isNumeric(right)) {
      // If the numeric operand is DECIMAL type, coerce the STRING operand to
      // max precision/scale DECIMAL.
      if (SqlTypeUtil.isDecimal(right)) {
        right = SqlTypeUtil.getMaxPrecisionScaleDecimal(factory);
      }
      return coerceOperandType(binding.getScope(), binding.getCall(), 0, right);
    } else if (SqlTypeUtil.isNumeric(left) && SqlTypeUtil.isString(right)) {
      if (SqlTypeUtil.isDecimal(left)) {
        left = SqlTypeUtil.getMaxPrecisionScaleDecimal(factory);
      }
      return coerceOperandType(binding.getScope(), binding.getCall(), 1, left);
    }
    return false;
  }

  /**
   * Coerces operands in binary comparison expressions.
   *
   * <p>Rules:
   * <ul>
   *   <li>For EQUALS(=) operator: 1. If operands are BOOLEAN and NUMERIC, evaluate
   *   `1=true` and `0=false` all to be true; 2. If operands are datetime and string,
   *   do nothing because the SqlToRelConverter already makes the type coercion;</li>
   *   <li>For binary comparison [=, &gt;, &gt;=, &lt;, &lt;=]: try to find the
   *   common type, i.e. "1 &gt; '1'" will be converted to "1 &gt; 1";</li>
   *   <li>For BETWEEN operator, find the common comparison data type of all the operands,
   *   the common type is deduced from left to right, i.e. for expression "A between B and C",
   *   finds common comparison type D between A and B
   *   then common comparison type E between D and C as the final common type.</li>
   * </ul>
   */
  @Override public boolean binaryComparisonCoercion(SqlCallBinding binding) {
    SqlOperator operator = binding.getOperator();
    SqlKind kind = operator.getKind();
    int operandCnt = binding.getOperandCount();
    boolean coerced = false;
    // Binary operator
    if (operandCnt == 2) {
      final RelDataType type1 = binding.getOperandType(0);
      final RelDataType type2 = binding.getOperandType(1);
      // EQUALS(=) NOT_EQUALS(<>)
      if (kind.belongsTo(SqlKind.BINARY_EQUALITY)) {
        // STRING and datetime
        coerced = dateTimeStringEquality(binding, type1, type2) || coerced;
        // BOOLEAN and NUMERIC
        // BOOLEAN and literal
        coerced = booleanEquality(binding, type1, type2) || coerced;
      }
      // Binary comparison operator like: = > >= < <=
      if (kind.belongsTo(SqlKind.BINARY_COMPARISON)) {
        final RelDataType commonType = commonTypeForBinaryComparison(type1, type2);
        if (null != commonType) {
          coerced = coerceOperandsType(binding.getScope(), binding.getCall(), commonType);
        }
      }
    }
    // Infix operator like: BETWEEN
    if (kind == SqlKind.BETWEEN) {
      final List<RelDataType> operandTypes = Util.range(operandCnt).stream()
          .map(binding::getOperandType)
          .collect(Collectors.toList());
      final RelDataType commonType = commonTypeForComparison(operandTypes);
      if (null != commonType) {
        coerced = coerceOperandsType(binding.getScope(), binding.getCall(), commonType);
      }
    }
    return coerced;
  }

  /**
   * Finds the common type for binary comparison
   * when the size of operands {@code dataTypes} is more than 2.
   * If there are N(more than 2) operands,
   * finds the common type between two operands from left to right:
   *
   * <p>Rules:
   * <pre>
   *   type1     type2    type3
   *    |         |        |
   *    +- type4 -+        |
   *         |             |
   *         +--- type5 ---+
   * </pre>
   * For operand data types (type1, type2, type3), deduce the common type type4
   * from type1 and type2, then common type type5 from type4 and type3.
   */
  protected @Nullable RelDataType commonTypeForComparison(List<RelDataType> dataTypes) {
    assert dataTypes.size() > 2;
    final RelDataType type1 = dataTypes.get(0);
    final RelDataType type2 = dataTypes.get(1);
    // No need to do type coercion if all the data types have the same type name.
    boolean allWithSameName = SqlTypeUtil.sameNamedType(type1, type2);
    for (int i = 2; i < dataTypes.size() && allWithSameName; i++) {
      allWithSameName = SqlTypeUtil.sameNamedType(dataTypes.get(i - 1), dataTypes.get(i));
    }
    if (allWithSameName) {
      return null;
    }

    RelDataType commonType = commonTypeForBinaryComparison(type1, type2);
    for (int i = 2; i < dataTypes.size() && commonType != null; i++) {
      commonType = commonTypeForBinaryComparison(commonType, dataTypes.get(i));
    }
    return commonType;
  }

  /**
   * Datetime and STRING equality: cast STRING type to datetime type, SqlToRelConverter already
   * makes the conversion but we still keep this interface overridable
   * so user can have their custom implementation.
   */
  protected boolean dateTimeStringEquality(
      SqlCallBinding binding,
      RelDataType left,
      RelDataType right) {
    // REVIEW Danny 2018-05-23 we do not need to coerce type for EQUALS
    // because SqlToRelConverter already does this.
    // REVIEW Danny 2019-09-23, we should unify the coercion rules in TypeCoercion
    // instead of SqlToRelConverter.
    if (SqlTypeUtil.isCharacter(left)
        && SqlTypeUtil.isDatetime(right)) {
      return coerceOperandType(binding.getScope(), binding.getCall(), 0, right);
    }
    if (SqlTypeUtil.isCharacter(right)
        && SqlTypeUtil.isDatetime(left)) {
      return coerceOperandType(binding.getScope(), binding.getCall(), 1, left);
    }
    return false;
  }

  /**
   * Casts "BOOLEAN = NUMERIC" to "NUMERIC = NUMERIC". Expressions like 1=`expr` and
   * 0=`expr` can be simplified to `expr` and `not expr`, but this better happens
   * in {@link org.apache.calcite.rex.RexSimplify}.
   *
   * <p>There are 2 cases that need type coercion here:
   * <ol>
   *   <li>Case1: `boolean expr1` = 1 or `boolean expr1` = 0, replace the numeric literal with
   *   `true` or `false` boolean literal.</li>
   *   <li>Case2: `boolean expr1` = `numeric expr2`, replace expr1 to `1` or `0` numeric
   *   literal.</li>
   * </ol>
   * For case2, wrap the operand in a cast operator, during sql-to-rel conversion
   * we would convert expression `cast(expr1 as right)` to `case when expr1 then 1 else 0.`
   */
  protected boolean booleanEquality(SqlCallBinding binding,
      RelDataType left, RelDataType right) {
    SqlNode lNode = binding.operand(0);
    SqlNode rNode = binding.operand(1);
    if (SqlTypeUtil.isNumeric(left)
        && !SqlUtil.isNullLiteral(lNode, false)
        && SqlTypeUtil.isBoolean(right)) {
      // Case1: numeric literal and boolean
      if (lNode.getKind() == SqlKind.LITERAL) {
        BigDecimal val = ((SqlLiteral) lNode).getValueAs(BigDecimal.class);
        if (val.compareTo(BigDecimal.ZERO) == 0) {
          SqlNode lNode1 = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
          binding.getCall().setOperand(0, lNode1);
          return true;
        } else {
          SqlNode lNode1 = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
          binding.getCall().setOperand(0, lNode1);
          return true;
        }
      }
      // Case2: boolean and numeric
      return coerceOperandType(binding.getScope(), binding.getCall(), 1, left);
    }

    if (SqlTypeUtil.isNumeric(right)
        && !SqlUtil.isNullLiteral(rNode, false)
        && SqlTypeUtil.isBoolean(left)) {
      // Case1: literal numeric + boolean
      if (rNode.getKind() == SqlKind.LITERAL) {
        BigDecimal val = ((SqlLiteral) rNode).getValueAs(BigDecimal.class);
        if (val.compareTo(BigDecimal.ZERO) == 0) {
          SqlNode rNode1 = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
          binding.getCall().setOperand(1, rNode1);
          return true;
        } else {
          SqlNode rNode1 = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
          binding.getCall().setOperand(1, rNode1);
          return true;
        }
      }
      // Case2: boolean + numeric
      return coerceOperandType(binding.getScope(), binding.getCall(), 0, right);
    }
    return false;
  }

  /**
   * CASE WHEN type coercion, collect all the branches types including then
   * operands and else operands to find a common type, then cast the operands to the common type
   * when needed.
   */
  @SuppressWarnings("deprecation")
  public boolean caseWhenCoercion(SqlCallBinding callBinding) {
    // For sql statement like:
    // `case when ... then (a, b, c) when ... then (d, e, f) else (g, h, i)`
    // an exception throws when entering this method.
    SqlCase caseCall = (SqlCase) callBinding.getCall();
    SqlNodeList thenList = caseCall.getThenOperands();
    List<RelDataType> argTypes = new ArrayList<RelDataType>();
    SqlValidatorScope scope = getScope(callBinding);
    for (SqlNode node : thenList) {
      argTypes.add(
          validator.deriveType(
              scope, node));
    }
    SqlNode elseOp =
        requireNonNull(caseCall.getElseOperand(),
            () -> "getElseOperand() is null for " + caseCall);
    RelDataType elseOpType = validator.deriveType(scope, elseOp);
    argTypes.add(elseOpType);
    // Entering this method means we have already got a wider type, recompute it here
    // just to make the interface more clear.
    RelDataType widerType = getWiderTypeFor(argTypes, true);
    if (null != widerType) {
      boolean coerced = false;
      for (int i = 0; i < thenList.size(); i++) {
        coerced = coerceColumnType(scope, thenList, i, widerType) || coerced;
      }
      if (needToCast(scope, elseOp, widerType)) {
        coerced = coerceOperandType(scope, caseCall, 3, widerType)
            || coerced;
      }
      return coerced;
    }
    return false;
  }

  /**
   * Coerces CASE WHEN and COALESCE statement branches to a unified type.
   * NULLIF returns the same type as the first operand without return type coercion.
   */
  @Override public boolean caseOrEquivalentCoercion(SqlCallBinding callBinding) {
    if (callBinding.getCall().getKind() == SqlKind.COALESCE) {
      // For sql statement like: `coalesce(a, b, c)`
      return coalesceCoercion(callBinding);
    } else if (callBinding.getCall().getKind() == SqlKind.NULLIF) {
      // For sql statement like: `nullif(a, b)`
      return false;
    } else {
      assert callBinding.getCall() instanceof SqlCase;
      return caseWhenCoercion(callBinding);
    }
  }

  /**
   * COALESCE type coercion, collect all the branches types to find a common type,
   * then cast the operands to the common type when needed.
   */
  private boolean coalesceCoercion(SqlCallBinding callBinding) {
    List<RelDataType> argTypes = new ArrayList<>();
    SqlValidatorScope scope = getScope(callBinding);
    for (SqlNode node : callBinding.operands()) {
      argTypes.add(validator.deriveType(scope, node));
    }
    RelDataType widerType = getWiderTypeFor(argTypes, true);
    if (null != widerType) {
      return coerceOperandsType(scope, callBinding.getCall(), widerType);
    }
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * <p>STRATEGIES
   *
   * <p>With(Without) sub-query:
   *
   * <ul>
   *
   * <li>With sub-query: find the common type through comparing the left hand
   * side (LHS) expression types with corresponding right hand side (RHS)
   * expression derived from the sub-query expression's row type. Wrap the
   * fields of the LHS and RHS in CAST operators if it is needed.
   *
   * <li>Without sub-query: convert the nodes of the RHS to the common type by
   * checking all the argument types and find out the minimum common type that
   * all the arguments can be cast to.
   *
   * </ul>
   *
   * <p>How to find the common type:
   *
   * <ul>
   *
   * <li>For both struct sql types (LHS and RHS), find the common type of every
   * LHS and RHS fields pair:
   *
   *<pre>
   * (field1, field2, field3)    (field4, field5, field6)
   *    |        |       |          |       |       |
   *    +--------+---type1----------+       |       |
   *             |       |                  |       |
   *             +-------+----type2---------+       |
   *                     |                          |
   *                     +-------------type3--------+
   *</pre>
   *   <li>For both basic sql types(LHS and RHS),
   *   find the common type of LHS and RHS nodes.
   * </ul>
   */
  @Override public boolean inOperationCoercion(SqlCallBinding binding) {
    SqlOperator operator = binding.getOperator();
    if (operator.getKind() == SqlKind.IN || operator.getKind() == SqlKind.NOT_IN) {
      assert binding.getOperandCount() == 2;
      final RelDataType type1 = binding.getOperandType(0);
      final RelDataType type2 = binding.getOperandType(1);
      final SqlNode node1 = binding.operand(0);
      final SqlNode node2 = binding.operand(1);
      final SqlValidatorScope scope = binding.getScope();
      if (type1.isStruct()
          && type2.isStruct()
          && type1.getFieldCount() != type2.getFieldCount()) {
        return false;
      }
      int colCount = type1.isStruct() ? type1.getFieldCount() : 1;
      RelDataType[] argTypes = new RelDataType[2];
      argTypes[0] = type1;
      argTypes[1] = type2;
      boolean coerced = false;

      // Find the common types for RSH and LSH columns.
      List<RelDataType> widenTypes = new ArrayList<>();
      for (int i = 0; i < colCount; i++) {
        final int i2 = i;
        List<RelDataType> columnIthTypes = new AbstractList<RelDataType>() {
          @Override public RelDataType get(int index) {
            return argTypes[index].isStruct()
                ? argTypes[index].getFieldList().get(i2).getType()
                : argTypes[index];
          }

          @Override public int size() {
            return argTypes.length;
          }
        };

        RelDataType widenType =
            commonTypeForBinaryComparison(columnIthTypes.get(0),
                columnIthTypes.get(1));
        if (widenType == null) {
          widenType =
              getTightestCommonType(columnIthTypes.get(0),
                  columnIthTypes.get(1));
        }
        if (widenType == null) {
          // Can not find any common type, just return early.
          return false;
        }
        widenTypes.add(widenType);
      }

      assert widenTypes.size() == colCount;
      if (!type1.isStruct()) {
        // A "scalar" (non-ROW) type is in the LHS of the IN
        coerced = coerceOperandType(scope, binding.getCall(), 0, widenTypes.get(0))
            || coerced;
      }

      for (int i = 0; i < widenTypes.size(); i++) {
        RelDataType desired = widenTypes.get(i);
        // LSH maybe a row values or single node.
        if (node1.getKind() == SqlKind.ROW) {
          // E.g., SELECT ROW('a', 'b') IN (...)
          // Coerce every field of the ROW to the desired type
          assert node1 instanceof SqlCall;
          if (coerceOperandType(scope, (SqlCall) node1, i, desired)) {
            updateInferredColumnType(
                requireNonNull(scope, "scope"),
                node1, i, widenTypes.get(i));
            coerced = true;
          }
        }

        // RHS may be a list of expressions or a sub-query.
        if (node2 instanceof SqlNodeList) {
          // A list of expressions, e.g., SELECT x IN (a, b, c)
          //                                          ^^^^^^^^^
          // In this case we try to coerce every expression in the list to the same type
          // (this may not be possible)
          final SqlNodeList node3 = (SqlNodeList) node2;
          boolean listCoerced = false;
          if (type2.isStruct()) {
            for (SqlNode node : node3) {
              if (node instanceof SqlCall) {
                SqlCall call = (SqlCall) node;
                if (call.getKind() == SqlKind.ROW) {
                  // An expression in the RHS list is a ROW expression.
                  // e.g., SELECT x IN (ROW(1, 2), ...)
                  //                    ^^^^^^^^^
                  // In this case cast every field of the ROW to the corresponding desired type
                  listCoerced = coerceOperandType(scope, (SqlCall) node, i, desired) || listCoerced;
                }
              }
            }
            if (listCoerced) {
              updateInferredColumnType(
                  requireNonNull(scope, "scope"),
                  node2, i, desired);
            }
          } else {
            for (int j = 0; j < ((SqlNodeList) node2).size(); j++) {
              listCoerced = coerceColumnType(scope, node3, j, desired) || listCoerced;
            }
            if (listCoerced) {
              updateInferredType(node2, desired);
            }
          }
          coerced = coerced || listCoerced;
        } else {
          // Another sub-query, e.t. SELECT x IN (SELECT a, b FROM ...)
          // x must have the type ROW(typeof(a), typeof(b))
          SqlValidatorScope scope1 = node2 instanceof SqlSelect
              ? validator.getSelectScope((SqlSelect) node2)
              : scope;
          coerced = rowTypeCoercion(scope1, node2, i, desired) || coerced;
        }
      }
      return coerced;
    }
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * <p>STRATEGIES
   *
   * <p>To determine the common type:
   *
   * <ul>
   *
   * <li>When the LHS has a Simple type and RHS has a Collection type determined by {@link SqlTypeUtil#isCollection},
   * to find the common type of LHS's type and RHS's component type.
   * <li>If the common type differs from the LHS type, then coerced LHS type,
   * and the nullability of the result type remains unchanged.
   * <li>Create a new Collection type that matches the common type,
   * the nullability of the new type keep same as RHS's component type and RHS's collection type.
   * <li>If this new type differs from the RHS type, adjust the RHS type as needed.
   *
   *<pre>
   * field1       ARRAY(field2, field3, field4)
   *    |                |       |       |
   *    |                +-------+-------+
   *    |                        |
   *    |                  component type
   *    |                        |
   *    +------common type-------+
   *</pre>
   *
   * <li>If LHS type and the component type of RHS are different from the common type,
   * CAST needs to be added.
   * </ul>
   */
  @Override public boolean quantifyOperationCoercion(SqlCallBinding binding) {
    final RelDataType type1 = binding.getOperandType(0);
    final RelDataType collectionType = binding.getOperandType(1);
    final RelDataType type2 = collectionType.getComponentType();
    requireNonNull(type2, "type2");
    final SqlCall sqlCall = binding.getCall();
    final SqlValidatorScope scope = binding.getScope();
    final SqlNode node1 = binding.operand(0);
    final SqlNode node2 = binding.operand(1);
    RelDataType widenType = commonTypeForBinaryComparison(type1, type2);
    if (widenType == null) {
      widenType = getTightestCommonType(type1, type2);
    }
    if (widenType == null) {
      return false;
    }
    final RelDataType leftWidenType =
        binding.getTypeFactory().enforceTypeWithNullability(widenType, type1.isNullable());
    boolean coercedLeft =
        coerceOperandType(scope, sqlCall, 0, leftWidenType);
    if (coercedLeft) {
      updateInferredType(node1, leftWidenType);
    }
    final RelDataType rightWidenType =
        binding.getTypeFactory().enforceTypeWithNullability(widenType, type2.isNullable());
    RelDataType collectionWidenType =
        binding.getTypeFactory().createArrayType(rightWidenType, -1);
    collectionWidenType =
        binding
            .getTypeFactory()
            .enforceTypeWithNullability(collectionWidenType, collectionType.isNullable());
    boolean coercedRight =
        coerceOperandType(scope, sqlCall, 1, collectionWidenType);
    if (coercedRight) {
      updateInferredType(node2, collectionWidenType);
    }
    return coercedLeft || coercedRight;
  }

  @Override public boolean builtinFunctionCoercion(
      SqlCallBinding binding,
      List<RelDataType> operandTypes,
      List<SqlTypeFamily> expectedFamilies) {
    assert binding.getOperandCount() == operandTypes.size();
    if (!canImplicitTypeCast(operandTypes, expectedFamilies)) {
      return false;
    }
    boolean coerced = false;
    for (int i = 0; i < operandTypes.size(); i++) {
      RelDataType implicitType = implicitCast(operandTypes.get(i), expectedFamilies.get(i));
      coerced = null != implicitType
          && operandTypes.get(i) != implicitType
          && coerceOperandType(binding.getScope(), binding.getCall(), i, implicitType)
          || coerced;
    }
    return coerced;
  }

  /**
   * Type coercion for user-defined functions (UDFs).
   */
  @Override public boolean userDefinedFunctionCoercion(SqlValidatorScope scope,
      SqlCall call, SqlFunction function) {
    final SqlOperandMetadata operandMetadata =
        requireNonNull((SqlOperandMetadata) function.getOperandTypeChecker(),
            () -> "getOperandTypeChecker is not defined for " + function);
    final List<RelDataType> paramTypes =
        operandMetadata.paramTypes(scope.getValidator().getTypeFactory());
    boolean coerced = false;
    for (int i = 0; i < call.operandCount(); i++) {
      SqlNode operand = call.operand(i);
      if (operand.getKind() == SqlKind.ARGUMENT_ASSIGNMENT) {
        final List<SqlNode> operandList = ((SqlCall) operand).getOperandList();
        String name = ((SqlIdentifier) operandList.get(1)).getSimple();
        final List<String> paramNames = operandMetadata.paramNames();
        int formalIndex = paramNames.indexOf(name);
        if (formalIndex < 0) {
          return false;
        }
        // Column list operand type is not supported now.
        coerced =
            coerceOperandType(scope, (SqlCall) operand, 0,
                paramTypes.get(formalIndex)) || coerced;
      } else {
        coerced =
            coerceOperandType(scope, call, i, paramTypes.get(i)) || coerced;
      }
    }
    return coerced;
  }

  @Override public boolean querySourceCoercion(@Nullable SqlValidatorScope scope,
      RelDataType sourceRowType, RelDataType targetRowType, SqlNode query) {
    final List<RelDataTypeField> sourceFields = sourceRowType.getFieldList();
    final List<RelDataTypeField> targetFields = targetRowType.getFieldList();
    final int sourceCount = sourceFields.size();
    SqlTypeMappingRule mappingRule = validator.getTypeMappingRule();
    for (int i = 0; i < sourceCount; i++) {
      RelDataType sourceType = sourceFields.get(i).getType();
      RelDataType targetType = targetFields.get(i).getType();
      if (!SqlTypeUtil.equalSansNullability(validator.getTypeFactory(), sourceType, targetType)
          && !SqlTypeUtil.canCastFrom(targetType, sourceType, mappingRule)) {
        // Returns early if types not equals and can not do type coercion.
        return false;
      }
    }
    boolean coerced = false;
    for (int i = 0; i < sourceFields.size(); i++) {
      RelDataType targetType = targetFields.get(i).getType();
      coerced = coerceSourceRowType(scope, query, i, targetType) || coerced;
    }
    return coerced;
  }

  /**
   * Coerces the field expression at index {@code columnIndex} of source
   * in an INSERT or UPDATE query to target type.
   *
   * @param sourceScope  Query source scope
   * @param query        Query
   * @param columnIndex  Source column index to coerce type
   * @param targetType   Target type
   */
  private boolean coerceSourceRowType(
      @Nullable SqlValidatorScope sourceScope,
      SqlNode query,
      int columnIndex,
      RelDataType targetType) {
    switch (query.getKind()) {
    case INSERT:
      SqlInsert insert = (SqlInsert) query;
      return coerceSourceRowType(sourceScope,
          insert.getSource(),
          columnIndex,
          targetType);
    case UPDATE:
      SqlUpdate update = (SqlUpdate) query;
      return coerceSourceRowType(sourceScope,
          requireNonNull(update.getSourceSelect()),
          columnIndex,
          targetType);
    default:
      return rowTypeCoercion(sourceScope, query, columnIndex, targetType);
    }
  }
}
