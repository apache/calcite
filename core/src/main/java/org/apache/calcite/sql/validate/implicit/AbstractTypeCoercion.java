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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.DynamicRecordType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeAssignmentRules;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Base class for all the type coercion rules. If you want to have a custom type coercion rules,
 * inheriting this class is not necessary, but would have some convenient tool methods.
 *
 * <p>We make tool methods: {@link #coerceOperandType}, {@link #coerceColumnType},
 * {@link #needToCast}, {@link #updateInferredType}, {@link #updateInferredColumnType}
 * all overridable by derived classes, you can define system specific type coercion logic.
 *
 * <p>Caution that these methods may modify the {@link SqlNode} tree, you should know what the
 * effect is when using these methods to customize your type coercion rules.</p>
 *
 * <p>This class also defines the default implementation of the type widening strategies, see
 * {@link TypeCoercion} doc and methods: {@link #getTightestCommonType}, {@link #getWiderTypeFor},
 * {@link #getWiderTypeForTwo}, {@link #getWiderTypeForDecimal},
 * {@link #commonTypeForBinaryComparison} for the detail strategies.</p>
 */
public abstract class AbstractTypeCoercion implements TypeCoercion {
  protected SqlValidator validator;
  protected RelDataTypeFactory factory;

  //~ Constructors -----------------------------------------------------------

  AbstractTypeCoercion(SqlValidator validator) {
    Objects.requireNonNull(validator);
    this.validator = validator;
    this.factory = validator.getTypeFactory();
  }

  //~ Methods ----------------------------------------------------------------

  public RelDataTypeFactory getFactory() {
    return this.factory;
  }

  public SqlValidator getValidator() {
    return this.validator;
  }

  /**
   * Cast operand at index {@code index} to target type.
   * we do this base on the fact that validate happens before type coercion.
   */
  protected boolean coerceOperandType(
      SqlValidatorScope scope,
      SqlCall call,
      int index,
      RelDataType targetType) {
    // Transform the JavaType to SQL type because the SqlDataTypeSpec
    // does not support deriving JavaType yet.
    if (RelDataTypeFactoryImpl.isJavaType(targetType)) {
      targetType = ((JavaTypeFactory) factory).toSql(targetType);
    }

    SqlNode operand = call.getOperandList().get(index);
    // Check it early.
    if (!needToCast(scope, operand, targetType)) {
      return false;
    }
    // Fix up nullable attr.
    RelDataType targetType1 = syncAttributes(validator.deriveType(scope, operand), targetType);
    SqlNode desired = castTo(operand, targetType1);
    call.setOperand(index, desired);
    updateInferredType(desired, targetType1);
    return true;
  }

  /**
   * Cast column at index {@code index} to target type.
   *
   * @param scope      validator scope for the node list
   * @param nodeList   column node list
   * @param index      index of column
   * @param targetType target type to cast to
   *
   * @return true if type coercion actually happens.
   */
  protected boolean coerceColumnType(
      SqlValidatorScope scope,
      SqlNodeList nodeList,
      int index,
      RelDataType targetType) {
    // Transform the JavaType to SQL type because the SqlDataTypeSpec
    // does not support deriving JavaType yet.
    if (RelDataTypeFactoryImpl.isJavaType(targetType)) {
      targetType = ((JavaTypeFactory) factory).toSql(targetType);
    }

    // This will happen when there is a star/dynamic-star column in the select list,
    // and the source is values expression, i.e. `select * from (values(1, 2, 3))`.
    // There is no need to coerce the column type, only remark
    // the inferred row type has changed, we will then add in type coercion
    // when expanding star/dynamic-star.

    // See SqlToRelConverter#convertSelectList for details.
    if (index >= nodeList.getList().size()) {
      // Can only happen when there is a star(*) in the column,
      // just return true.
      return true;
    }

    final SqlNode node = nodeList.get(index);
    if (node instanceof SqlIdentifier) {
      // Do not expand a star/dynamic table col.
      SqlIdentifier node1 = (SqlIdentifier) node;
      if (node1.isStar()) {
        return true;
      } else if (DynamicRecordType.isDynamicStarColName(Util.last(node1.names))) {
        // Should support implicit cast for dynamic table.
        return false;
      }
    }

    if (node instanceof SqlCall) {
      SqlCall node2 = (SqlCall) node;
      if (node2.getOperator().kind == SqlKind.AS) {
        final SqlNode operand = node2.operand(0);
        if (!needToCast(scope, operand, targetType)) {
          return false;
        }
        RelDataType targetType2 = syncAttributes(validator.deriveType(scope, operand), targetType);
        final SqlNode casted = castTo(operand, targetType2);
        node2.setOperand(0, casted);
        updateInferredType(casted, targetType2);
        return true;
      }
    }
    if (!needToCast(scope, node, targetType)) {
      return false;
    }
    RelDataType targetType3 = syncAttributes(validator.deriveType(scope, node), targetType);
    final SqlNode node3 = castTo(node, targetType3);
    nodeList.set(index, node3);
    updateInferredType(node3, targetType3);
    return true;
  }

  /**
   * Sync the data type additional attributes before casting,
   * i.e. nullability, charset, collation.
   */
  RelDataType syncAttributes(
      RelDataType type1,
      RelDataType targetType) {
    RelDataType targetType1 = targetType;
    if (type1 != null) {
      targetType1 = factory.createTypeWithNullability(targetType1, type1.isNullable());
      if (SqlTypeUtil.inCharOrBinaryFamilies(type1)
          && SqlTypeUtil.inCharOrBinaryFamilies(targetType)) {
        Charset charset1 = type1.getCharset();
        SqlCollation collation1 = type1.getCollation();
        if (charset1 != null && SqlTypeUtil.inCharFamily(targetType1)) {
          targetType1 = factory.createTypeWithCharsetAndCollation(targetType1, charset1,
              collation1);
        }
      }
    }
    return targetType1;
  }

  /** Decide if a SqlNode should be casted to target type, derived class
   * can override this strategy. */
  protected boolean needToCast(SqlValidatorScope scope, SqlNode node1, RelDataType targetType) {
    RelDataType type1 = validator.deriveType(scope, node1);
    // This depends on the fact that type validate happens before coercion.
    // We do not have inferred type for some node, i.e. LOCALTIME.
    if (type1 == null) {
      return false;
    }

    // This prevents that we cast a JavaType to normal RelDataType.
    if (targetType.getSqlTypeName() == type1.getSqlTypeName()) {
      return false;
    }

    // Do not make a cast when we don't know specific type (ANY) of the origin node.
    if (targetType.getSqlTypeName() == SqlTypeName.ANY
        || type1.getSqlTypeName() == SqlTypeName.ANY) {
      return false;
    }

    // No need to cast between char and varchar.
    if (targetType.getSqlTypeName() == SqlTypeName.VARCHAR
        && type1.getSqlTypeName() == SqlTypeName.CHAR
        || targetType.getSqlTypeName() == SqlTypeName.CHAR
        && type1.getSqlTypeName() == SqlTypeName.VARCHAR) {
      return false;
    }
    // No need to cast if the source type precedence list
    // contains target type. i.e. do not cast from
    // tinyint to int or int to bigint.
    if (type1.getPrecedenceList().containsType(targetType)
        && SqlTypeUtil.isIntType(type1)
        && SqlTypeUtil.isIntType(targetType)) {
      return false;
    }
    // Should keep sync with rules in SqlTypeAssignmentRules.
    return !SqlTypeUtil.equalSansNullability(factory, type1, targetType)
        && SqlTypeAssignmentRules.instance(true)
            .canCastFrom(targetType.getSqlTypeName(), targetType.getSqlTypeName());
  }

  /** It should not be used directly, because some other work should be done
   * before cast operation, see {@link #coerceColumnType}, {@link #coerceOperandType}.
   *
   * <p>Ignore constant reduction which should happen in RexSimplify.
   * */
  private SqlNode castTo(SqlNode node, RelDataType type) {
    return SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO, node,
        SqlTypeUtil.convertTypeToSpec(type).withNullable(type.isNullable()));
  }

  /**
   * Update inferred type for a SqlNode.
   */
  @SuppressWarnings("deprecation")
  protected void updateInferredType(SqlNode node, RelDataType type) {
    validator.setValidatedNodeType(node, type);
    final SqlValidatorNamespace namespace = validator.getNamespace(node);
    if (namespace != null) {
      namespace.setType(type);
    }
  }

  /**
   * Update inferred row type for a query, i.e. SqlCall that returns struct type
   * or SqlSelect.
   *
   * @param scope       validator scope
   * @param query       node to inferred type
   * @param columnIndex column index to update
   * @param targetType1 desired column type
   */
  protected void updateInferredColumnType(
      SqlValidatorScope scope,
      SqlNode query,
      int columnIndex,
      RelDataType targetType1) {
    final RelDataType rowType = validator.deriveType(scope, query);
    assert rowType.isStruct();

    final List<Map.Entry<String, RelDataType>> fieldList = new ArrayList<>();
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      final RelDataTypeField field = rowType.getFieldList().get(i);
      final String name = field.getName();
      final RelDataType type = field.getType();
      final RelDataType targetType = i == columnIndex ? targetType1 : type;
      fieldList.add(Pair.of(name, targetType));
    }
    updateInferredType(query, factory.createStructType(fieldList));
  }

  /**
   * Case1: type widening with no precision loss.
   * Find the tightest common type of two types that might be used in binary expression.
   *
   * @return tightest common type i.e. INTEGER + DECIMAL(10, 2) will return DECIMAL(10, 2)
   */
  public RelDataType getTightestCommonType(RelDataType type1, RelDataType type2) {
    if (type1 == null || type2 == null) {
      return null;
    }
    // If only different with nullability, return type with be nullable true.
    if (type1.equals(type2)
        || (type1.isNullable() != type2.isNullable()
        && factory.createTypeWithNullability(type1, type2.isNullable()).equals(type2))) {
      return factory.createTypeWithNullability(type1,
          type1.isNullable() || type2.isNullable());
    }
    // If one type is with Null type name: returns the other.
    if (SqlTypeUtil.isNull(type1)) {
      return type2;
    }
    if (SqlTypeUtil.isNull(type2)) {
      return type1;
    }
    RelDataType resultType = null;
    if (SqlTypeUtil.isString(type1)
        && SqlTypeUtil.isString(type2)) {
      resultType = factory.leastRestrictive(ImmutableList.of(type1, type2));
    }
    // For numeric types: promote to highest type.
    // i.e. SQL-SERVER/MYSQL supports numeric types cast from/to each other.
    if (SqlTypeUtil.isNumeric(type1) && SqlTypeUtil.isNumeric(type2)) {
      // For fixed precision decimals casts from(to) each other or other numeric types,
      // we let the operator decide the precision and scale of the result.
      if (!SqlTypeUtil.isDecimal(type1) && !SqlTypeUtil.isDecimal(type2)) {
        resultType = factory.leastRestrictive(ImmutableList.of(type1, type2));
      }
    }
    // Date + Timestamp -> Timestamp.
    if (SqlTypeUtil.isDate(type1) && SqlTypeUtil.isTimestamp(type2)) {
      resultType = type2;
    }
    if (SqlTypeUtil.isDate(type2) && SqlTypeUtil.isTimestamp(type1)) {
      resultType = type1;
    }

    if (type1.isStruct() && type2.isStruct()) {
      if (SqlTypeUtil.equalAsStructSansNullability(factory, type1, type2,
          validator.getCatalogReader().nameMatcher())) {
        // If two fields only differs with name case and/or nullability:
        // - different names: use f1.name
        // - different nullabilities: `nullable` is true if one of them is nullable.
        List<RelDataType> fields = new ArrayList<>();
        List<String> fieldNames = type1.getFieldNames();
        for (Pair<RelDataTypeField, RelDataTypeField> pair
            : Pair.zip(type1.getFieldList(), type2.getFieldList())) {
          RelDataType leftType = pair.left.getType();
          RelDataType rightType = pair.right.getType();
          RelDataType dataType = getTightestCommonType(leftType, rightType);
          boolean isNullable = leftType.isNullable() || rightType.isNullable();
          fields.add(factory.createTypeWithNullability(dataType, isNullable));
        }
        return factory.createStructType(type1.getStructKind(), fields, fieldNames);
      }
    }

    if (SqlTypeUtil.isArray(type1) && SqlTypeUtil.isArray(type2)) {
      if (SqlTypeUtil.equalSansNullability(factory, type1, type2)) {
        resultType = factory.createTypeWithNullability(type1,
            type1.isNullable() || type2.isNullable());
      }
    }

    if (SqlTypeUtil.isMap(type1) && SqlTypeUtil.isMap(type2)) {
      if (SqlTypeUtil.equalSansNullability(factory, type1, type2)) {
        RelDataType keyType = getTightestCommonType(type1.getKeyType(), type2.getKeyType());
        RelDataType valType = getTightestCommonType(type1.getValueType(), type2.getValueType());
        resultType = factory.createMapType(keyType, valType);
      }
    }

    return resultType;
  }

  /**
   * Promote all the way to VARCHAR.
   */
  private RelDataType promoteToVarChar(RelDataType type1, RelDataType type2) {
    RelDataType resultType = null;
    // No promotion for char and varchar.
    if (SqlTypeUtil.isCharacter(type1) && SqlTypeUtil.isCharacter(type2)) {
      return null;
    }
    // 1. Do not distinguish CHAR and VARCHAR, i.e. (INTEGER + CHAR(3))
    //    and (INTEGER + VARCHAR(5)) would both deduce VARCHAR type.
    // 2. VARCHAR has 65536 as default precision.
    // 3. Following SQL-SERVER: BINARY or BOOLEAN can be casted to VARCHAR.
    if (SqlTypeUtil.isAtomic(type1) && SqlTypeUtil.isCharacter(type2)) {
      resultType = factory.createSqlType(SqlTypeName.VARCHAR);
    }

    if (SqlTypeUtil.isCharacter(type1) && SqlTypeUtil.isAtomic(type2)) {
      resultType = factory.createSqlType(SqlTypeName.VARCHAR);
    }
    return resultType;
  }

  /**
   * Determines common type for a comparison operator when one operand is String type and the
   * other is not. For date + timestamp operands, use timestamp as common type,
   * i.e. Timestamp(2017-01-01 00:00 ...) &gt; Date(2018) evaluates to be false.
   */
  public RelDataType commonTypeForBinaryComparison(RelDataType type1, RelDataType type2) {
    SqlTypeName typeName1 = type1.getSqlTypeName();
    SqlTypeName typeName2 = type2.getSqlTypeName();

    if (typeName1 == null || typeName2 == null) {
      return null;
    }

    if (SqlTypeUtil.isString(type1) && SqlTypeUtil.isDatetime(type2)
        || SqlTypeUtil.isDatetime(type1) && SqlTypeUtil.isString(type2)) {
      // Returns null instead of VARCHAR,
      // because calcite will do the cast in SqlToRelConverter.
      return null;
    }

    // DATE + TIMESTAMP -> TIMESTAMP
    if (SqlTypeUtil.isDate(type1) && SqlTypeUtil.isTimestamp(type2)) {
      return type2;
    }

    if (SqlTypeUtil.isDate(type2) && SqlTypeUtil.isTimestamp(type1)) {
      return type1;
    }

    if (SqlTypeUtil.isString(type1) && typeName2 == SqlTypeName.NULL) {
      return type1;
    }

    if (typeName1 == SqlTypeName.NULL && SqlTypeUtil.isString(type2)) {
      return type2;
    }

    if (SqlTypeUtil.isDecimal(type1) && SqlTypeUtil.isCharacter(type2)
        || SqlTypeUtil.isCharacter(type1) && SqlTypeUtil.isDecimal(type2)) {
      // There is no proper DECIMAL type for VARCHAR, using max precision/scale DECIMAL
      // as the best we can do.
      return SqlTypeUtil.getMaxPrecisionScaleDecimal(factory);
    }

    // Keep sync with SQL-SERVER:
    // 1. BINARY/VARBINARY can not cast to FLOAT/REAL/DOUBLE
    // because of precision loss,
    // 2. CHARACTER to TIMESTAMP need explicit cast because of TimeZone.
    // Hive:
    // 1. BINARY can not cast to any other types,
    // 2. CHARACTER can only be coerced to DOUBLE/DECIMAL.
    if (SqlTypeUtil.isBinary(type2) && SqlTypeUtil.isApproximateNumeric(type1)
        || SqlTypeUtil.isBinary(type1) && SqlTypeUtil.isApproximateNumeric(type2)) {
      return null;
    }

    // 1 > '1' will be coerced to 1 > 1.
    if (SqlTypeUtil.isAtomic(type1) && SqlTypeUtil.isCharacter(type2)) {
      if (SqlTypeUtil.isTimestamp(type1)) {
        return null;
      }
      return type1;
    }

    if (SqlTypeUtil.isCharacter(type1) && SqlTypeUtil.isAtomic(type2)) {
      if (SqlTypeUtil.isTimestamp(type2)) {
        return null;
      }
      return type2;
    }

    return null;
  }

  /**
   * Case2: type widening. The main difference with {@link #getTightestCommonType}
   * is that we allow some precision loss when widening decimal to fractional,
   * or promote fractional to string type.
   */
  public RelDataType getWiderTypeForTwo(
      RelDataType type1,
      RelDataType type2,
      boolean stringPromotion) {
    RelDataType resultType = getTightestCommonType(type1, type2);
    if (null == resultType) {
      resultType = getWiderTypeForDecimal(type1, type2);
    }
    if (null == resultType && stringPromotion) {
      resultType = promoteToVarChar(type1, type2);
    }
    if (null == resultType) {
      if (SqlTypeUtil.isArray(type1) && SqlTypeUtil.isArray(type2)) {
        RelDataType valType = getWiderTypeForTwo(type1.getComponentType(),
            type2.getComponentType(), stringPromotion);
        if (null != valType) {
          resultType = factory.createArrayType(valType, -1);
        }
      }
    }
    return resultType;
  }

  /**
   * Finds a wider type when one or both types are decimal type.
   * If the wider decimal type's precision/scale exceeds system limitation,
   * this rule will truncate the decimal type to the max precision/scale.
   * For decimal and fractional types, returns a decimal type
   * which has the higher precision of the two.
   *
   * <p>The default implementation depends on the max precision/scale of the type system,
   * you can override it based on the specific system requirement in
   * {@link org.apache.calcite.rel.type.RelDataTypeSystem}.
   */
  public RelDataType getWiderTypeForDecimal(RelDataType type1, RelDataType type2) {
    if (!SqlTypeUtil.isDecimal(type1) && !SqlTypeUtil.isDecimal(type2)) {
      return null;
    }
    // For Calcite `DECIMAL` default to have max allowed precision,
    // so just return decimal type.
    // This is based on the RelDataTypeSystem implementation,
    // subclass should override it correctly.
    if (SqlTypeUtil.isNumeric(type1) && SqlTypeUtil.isNumeric(type2)) {
      return factory.leastRestrictive(ImmutableList.of(type1, type2));
    }
    return null;
  }

  /**
   * Similar to {@link #getWiderTypeForTwo}, but can handle
   * sequence types. {@link #getWiderTypeForTwo} doesn't satisfy the associative law,
   * i.e. (a op b) op c may not equal to a op (b op c). This is only a problem for STRING type or
   * nested STRING type in collection type like ARRAY. Excluding these types,
   * {@link #getWiderTypeForTwo} satisfies the associative law. For instance,
   * (DATE, INTEGER, VARCHAR) should have VARCHAR as the wider common type.
   */
  public RelDataType getWiderTypeFor(List<RelDataType> typeList, boolean stringPromotion) {
    assert typeList.size() > 1;
    RelDataType resultType = typeList.get(0);

    List<RelDataType> target = stringPromotion ? partitionByCharacter(typeList) : typeList;
    for (RelDataType tp : target) {
      resultType = getWiderTypeForTwo(tp, resultType, stringPromotion);
      if (null == resultType) {
        return null;
      }
    }
    return resultType;
  }

  private List<RelDataType> partitionByCharacter(List<RelDataType> types) {
    List<RelDataType> withCharacterTypes = new ArrayList<>();
    List<RelDataType> nonCharacterTypes = new ArrayList<>();

    for (RelDataType tp : types) {
      if (SqlTypeUtil.hasCharactor(tp)) {
        withCharacterTypes.add(tp);
      } else {
        nonCharacterTypes.add(tp);
      }
    }

    List<RelDataType> partitioned = new ArrayList<>();
    partitioned.addAll(withCharacterTypes);
    partitioned.addAll(nonCharacterTypes);
    return partitioned;
  }

  /**
   * Check if the types and families can have implicit type coercion.
   * We will check the type one by one, that means the 1th type and 1th family,
   * 2th type and 2th family, and the like.
   *
   * @param types    data type need to check.
   * @param families desired type families list.
   * @return true if we can do type coercion.
   */
  boolean canImplicitTypeCast(List<RelDataType> types, List<SqlTypeFamily> families) {
    boolean needed = false;
    if (types.size() != families.size()) {
      return false;
    }
    for (Pair<RelDataType, SqlTypeFamily> pair : Pair.zip(types, families)) {
      RelDataType implicitType = implicitCast(pair.left, pair.right);
      if (null == implicitType) {
        return false;
      }
      needed = pair.left != implicitType || needed;
    }
    return needed;
  }

  /**
   * Type coercion based on the inferred type from passed in operand
   * and the {@link SqlTypeFamily} defined in the checkers,
   * e.g. the {@link org.apache.calcite.sql.type.FamilyOperandTypeChecker}.
   *
   * <p>Caution that we do not cast from NUMERIC to NUMERIC.
   * See <a href="https://docs.google.com/spreadsheets/d/1GhleX5h5W8-kJKh7NMJ4vtoE78pwfaZRJl88ULX_MgU/edit?usp=sharing">CalciteImplicitCasts</a>
   * for the details.
   *
   * @param in       inferred operand type.
   * @param expected expected {@link SqlTypeFamily} of registered SqlFunction.
   * @return common type of implicit cast, null if we do not find any.
   */
  public RelDataType implicitCast(RelDataType in, SqlTypeFamily expected) {
    List<SqlTypeFamily> numericFamilies = ImmutableList.of(
        SqlTypeFamily.NUMERIC,
        SqlTypeFamily.DECIMAL,
        SqlTypeFamily.APPROXIMATE_NUMERIC,
        SqlTypeFamily.EXACT_NUMERIC,
        SqlTypeFamily.INTEGER);
    List<SqlTypeFamily> dateTimeFamilies = ImmutableList.of(SqlTypeFamily.DATE,
        SqlTypeFamily.TIME, SqlTypeFamily.TIMESTAMP);
    // If the expected type is already a parent of the input type, no need to cast.
    if (expected.getTypeNames().contains(in.getSqlTypeName())) {
      return in;
    }
    // Cast null type (usually from null literals) into target type.
    if (SqlTypeUtil.isNull(in)) {
      return expected.getDefaultConcreteType(factory);
    }
    if (SqlTypeUtil.isNumeric(in) && expected == SqlTypeFamily.DECIMAL) {
      return factory.decimalOf(in);
    }
    // FLOAT/DOUBLE -> DECIMAL
    if (SqlTypeUtil.isApproximateNumeric(in) && expected == SqlTypeFamily.EXACT_NUMERIC) {
      return factory.decimalOf(in);
    }
    // DATE to TIMESTAMP
    if (SqlTypeUtil.isDate(in) && expected == SqlTypeFamily.TIMESTAMP) {
      return factory.createSqlType(SqlTypeName.TIMESTAMP);
    }
    // TIMESTAMP to DATE.
    if (SqlTypeUtil.isTimestamp(in) && expected == SqlTypeFamily.DATE) {
      return factory.createSqlType(SqlTypeName.DATE);
    }
    // If the function accepts any NUMERIC type and the input is a STRING,
    // returns the expected type family's default type.
    // REVIEW Danny 2018-05-22: same with SQL-SERVER and MYSQL.
    if (SqlTypeUtil.isCharacter(in) && numericFamilies.contains(expected)) {
      return expected.getDefaultConcreteType(factory);
    }
    // STRING + DATE -> DATE;
    // STRING + TIME -> TIME;
    // STRING + TIMESTAMP -> TIMESTAMP
    if (SqlTypeUtil.isCharacter(in) && dateTimeFamilies.contains(expected)) {
      return expected.getDefaultConcreteType(factory);
    }
    // STRING + BINARY -> VARBINARY
    if (SqlTypeUtil.isCharacter(in) && expected == SqlTypeFamily.BINARY) {
      return expected.getDefaultConcreteType(factory);
    }
    // If we get here, `in` will never be STRING type.
    if (SqlTypeUtil.isAtomic(in)
        && (expected == SqlTypeFamily.STRING
        || expected == SqlTypeFamily.CHARACTER)) {
      return expected.getDefaultConcreteType(factory);
    }
    return null;
  }
}

// End AbstractTypeCoercion.java
