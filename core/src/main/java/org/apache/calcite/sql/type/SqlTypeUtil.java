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
package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.NumberUtil;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.nio.charset.Charset;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Contains utility methods used during SQL validation or type derivation.
 */
public abstract class SqlTypeUtil {
  //~ Methods ----------------------------------------------------------------

  /**
   * Checks whether two types or more are char comparable.
   *
   * @return Returns true if all operands are of char type and if they are
   * comparable, i.e. of the same charset and collation of same charset
   */
  public static boolean isCharTypeComparable(List<RelDataType> argTypes) {
    assert argTypes != null;
    assert argTypes.size() >= 2;

    // Filter out ANY and NULL elements.
    List<RelDataType> argTypes2 = new ArrayList<>();
    for (RelDataType t : argTypes) {
      if (!isAny(t) && !isNull(t)) {
        argTypes2.add(t);
      }
    }

    for (Pair<RelDataType, RelDataType> pair : Pair.adjacents(argTypes2)) {
      RelDataType t0 = pair.left;
      RelDataType t1 = pair.right;

      if (!inCharFamily(t0) || !inCharFamily(t1)) {
        return false;
      }

      if (t0.getCharset() == null) {
        throw new AssertionError("RelDataType object should have been assigned "
            + "a (default) charset when calling deriveType");
      } else if (!t0.getCharset().equals(t1.getCharset())) {
        return false;
      }

      if (t0.getCollation() == null) {
        throw new AssertionError("RelDataType object should have been assigned "
            + "a (default) collation when calling deriveType");
      } else if (!t0.getCollation().getCharset().equals(
          t1.getCollation().getCharset())) {
        return false;
      }
    }

    return true;
  }

  /**
   * Returns whether the operands to a call are char type-comparable.
   *
   * @param binding        Binding of call to operands
   * @param operands       Operands to check for compatibility; usually the
   *                       operands of the bound call, but not always
   * @param throwOnFailure Whether to throw an exception on failure
   * @return whether operands are valid
   */
  public static boolean isCharTypeComparable(
      SqlCallBinding binding,
      List<SqlNode> operands,
      boolean throwOnFailure) {
    final SqlValidator validator = binding.getValidator();
    final SqlValidatorScope scope = binding.getScope();
    assert operands != null;
    assert operands.size() >= 2;

    if (!isCharTypeComparable(
        deriveAndCollectTypes(validator, scope, operands))) {
      if (throwOnFailure) {
        String msg = "";
        for (int i = 0; i < operands.size(); i++) {
          if (i > 0) {
            msg += ", ";
          }
          msg += operands.get(i).toString();
        }
        throw binding.newError(RESOURCE.operandNotComparable(msg));
      }
      return false;
    }
    return true;
  }

  /**
   * Iterates over all operands, derives their types, and collects them into
   * a list.
   */
  public static List<RelDataType> deriveAndCollectTypes(
      SqlValidator validator,
      SqlValidatorScope scope,
      List<SqlNode> operands) {
    // NOTE: Do not use an AbstractList. Don't want to be lazy. We want
    // errors.
    List<RelDataType> types = new ArrayList<>();
    for (SqlNode operand : operands) {
      types.add(validator.deriveType(scope, operand));
    }
    return types;
  }

  /**
   * Promotes a type to a row type (does nothing if it already is one).
   *
   * @param type      type to be promoted
   * @param fieldName name to give field in row type; null for default of
   *                  "ROW_VALUE"
   * @return row type
   */
  public static RelDataType promoteToRowType(
      RelDataTypeFactory typeFactory,
      RelDataType type,
      String fieldName) {
    if (!type.isStruct()) {
      if (fieldName == null) {
        fieldName = "ROW_VALUE";
      }
      type = typeFactory.builder().add(fieldName, type).build();
    }
    return type;
  }

  /**
   * Recreates a given RelDataType with nullability iff any of the operands
   * of a call are nullable.
   */
  public static RelDataType makeNullableIfOperandsAre(
      final SqlValidator validator,
      final SqlValidatorScope scope,
      final SqlCall call,
      RelDataType type) {
    for (SqlNode operand : call.getOperandList()) {
      RelDataType operandType = validator.deriveType(scope, operand);

      if (containsNullable(operandType)) {
        RelDataTypeFactory typeFactory = validator.getTypeFactory();
        type = typeFactory.createTypeWithNullability(type, true);
        break;
      }
    }
    return type;
  }

  /**
   * Recreates a given RelDataType with nullability iff any of the param
   * argTypes are nullable.
   */
  public static RelDataType makeNullableIfOperandsAre(
      final RelDataTypeFactory typeFactory,
      final List<RelDataType> argTypes,
      RelDataType type) {
    Objects.requireNonNull(type);
    if (containsNullable(argTypes)) {
      type = typeFactory.createTypeWithNullability(type, true);
    }
    return type;
  }

  /**
   * Returns whether all of array of types are nullable.
   */
  public static boolean allNullable(List<RelDataType> types) {
    for (RelDataType type : types) {
      if (!containsNullable(type)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns whether one or more of an array of types is nullable.
   */
  public static boolean containsNullable(List<RelDataType> types) {
    for (RelDataType type : types) {
      if (containsNullable(type)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Determines whether a type or any of its fields (if a structured type) are
   * nullable.
   */
  public static boolean containsNullable(RelDataType type) {
    if (type.isNullable()) {
      return true;
    }
    if (!type.isStruct()) {
      return false;
    }
    for (RelDataTypeField field : type.getFieldList()) {
      if (containsNullable(field.getType())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns typeName.equals(type.getSqlTypeName()). If
   * typeName.equals(SqlTypeName.Any) true is always returned.
   */
  public static boolean isOfSameTypeName(
      SqlTypeName typeName,
      RelDataType type) {
    return SqlTypeName.ANY == typeName
        || typeName == type.getSqlTypeName();
  }

  /**
   * Returns true if any element in <code>typeNames</code> matches
   * type.getSqlTypeName().
   *
   * @see #isOfSameTypeName(SqlTypeName, RelDataType)
   */
  public static boolean isOfSameTypeName(
      Collection<SqlTypeName> typeNames,
      RelDataType type) {
    for (SqlTypeName typeName : typeNames) {
      if (isOfSameTypeName(typeName, type)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return true if type is DATE, TIME, or TIMESTAMP
   */
  public static boolean isDatetime(RelDataType type) {
    return SqlTypeFamily.DATETIME.contains(type);
  }

  /**
   * @return true if type is DATE
   */
  public static boolean isDate(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }

    return type.getSqlTypeName() == SqlTypeName.DATE;
  }

  /**
   * @return true if type is TIMESTAMP
   */
  public static boolean isTimestamp(RelDataType type) {
    return SqlTypeFamily.TIMESTAMP.contains(type);
  }

  /**
   * @return true if type is some kind of INTERVAL
   */
  public static boolean isInterval(RelDataType type) {
    return SqlTypeFamily.DATETIME_INTERVAL.contains(type);
  }

  /**
   * @return true if type is in SqlTypeFamily.Character
   */
  public static boolean inCharFamily(RelDataType type) {
    return type.getFamily() == SqlTypeFamily.CHARACTER;
  }

  /**
   * @return true if type is in SqlTypeFamily.Character
   */
  public static boolean inCharFamily(SqlTypeName typeName) {
    return typeName.getFamily() == SqlTypeFamily.CHARACTER;
  }

  /**
   * @return true if type is in SqlTypeFamily.Boolean
   */
  public static boolean inBooleanFamily(RelDataType type) {
    return type.getFamily() == SqlTypeFamily.BOOLEAN;
  }

  /**
   * @return true if two types are in same type family
   */
  public static boolean inSameFamily(RelDataType t1, RelDataType t2) {
    return t1.getFamily() == t2.getFamily();
  }

  /**
   * @return true if two types are in same type family, or one or the other is
   * of type {@link SqlTypeName#NULL}.
   */
  public static boolean inSameFamilyOrNull(RelDataType t1, RelDataType t2) {
    return (t1.getSqlTypeName() == SqlTypeName.NULL)
        || (t2.getSqlTypeName() == SqlTypeName.NULL)
        || (t1.getFamily() == t2.getFamily());
  }

  /**
   * @return true if type family is either character or binary
   */
  public static boolean inCharOrBinaryFamilies(RelDataType type) {
    return (type.getFamily() == SqlTypeFamily.CHARACTER)
        || (type.getFamily() == SqlTypeFamily.BINARY);
  }

  /**
   * @return true if type is a LOB of some kind
   */
  public static boolean isLob(RelDataType type) {
    // TODO jvs 9-Dec-2004:  once we support LOB types
    return false;
  }

  /**
   * @return true if type is variable width with bounded precision
   */
  public static boolean isBoundedVariableWidth(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }
    switch (typeName) {
    case VARCHAR:
    case VARBINARY:

      // TODO angel 8-June-2005: Multiset should be LOB
    case MULTISET:
      return true;
    default:
      return false;
    }
  }

  /**
   * @return true if type is one of the integer types
   */
  public static boolean isIntType(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }
    switch (typeName) {
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
      return true;
    default:
      return false;
    }
  }

  /**
   * @return true if type is decimal
   */
  public static boolean isDecimal(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }
    return typeName == SqlTypeName.DECIMAL;
  }

  /**
   * @return true if type is double
   */
  public static boolean isDouble(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }
    return typeName == SqlTypeName.DOUBLE;
  }

  /**
   * @return true if type is bigint
   */
  public static boolean isBigint(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }
    return typeName == SqlTypeName.BIGINT;
  }

  /**
   * @return true if type is numeric with exact precision
   */
  public static boolean isExactNumeric(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }
    switch (typeName) {
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
    case DECIMAL:
      return true;
    default:
      return false;
    }
  }

  /** Returns whether a type's scale is set. */
  public static boolean hasScale(RelDataType type) {
    return type.getScale() != Integer.MIN_VALUE;
  }

  /**
   * Returns the maximum value of an integral type, as a long value
   */
  public static long maxValue(RelDataType type) {
    assert SqlTypeUtil.isIntType(type);
    switch (type.getSqlTypeName()) {
    case TINYINT:
      return Byte.MAX_VALUE;
    case SMALLINT:
      return Short.MAX_VALUE;
    case INTEGER:
      return Integer.MAX_VALUE;
    case BIGINT:
      return Long.MAX_VALUE;
    default:
      throw Util.unexpected(type.getSqlTypeName());
    }
  }

  /**
   * @return true if type is numeric with approximate precision
   */
  public static boolean isApproximateNumeric(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }
    switch (typeName) {
    case FLOAT:
    case REAL:
    case DOUBLE:
      return true;
    default:
      return false;
    }
  }

  /**
   * @return true if type is numeric
   */
  public static boolean isNumeric(RelDataType type) {
    return isExactNumeric(type) || isApproximateNumeric(type);
  }

  /**
   * @return true if type is null.
   */
  public static boolean isNull(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();

    if (typeName == null) {
      return false;
    }

    return typeName == SqlTypeName.NULL;
  }

  /**
   * Tests whether two types have the same name and structure, possibly with
   * differing modifiers. For example, VARCHAR(1) and VARCHAR(10) are
   * considered the same, while VARCHAR(1) and CHAR(1) are considered
   * different. Likewise, VARCHAR(1) MULTISET and VARCHAR(10) MULTISET are
   * considered the same.
   *
   * @return true if types have same name and structure
   */
  public static boolean sameNamedType(RelDataType t1, RelDataType t2) {
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
        if (!sameNamedType(
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
      if (!sameNamedType(comp1, comp2)) {
        return false;
      }
    }
    return t1.getSqlTypeName() == t2.getSqlTypeName();
  }

  /**
   * Computes the maximum number of bytes required to represent a value of a
   * type having user-defined precision. This computation assumes no overhead
   * such as length indicators and NUL-terminators. Complex types for which
   * multiple representations are possible (e.g. DECIMAL or TIMESTAMP) return
   * 0.
   *
   * @param type type for which to compute storage
   * @return maximum bytes, or 0 for a fixed-width type or type with unknown
   * maximum
   */
  public static int getMaxByteSize(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();

    if (typeName == null) {
      return 0;
    }

    switch (typeName) {
    case CHAR:
    case VARCHAR:
      return (int) Math.ceil(
          ((double) type.getPrecision())
              * type.getCharset().newEncoder().maxBytesPerChar());

    case BINARY:
    case VARBINARY:
      return type.getPrecision();

    case MULTISET:

      // TODO Wael Jan-24-2005: Need a better way to tell fennel this
      // number. This a very generic place and implementation details like
      // this doesnt belong here. Waiting to change this once we have blob
      // support
      return 4096;

    default:
      return 0;
    }
  }

  /**
   * Determines the minimum unscaled value of a numeric type
   *
   * @param type a numeric type
   */
  public static long getMinValue(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    switch (typeName) {
    case TINYINT:
      return Byte.MIN_VALUE;
    case SMALLINT:
      return Short.MIN_VALUE;
    case INTEGER:
      return Integer.MIN_VALUE;
    case BIGINT:
    case DECIMAL:
      return NumberUtil.getMinUnscaled(type.getPrecision()).longValue();
    default:
      throw new AssertionError("getMinValue(" + typeName + ")");
    }
  }

  /**
   * Determines the maximum unscaled value of a numeric type
   *
   * @param type a numeric type
   */
  public static long getMaxValue(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    switch (typeName) {
    case TINYINT:
      return Byte.MAX_VALUE;
    case SMALLINT:
      return Short.MAX_VALUE;
    case INTEGER:
      return Integer.MAX_VALUE;
    case BIGINT:
    case DECIMAL:
      return NumberUtil.getMaxUnscaled(type.getPrecision()).longValue();
    default:
      throw new AssertionError("getMaxValue(" + typeName + ")");
    }
  }

  /**
   * @return true if type has a representation as a Java primitive (ignoring
   * nullability)
   */
  @Deprecated // to be removed before 2.0
  public static boolean isJavaPrimitive(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }

    switch (typeName) {
    case BOOLEAN:
    case TINYINT:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
    case FLOAT:
    case REAL:
    case DOUBLE:
    case SYMBOL:
      return true;
    default:
      return false;
    }
  }

  /**
   * @return class name of the wrapper for the primitive data type.
   */
  @Deprecated // to be removed before 2.0
  public static String getPrimitiveWrapperJavaClassName(RelDataType type) {
    if (type == null) {
      return null;
    }
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return null;
    }

    switch (typeName) {
    case BOOLEAN:
      return "Boolean";
    default:
      //noinspection deprecation
      return getNumericJavaClassName(type);
    }
  }

  /**
   * @return class name of the numeric data type.
   */
  @Deprecated // to be removed before 2.0
  public static String getNumericJavaClassName(RelDataType type) {
    if (type == null) {
      return null;
    }
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return null;
    }

    switch (typeName) {
    case TINYINT:
      return "Byte";
    case SMALLINT:
      return "Short";
    case INTEGER:
      return "Integer";
    case BIGINT:
      return "Long";
    case REAL:
      return "Float";
    case DECIMAL:
    case FLOAT:
    case DOUBLE:
      return "Double";
    default:
      return null;
    }
  }

  private static boolean isAny(RelDataType t) {
    return t.getFamily() == SqlTypeFamily.ANY;
  }

  /**
   * Tests whether a value can be assigned to a site.
   *
   * @param toType   type of the target site
   * @param fromType type of the source value
   * @return true iff assignable
   */
  public static boolean canAssignFrom(
      RelDataType toType,
      RelDataType fromType) {
    if (isAny(toType) || isAny(fromType)) {
      return true;
    }

    // TODO jvs 2-Jan-2005:  handle all the other cases like
    // rows, collections, UDT's
    if (fromType.getSqlTypeName() == SqlTypeName.NULL) {
      // REVIEW jvs 4-Dec-2008: We allow assignment from NULL to any
      // type, including NOT NULL types, since in the case where no
      // rows are actually processed, the assignment is legal
      // (FRG-365).  However, it would be better if the validator's
      // NULL type inference guaranteed that we had already
      // assigned a real (nullable) type to every NULL literal.
      return true;
    }

    if (fromType.getSqlTypeName() == SqlTypeName.ARRAY) {
      if (toType.getSqlTypeName() != SqlTypeName.ARRAY) {
        return false;
      }
      return canAssignFrom(toType.getComponentType(), fromType.getComponentType());
    }

    if (areCharacterSetsMismatched(toType, fromType)) {
      return false;
    }

    return toType.getFamily() == fromType.getFamily();
  }

  /**
   * Determines whether two types both have different character sets. If one
   * or the other type has no character set (e.g. in cast from INT to
   * VARCHAR), that is not a mismatch.
   *
   * @param t1 first type
   * @param t2 second type
   * @return true iff mismatched
   */
  public static boolean areCharacterSetsMismatched(
      RelDataType t1,
      RelDataType t2) {
    if (isAny(t1) || isAny(t2)) {
      return false;
    }

    Charset cs1 = t1.getCharset();
    Charset cs2 = t2.getCharset();
    if ((cs1 != null) && (cs2 != null)) {
      if (!cs1.equals(cs2)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Compares two types and returns true if fromType can be cast to toType.
   *
   * <p>REVIEW jvs 17-Dec-2004: the coerce param below shouldn't really be
   * necessary. We're using it as a hack because
   * {@link SqlTypeFactoryImpl#leastRestrictiveSqlType} isn't complete enough
   * yet.  Once it is, this param (and the non-coerce rules of
   * {@link SqlTypeAssignmentRules}) should go away.
   *
   * @param toType   target of assignment
   * @param fromType source of assignment
   * @param coerce   if true, the SQL rules for CAST are used; if false, the
   *                 rules are similar to Java; e.g. you can't assign short x =
   *                 (int) y, and you can't assign int x = (String) z.
   * @return true iff cast is legal
   */
  public static boolean canCastFrom(
      RelDataType toType,
      RelDataType fromType,
      boolean coerce) {
    if (toType == fromType) {
      return true;
    }
    if (isAny(toType) || isAny(fromType)) {
      return true;
    }

    final SqlTypeName fromTypeName = fromType.getSqlTypeName();
    final SqlTypeName toTypeName = toType.getSqlTypeName();
    if (toType.isStruct() || fromType.isStruct()) {
      if (toTypeName == SqlTypeName.DISTINCT) {
        if (fromTypeName == SqlTypeName.DISTINCT) {
          // can't cast between different distinct types
          return false;
        }
        return canCastFrom(
            toType.getFieldList().get(0).getType(), fromType, coerce);
      } else if (fromTypeName == SqlTypeName.DISTINCT) {
        return canCastFrom(
            toType, fromType.getFieldList().get(0).getType(), coerce);
      } else if (toTypeName == SqlTypeName.ROW) {
        if (fromTypeName != SqlTypeName.ROW) {
          return false;
        }
        int n = toType.getFieldCount();
        if (fromType.getFieldCount() != n) {
          return false;
        }
        for (int i = 0; i < n; ++i) {
          RelDataTypeField toField = toType.getFieldList().get(i);
          RelDataTypeField fromField = fromType.getFieldList().get(i);
          if (!canCastFrom(
              toField.getType(),
              fromField.getType(),
              coerce)) {
            return false;
          }
        }
        return true;
      } else if (toTypeName == SqlTypeName.MULTISET) {
        if (!fromType.isStruct()) {
          return false;
        }
        if (fromTypeName != SqlTypeName.MULTISET) {
          return false;
        }
        return canCastFrom(
            toType.getComponentType(),
            fromType.getComponentType(),
            coerce);
      } else if (fromTypeName == SqlTypeName.MULTISET) {
        return false;
      } else {
        return toType.getFamily() == fromType.getFamily();
      }
    }
    RelDataType c1 = toType.getComponentType();
    if (c1 != null) {
      RelDataType c2 = fromType.getComponentType();
      if (c2 == null) {
        return false;
      }
      return canCastFrom(c1, c2, coerce);
    }
    if ((isInterval(fromType) && isExactNumeric(toType))
        || (isInterval(toType) && isExactNumeric(fromType))) {
      IntervalSqlType intervalType =
          (IntervalSqlType) (isInterval(fromType) ? fromType : toType);
      if (!intervalType.getIntervalQualifier().isSingleDatetimeField()) {
        // Casts between intervals and exact numerics must involve
        // intervals with a single datetime field.
        return false;
      }
    }
    if (toTypeName == null || fromTypeName == null) {
      return false;
    }

    // REVIEW jvs 9-Feb-2009: we don't impose SQL rules for character sets
    // here; instead, we do that in SqlCastFunction.  The reason is that
    // this method is called from at least one place (MedJdbcNameDirectory)
    // where internally a cast across character repertoires is OK.  Should
    // probably clean that up.

    SqlTypeAssignmentRules rules = SqlTypeAssignmentRules.instance(coerce);
    return rules.canCastFrom(toTypeName, fromTypeName);
  }

  /**
   * Flattens a record type by recursively expanding any fields which are
   * themselves record types. For each record type, a representative null
   * value field is also prepended (with state NULL for a null value and FALSE
   * for non-null), and all component types are asserted to be nullable, since
   * SQL doesn't allow NOT NULL to be specified on attributes.
   *
   * @param typeFactory   factory which should produced flattened type
   * @param recordType    type with possible nesting
   * @param flatteningMap if non-null, receives map from unflattened ordinal
   *                      to flattened ordinal (must have length at least
   *                      recordType.getFieldList().size())
   * @return flattened equivalent
   */
  public static RelDataType flattenRecordType(
      RelDataTypeFactory typeFactory,
      RelDataType recordType,
      int[] flatteningMap) {
    if (!recordType.isStruct()) {
      return recordType;
    }
    List<RelDataTypeField> fieldList = new ArrayList<>();
    boolean nested =
        flattenFields(
            typeFactory,
            recordType,
            fieldList,
            flatteningMap);
    if (!nested) {
      return recordType;
    }
    List<RelDataType> types = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>();
    int i = -1;
    for (RelDataTypeField field : fieldList) {
      ++i;
      types.add(field.getType());
      fieldNames.add(field.getName() + "_" + i);
    }
    return typeFactory.createStructType(types, fieldNames);
  }

  public static boolean needsNullIndicator(RelDataType recordType) {
    // NOTE jvs 9-Mar-2005: It would be more storage-efficient to say that
    // no null indicator is required for structured type columns declared
    // as NOT NULL.  However, the uniformity of always having a null
    // indicator makes things cleaner in many places.
    return recordType.getSqlTypeName() == SqlTypeName.STRUCTURED;
  }

  private static boolean flattenFields(
      RelDataTypeFactory typeFactory,
      RelDataType type,
      List<RelDataTypeField> list,
      int[] flatteningMap) {
    boolean nested = false;
    for (RelDataTypeField field : type.getFieldList()) {
      if (flatteningMap != null) {
        flatteningMap[field.getIndex()] = list.size();
      }
      if (field.getType().isStruct()) {
        nested = true;
        flattenFields(
            typeFactory,
            field.getType(),
            list,
            null);
      } else if (field.getType().getComponentType() != null) {
        nested = true;

        // TODO jvs 14-Feb-2005:  generalize to any kind of
        // collection type
        RelDataType flattenedCollectionType =
            typeFactory.createMultisetType(
                flattenRecordType(
                    typeFactory,
                    field.getType().getComponentType(),
                    null),
                -1);
        if (field.getType() instanceof ArraySqlType) {
          flattenedCollectionType =
              typeFactory.createArrayType(
                  flattenRecordType(
                      typeFactory,
                      field.getType().getComponentType(),
                      null),
                  -1);
        }
        field =
            new RelDataTypeFieldImpl(
                field.getName(),
                field.getIndex(),
                flattenedCollectionType);
        list.add(field);
      } else {
        list.add(field);
      }
    }
    return nested;
  }

  /**
   * Converts an instance of RelDataType to an instance of SqlDataTypeSpec.
   *
   * @param type         type descriptor
   * @param charSetName  charSet name
   * @param maxPrecision The max allowed precision.
   * @return corresponding parse representation
   */
  public static SqlDataTypeSpec convertTypeToSpec(RelDataType type,
      String charSetName, int maxPrecision) {
    SqlTypeName typeName = type.getSqlTypeName();

    // TODO jvs 28-Dec-2004:  support row types, user-defined types,
    // interval types, multiset types, etc
    assert typeName != null;

    int precision = typeName.allowsPrec() ? type.getPrecision() : -1;
    // fix up the precision.
    if (maxPrecision > 0 && precision > maxPrecision) {
      precision = maxPrecision;
    }
    int scale = typeName.allowsScale() ? type.getScale() : -1;

    final SqlBasicTypeNameSpec typeNameSpec = new SqlBasicTypeNameSpec(
        typeName,
        precision,
        scale,
        charSetName,
        SqlParserPos.ZERO);

    // REVIEW jvs 28-Dec-2004:  discriminate between precision/scale
    // zero and unspecified?

    // REVIEW angel 11-Jan-2006:
    // Use neg numbers to indicate unspecified precision/scale

    return new SqlDataTypeSpec(typeNameSpec, SqlParserPos.ZERO);
  }

  /**
   * Converts an instance of RelDataType to an instance of SqlDataTypeSpec.
   *
   * @param type type descriptor
   * @return corresponding parse representation
   */
  public static SqlDataTypeSpec convertTypeToSpec(RelDataType type) {
    // TODO jvs 28-Dec-2004:  collation
    String charSetName = inCharFamily(type) ? type.getCharset().name() : null;
    return convertTypeToSpec(type, charSetName, -1);
  }

  public static RelDataType createMultisetType(
      RelDataTypeFactory typeFactory,
      RelDataType type,
      boolean nullable) {
    RelDataType ret = typeFactory.createMultisetType(type, -1);
    return typeFactory.createTypeWithNullability(ret, nullable);
  }

  public static RelDataType createArrayType(
      RelDataTypeFactory typeFactory,
      RelDataType type,
      boolean nullable) {
    RelDataType ret = typeFactory.createArrayType(type, -1);
    return typeFactory.createTypeWithNullability(ret, nullable);
  }

  public static RelDataType createMapType(
      RelDataTypeFactory typeFactory,
      RelDataType keyType,
      RelDataType valueType,
      boolean nullable) {
    RelDataType ret = typeFactory.createMapType(keyType, valueType);
    return typeFactory.createTypeWithNullability(ret, nullable);
  }

  /**
   * Adds collation and charset to a character type, returns other types
   * unchanged.
   *
   * @param type        Type
   * @param typeFactory Type factory
   * @return Type with added charset and collation, or unchanged type if it is
   * not a char type.
   */
  public static RelDataType addCharsetAndCollation(
      RelDataType type,
      RelDataTypeFactory typeFactory) {
    if (!inCharFamily(type)) {
      return type;
    }
    Charset charset = type.getCharset();
    if (charset == null) {
      charset = typeFactory.getDefaultCharset();
    }
    SqlCollation collation = type.getCollation();
    if (collation == null) {
      collation = SqlCollation.IMPLICIT;
    }

    // todo: should get the implicit collation from repository
    //   instead of null
    type =
        typeFactory.createTypeWithCharsetAndCollation(
            type,
            charset,
            collation);
    SqlValidatorUtil.checkCharsetAndCollateConsistentIfCharType(type);
    return type;
  }

  /**
   * Returns whether two types are equal, ignoring nullability.
   *
   * <p>They need not come from the same factory.
   *
   * @param factory Type factory
   * @param type1   First type
   * @param type2   Second type
   * @return whether types are equal, ignoring nullability
   */
  public static boolean equalSansNullability(
      RelDataTypeFactory factory,
      RelDataType type1,
      RelDataType type2) {
    if (type1.equals(type2)) {
      return true;
    }

    if (type1.isNullable() == type2.isNullable()) {
      // If types have the same nullability and they weren't equal above,
      // they must be different.
      return false;
    }
    return type1.equals(
        factory.createTypeWithNullability(type2, type1.isNullable()));
  }

  /**
   * Returns whether two struct types are equal, ignoring nullability.
   *
   * <p>They do not need to come from the same factory.
   *
   * @param factory       Type factory
   * @param type1         First type
   * @param type2         Second type
   * @param nameMatcher   Name matcher used to compare the field names
   * @return Whether types are equal, ignoring nullability
   */
  public static boolean equalAsStructSansNullability(
      RelDataTypeFactory factory,
      RelDataType type1,
      RelDataType type2,
      SqlNameMatcher nameMatcher) {
    assert type1.isStruct();
    assert type2.isStruct();

    if (type1.getFieldCount() != type2.getFieldCount()) {
      return false;
    }

    for (Pair<RelDataTypeField, RelDataTypeField> pair
        : Pair.zip(type1.getFieldList(), type2.getFieldList())) {
      if (!nameMatcher.matches(pair.left.getName(), pair.right.getName())) {
        return false;
      }
      if (!equalSansNullability(factory, pair.left.getType(), pair.right.getType())) {
        return false;
      }
    }

    return true;
  }

  /**
   * Returns the ordinal of a given field in a record type, or -1 if the field
   * is not found.
   *
   * @param type      Record type
   * @param fieldName Name of field
   * @return Ordinal of field
   */
  public static int findField(RelDataType type, String fieldName) {
    List<RelDataTypeField> fields = type.getFieldList();
    for (int i = 0; i < fields.size(); i++) {
      RelDataTypeField field = fields.get(i);
      if (field.getName().equals(fieldName)) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Selects data types of the specified fields from an input row type.
   * This is useful when identifying data types of a function that is going
   * to operate on inputs that are specified as field ordinals (e.g.
   * aggregate calls).
   *
   * @param rowType input row type
   * @param requiredFields ordinals of the projected fields
   * @return list of data types that are requested by requiredFields
   */
  public static List<RelDataType> projectTypes(final RelDataType rowType,
      final List<? extends Number> requiredFields) {
    final List<RelDataTypeField> fields = rowType.getFieldList();

    return new AbstractList<RelDataType>() {
      @Override public RelDataType get(int index) {
        return fields.get(requiredFields.get(index).intValue()).getType();
      }

      @Override public int size() {
        return requiredFields.size();
      }
    };
  }

  /**
   * Records a struct type with no fields.
   *
   * @param typeFactory Type factory
   * @return Struct type with no fields
   */
  public static RelDataType createEmptyStructType(
      RelDataTypeFactory typeFactory) {
    return typeFactory.createStructType(
        ImmutableList.of(),
        ImmutableList.of());
  }

  /** Returns whether a type is flat. It is not flat if it is a record type that
   * has one or more fields that are themselves record types. */
  public static boolean isFlat(RelDataType type) {
    if (type.isStruct()) {
      for (RelDataTypeField field : type.getFieldList()) {
        if (field.getType().isStruct()) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Returns whether two types are comparable. They need to be scalar types of
   * the same family, or struct types whose fields are pairwise comparable.
   *
   * @param type1 First type
   * @param type2 Second type
   * @return Whether types are comparable
   */
  public static boolean isComparable(RelDataType type1, RelDataType type2) {
    if (type1.isStruct() != type2.isStruct()) {
      return false;
    }

    if (type1.isStruct()) {
      int n = type1.getFieldCount();
      if (n != type2.getFieldCount()) {
        return false;
      }
      for (Pair<RelDataTypeField, RelDataTypeField> pair
          : Pair.zip(type1.getFieldList(), type2.getFieldList())) {
        if (!isComparable(pair.left.getType(), pair.right.getType())) {
          return false;
        }
      }
      return true;
    }

    final RelDataTypeFamily family1 = family(type1);
    final RelDataTypeFamily family2 = family(type2);
    if (family1 == family2) {
      return true;
    }

    // If one of the arguments is of type 'ANY', return true.
    if (family1 == SqlTypeFamily.ANY
        || family2 == SqlTypeFamily.ANY) {
      return true;
    }

    // If one of the arguments is of type 'NULL', return true.
    if (family1 == SqlTypeFamily.NULL
        || family2 == SqlTypeFamily.NULL) {
      return true;
    }

    // We can implicitly convert from character to date
    if (family1 == SqlTypeFamily.CHARACTER
        && canConvertStringInCompare(family2)
        || family2 == SqlTypeFamily.CHARACTER
        && canConvertStringInCompare(family1)) {
      return true;
    }

    return false;
  }

  /** Returns the least restrictive type T, such that a value of type T can be
   * compared with values of type {@code type0} and {@code type1} using
   * {@code =}. */
  public static RelDataType leastRestrictiveForComparison(
      RelDataTypeFactory typeFactory, RelDataType type1, RelDataType type2) {
    final RelDataType type =
        typeFactory.leastRestrictive(ImmutableList.of(type1, type2));
    if (type != null) {
      return type;
    }
    final RelDataTypeFamily family1 = family(type1);
    final RelDataTypeFamily family2 = family(type2);

    // If one of the arguments is of type 'ANY', we can compare.
    if (family1 == SqlTypeFamily.ANY) {
      return type2;
    }
    if (family2 == SqlTypeFamily.ANY) {
      return type1;
    }

    // If one of the arguments is of type 'NULL', we can compare.
    if (family1 == SqlTypeFamily.NULL) {
      return type2;
    }
    if (family2 == SqlTypeFamily.NULL) {
      return type1;
    }

    // We can implicitly convert from character to date, numeric, etc.
    if (family1 == SqlTypeFamily.CHARACTER
        && canConvertStringInCompare(family2)) {
      return type2;
    }
    if (family2 == SqlTypeFamily.CHARACTER
        && canConvertStringInCompare(family1)) {
      return type1;
    }

    return null;
  }

  protected static RelDataTypeFamily family(RelDataType type) {
    // REVIEW jvs 2-June-2005:  This is needed to keep
    // the Saffron type system happy.
    RelDataTypeFamily family = null;
    if (type.getSqlTypeName() != null) {
      family = type.getSqlTypeName().getFamily();
    }
    if (family == null) {
      family = type.getFamily();
    }
    return family;
  }

  /**
   * Returns whether all types in a collection have the same family, as
   * determined by {@link #isSameFamily(RelDataType, RelDataType)}.
   *
   * @param types Types to check
   * @return true if all types are of the same family
   */
  public static boolean areSameFamily(Iterable<RelDataType> types) {
    final List<RelDataType> typeList = ImmutableList.copyOf(types);
    if (Sets.newHashSet(RexUtil.families(typeList)).size() < 2) {
      return true;
    }
    for (Pair<RelDataType, RelDataType> adjacent : Pair.adjacents(typeList)) {
      if (!isSameFamily(adjacent.left, adjacent.right)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns whether two types are scalar types of the same family, or struct types whose fields
   * are pairwise of the same family.
   *
   * @param type1 First type
   * @param type2 Second type
   * @return Whether types have the same family
   */
  private static boolean isSameFamily(RelDataType type1, RelDataType type2) {
    if (type1.isStruct() != type2.isStruct()) {
      return false;
    }

    if (type1.isStruct()) {
      int n = type1.getFieldCount();
      if (n != type2.getFieldCount()) {
        return false;
      }
      for (Pair<RelDataTypeField, RelDataTypeField> pair
          : Pair.zip(type1.getFieldList(), type2.getFieldList())) {
        if (!isSameFamily(pair.left.getType(), pair.right.getType())) {
          return false;
        }
      }
      return true;
    }

    final RelDataTypeFamily family1 = family(type1);
    final RelDataTypeFamily family2 = family(type2);
    return family1 == family2;
  }

  /** Returns whether a character data type can be implicitly converted to a
   * given family in a compare operation. */
  private static boolean canConvertStringInCompare(RelDataTypeFamily family) {
    if (family instanceof SqlTypeFamily) {
      SqlTypeFamily sqlTypeFamily = (SqlTypeFamily) family;
      switch (sqlTypeFamily) {
      case DATE:
      case TIME:
      case TIMESTAMP:
      case INTERVAL_DAY_TIME:
      case INTERVAL_YEAR_MONTH:
      case NUMERIC:
      case APPROXIMATE_NUMERIC:
      case EXACT_NUMERIC:
      case INTEGER:
      case BOOLEAN:
        return true;
      }
    }
    return false;
  }

  /**
   * Checks whether a type represents Unicode character data.
   *
   * @param type type to test
   * @return whether type represents Unicode character data
   */
  public static boolean isUnicode(RelDataType type) {
    Charset charset = type.getCharset();
    if (charset == null) {
      return false;
    }
    return charset.name().startsWith("UTF");
  }

  /** Returns the larger of two precisions, treating
   * {@link RelDataType#PRECISION_NOT_SPECIFIED} as infinity. */
  public static int maxPrecision(int p0, int p1) {
    return (p0 == RelDataType.PRECISION_NOT_SPECIFIED
        || p0 >= p1
        && p1 != RelDataType.PRECISION_NOT_SPECIFIED) ? p0 : p1;
  }

  /** Returns whether a precision is greater or equal than another,
   * treating {@link RelDataType#PRECISION_NOT_SPECIFIED} as infinity. */
  public static int comparePrecision(int p0, int p1) {
    if (p0 == p1) {
      return 0;
    }
    if (p0 == RelDataType.PRECISION_NOT_SPECIFIED) {
      return 1;
    }
    if (p1 == RelDataType.PRECISION_NOT_SPECIFIED) {
      return -1;
    }
    return Integer.compare(p0, p1);
  }

  /**
   * @return true if type is ARRAY
   */
  public static boolean isArray(RelDataType type) {
    return type.getSqlTypeName() == SqlTypeName.ARRAY;
  }

  /**
   * @return true if type is MAP
   */
  public static boolean isMap(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }

    return type.getSqlTypeName() == SqlTypeName.MAP;
  }

  /**
   * @return true if type is CHARACTER
   */
  public static boolean isCharacter(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }

    return SqlTypeFamily.CHARACTER.contains(type);
  }

  /**
   * @return true if the type is a CHARACTER or contains a CHARACTER type
   */
  public static boolean hasCharactor(RelDataType type) {
    if (isCharacter(type)) {
      return true;
    }
    if (isArray(type)) {
      return hasCharactor(type.getComponentType());
    }
    return false;
  }

  /**
   * @return true if type is STRING
   */
  public static boolean isString(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }

    return SqlTypeFamily.STRING.contains(type);
  }

  /**
   * @return true if type is BOOLEAN
   */
  public static boolean isBoolean(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }

    return SqlTypeFamily.BOOLEAN.contains(type);
  }

  /**
   * @return true if type is BINARY
   */
  public static boolean isBinary(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }

    return SqlTypeFamily.BINARY.contains(type);
  }

  /**
   * @return true if type is Atomic
   */
  public static boolean isAtomic(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      return false;
    }

    return SqlTypeUtil.isDatetime(type)
        || SqlTypeUtil.isNumeric(type)
        || SqlTypeUtil.isString(type)
        || SqlTypeUtil.isBoolean(type);
  }

  /** Get decimal with max precision/scale for the current type system. */
  public static RelDataType getMaxPrecisionScaleDecimal(RelDataTypeFactory factory) {
    int maxPrecision = factory.getTypeSystem().getMaxNumericPrecision();
    int maxScale = factory.getTypeSystem().getMaxNumericScale();

    return factory.createSqlType(SqlTypeName.DECIMAL, maxPrecision, maxScale);
  }
}

// End SqlTypeUtil.java
