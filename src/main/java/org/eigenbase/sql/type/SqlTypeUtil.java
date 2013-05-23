/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.sql.type;

import java.nio.charset.*;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;
import org.eigenbase.util14.*;


/**
 * Contains utility methods used during SQL validation or type derivation.
 *
 * @author Wael Chatila
 * @version $Id$
 * @since Sep 3, 2004
 */
public abstract class SqlTypeUtil
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Checks whether two types or more are char comparable.
     *
     * @return Returns true if all operands are of char type and if they are
     * comparable, i.e. of the same charset and collation of same charset
     *
     * @pre argTypes != null
     * @pre argTypes.length >= 2
     */
    public static boolean isCharTypeComparable(List<RelDataType> argTypes)
    {
        assert null != argTypes : "precondition failed";
        assert 2 <= argTypes.size() : "precondition failed";

        for (int j = 0; j < (argTypes.size() - 1); j++) {
            RelDataType t0 = argTypes.get(j);
            RelDataType t1 = argTypes.get(j + 1);

            if (!inCharFamily(t0) || !inCharFamily(t1)) {
                return false;
            }

            if (null == t0.getCharset()) {
                throw Util.newInternal(
                    "RelDataType object should have been assigned a "
                    + "(default) charset when calling deriveType");
            } else if (!t0.getCharset().equals(t1.getCharset())) {
                return false;
            }

            if (null == t0.getCollation()) {
                throw Util.newInternal(
                    "RelDataType object should have been assigned a "
                    + "(default) collation when calling deriveType");
            } else if (
                !t0.getCollation().getCharset().equals(
                    t1.getCollation().getCharset()))
            {
                return false;
            }
        }

        return true;
    }

    /**
     * Returns whether the operands to a call are char type-comparable.
     *
     * @param binding Binding of call to operands
     * @param operands Operands to check for compatibility; usually the operands
     * of the bound call, but not always
     * @param throwOnFailure Whether to throw an exception on failure
     *
     * @return whether operands are valid
     *
     * @pre null != operands
     * @pre 2 <= operands.length
     */
    public static boolean isCharTypeComparable(
        SqlCallBinding binding,
        SqlNode [] operands,
        boolean throwOnFailure)
    {
        final SqlValidator validator = binding.getValidator();
        final SqlValidatorScope scope = binding.getScope();
        assert null != operands : "precondition failed";
        assert 2 <= operands.length : "precondition failed";

        if (!isCharTypeComparable(
                deriveAndCollectTypes(validator, scope, operands)))
        {
            if (throwOnFailure) {
                String msg = "";
                for (int i = 0; i < operands.length; i++) {
                    if (i > 0) {
                        msg += ", ";
                    }
                    msg += operands[i].toString();
                }
                throw binding.newError(
                    EigenbaseResource.instance().OperandNotComparable.ex(msg));
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
        SqlNode [] operands)
    {
        // NOTE: Do not use an AbstractList. Don't want to be lazy. We want
        // errors.
        List<RelDataType> types = new ArrayList<RelDataType>();
        for (SqlNode operand : operands) {
            types.add(validator.deriveType(scope, operand));
        }
        return types;
    }

    /**
     * Collects the row types of an array of relational expressions.
     *
     * @param rels array of relational expressions
     *
     * @return array of row types
     */
    public static RelDataType [] collectTypes(RelNode [] rels)
    {
        RelDataType [] types = new RelDataType[rels.length];
        for (int i = 0; i < types.length; i++) {
            types[i] = rels[i].getRowType();
        }
        return types;
    }

    /**
     * Promotes a type to a row type (does nothing if it already is one).
     *
     * @param type type to be promoted
     * @param fieldName name to give field in row type; null for default of
     * "ROW_VALUE"
     *
     * @return row type
     */
    public static RelDataType promoteToRowType(
        RelDataTypeFactory typeFactory,
        RelDataType type,
        String fieldName)
    {
        if (!type.isStruct()) {
            if (fieldName == null) {
                fieldName = "ROW_VALUE";
            }
            type =
                typeFactory.createStructType(
                    new RelDataType[] { type },
                    new String[] { fieldName });
        }
        return type;
    }

    /**
     * Recreates a given RelDataType with nullablility iff any of a calls
     * operand types are nullable.
     */
    public static RelDataType makeNullableIfOperandsAre(
        final SqlValidator validator,
        final SqlValidatorScope scope,
        final SqlCall call,
        RelDataType type)
    {
        for (SqlNode operand : call.operands) {
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
        RelDataType type)
    {
        if (containsNullable(argTypes)) {
            type = typeFactory.createTypeWithNullability(type, true);
        }
        return type;
    }

    /**
     * Returns whether one or more of an array of types is nullable.
     */
    public static boolean containsNullable(List<RelDataType> types)
    {
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
    public static boolean containsNullable(RelDataType type)
    {
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
        RelDataType type)
    {
        return SqlTypeName.ANY.equals(typeName)
            || typeName.equals(type.getSqlTypeName());
    }

    /**
     * Returns true if any element in <code>typeNames</code> matches
     * type.getSqlTypeName().
     *
     * @see #isOfSameTypeName(SqlTypeName, RelDataType)
     */
    public static boolean isOfSameTypeName(
        List<SqlTypeName> typeNames,
        RelDataType type)
    {
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
    public static boolean isDatetime(RelDataType type)
    {
        return SqlTypeFamily.DATETIME.contains(type);
    }

    /**
     * @return true if type is some kind of INTERVAL
     */
    public static boolean isInterval(RelDataType type)
    {
        return SqlTypeFamily.DATETIME_INTERVAL.contains(type);
    }

    /**
     * @return true if type is in SqlTypeFamily.Character
     */
    public static boolean inCharFamily(RelDataType type)
    {
        return type.getFamily() == SqlTypeFamily.CHARACTER;
    }

    /**
     * @return true if type is in SqlTypeFamily.Character
     */
    public static boolean inCharFamily(SqlTypeName typeName)
    {
        return SqlTypeFamily.getFamilyForSqlType(typeName)
            == SqlTypeFamily.CHARACTER;
    }

    /**
     * @return true if type is in SqlTypeFamily.Boolean
     */
    public static boolean inBooleanFamily(RelDataType type)
    {
        return type.getFamily() == SqlTypeFamily.BOOLEAN;
    }

    /**
     * @return true if two types are in same type family
     */
    public static boolean inSameFamily(RelDataType t1, RelDataType t2)
    {
        return t1.getFamily() == t2.getFamily();
    }

    /**
     * @return true if two types are in same type family, or one or the other is
     * of type SqlTypeName.Null
     */
    public static boolean inSameFamilyOrNull(RelDataType t1, RelDataType t2)
    {
        return (t1.getSqlTypeName() == SqlTypeName.NULL)
            || (t2.getSqlTypeName() == SqlTypeName.NULL)
            || (t1.getFamily() == t2.getFamily());
    }

    /**
     * @return true if type family is either character or binary
     */
    public static boolean inCharOrBinaryFamilies(RelDataType type)
    {
        return (type.getFamily() == SqlTypeFamily.CHARACTER)
            || (type.getFamily() == SqlTypeFamily.BINARY);
    }

    /**
     * @return true if type is a LOB of some kind
     */
    public static boolean isLob(RelDataType type)
    {
        // TODO jvs 9-Dec-2004:  once we support LOB types
        return false;
    }

    /**
     * @return true if type is variable width with bounded precision
     */
    public static boolean isBoundedVariableWidth(RelDataType type)
    {
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
    public static boolean isIntType(RelDataType type)
    {
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
    public static boolean isDecimal(RelDataType type)
    {
        SqlTypeName typeName = type.getSqlTypeName();
        if (typeName == null) {
            return false;
        }
        return typeName == SqlTypeName.DECIMAL;
    }

    /**
     * @return true if type is bigint
     */
    public static boolean isBigint(RelDataType type)
    {
        SqlTypeName typeName = type.getSqlTypeName();
        if (typeName == null) {
            return false;
        }
        return typeName == SqlTypeName.BIGINT;
    }

    /**
     * @return true if type is numeric with exact precision
     */
    public static boolean isExactNumeric(RelDataType type)
    {
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

    /**
     * Returns the maximum value of an integral type, as a long value
     */
    public static long maxValue(RelDataType type)
    {
        assert (SqlTypeUtil.isIntType(type));
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
    public static boolean isApproximateNumeric(RelDataType type)
    {
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
    public static boolean isNumeric(RelDataType type)
    {
        return isExactNumeric(type) || isApproximateNumeric(type);
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
    public static boolean sameNamedType(RelDataType t1, RelDataType t2)
    {
        if (t1.isStruct() || t2.isStruct()) {
            if (!t1.isStruct() || !t2.isStruct()) {
                return false;
            }
            if (t1.getFieldCount() != t2.getFieldCount()) {
                return false;
            }
            RelDataTypeField [] fields1 = t1.getFields();
            RelDataTypeField [] fields2 = t2.getFields();
            for (int i = 0; i < fields1.length; ++i) {
                if (!sameNamedType(
                        fields1[i].getType(),
                        fields2[i].getType()))
                {
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
     *
     * @return maximum bytes, or 0 for a fixed-width type or type with unknown
     * maximum
     */
    public static int getMaxByteSize(RelDataType type)
    {
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
    public static long getMinValue(RelDataType type)
    {
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
            throw Util.newInternal("getMinValue(" + typeName + ")");
        }
    }

    /**
     * Determines the maximum unscaled value of a numeric type
     *
     * @param type a numeric type
     */
    public static long getMaxValue(RelDataType type)
    {
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
            throw Util.newInternal("getMaxValue(" + typeName + ")");
        }
    }

    /**
     * @return true if type has a representation as a Java primitive (ignoring
     * nullability)
     */
    public static boolean isJavaPrimitive(RelDataType type)
    {
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
    public static String getPrimitiveWrapperJavaClassName(RelDataType type)
    {
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
            return getNumericJavaClassName(type);
        }
    }

    /**
     * @return class name of the numeric data type.
     */
    public static String getNumericJavaClassName(RelDataType type)
    {
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

    /**
     * Tests assignability of a value to a site.
     *
     * @param toType type of the target site
     * @param fromType type of the source value
     *
     * @return true iff assignable
     */
    public static boolean canAssignFrom(
        RelDataType toType,
        RelDataType fromType)
    {
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
     *
     * @return true iff mismatched
     */
    public static boolean areCharacterSetsMismatched(
        RelDataType t1,
        RelDataType t2)
    {
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
     * necessary. We're using it as a hack because {@link
     * SqlTypeFactoryImpl#leastRestrictiveSqlType} isn't complete enough yet.
     * Once it is, this param (and the non-coerce rules of {@link
     * SqlTypeAssignmentRules}) should go away.
     *
     * @param toType target of assignment
     * @param fromType source of assignment
     * @param coerce if true, the SQL rules for CAST are used; if false, the
     * rules are similar to Java; e.g. you can't assign short x = (int) y, and
     * you can't assign int x = (String) z.
     *
     * @return true iff cast is legal
     */
    public static boolean canCastFrom(
        RelDataType toType,
        RelDataType fromType,
        boolean coerce)
    {
        if (toType == fromType) {
            return true;
        }
        final SqlTypeName fromTypeName = fromType.getSqlTypeName();
        if (fromTypeName == SqlTypeName.ANY) {
            return true;
        }
        final SqlTypeName toTypeName = toType.getSqlTypeName();
        if (toType.isStruct() || fromType.isStruct()) {
            if (toTypeName == SqlTypeName.DISTINCT) {
                if (fromTypeName == SqlTypeName.DISTINCT) {
                    // can't cast between different distinct types
                    return false;
                }
                return canCastFrom(
                    toType.getFields()[0].getType(),
                    fromType,
                    coerce);
            } else if (fromTypeName == SqlTypeName.DISTINCT) {
                return canCastFrom(
                    toType,
                    fromType.getFields()[0].getType(),
                    coerce);
            } else if (toTypeName == SqlTypeName.ROW) {
                if (fromTypeName != SqlTypeName.ROW) {
                    return false;
                }
                int n = toType.getFieldCount();
                if (fromType.getFieldCount() != n) {
                    return false;
                }
                for (int i = 0; i < n; ++i) {
                    RelDataTypeField toField = toType.getFields()[i];
                    RelDataTypeField fromField = fromType.getFields()[i];
                    if (!canCastFrom(
                            toField.getType(),
                            fromField.getType(),
                            coerce))
                    {
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
            || (isInterval(toType) && isExactNumeric(fromType)))
        {
            IntervalSqlType intervalType =
                (IntervalSqlType) (isInterval(fromType) ? fromType : toType);
            if (!intervalType.getIntervalQualifier().isSingleDatetimeField()) {
                // Casts between intervals and exact numerics must involve
                // intervals with a single datetime field.
                return false;
            }
        }
        SqlTypeName tn1 = toTypeName;
        SqlTypeName tn2 = fromTypeName;
        if ((tn1 == null) || (tn2 == null)) {
            return false;
        }

        // REVIEW jvs 9-Feb-2009: we don't impose SQL rules for character sets
        // here; instead, we do that in SqlCastFunction.  The reason is that
        // this method is called from at least one place (MedJdbcNameDirectory)
        // where internally a cast across character repertoires is OK.  Should
        // probably clean that up.

        SqlTypeAssignmentRules rules = SqlTypeAssignmentRules.instance();
        return rules.canCastFrom(tn1, tn2, coerce);
    }

    /**
     * @return the field names of a struct type
     */
    public static String [] getFieldNames(RelDataType type)
    {
        RelDataTypeField [] fields = type.getFields();
        String [] ret = new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            ret[i] = fields[i].getName();
        }
        return ret;
    }

    /**
     * @return the field types of a struct type
     */
    public static RelDataType [] getFieldTypes(RelDataType type)
    {
        RelDataTypeField [] fields = type.getFields();
        RelDataType [] ret = new RelDataType[fields.length];
        for (int i = 0; i < fields.length; i++) {
            ret[i] = fields[i].getType();
        }
        return ret;
    }

    /**
     * Flattens a record type by recursively expanding any fields which are
     * themselves record types. For each record type, a representative null
     * value field is also prepended (with state NULL for a null value and FALSE
     * for non-null), and all component types are asserted to be nullable, since
     * SQL doesn't allow NOT NULL to be specified on attributes.
     *
     * @param typeFactory factory which should produced flattened type
     * @param recordType type with possible nesting
     * @param flatteningMap if non-null, receives map from unflattened ordinal
     * to flattened ordinal (must have length at least
     * recordType.getFieldList().size())
     *
     * @return flattened equivalent
     */
    public static RelDataType flattenRecordType(
        RelDataTypeFactory typeFactory,
        RelDataType recordType,
        int [] flatteningMap)
    {
        if (!recordType.isStruct()) {
            return recordType;
        }
        List<RelDataTypeField> fieldList = new ArrayList<RelDataTypeField>();
        boolean nested =
            flattenFields(
                typeFactory,
                recordType,
                fieldList,
                flatteningMap);
        if (!nested) {
            return recordType;
        }
        List<RelDataType> types = new ArrayList<RelDataType>();
        List<String> fieldNames = new ArrayList<String>();
        int i = -1;
        for (RelDataTypeField field : fieldList) {
            ++i;
            types.add(field.getType());
            fieldNames.add(field.getName() + "_" + i);
        }
        return typeFactory.createStructType(types, fieldNames);
    }

    /**
     * Creates a record type with anonymous field names.
     */
    public static RelDataType createStructType(
        RelDataTypeFactory typeFactory,
        final RelDataType [] types)
    {
        return typeFactory.createStructType(
            new RelDataTypeFactory.FieldInfo() {
                public int getFieldCount()
                {
                    return types.length;
                }

                public String getFieldName(int index)
                {
                    return "$" + index;
                }

                public RelDataType getFieldType(int index)
                {
                    return types[index];
                }
            });
    }

    public static boolean needsNullIndicator(RelDataType recordType)
    {
        // NOTE jvs 9-Mar-2005: It would be more storage-efficient to say that
        // no null indicator is required for structured type columns declared
        // as NOT NULL.  However, the uniformity of always having a null
        // indicator makes things cleaner in many places.
        return (recordType.getSqlTypeName() == SqlTypeName.STRUCTURED);
    }

    private static boolean flattenFields(
        RelDataTypeFactory typeFactory,
        RelDataType type,
        List<RelDataTypeField> list,
        int [] flatteningMap)
    {
        boolean nested = false;
        if (needsNullIndicator(type)) {
            // NOTE jvs 9-Mar-2005:  other code
            // (e.g. RelStructuredTypeFlattener) relies on the
            // null indicator field coming first.
            RelDataType indicatorType =
                typeFactory.createSqlType(SqlTypeName.BOOLEAN);
            if (type.isNullable()) {
                indicatorType =
                    typeFactory.createTypeWithNullability(
                        indicatorType,
                        true);
            }
            RelDataTypeField nullIndicatorField =
                new RelDataTypeFieldImpl(
                    "NULL_VALUE",
                    0,
                    indicatorType);
            list.add(nullIndicatorField);
            nested = true;
        }
        for (RelDataTypeField field1 : type.getFieldList()) {
            RelDataTypeField field = (RelDataTypeField) field1;
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
     * @param type type descriptor
     *
     * @return corresponding parse representation
     */
    public static SqlDataTypeSpec convertTypeToSpec(RelDataType type)
    {
        SqlTypeName typeName = type.getSqlTypeName();

        // TODO jvs 28-Dec-2004:  support row types, user-defined types,
        // interval types, multiset types, etc
        assert (typeName != null);
        SqlIdentifier typeIdentifier =
            new SqlIdentifier(
                typeName.name(),
                SqlParserPos.ZERO);

        String charSetName = null;

        if (inCharFamily(type)) {
            charSetName = type.getCharset().name();
            // TODO jvs 28-Dec-2004:  collation
        }

        // REVIEW jvs 28-Dec-2004:  discriminate between precision/scale
        // zero and unspecified?

        // REVIEW angel 11-Jan-2006:
        // Use neg numbers to indicate unspecified precision/scale

        if (typeName.allowsScale()) {
            return new SqlDataTypeSpec(
                typeIdentifier,
                type.getPrecision(),
                type.getScale(),
                charSetName,
                null,
                SqlParserPos.ZERO);
        } else if (typeName.allowsPrec()) {
            return new SqlDataTypeSpec(
                typeIdentifier,
                type.getPrecision(),
                -1,
                charSetName,
                null,
                SqlParserPos.ZERO);
        } else {
            return new SqlDataTypeSpec(
                typeIdentifier,
                -1,
                -1,
                charSetName,
                null,
                SqlParserPos.ZERO);
        }
    }

    public static RelDataType createMultisetType(
        RelDataTypeFactory typeFactory,
        RelDataType type,
        boolean nullable)
    {
        RelDataType ret = typeFactory.createMultisetType(type, -1);
        return typeFactory.createTypeWithNullability(ret, nullable);
    }

    public static RelDataType createArrayType(
        RelDataTypeFactory typeFactory,
        RelDataType type,
        boolean nullable)
    {
        RelDataType ret = typeFactory.createArrayType(type, -1);
        return typeFactory.createTypeWithNullability(ret, nullable);
    }

    public static RelDataType createMapType(
        RelDataTypeFactory typeFactory,
        RelDataType keyType,
        RelDataType valueType,
        boolean nullable)
    {
        RelDataType ret = typeFactory.createMapType(keyType, valueType);
        return typeFactory.createTypeWithNullability(ret, nullable);
    }

    /**
     * Adds collation and charset to a character type, returns other types
     * unchanged.
     *
     * @param type Type
     * @param typeFactory Type factory
     *
     * @return Type with added charset and collation, or unchanged type if it is
     * not a char type.
     */
    public static RelDataType addCharsetAndCollation(
        RelDataType type,
        RelDataTypeFactory typeFactory)
    {
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
     * @param type1 First type
     * @param type2 Second type
     *
     * @return whether types are equal, ignoring nullability
     */
    public static boolean equalSansNullability(
        RelDataTypeFactory factory,
        RelDataType type1,
        RelDataType type2)
    {
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
     * Adds a field to a record type at a specified position.
     *
     * <p>For example, if type is <code>(A integer, B boolean)</code>, and
     * fieldType is <code>varchar(10)</code>, then <code>prepend(typeFactory,
     * type, 0, "Z", fieldType)</code> will return <code>(Z varchar(10), A
     * integer, B boolean)</code>.
     *
     * @param typeFactory Type factory
     * @param type Record type
     * @param at Ordinal to add field
     * @param fieldName Name of new field
     * @param fieldType Type of new field
     *
     * @return Extended record type
     */
    public static RelDataType addField(
        RelDataTypeFactory typeFactory,
        final RelDataType type,
        final int at,
        final String fieldName,
        final RelDataType fieldType)
    {
        return typeFactory.createStructType(
            new RelDataTypeFactory.FieldInfo() {
                public int getFieldCount()
                {
                    return type.getFieldCount() + 1;
                }

                public String getFieldName(int index)
                {
                    if (index == at) {
                        return fieldName;
                    }
                    if (index > at) {
                        --index;
                    }
                    return type.getFieldList().get(index).getName();
                }

                public RelDataType getFieldType(int index)
                {
                    if (index == at) {
                        return fieldType;
                    }
                    if (index > at) {
                        --index;
                    }
                    return type.getFieldList().get(index).getType();
                }
            });
    }

    /**
     * Returns the ordinal of a given field in a record type, or -1 if the field
     * is not found.
     *
     * @param type Record type
     * @param fieldName Name of field
     *
     * @return Ordinal of field
     */
    public static int findField(RelDataType type, String fieldName)
    {
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
     * Records a struct type with no fields.
     *
     * @param typeFactory Type factory
     *
     * @return Struct type with no fields
     */
    public static RelDataType createEmptyStructType(
        RelDataTypeFactory typeFactory)
    {
        return typeFactory.createStructType(
            new RelDataType[0],
            new String[0]);
    }

    /**
     * Returns whether two types are comparable. They need to be scalar types of
     * the same family, or struct types whose fields are pairwise comparable.
     *
     * @param type1 First type
     * @param type2 Second type
     *
     * @return Whether types are comparable
     */
    public static boolean isComparable(RelDataType type1, RelDataType type2)
    {
        if (type1.isStruct() != type2.isStruct()) {
            return false;
        }

        if (type1.isStruct()) {
            int n = type1.getFieldCount();
            if (n != type2.getFieldCount()) {
                return false;
            }
            for (int i = 0; i < n; ++i) {
                RelDataTypeField field1 =
                    (RelDataTypeField) type1.getFieldList().get(i);
                RelDataTypeField field2 =
                    (RelDataTypeField) type2.getFieldList().get(i);
                if (!isComparable(
                        field1.getType(),
                        field2.getType()))
                {
                    return false;
                }
            }
            return true;
        }
        RelDataTypeFamily family1 = null;
        RelDataTypeFamily family2 = null;

        // REVIEW jvs 2-June-2005:  This is needed to keep
        // the Saffron type system happy.
        if (type1.getSqlTypeName() != null) {
            family1 = type1.getSqlTypeName().getFamily();
        }
        if (type2.getSqlTypeName() != null) {
            family2 = type2.getSqlTypeName().getFamily();
        }
        if (family1 == null) {
            family1 = type1.getFamily();
        }
        if (family2 == null) {
            family2 = type2.getFamily();
        }
        if (family1 == family2) {
            return true;
        }
        return false;
    }

    /**
     * Checks whether a type represents Unicode character data.
     *
     * @param type type to test
     *
     * @return whether type represents Unicode character data
     */
    public static boolean isUnicode(RelDataType type)
    {
        Charset charset = type.getCharset();
        if (charset == null) {
            return false;
        }
        return charset.name().startsWith("UTF");
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Convenience class for building a struct type with several fields.
     *
     * <p>TypeBuilder is more convenient because it supports chained calls to
     * add one field at a time. The chained syntax means that you can use a
     * TypeBuilder in contexts where only an expression is allowed, for example
     * in a field initializer. There are several overloadings of the <code>
     * add</code> method for types which take different parameters, and you can
     * explicitly state whether you want a field to be nullable.
     *
     * <p>For example, to create the type
     *
     * <blockquote>
     * <pre>
     * (A BOOLEAN NOT NULL, B VARCHAR(10), C DECIMAL(6,2))
     * </pre>
     * </blockquote>
     *
     * the code is
     *
     * <blockquote>
     * <pre>
     * RelDataTypeFactory typeFactory;
     * return new TypeBuilder(typeFactory)
     *     .add("A", SqlTypeName.BOOLEAN, false)
     *     .add("B", SqlTypeName.VARCHAR(10), true)
     *     .add("C", SqlTypeName.DECIMAL, 6, 2, false)
     *     .type();
     * </pre>
     * </blockquote>
     *
     * <p>The equivalent conventional code is:
     *
     * <blockquote>
     * <pre>
     * RelDataTypeFactory typeFactory;
     * String[] names = {"A", "B", "C"};
     * RelDataType[] types = {
     *     typeFactory.createSqlType(SqlTypeName.BOOLEAN),
     *     typeFactory.createTypeWithNullability(
     *         typeFactory.createSqlType(SqlTypeName.VARCHAR, 10),
     *         true),
     *    typeFactory.createSqlType(SqlTypeName.DECIMAL, 6, 2)
     * };
     * return typeFactory.createStructType(names, types);
     * </pre>
     * </blockquote>
     */
    public static class TypeBuilder
    {
        private final List<RelDataTypeFieldImpl> fieldList =
            new ArrayList<RelDataTypeFieldImpl>();
        private final RelDataTypeFactory typeFactory;

        public TypeBuilder(RelDataTypeFactory typeFactory)
        {
            this.typeFactory = typeFactory;
        }

        public TypeBuilder add(
            String name,
            SqlTypeName typeName,
            boolean nullable)
        {
            RelDataType type = typeFactory.createSqlType(typeName);
            if (nullable) {
                type = typeFactory.createTypeWithNullability(type, nullable);
            }
            fieldList.add(new RelDataTypeFieldImpl(name, -1, type));
            return this;
        }

        public TypeBuilder add(
            String name,
            SqlTypeName typeName,
            int precision,
            boolean nullable)
        {
            RelDataType type = typeFactory.createSqlType(typeName, precision);
            if (nullable) {
                type = typeFactory.createTypeWithNullability(type, nullable);
            }
            fieldList.add(new RelDataTypeFieldImpl(name, -1, type));
            return this;
        }

        public TypeBuilder add(
            String name,
            SqlTypeName typeName,
            int precision,
            int scale,
            boolean nullable)
        {
            RelDataType type =
                typeFactory.createSqlType(typeName, precision, scale);
            if (nullable) {
                type = typeFactory.createTypeWithNullability(type, nullable);
            }
            fieldList.add(new RelDataTypeFieldImpl(name, -1, type));
            return this;
        }

        public RelDataType type()
        {
            return typeFactory.createStructType(
                new RelDataTypeFactory.FieldInfo() {
                    public int getFieldCount()
                    {
                        return fieldList.size();
                    }

                    public String getFieldName(int index)
                    {
                        return fieldList.get(index).getName();
                    }

                    public RelDataType getFieldType(int index)
                    {
                        return fieldList.get(index).getType();
                    }
                });
        }
    }
}

// End SqlTypeUtil.java
