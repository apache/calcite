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
package org.eigenbase.reltype;

import java.lang.reflect.*;

import java.nio.charset.*;

import java.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * Abstract base for implementations of {@link RelDataTypeFactory}.
 *
 * @author jhyde
 * @version $Id$
 * @since May 31, 2003
 */
public abstract class RelDataTypeFactoryImpl
    implements RelDataTypeFactory
{
    //~ Instance fields --------------------------------------------------------

    private HashMap<RelDataType, RelDataType> map =
        new HashMap<RelDataType, RelDataType>();

    //~ Constructors -----------------------------------------------------------

    protected RelDataTypeFactoryImpl()
    {
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelDataTypeFactory
    public RelDataType createJavaType(Class clazz)
    {
        return canonize(new JavaType(clazz));
    }

    // implement RelDataTypeFactory
    public RelDataType createJoinType(RelDataType [] types)
    {
        final RelDataType [] flattenedTypes = getTypeArray(types);
        return canonize(
            new RelCrossType(
                flattenedTypes,
                getFieldArray(flattenedTypes)));
    }

    // implement RelDataTypeFactory
    public RelDataType createStructType(
        RelDataType [] types,
        String [] fieldNames)
    {
        final RelDataTypeField [] fields = new RelDataTypeField[types.length];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = new RelDataTypeFieldImpl(fieldNames[i], i, types[i]);
        }
        return canonize(new RelRecordType(fields));
    }

    // implement RelDataTypeFactory
    public RelDataType createStructType(
        List<RelDataType> typeList,
        List<String> fieldNameList)
    {
        final RelDataTypeField [] fields =
            new RelDataTypeField[typeList.size()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] =
                new RelDataTypeFieldImpl(
                    fieldNameList.get(i),
                    i,
                    typeList.get(i));
        }
        return canonize(new RelRecordType(fields));
    }

    // implement RelDataTypeFactory
    public RelDataType createStructType(
        RelDataTypeFactory.FieldInfo fieldInfo)
    {
        final int fieldCount = fieldInfo.getFieldCount();
        final RelDataTypeField [] fields = new RelDataTypeField[fieldCount];
        for (int i = 0; i < fields.length; i++) {
            fields[i] =
                new RelDataTypeFieldImpl(
                    fieldInfo.getFieldName(i),
                    i,
                    fieldInfo.getFieldType(i));
        }
        return canonize(new RelRecordType(fields));
    }

    // implement RelDataTypeFactory
    public RelDataType leastRestrictive(RelDataType [] types)
    {
        assert (types != null);
        assert (types.length >= 1);
        RelDataType type0 = types[0];
        if (type0.isStruct()) {
            return leastRestrictiveStructuredType(types);
        }
        return null;
    }

    protected RelDataType leastRestrictiveStructuredType(RelDataType [] types)
    {
        RelDataType type0 = types[0];
        int nFields = type0.getFieldList().size();

        // precheck that all types are structs with same number of fields
        for (int i = 0; i < types.length; ++i) {
            if (!types[i].isStruct()) {
                return null;
            }
            if (types[i].getFieldList().size() != nFields) {
                return null;
            }
        }

        // recursively compute column-wise least restrictive
        RelDataType [] inputTypes = new RelDataType[types.length];
        RelDataType [] outputTypes = new RelDataType[nFields];
        String [] fieldNames = new String[nFields];
        for (int j = 0; j < nFields; ++j) {
            // REVIEW jvs 22-Jan-2004:  Always use the field name from the
            // first type?
            fieldNames[j] = type0.getFields()[j].getName();
            for (int i = 0; i < types.length; ++i) {
                inputTypes[i] = types[i].getFields()[j].getType();
            }
            outputTypes[j] = leastRestrictive(inputTypes);
        }
        return createStructType(outputTypes, fieldNames);
    }

    // copy a non-record type, setting nullability
    private RelDataType copySimpleType(
        RelDataType type,
        boolean nullable)
    {
        if (type instanceof JavaType) {
            JavaType javaType = (JavaType) type;
            if (SqlTypeUtil.inCharFamily(javaType)) {
                return new JavaType(
                    javaType.clazz,
                    nullable,
                    javaType.charset,
                    javaType.collation);
            } else {
                return new JavaType(
                    javaType.clazz,
                    nullable);
            }
        } else {
            // REVIEW: RelCrossType if it stays around; otherwise get rid of
            // this comment
            return type;
        }
    }

    // recursively copy a record type
    private RelDataType copyRecordType(
        final RelRecordType type,
        final boolean ignoreNullable,
        final boolean nullable)
    {
        // REVIEW: angel 18-Aug-2005 dtbug336
        // Shouldn't null refer to the nullability of the record type
        // not the individual field types?
        // For flattening and outer joins, it is desirable to change
        // the nullability of the individual fields.

        return createStructType(
            new FieldInfo() {
                public int getFieldCount()
                {
                    return type.getFieldList().size();
                }

                public String getFieldName(int index)
                {
                    return type.getFields()[index].getName();
                }

                public RelDataType getFieldType(int index)
                {
                    RelDataType fieldType = type.getFields()[index].getType();

                    if (ignoreNullable) {
                        return copyType(fieldType);
                    } else {
                        return createTypeWithNullability(fieldType, nullable);
                    }
                }
            });
    }

    // implement RelDataTypeFactory
    public RelDataType copyType(RelDataType type)
    {
        if (type instanceof RelRecordType) {
            return copyRecordType((RelRecordType) type, true, false);
        } else {
            return createTypeWithNullability(
                type,
                type.isNullable());
        }
    }

    // implement RelDataTypeFactory
    public RelDataType createTypeWithNullability(
        final RelDataType type,
        final boolean nullable)
    {
        RelDataType newType;
        if (type instanceof RelRecordType) {
            // REVIEW: angel 18-Aug-2005 dtbug 336 workaround
            // Changed to ignore nullable parameter if nullable is false since
            // copyRecordType implementation is doubtful
            if (nullable) {
                // Do a deep copy, setting all fields of the record type
                // to be nullable regardless of initial nullability
                newType = copyRecordType((RelRecordType) type, false, true);
            } else {
                // Keep same type as before, ignore nullable parameter
                // RelRecordType currently always returns a nullability of false
                newType = copyRecordType((RelRecordType) type, true, false);
            }
        } else {
            newType = copySimpleType(type, nullable);
        }
        return canonize(newType);
    }

    /**
     * Registers a type, or returns the existing type if it is already
     * registered.
     */
    protected RelDataType canonize(RelDataType type)
    {
        RelDataType type2 = map.get(type);
        if (type2 != null) {
            return type2;
        } else {
            map.put(type, type);
            return type;
        }
    }

    /**
     * Returns an array of the fields in an array of types.
     */
    private static RelDataTypeField [] getFieldArray(RelDataType [] types)
    {
        ArrayList<RelDataTypeField> fieldList =
            new ArrayList<RelDataTypeField>();
        for (int i = 0; i < types.length; i++) {
            RelDataType type = types[i];
            addFields(type, fieldList);
        }
        return (RelDataTypeField []) fieldList.toArray(
            new RelDataTypeField[fieldList.size()]);
    }

    /**
     * Returns an array of all atomic types in an array.
     */
    private static RelDataType [] getTypeArray(RelDataType [] types)
    {
        ArrayList<RelDataType> typeList = new ArrayList<RelDataType>();
        getTypeArray(types, typeList);
        return (RelDataType []) typeList.toArray(
            new RelDataType[typeList.size()]);
    }

    private static void getTypeArray(
        RelDataType [] types,
        ArrayList<RelDataType> typeList)
    {
        for (int i = 0; i < types.length; i++) {
            RelDataType type = types[i];
            if (type instanceof RelCrossType) {
                getTypeArray(((RelCrossType) type).types, typeList);
            } else {
                typeList.add(type);
            }
        }
    }

    /**
     * Adds all fields in <code>type</code> to <code>fieldList</code>.
     */
    private static void addFields(
        RelDataType type,
        ArrayList<RelDataTypeField> fieldList)
    {
        if (type instanceof RelCrossType) {
            final RelCrossType crossType = (RelCrossType) type;
            for (int i = 0; i < crossType.types.length; i++) {
                addFields(crossType.types[i], fieldList);
            }
        } else {
            RelDataTypeField [] fields = type.getFields();
            for (int j = 0; j < fields.length; j++) {
                RelDataTypeField field = fields[j];
                fieldList.add(field);
            }
        }
    }

    public static boolean isJavaType(RelDataType t)
    {
        return t instanceof JavaType;
    }

    private RelDataTypeField [] fieldsOf(Class clazz)
    {
        final Field [] fields = clazz.getFields();
        ArrayList<RelDataTypeFieldImpl> list =
            new ArrayList<RelDataTypeFieldImpl>();
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            list.add(
                new RelDataTypeFieldImpl(
                    field.getName(),
                    list.size(),
                    createJavaType(field.getType())));
        }

        if (list.isEmpty()) {
            return null;
        }

        return (RelDataTypeField []) list.toArray(
            new RelDataTypeField[list.size()]);
    }

    // implement RelDataTypeFactory
    public RelDataType createArrayType(
        RelDataType elementType,
        long maxCardinality)
    {
        if (elementType instanceof JavaType) {
            JavaType javaType = (JavaType) elementType;
            Class arrayClass = Array.newInstance(javaType.clazz, 0).getClass();
            return createJavaType(arrayClass);
        }
        throw Util.newInternal("array of non-Java type unsupported");
    }

    /**
     * implement RelDataTypeFactory with SQL 2003 compliant behavior. Let p1, s1
     * be the precision and scale of the first operand Let p2, s2 be the
     * precision and scale of the second operand Let p, s be the precision and
     * scale of the result, Then the result type is a decimal with:
     *
     * <ul>
     * <li>p = p1 + p2</li>
     * <li>s = s1 + s2</li>
     * </ul>
     *
     * p and s are capped at their maximum values
     *
     * @sql.2003 Part 2 Section 6.26
     */
    public RelDataType createDecimalProduct(
        RelDataType type1,
        RelDataType type2)
    {
        if (SqlTypeUtil.isExactNumeric(type1)
            && SqlTypeUtil.isExactNumeric(type2))
        {
            if (SqlTypeUtil.isDecimal(type1)
                || SqlTypeUtil.isDecimal(type2))
            {
                int p1 = type1.getPrecision();
                int p2 = type2.getPrecision();
                int s1 = type1.getScale();
                int s2 = type2.getScale();

                int scale = s1 + s2;
                scale = Math.min(scale, SqlTypeName.MAX_NUMERIC_SCALE);
                int precision = p1 + p2;
                precision =
                    Math.min(
                        precision,
                        SqlTypeName.MAX_NUMERIC_PRECISION);

                RelDataType ret;
                ret =
                    createSqlType(
                        SqlTypeName.DECIMAL,
                        precision,
                        scale);

                return ret;
            }
        }

        return null;
    }

    // implement RelDataTypeFactory
    public boolean useDoubleMultiplication(
        RelDataType type1,
        RelDataType type2)
    {
        assert (createDecimalProduct(type1, type2) != null);
        return false;
    }

    /**
     * implement RelDataTypeFactory Let p1, s1 be the precision and scale of the
     * first operand Let p2, s2 be the precision and scale of the second operand
     * Let p, s be the precision and scale of the result, Let d be the number of
     * whole digits in the result Then the result type is a decimal with:
     *
     * <ul>
     * <li>d = p1 - s1 + s2</li>
     * <li>s <= max(6, s1 + p2 + 1)</li>
     * <li>p = d + s</li>
     * </ul>
     *
     * p and s are capped at their maximum values
     *
     * @sql.2003 Part 2 Section 6.26
     */
    public RelDataType createDecimalQuotient(
        RelDataType type1,
        RelDataType type2)
    {
        if (SqlTypeUtil.isExactNumeric(type1)
            && SqlTypeUtil.isExactNumeric(type2))
        {
            if (SqlTypeUtil.isDecimal(type1)
                || SqlTypeUtil.isDecimal(type2))
            {
                int p1 = type1.getPrecision();
                int p2 = type2.getPrecision();
                int s1 = type1.getScale();
                int s2 = type2.getScale();

                int dout =
                    Math.min(
                        p1 - s1 + s2,
                        SqlTypeName.MAX_NUMERIC_PRECISION);

                int scale = Math.max(6, s1 + p2 + 1);
                scale =
                    Math.min(
                        scale,
                        SqlTypeName.MAX_NUMERIC_PRECISION - dout);
                scale = Math.min(scale, SqlTypeName.MAX_NUMERIC_SCALE);

                int precision = dout + scale;
                assert (precision <= SqlTypeName.MAX_NUMERIC_PRECISION);
                assert (precision > 0);

                RelDataType ret;
                ret =
                    createSqlType(
                        SqlTypeName.DECIMAL,
                        precision,
                        scale);

                return ret;
            }
        }

        return null;
    }

    public Charset getDefaultCharset()
    {
        return Util.getDefaultCharset();
    }

    //~ Inner Classes ----------------------------------------------------------

    // TODO jvs 13-Dec-2004:  move to OJTypeFactoryImpl?
    /**
     * Type which is based upon a Java class.
     */
    protected class JavaType
        extends RelDataTypeImpl
    {
        private final Class clazz;
        private boolean isNullable;
        private SqlCollation collation;
        private Charset charset;

        public JavaType(Class clazz)
        {
            super(fieldsOf(clazz));
            this.clazz = clazz;

            isNullable =
                clazz.equals(Integer.class)
                || clazz.equals(Long.class)
                || clazz.equals(Short.class)
                || clazz.equals(Integer.class)
                || clazz.equals(Byte.class)
                || clazz.equals(Double.class)
                || clazz.equals(Float.class)
                || clazz.equals(Boolean.class)
                || clazz.equals(byte [].class)
                || clazz.equals(String.class);
            computeDigest();
        }

        public JavaType(
            Class clazz,
            boolean nullable)
        {
            this(clazz);
            this.isNullable = nullable;
            computeDigest();
        }

        public JavaType(
            Class clazz,
            boolean nullable,
            Charset charset,
            SqlCollation collation)
        {
            this(clazz);
            Util.pre(
                SqlTypeUtil.inCharFamily(this),
                "Need to be a chartype");
            this.isNullable = nullable;
            this.charset = charset;
            this.collation = collation;
            computeDigest();
        }

        public Class getJavaClass()
        {
            return clazz;
        }

        public boolean isNullable()
        {
            return isNullable;
        }

        protected void generateTypeString(StringBuilder sb, boolean withDetail)
        {
            sb.append("JavaType(");
            sb.append(clazz);
            sb.append(")");
        }

        public RelDataType getComponentType()
        {
            final Class componentType = clazz.getComponentType();
            if (componentType == null) {
                return null;
            } else {
                return createJavaType(componentType);
            }
        }

        public Charset getCharset()
            throws RuntimeException
        {
            return this.charset;
        }

        public SqlCollation getCollation()
            throws RuntimeException
        {
            return this.collation;
        }

        public SqlTypeName getSqlTypeName()
        {
            return JavaToSqlTypeConversionRules.instance().lookup(clazz);
        }
    }
}

// End RelDataTypeFactoryImpl.java
