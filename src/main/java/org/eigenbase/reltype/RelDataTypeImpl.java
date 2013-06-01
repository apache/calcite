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

import java.io.*;

import java.nio.charset.*;

import java.util.*;

import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;


/**
 * RelDataTypeImpl is an abstract base for implementations of {@link
 * RelDataType}.
 *
 * <p>Identity is based upon the {@link #digest} field, which each derived class
 * should set during construction.</p>
 *
 * @author jhyde
 * @version $Id$
 */
public abstract class RelDataTypeImpl
    implements RelDataType,
        RelDataTypeFamily
{
    //~ Instance fields --------------------------------------------------------

    protected RelDataTypeField [] fields;
    protected List<RelDataTypeField> fieldList;
    protected String digest;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a RelDataTypeImpl.
     *
     * @param fieldList List of fields
     */
    protected RelDataTypeImpl(List<? extends RelDataTypeField> fieldList)
    {
        if (fieldList != null) {
            // Create a defensive copy of the list.
            this.fields =
                fieldList.toArray(new RelDataTypeField[fieldList.size()]);
            this.fieldList =
                Collections.unmodifiableList(Arrays.asList(fields));
        } else {
            this.fieldList = null;
            this.fields = null;
        }
    }

    /**
     * Default constructor, to allow derived classes such as {@link
     * BasicSqlType} to be {@link Serializable}.
     *
     * <p>(The serialization specification says that a class can be serializable
     * even if its base class is not serializable, provided that the base class
     * has a public or protected zero-args constructor.)
     */
    protected RelDataTypeImpl()
    {
        this(null);
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelDataType
    public RelDataTypeField getField(String fieldName)
    {
        for (RelDataTypeField field : fields) {
            if (field.getName().equals(fieldName)) {
                return field;
            }
        }
        // Extra field
        if (fields.length > 0
            && fields[fields.length - 1].getName().equals("_extra"))
        {
            return new RelDataTypeFieldImpl(
                fieldName,
                -1,
                fields[fields.length - 1].getType());
        }
        return null;
    }

    // implement RelDataType
    public int getFieldOrdinal(String fieldName)
    {
        for (int i = 0; i < fields.length; i++) {
            RelDataTypeField field = fields[i];
            if (field.getName().equals(fieldName)) {
                return i;
            }
        }
        return -1;
    }

    // implement RelDataType
    public List<RelDataTypeField> getFieldList()
    {
        assert (isStruct());
        return fieldList;
    }

    public List<String> getFieldNames() {
        return RelOptUtil.getFieldNameList(this);
    }

    // implement RelDataType
    public RelDataTypeField [] getFields()
    {
        assert isStruct() : this;
        return fields;
    }

    // implement RelDataType
    public int getFieldCount()
    {
        assert isStruct() : this;
        return fields.length;
    }

    // implement RelDataType
    public RelDataType getComponentType()
    {
        // this is not a collection type
        return null;
    }

    public RelDataType getKeyType() {
        // this is not a map type
        return null;
    }

    public RelDataType getValueType() {
        // this is not a map type
        return null;
    }

    // implement RelDataType
    public boolean isStruct()
    {
        return fields != null;
    }

    // implement RelDataType
    public boolean equals(Object obj)
    {
        if (obj instanceof RelDataTypeImpl) {
            final RelDataTypeImpl that = (RelDataTypeImpl) obj;
            return this.digest.equals(that.digest);
        }
        return false;
    }

    // implement RelDataType
    public int hashCode()
    {
        return digest.hashCode();
    }

    // implement RelDataType
    public String getFullTypeString()
    {
        return digest;
    }

    // implement RelDataType
    public boolean isNullable()
    {
        return false;
    }

    // implement RelDataType
    public Charset getCharset()
    {
        return null;
    }

    // implement RelDataType
    public SqlCollation getCollation()
        throws RuntimeException
    {
        return null;
    }

    // implement RelDataType
    public SqlIntervalQualifier getIntervalQualifier()
    {
        return null;
    }

    // implement RelDataType
    public int getPrecision()
    {
        return PRECISION_NOT_SPECIFIED;
    }

    // implement RelDataType
    public int getScale()
    {
        return SCALE_NOT_SPECIFIED;
    }

    // implement RelDataType
    public SqlTypeName getSqlTypeName()
    {
        return null;
    }

    // implement RelDataType
    public SqlIdentifier getSqlIdentifier()
    {
        SqlTypeName typeName = getSqlTypeName();
        if (typeName == null) {
            return null;
        }
        return new SqlIdentifier(
            typeName.name(),
            SqlParserPos.ZERO);
    }

    // implement RelDataType
    public RelDataTypeFamily getFamily()
    {
        // by default, put each type into its own family
        return this;
    }

    /**
     * Generates a string representation of this type.
     *
     * @param sb StringBuffer into which to generate the string
     * @param withDetail when true, all detail information needed to compute a
     * unique digest (and return from getFullTypeString) should be included;
     */
    protected abstract void generateTypeString(
        StringBuilder sb,
        boolean withDetail);

    /**
     * Computes the digest field. This should be called in every non-abstract
     * subclass constructor once the type is fully defined.
     */
    protected void computeDigest()
    {
        StringBuilder sb = new StringBuilder();
        generateTypeString(sb, true);
        if (!isNullable()) {
            sb.append(" NOT NULL");
        }
        digest = sb.toString();
    }

    // implement RelDataType
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        generateTypeString(sb, false);
        return sb.toString();
    }

    // implement RelDataType
    public RelDataTypePrecedenceList getPrecedenceList()
    {
        // by default, make each type have a precedence list containing
        // only other types in the same family
        return new RelDataTypePrecedenceList() {
            public boolean containsType(RelDataType type)
            {
                return getFamily() == type.getFamily();
            }

            public int compareTypePrecedence(
                RelDataType type1,
                RelDataType type2)
            {
                assert (containsType(type1));
                assert (containsType(type2));
                return 0;
            }
        };
    }

    // implement RelDataType
    public RelDataTypeComparability getComparability()
    {
        return RelDataTypeComparability.All;
    }
}

// End RelDataTypeImpl.java
