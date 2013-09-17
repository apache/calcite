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

import java.util.List;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;

import com.google.common.base.Preconditions;

/**
 * SqlTypeTransforms defines a number of reusable instances of {@link
 * SqlTypeTransform}.
 *
 * <p>NOTE: avoid anonymous inner classes here except for unique,
 * non-generalizable strategies; anything else belongs in a reusable top-level
 * class. If you find yourself copying and pasting an existing strategy's
 * anonymous inner class, you're making a mistake.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public abstract class SqlTypeTransforms
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Parameter type-inference transform strategy where a derived type is
     * transformed into the same type but nullable if any of a calls operands is
     * nullable
     */
    public static final SqlTypeTransform toNullable =
        new SqlTypeTransform() {
            public RelDataType transformType(
                SqlOperatorBinding opBinding,
                RelDataType typeToTransform)
            {
                return SqlTypeUtil.makeNullableIfOperandsAre(
                    opBinding.getTypeFactory(),
                    opBinding.collectOperandTypes(),
                    Preconditions.checkNotNull(typeToTransform));
            }
        };

    /**
     * Parameter type-inference transform strategy where a derived type is
     * transformed into the same type but not nullable.
     */
    public static final SqlTypeTransform toNotNullable =
        new SqlTypeTransform() {
            public RelDataType transformType(
                SqlOperatorBinding opBinding,
                RelDataType typeToTransform)
            {
                return opBinding.getTypeFactory().createTypeWithNullability(
                    Preconditions.checkNotNull(typeToTransform),
                    false);
            }
        };

    /**
     * Parameter type-inference transform strategy where a derived type is
     * transformed into the same type with nulls allowed.
     */
    public static final SqlTypeTransform forceNullable =
        new SqlTypeTransform() {
            public RelDataType transformType(
                SqlOperatorBinding opBinding,
                RelDataType typeToTransform)
            {
                return opBinding.getTypeFactory().createTypeWithNullability(
                    Preconditions.checkNotNull(typeToTransform),
                    true);
            }
        };

    /**
     * Type-inference strategy whereby the result type of a call is VARYING the
     * type given. The length returned is the same as length of the first
     * argument. Return type will have same nullablilty as input type
     * nullablility. First Arg must be of string type.
     */
    public static final SqlTypeTransform toVarying =
        new SqlTypeTransform() {
            public RelDataType transformType(
                SqlOperatorBinding opBinding,
                RelDataType typeToTransform)
            {
                switch (typeToTransform.getSqlTypeName()) {
                case VARCHAR:
                case VARBINARY:
                    return typeToTransform;
                }

                SqlTypeName retTypeName = toVar(typeToTransform);

                RelDataType ret =
                    opBinding.getTypeFactory().createSqlType(
                        retTypeName,
                        typeToTransform.getPrecision());
                if (SqlTypeUtil.inCharFamily(typeToTransform)) {
                    ret =
                        opBinding.getTypeFactory()
                        .createTypeWithCharsetAndCollation(
                            ret,
                            typeToTransform.getCharset(),
                            typeToTransform.getCollation());
                }
                return opBinding.getTypeFactory().createTypeWithNullability(
                    ret,
                    typeToTransform.isNullable());
            }

            private SqlTypeName toVar(RelDataType type)
            {
                final SqlTypeName sqlTypeName = type.getSqlTypeName();
                switch (sqlTypeName) {
                case CHAR:
                    return SqlTypeName.VARCHAR;
                case BINARY:
                    return SqlTypeName.VARBINARY;
                default:
                    throw Util.unexpected(sqlTypeName);
                }
            }
        };

    /**
     * Parameter type-inference transform strategy where a derived type must be
     * a multiset type and the returned type is the multiset's element type.
     *
     * @see MultisetSqlType#getComponentType
     */
    public static final SqlTypeTransform toMultisetElementType =
        new SqlTypeTransform() {
            public RelDataType transformType(
                SqlOperatorBinding opBinding,
                RelDataType typeToTransform)
            {
                return typeToTransform.getComponentType();
            }
        };

    /**
     * Parameter type-inference transform strategy where a derived type must be
     * a struct type with precisely one field and the returned type is the type
     * of that field.
     */
    public static final SqlTypeTransform onlyColumn =
        new SqlTypeTransform() {
            public RelDataType transformType(
                SqlOperatorBinding opBinding,
                RelDataType typeToTransform)
            {
                final List<RelDataTypeField> fields =
                    typeToTransform.getFieldList();
                assert fields.size() == 1;
                return fields.get(0).getType();
            }
        };
}

// End SqlTypeTransforms.java
