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
package net.hydromatic.optiq.jdbc;

import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.runtime.ByteString;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.BasicSqlType;
import org.eigenbase.sql.type.SqlTypeFactoryImpl;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link JavaTypeFactory}.
 *
 * <p><strong>NOTE: This class is experimental and subject to
 * change/removal without notice</strong>.</p>
 */
public class JavaTypeFactoryImpl
    extends SqlTypeFactoryImpl
    implements JavaTypeFactory
{
    public RelDataType createStructType(Class type) {
        List<RelDataTypeField> list = new ArrayList<RelDataTypeField>();
        for (Field field : type.getFields()) {
            // FIXME: watch out for recursion
            list.add(
                new RelDataTypeFieldImpl(
                    field.getName(),
                    list.size(),
                    createType(field.getType())));
        }
        return canonize(
            new JavaRecordType(
                list.toArray(new RelDataTypeField[list.size()]),
                type));
    }

    public RelDataType createType(Type type) {
        if (type instanceof RelDataType) {
            return (RelDataType) type;
        }
        if (!(type instanceof Class)) {
            throw new UnsupportedOperationException(
                "TODO: implement " + type + ": " + type.getClass());
        }
        final Class clazz = (Class) type;
        if (clazz.isPrimitive()) {
            return createJavaType(clazz);
        } else if (clazz == String.class) {
            // TODO: similar special treatment for BigDecimal, BigInteger,
            //  Date, Time, Timestamp, Double etc.
            return createJavaType(clazz);
        } else if (clazz.isArray()) {
            return createMultisetType(
                createType(clazz.getComponentType()), -1);
        } else {
            return createStructType(clazz);
        }
    }

    public Type getJavaClass(RelDataType type) {
        if (type instanceof RelRecordType) {
            JavaRecordType javaRecordType;
            if (type instanceof JavaRecordType) {
                javaRecordType = (JavaRecordType) type;
                return javaRecordType.clazz;
            } else {
                return type;
            }
        }
        if (type instanceof JavaType) {
            JavaType javaType = (JavaType) type;
            return javaType.getJavaClass();
        }
        if (type.isStruct() && type.getFieldCount() == 1) {
            return getJavaClass(type.getFieldList().get(0).getType());
        }
        if (type instanceof BasicSqlType) {
            switch (type.getSqlTypeName()) {
            case VARCHAR:
            case CHAR:
                return String.class;
            case INTEGER:
                return type.isNullable() ? int.class : Integer.class;
            case BIGINT:
                return type.isNullable() ? long.class : Long.class;
            case SMALLINT:
                return type.isNullable() ? short.class : Short.class;
            case TINYINT:
                return type.isNullable() ? byte.class : Byte.class;
            case DECIMAL:
                return BigDecimal.class;
            case BOOLEAN:
                return type.isNullable() ? boolean.class : Boolean.class;
            case BINARY:
            case VARBINARY:
                return ByteString.class;
            case DATE:
                return java.sql.Date.class;
            case TIME:
                return Time.class;
            case TIMESTAMP:
                return Timestamp.class;
            }
        }
        return null;
    }
}

// End JavaTypeFactoryImpl.java
