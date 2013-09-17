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

import net.hydromatic.linq4j.Ord;
import net.hydromatic.linq4j.expressions.Primitive;
import net.hydromatic.linq4j.expressions.Types;
import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.linq4j.function.Functions;

import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.runtime.ByteString;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.Pair;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.*;

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
  private final Map<List<Pair<Type, Boolean>>, SyntheticRecordType>
      syntheticTypes =
      new HashMap<List<Pair<Type, Boolean>>, SyntheticRecordType>();

  public RelDataType createStructType(Class type) {
    List<RelDataTypeField> list = new ArrayList<RelDataTypeField>();
    for (Field field : type.getFields()) {
      if ((field.getModifiers() & Modifier.STATIC) == 0) {
        // FIXME: watch out for recursion
        list.add(
            new RelDataTypeFieldImpl(
                field.getName(),
                list.size(),
                createType(field.getType())));
      }
    }
    return canonize(new JavaRecordType(list, type));
  }

  public RelDataType createType(Type type) {
    if (type instanceof RelDataType) {
      return (RelDataType) type;
    }
    if (type instanceof SyntheticRecordType) {
      SyntheticRecordType syntheticRecordType =
          (SyntheticRecordType) type;
      return syntheticRecordType.relType;
    }
    if (!(type instanceof Class)) {
      throw new UnsupportedOperationException("TODO: implement " + type);
    }
    final Class clazz = (Class) type;
    switch (Primitive.flavor(clazz)) {
    case PRIMITIVE:
      return createJavaType(clazz);
    case BOX:
      return createJavaType(Primitive.ofBox(clazz).boxClass);
    }
    if (JavaToSqlTypeConversionRules.instance().lookup(clazz) != null) {
      return createJavaType(clazz);
    } else if (clazz.isArray()) {
      return createMultisetType(
          createType(clazz.getComponentType()), -1);
    } else if (List.class.isAssignableFrom(clazz)) {
      return createArrayType(
          createSqlType(SqlTypeName.ANY), -1);
    } else if (Map.class.isAssignableFrom(clazz)) {
      return createMapType(
          createSqlType(SqlTypeName.ANY),
          createSqlType(SqlTypeName.ANY));
    } else {
      return createStructType(clazz);
    }
  }

  public Type getJavaClass(RelDataType type) {
    switch (type.getSqlTypeName()) {
    case ROW:
      assert type instanceof RelRecordType;
      if (type instanceof JavaRecordType) {
        return ((JavaRecordType) type).clazz;
      } else {
        return createSyntheticType((RelRecordType)type);
      }
    case MAP:
      return Map.class;
    case ARRAY:
      return List.class;
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
      case DATE:
      case TIME:
      case INTEGER:
        return type.isNullable() ? Integer.class : int.class;
      case TIMESTAMP:
      case BIGINT:
        return type.isNullable() ? Long.class : long.class;
      case SMALLINT:
        return type.isNullable() ? Short.class : short.class;
      case TINYINT:
        return type.isNullable() ? Byte.class : byte.class;
      case DECIMAL:
        return BigDecimal.class;
      case BOOLEAN:
        return type.isNullable() ? Boolean.class : boolean.class;
      case DOUBLE:
        return type.isNullable() ? Double.class : double.class;
      case REAL:
      case FLOAT:
        return type.isNullable() ? Float.class : float.class;
      case BINARY:
      case VARBINARY:
        return ByteString.class;
      case ANY:
        return Object.class;
      }
    }
    return null;
  }

  public RelDataType toSql(RelDataType type) {
    if (type instanceof RelRecordType) {
      RelRecordType relRecordType = (RelRecordType) type;
      final List<RelDataTypeField> fields = relRecordType.getFieldList();
      return createStructType(
          Functions.adapt(
              fields,
              new Function1<RelDataTypeField, Pair<String, RelDataType>>() {
                public Pair<String, RelDataType> apply(
                    RelDataTypeField a0) {
                  return Pair.of(a0.getName(), toSql(a0.getType()));
                }
              }));
    }
    if (type instanceof JavaType) {
      return createTypeWithNullability(
          new BasicSqlType(type.getSqlTypeName()),
          type.isNullable());
    }
    return type;
  }

  public Type createSyntheticType(List<Type> types) {
    final String name =
        "Record" + types.size() + "_" + syntheticTypes.size();
    final SyntheticRecordType syntheticType =
        new SyntheticRecordType(null, name);
    for (final Ord<Type> ord : Ord.zip(types)) {
      syntheticType.fields.add(
          new RecordFieldImpl(
              syntheticType,
              "f" + ord.i,
              ord.e,
              !Primitive.is(ord.e)));
    }
    return register(syntheticType);
  }

  private SyntheticRecordType register(
      final SyntheticRecordType syntheticType) {
    final List<Pair<Type, Boolean>> key =
        new AbstractList<Pair<Type, Boolean>>() {
          public Pair<Type, Boolean> get(int index) {
            final Types.RecordField field =
                syntheticType.getRecordFields().get(index);
            return Pair.of(field.getType(), field.nullable());
          }

          public int size() {
            return syntheticType.getRecordFields().size();
          }
        };
    SyntheticRecordType syntheticType2 = syntheticTypes.get(key);
    if (syntheticType2 == null) {
      syntheticTypes.put(key, syntheticType);
      return syntheticType;
    } else {
      return syntheticType2;
    }
  }

  /** Creates a synthetic Java class whose fields have the same names and
   * relational types. */
  private Type createSyntheticType(RelRecordType type) {
    final String name =
        "Record" + type.getFieldCount() + "_" + syntheticTypes.size();
    final SyntheticRecordType syntheticType =
        new SyntheticRecordType(type, name);
    for (final RelDataTypeField recordField : type.getFieldList()) {
      final Type javaClass = getJavaClass(recordField.getType());
      syntheticType.fields.add(
          new RecordFieldImpl(
              syntheticType,
              recordField.getName(),
              javaClass,
              recordField.getType().isNullable()
                  && !Primitive.is(javaClass)));
    }
    return register(syntheticType);
  }

  public static class SyntheticRecordType implements Types.RecordType {
    final List<Types.RecordField> fields =
        new ArrayList<Types.RecordField>();
    final RelDataType relType;
    private final String name;

    private SyntheticRecordType(RelDataType relType, String name) {
      this.relType = relType;
      this.name = name;
      assert relType == null
             || new HashSet<String>(relType.getFieldNames()).size()
                == relType.getFieldNames().size()
          : "field names not distinct: " + relType;
    }

    public String getName() {
      return name;
    }

    public List<Types.RecordField> getRecordFields() {
      return fields;
    }

    public String toString() {
      return name;
    }
  }

  private static class RecordFieldImpl implements Types.RecordField {
    private final SyntheticRecordType syntheticType;
    private final String name;
    private final Type type;
    private final boolean nullable;

    public RecordFieldImpl(
        SyntheticRecordType syntheticType,
        String name,
        Type type,
        boolean nullable) {
      this.syntheticType = syntheticType;
      this.name = name;
      this.type = type;
      this.nullable = nullable;
      assert syntheticType != null;
      assert name != null;
      assert type != null;
      assert !(nullable && Primitive.is(type))
          : "type [" + type + "] can never be null";
    }

    public Type getType() {
      return type;
    }

    public String getName() {
      return name;
    }

    public int getModifiers() {
      return Modifier.PUBLIC;
    }

    public boolean nullable() {
      return nullable;
    }

    public Object get(Object o) {
      throw new UnsupportedOperationException();
    }

    public Type getDeclaringClass() {
      return syntheticType;
    }
  }
}

// End JavaTypeFactoryImpl.java
