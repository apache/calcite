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
package org.eigenbase.reltype;

import java.lang.reflect.*;
import java.nio.charset.*;
import java.sql.*;
import java.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;

import net.hydromatic.linq4j.expressions.Primitive;

import com.google.common.base.Preconditions;
import com.google.common.cache.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Abstract base for implementations of {@link RelDataTypeFactory}.
 */
public abstract class RelDataTypeFactoryImpl implements RelDataTypeFactory {
  //~ Instance fields --------------------------------------------------------

  /**
   * Global cache. Uses soft values to allow GC.
   */
  private static final LoadingCache<Object, RelDataType> CACHE =
      CacheBuilder.newBuilder()
          .softValues()
          .build(
              new CacheLoader<Object, RelDataType>() {
                @Override
                public RelDataType load(Object key) {
                  if (key instanceof RelDataType) {
                    return (RelDataType) key;
                  }
                  @SuppressWarnings("unchecked")
                  final Pair<List<String>, List<RelDataType>> pair =
                      (Pair<List<String>, List<RelDataType>>) key;
                  final ImmutableList.Builder<RelDataTypeField> list =
                      ImmutableList.builder();
                  for (int i = 0; i < pair.left.size(); i++) {
                    list.add(
                        new RelDataTypeFieldImpl(
                            pair.left.get(i), i, pair.right.get(i)));
                  }
                  return new RelRecordType(list.build());
                }
              });

  private static final Map<Class, RelDataTypeFamily> CLASS_FAMILIES =
      ImmutableMap.<Class, RelDataTypeFamily>builder()
          .put(String.class, SqlTypeFamily.CHARACTER)
          .put(byte[].class, SqlTypeFamily.BINARY)
          .put(boolean.class, SqlTypeFamily.BOOLEAN)
          .put(Boolean.class, SqlTypeFamily.BOOLEAN)
          .put(char.class, SqlTypeFamily.NUMERIC)
          .put(Character.class, SqlTypeFamily.NUMERIC)
          .put(short.class, SqlTypeFamily.NUMERIC)
          .put(Short.class, SqlTypeFamily.NUMERIC)
          .put(int.class, SqlTypeFamily.NUMERIC)
          .put(Integer.class, SqlTypeFamily.NUMERIC)
          .put(long.class, SqlTypeFamily.NUMERIC)
          .put(Long.class, SqlTypeFamily.NUMERIC)
          .put(float.class, SqlTypeFamily.APPROXIMATE_NUMERIC)
          .put(Float.class, SqlTypeFamily.APPROXIMATE_NUMERIC)
          .put(double.class, SqlTypeFamily.APPROXIMATE_NUMERIC)
          .put(Double.class, SqlTypeFamily.APPROXIMATE_NUMERIC)
          .put(java.sql.Date.class, SqlTypeFamily.DATE)
          .put(Time.class, SqlTypeFamily.TIME)
          .put(Timestamp.class, SqlTypeFamily.TIMESTAMP)
          .build();

  protected final RelDataTypeSystem typeSystem;

  //~ Constructors -----------------------------------------------------------

  /** Creates a type factory with default type system.
   *
   * @deprecated Will be removed after optiq-0.9.1.
   */
  protected RelDataTypeFactoryImpl() {
    this(RelDataTypeSystem.DEFAULT);
    Bug.upgrade("optiq-0.9.1");
  }

  /** Creates a type factory. */
  protected RelDataTypeFactoryImpl(RelDataTypeSystem typeSystem) {
    this.typeSystem = Preconditions.checkNotNull(typeSystem);
  }

  //~ Methods ----------------------------------------------------------------

  public RelDataTypeSystem getTypeSystem() {
    return typeSystem;
  }

  // implement RelDataTypeFactory
  public RelDataType createJavaType(Class clazz) {
    final JavaType javaType =
        clazz == String.class
            ? new JavaType(clazz, true, getDefaultCharset(),
                SqlCollation.IMPLICIT)
            : new JavaType(clazz);
    return canonize(javaType);
  }

  // implement RelDataTypeFactory
  public RelDataType createJoinType(RelDataType... types) {
    assert types != null;
    assert types.length >= 1;
    final List<RelDataType> flattenedTypes =
        getTypeArray(ImmutableList.copyOf(types));
    return canonize(
        new RelCrossType(
            flattenedTypes,
            getFieldArray(flattenedTypes)));
  }

  // implement RelDataTypeFactory
  public RelDataType createStructType(
      final List<RelDataType> typeList,
      final List<String> fieldNameList) {
    assert typeList.size() == fieldNameList.size();
    return canonize(fieldNameList, typeList);
  }

  // implement RelDataTypeFactory
  public RelDataType createStructType(
      final RelDataTypeFactory.FieldInfo fieldInfo) {
    return canonize(
        new AbstractList<String>() {
          @Override
          public String get(int index) {
            return fieldInfo.getFieldName(index);
          }

          @Override
          public int size() {
            return fieldInfo.getFieldCount();
          }
        },
        new AbstractList<RelDataType>() {
          @Override
          public RelDataType get(int index) {
            return fieldInfo.getFieldType(index);
          }

          @Override
          public int size() {
            return fieldInfo.getFieldCount();
          }
        });
  }

  // implement RelDataTypeFactory
  public final RelDataType createStructType(
      final List<? extends Map.Entry<String, RelDataType>> fieldList) {
    return createStructType(
        new FieldInfo() {
          public int getFieldCount() {
            return fieldList.size();
          }

          public String getFieldName(int index) {
            return fieldList.get(index).getKey();
          }

          public RelDataType getFieldType(int index) {
            return fieldList.get(index).getValue();
          }
        });
  }

  // implement RelDataTypeFactory
  public RelDataType leastRestrictive(List<RelDataType> types) {
    assert types != null;
    assert types.size() >= 1;
    RelDataType type0 = types.get(0);
    if (type0.isStruct()) {
      return leastRestrictiveStructuredType(types);
    }
    return null;
  }

  protected RelDataType leastRestrictiveStructuredType(
      final List<RelDataType> types) {
    final RelDataType type0 = types.get(0);
    final int fieldCount = type0.getFieldCount();

    // precheck that all types are structs with same number of fields
    for (RelDataType type : types) {
      if (!type.isStruct()) {
        return null;
      }
      if (type.getFieldList().size() != fieldCount) {
        return null;
      }
    }

    // recursively compute column-wise least restrictive
    final FieldInfoBuilder builder = builder();
    for (int j = 0; j < fieldCount; ++j) {
      // REVIEW jvs 22-Jan-2004:  Always use the field name from the
      // first type?
      final int k = j;
      builder.add(
          type0.getFieldList().get(j).getName(),
          leastRestrictive(
              new AbstractList<RelDataType>() {
                public RelDataType get(int index) {
                  return types.get(index).getFieldList().get(k).getType();
                }

                public int size() {
                  return types.size();
                }
              }));
    }
    return builder.build();
  }

  // copy a non-record type, setting nullability
  private RelDataType copySimpleType(
      RelDataType type,
      boolean nullable) {
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
            nullable
                ? Primitive.box(javaType.clazz)
                : Primitive.unbox(javaType.clazz),
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
      final boolean nullable) {
    // REVIEW: angel 18-Aug-2005 dtbug336
    // Shouldn't null refer to the nullability of the record type
    // not the individual field types?
    // For flattening and outer joins, it is desirable to change
    // the nullability of the individual fields.

    return createStructType(
        new FieldInfo() {
          public int getFieldCount() {
            return type.getFieldList().size();
          }

          public String getFieldName(int index) {
            return type.getFieldList().get(index).getName();
          }

          public RelDataType getFieldType(int index) {
            RelDataType fieldType =
                type.getFieldList().get(index).getType();
            if (ignoreNullable) {
              return copyType(fieldType);
            } else {
              return createTypeWithNullability(fieldType, nullable);
            }
          }
        });
  }

  // implement RelDataTypeFactory
  public RelDataType copyType(RelDataType type) {
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
      final boolean nullable) {
    Preconditions.checkNotNull(type);
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
   *
   * @throws NullPointerException if type is null
   */
  protected RelDataType canonize(final RelDataType type) {
    return CACHE.getUnchecked(type);
  }

  /**
   * Looks up a type using a temporary key, and if not present, creates
   * a permanent key and type.
   *
   * <p>This approach allows us to use a cheap temporary key. A permanent
   * key is more expensive, because it must be immutable and not hold
   * references into other data structures.</p>
   */
  protected RelDataType canonize(
      final List<String> names,
      final List<RelDataType> types) {
    final RelDataType type = CACHE.getIfPresent(Pair.of(names, types));
    if (type != null) {
      return type;
    }
    final ImmutableList<String> names2 = ImmutableList.copyOf(names);
    final ImmutableList<RelDataType> types2 = ImmutableList.copyOf(types);
    return CACHE.getUnchecked(Pair.of(names2, types2));
  }

  /**
   * Returns an array of the fields in an array of types.
   */
  private static List<RelDataTypeField> getFieldArray(List<RelDataType> types) {
    ArrayList<RelDataTypeField> fieldList =
        new ArrayList<RelDataTypeField>();
    for (RelDataType type : types) {
      addFields(type, fieldList);
    }
    return fieldList;
  }

  /**
   * Returns an array of all atomic types in an array.
   */
  private static List<RelDataType> getTypeArray(List<RelDataType> types) {
    List<RelDataType> flatTypes = new ArrayList<RelDataType>();
    getTypeArray(types, flatTypes);
    return flatTypes;
  }

  private static void getTypeArray(
      List<RelDataType> inTypes,
      List<RelDataType> flatTypes) {
    for (RelDataType inType : inTypes) {
      if (inType instanceof RelCrossType) {
        getTypeArray(((RelCrossType) inType).types, flatTypes);
      } else {
        flatTypes.add(inType);
      }
    }
  }

  /**
   * Adds all fields in <code>type</code> to <code>fieldList</code>.
   */
  private static void addFields(
      RelDataType type,
      ArrayList<RelDataTypeField> fieldList) {
    if (type instanceof RelCrossType) {
      final RelCrossType crossType = (RelCrossType) type;
      for (RelDataType type1 : crossType.types) {
        addFields(type1, fieldList);
      }
    } else {
      List<RelDataTypeField> fields = type.getFieldList();
      for (RelDataTypeField field : fields) {
        fieldList.add(field);
      }
    }
  }

  public static boolean isJavaType(RelDataType t) {
    return t instanceof JavaType;
  }

  private List<RelDataTypeFieldImpl> fieldsOf(Class clazz) {
    final List<RelDataTypeFieldImpl> list =
        new ArrayList<RelDataTypeFieldImpl>();
    for (Field field : clazz.getFields()) {
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

    return list;
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
      RelDataType type2) {
    if (SqlTypeUtil.isExactNumeric(type1)
        && SqlTypeUtil.isExactNumeric(type2)) {
      if (SqlTypeUtil.isDecimal(type1)
          || SqlTypeUtil.isDecimal(type2)) {
        int p1 = type1.getPrecision();
        int p2 = type2.getPrecision();
        int s1 = type1.getScale();
        int s2 = type2.getScale();

        int scale = s1 + s2;
        scale = Math.min(scale, typeSystem.getMaxNumericScale());
        int precision = p1 + p2;
        precision =
            Math.min(
                precision,
                typeSystem.getMaxNumericPrecision());

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
      RelDataType type2) {
    assert createDecimalProduct(type1, type2) != null;
    return false;
  }

  /**
   * Rules:
   *
   * <ul>
   * <li>Let p1, s1 be the precision and scale of the first operand
   * <li>Let p2, s2 be the precision and scale of the second operand
   * <li>Let p, s be the precision and scale of the result
   * <li>Let d be the number of whole digits in the result
   * <li>Then the result type is a decimal with:
   *   <ul>
   *   <li>d = p1 - s1 + s2</li>
   *   <li>s &lt; max(6, s1 + p2 + 1)</li>
   *   <li>p = d + s</li>
   *   </ul>
   * </li>
   * <li>p and s are capped at their maximum values</li>
   * </ul>
   *
   * @sql.2003 Part 2 Section 6.26
   */
  public RelDataType createDecimalQuotient(
      RelDataType type1,
      RelDataType type2) {
    if (SqlTypeUtil.isExactNumeric(type1)
        && SqlTypeUtil.isExactNumeric(type2)) {
      if (SqlTypeUtil.isDecimal(type1)
          || SqlTypeUtil.isDecimal(type2)) {
        int p1 = type1.getPrecision();
        int p2 = type2.getPrecision();
        int s1 = type1.getScale();
        int s2 = type2.getScale();

        final int maxNumericPrecision = typeSystem.getMaxNumericPrecision();
        int dout =
            Math.min(
                p1 - s1 + s2,
                maxNumericPrecision);

        int scale = Math.max(6, s1 + p2 + 1);
        scale =
            Math.min(
                scale,
                maxNumericPrecision - dout);
        scale = Math.min(scale, getTypeSystem().getMaxNumericScale());

        int precision = dout + scale;
        assert precision <= maxNumericPrecision;
        assert precision > 0;

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

  public Charset getDefaultCharset() {
    return Util.getDefaultCharset();
  }

  public FieldInfoBuilder builder() {
    return new FieldInfoBuilder(this);
  }

  //~ Inner Classes ----------------------------------------------------------

  // TODO jvs 13-Dec-2004:  move to OJTypeFactoryImpl?

  /**
   * Type which is based upon a Java class.
   */
  public class JavaType extends RelDataTypeImpl {
    private final Class clazz;
    private final boolean nullable;
    private SqlCollation collation;
    private Charset charset;

    public JavaType(Class clazz) {
      this(clazz, !clazz.isPrimitive());
    }

    public JavaType(
        Class clazz,
        boolean nullable) {
      this(clazz, nullable, null, null);
    }

    public JavaType(
        Class clazz,
        boolean nullable,
        Charset charset,
        SqlCollation collation) {
      super(fieldsOf(clazz));
      this.clazz = clazz;
      this.nullable = nullable;
      assert (charset != null) == SqlTypeUtil.inCharFamily(this)
          : "Need to be a chartype";
      this.charset = charset;
      this.collation = collation;
      computeDigest();
    }

    public Class getJavaClass() {
      return clazz;
    }

    public boolean isNullable() {
      return nullable;
    }

    @Override
    public RelDataTypeFamily getFamily() {
      RelDataTypeFamily family = CLASS_FAMILIES.get(clazz);
      return family != null ? family : this;
    }

    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
      sb.append("JavaType(");
      sb.append(clazz);
      sb.append(")");
    }

    public RelDataType getComponentType() {
      final Class componentType = clazz.getComponentType();
      if (componentType == null) {
        return null;
      } else {
        return createJavaType(componentType);
      }
    }

    public Charset getCharset() {
      return this.charset;
    }

    public SqlCollation getCollation() {
      return this.collation;
    }

    public SqlTypeName getSqlTypeName() {
      final SqlTypeName typeName =
          JavaToSqlTypeConversionRules.instance().lookup(clazz);
      if (typeName == null) {
        return SqlTypeName.OTHER;
      }
      return typeName;
    }
  }
}

// End RelDataTypeFactoryImpl.java
