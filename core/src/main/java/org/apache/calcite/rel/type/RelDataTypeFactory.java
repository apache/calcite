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
package org.apache.calcite.rel.type;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * RelDataTypeFactory is a factory for datatype descriptors. It defines methods
 * for instantiating and combining SQL, Java, and collection types. The factory
 * also provides methods for return type inference for arithmetic in cases where
 * SQL 2003 is implementation defined or impractical.
 *
 * <p>This interface is an example of the
 * {@link org.apache.calcite.util.Glossary#ABSTRACT_FACTORY_PATTERN abstract factory pattern}.
 * Any implementation of <code>RelDataTypeFactory</code> must ensure that type
 * objects are canonical: two types are equal if and only if they are
 * represented by the same Java object. This reduces memory consumption and
 * comparison cost.
 */
public interface RelDataTypeFactory {
  //~ Methods ----------------------------------------------------------------

  /**
   * Returns the type system.
   *
   * @return Type system
   */
  RelDataTypeSystem getTypeSystem();

  /**
   * Creates a type that corresponds to a Java class.
   *
   * @param clazz the Java class used to define the type
   * @return canonical Java type descriptor
   */
  RelDataType createJavaType(Class clazz);

  /**
   * Creates a cartesian product type.
   *
   * @return canonical join type descriptor
   * @param types array of types to be joined
   */
  RelDataType createJoinType(RelDataType... types);

  /**
   * Creates a type that represents a structured collection of fields, given
   * lists of the names and types of the fields.
   *
   * @param  kind         Name resolution policy
   * @param typeList      types of the fields
   * @param fieldNameList names of the fields
   * @return canonical struct type descriptor
   */
  RelDataType createStructType(StructKind kind,
      List<RelDataType> typeList,
      List<String> fieldNameList);

  /** Creates a type that represents a structured collection of fields.
   * Shorthand for <code>createStructType(StructKind.FULLY_QUALIFIED, typeList,
   * fieldNameList)</code>. */
  RelDataType createStructType(
      List<RelDataType> typeList,
      List<String> fieldNameList);

  /**
   * Creates a type that represents a structured collection of fields,
   * obtaining the field information via a callback.
   *
   * @param fieldInfo callback for field information
   * @return canonical struct type descriptor
   */
  @Deprecated // to be removed before 2.0
  RelDataType createStructType(FieldInfo fieldInfo);

  /**
   * Creates a type that represents a structured collection of fieldList,
   * obtaining the field information from a list of (name, type) pairs.
   *
   * @param fieldList List of (name, type) pairs
   * @return canonical struct type descriptor
   */
  RelDataType createStructType(
      List<? extends Map.Entry<String, RelDataType>> fieldList);

  /**
   * Creates an array type. Arrays are ordered collections of elements.
   *
   * @param elementType    type of the elements of the array
   * @param maxCardinality maximum array size, or -1 for unlimited
   * @return canonical array type descriptor
   */
  RelDataType createArrayType(
      RelDataType elementType,
      long maxCardinality);

  /**
   * Creates a map type. Maps are unordered collections of key/value pairs.
   *
   * @param keyType   type of the keys of the map
   * @param valueType type of the values of the map
   * @return canonical map type descriptor
   */
  RelDataType createMapType(
      RelDataType keyType,
      RelDataType valueType);

  /**
   * Creates a measure type.
   *
   * @param valueType type of the values of the measure
   * @return canonical measure type descriptor
   */
  RelDataType createMeasureType(RelDataType valueType);

  /**
   * Creates a multiset type. Multisets are unordered collections of elements.
   *
   * @param elementType    type of the elements of the multiset
   * @param maxCardinality maximum collection size, or -1 for unlimited
   * @return canonical multiset type descriptor
   */
  RelDataType createMultisetType(
      RelDataType elementType,
      long maxCardinality);

  /**
   * Duplicates a type, making a deep copy. Normally, this is a no-op, since
   * canonical type objects are returned. However, it is useful when copying a
   * type from one factory to another.
   *
   * @param type input type
   * @return output type, a new object equivalent to input type
   */
  RelDataType copyType(RelDataType type);

  /**
   * Creates a type that is the same as another type but with possibly
   * different nullability. The output type may be identical to the input
   * type. For type systems without a concept of nullability, the return value
   * is always the same as the input.
   *
   * @param type     input type
   * @param nullable true to request a nullable type; false to request a NOT
   *                 NULL type
   * @return output type, same as input type except with specified nullability
   * @throws NullPointerException if type is null
   */
  RelDataType createTypeWithNullability(
      RelDataType type,
      boolean nullable);

  /**
   * Creates a type that is the same as another type but with possibly
   * different charset or collation. For types without a concept of charset or
   * collation this function must throw an error.
   *
   * @param type      input type
   * @param charset   charset to assign
   * @param collation collation to assign
   * @return output type, same as input type except with specified charset and
   * collation
   */
  RelDataType createTypeWithCharsetAndCollation(
      RelDataType type,
      Charset charset,
      SqlCollation collation);

  /** Returns the default {@link Charset} (valid if this is a string type). */
  Charset getDefaultCharset();

  /**
   * Returns the most general of a set of types (that is, one type to which
   * they can all be cast), or null if conversion is not possible. The result
   * may be a new type that is less restrictive than any of the input types,
   * e.g. <code>leastRestrictive(INT, NUMERIC(3, 2))</code> could be
   * {@code NUMERIC(12, 2)}.
   *
   * @param types input types to be combined using union (not null, not empty)
   * @return canonical union type descriptor
   */
  @Nullable RelDataType leastRestrictive(List<RelDataType> types);

  /**
   * Creates a SQL type with no precision or scale.
   *
   * @param typeName Name of the type, for example {@link SqlTypeName#BOOLEAN},
   *   never null
   * @return canonical type descriptor
   */
  RelDataType createSqlType(SqlTypeName typeName);

  /**
   * Creates a SQL type that represents the "unknown" type.
   * It is only equal to itself, and is distinct from the NULL type.

   * @return unknown type
   */
  RelDataType createUnknownType();

  /**
   * Creates a SQL type with length (precision) but no scale.
   *
   * @param typeName  Name of the type, for example {@link SqlTypeName#VARCHAR}.
   *                  Never null.
   * @param precision Maximum length of the value (non-numeric types) or the
   *                  precision of the value (numeric/datetime types).
   *                  Must be non-negative or
   *                  {@link RelDataType#PRECISION_NOT_SPECIFIED}.
   * @return canonical type descriptor
   */
  RelDataType createSqlType(
      SqlTypeName typeName,
      int precision);

  /**
   * Creates a SQL type with precision and scale.
   *
   * @param typeName  Name of the type, for example {@link SqlTypeName#DECIMAL}.
   *                  Never null.
   * @param precision Precision of the value.
   *                  Must be non-negative or
   *                  {@link RelDataType#PRECISION_NOT_SPECIFIED}.
   * @param scale     scale of the values, i.e. the number of decimal places to
   *                  shift the value. For example, a NUMBER(10,3) value of
   *                  "123.45" is represented "123450" (that is, multiplied by
   *                  10^3). A negative scale <em>is</em> valid.
   * @return canonical type descriptor
   */
  RelDataType createSqlType(
      SqlTypeName typeName,
      int precision,
      int scale);

  /**
   * Creates a SQL interval type.
   *
   * @param intervalQualifier contains information if it is a year-month or a
   *                          day-time interval along with precision information
   * @return canonical type descriptor
   */
  RelDataType createSqlIntervalType(
      SqlIntervalQualifier intervalQualifier);

  /**
   * Infers the return type of a decimal multiplication. Decimal
   * multiplication involves at least one decimal operand and requires both
   * operands to have exact numeric types.
   *
   * @param type1 type of the first operand
   * @param type2 type of the second operand
   * @return the result type for a decimal multiplication, or null if decimal
   * multiplication should not be applied to the operands.
   * @deprecated Use
   * {@link RelDataTypeSystem#deriveDecimalMultiplyType(RelDataTypeFactory, RelDataType, RelDataType)}
   */
  @Deprecated // to be removed before 2.0
  @Nullable RelDataType createDecimalProduct(
      RelDataType type1,
      RelDataType type2);

  /**
   * Returns whether a decimal multiplication should be implemented by casting
   * arguments to double values.
   *
   * <p>Pre-condition: <code>createDecimalProduct(type1, type2) != null</code>
   *
   * @deprecated Use
   * {@link RelDataTypeSystem#shouldUseDoubleMultiplication(RelDataTypeFactory, RelDataType, RelDataType)}
   */
  @Deprecated // to be removed before 2.0
  boolean useDoubleMultiplication(
      RelDataType type1,
      RelDataType type2);

  /**
   * Infers the return type of a decimal division. Decimal division involves
   * at least one decimal operand and requires both operands to have exact
   * numeric types.
   *
   * @param type1 type of the first operand
   * @param type2 type of the second operand
   * @return the result type for a decimal division, or null if decimal
   * division should not be applied to the operands.
   *
   * @deprecated Use
   * {@link RelDataTypeSystem#deriveDecimalDivideType(RelDataTypeFactory, RelDataType, RelDataType)}
   */
  @Deprecated // to be removed before 2.0
  @Nullable RelDataType createDecimalQuotient(
      RelDataType type1,
      RelDataType type2);

  /**
   * Create a decimal type equivalent to the numeric {@code type},
   * this is related to specific system implementation,
   * you can override this logic if it is required.
   *
   * @param type the numeric type to create decimal type with
   * @return decimal equivalence of the numeric type.
   */
  RelDataType decimalOf(RelDataType type);

  /**
   * Creates a
   * {@link org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder}.
   * But since {@code FieldInfoBuilder} is deprecated, we recommend that you use
   * its base class {@link Builder}, which is not deprecated.
   */
  @SuppressWarnings("deprecation")
  FieldInfoBuilder builder();

  //~ Inner Interfaces -------------------------------------------------------

  /**
   * Callback that provides enough information to create fields.
   */
  @Deprecated // to be removed before 2.0
  interface FieldInfo {
    /**
     * Returns the number of fields.
     *
     * @return number of fields
     */
    int getFieldCount();

    /**
     * Returns the name of a given field.
     *
     * @param index Ordinal of field
     * @return Name of given field
     */
    String getFieldName(int index);

    /**
     * Returns the type of a given field.
     *
     * @param index Ordinal of field
     * @return Type of given field
     */
    RelDataType getFieldType(int index);
  }

  /**
   * Implementation of {@link FieldInfo} that provides a fluid API to build
   * a list of fields.
   */
  @Deprecated
  @SuppressWarnings("deprecation")
  class FieldInfoBuilder extends Builder implements FieldInfo {
    public FieldInfoBuilder(RelDataTypeFactory typeFactory) {
      super(typeFactory);
    }

    @Override public FieldInfoBuilder add(String name, RelDataType type) {
      return (FieldInfoBuilder) super.add(name, type);
    }

    @Override public FieldInfoBuilder add(String name, SqlTypeName typeName) {
      return (FieldInfoBuilder) super.add(name, typeName);
    }

    @Override public FieldInfoBuilder add(String name, SqlTypeName typeName,
        int precision) {
      return (FieldInfoBuilder) super.add(name, typeName, precision);
    }

    @Override public FieldInfoBuilder add(String name, SqlTypeName typeName,
        int precision, int scale) {
      return (FieldInfoBuilder) super.add(name, typeName, precision, scale);
    }

    @Override public FieldInfoBuilder add(String name, TimeUnit startUnit,
        int startPrecision, TimeUnit endUnit, int fractionalSecondPrecision) {
      return (FieldInfoBuilder) super.add(name, startUnit, startPrecision,
          endUnit, fractionalSecondPrecision);
    }

    @Override public FieldInfoBuilder nullable(boolean nullable) {
      return (FieldInfoBuilder) super.nullable(nullable);
    }

    @Override public FieldInfoBuilder add(RelDataTypeField field) {
      return (FieldInfoBuilder) super.add(field);
    }

    @Override public FieldInfoBuilder addAll(
        Iterable<? extends Map.Entry<String, RelDataType>> fields) {
      return (FieldInfoBuilder) super.addAll(fields);
    }

    @Override public FieldInfoBuilder kind(StructKind kind) {
      return (FieldInfoBuilder) super.kind(kind);
    }

    @Override public FieldInfoBuilder uniquify() {
      return (FieldInfoBuilder) super.uniquify();
    }
  }

  /** Fluid API to build a list of fields. */
  class Builder {
    private final List<String> names = new ArrayList<>();
    private final List<RelDataType> types = new ArrayList<>();
    private StructKind kind = StructKind.FULLY_QUALIFIED;
    private final RelDataTypeFactory typeFactory;
    private boolean nullableRecord = false;

    /**
     * Creates a Builder with the given type factory.
     */
    public Builder(RelDataTypeFactory typeFactory) {
      this.typeFactory = Objects.requireNonNull(typeFactory, "typeFactory");
    }

    /**
     * Returns the number of fields.
     *
     * @return number of fields
     */
    public int getFieldCount() {
      return names.size();
    }

    /**
     * Returns the name of a given field.
     *
     * @param index Ordinal of field
     * @return Name of given field
     */
    public String getFieldName(int index) {
      return names.get(index);
    }

    /**
     * Returns the type of a given field.
     *
     * @param index Ordinal of field
     * @return Type of given field
     */
    public RelDataType getFieldType(int index) {
      return types.get(index);
    }

    /**
     * Adds a field with given name and type.
     */
    public Builder add(String name, RelDataType type) {
      names.add(name);
      types.add(type);
      return this;
    }

    /**
     * Adds a field with a type created using
     * {@link org.apache.calcite.rel.type.RelDataTypeFactory#createSqlType(org.apache.calcite.sql.type.SqlTypeName)}.
     */
    public Builder add(String name, SqlTypeName typeName) {
      add(name, typeFactory.createSqlType(typeName));
      return this;
    }

    /**
     * Adds a field with a type created using
     * {@link org.apache.calcite.rel.type.RelDataTypeFactory#createSqlType(org.apache.calcite.sql.type.SqlTypeName, int)}.
     */
    public Builder add(String name, SqlTypeName typeName, int precision) {
      add(name, typeFactory.createSqlType(typeName, precision));
      return this;
    }

    /**
     * Adds a field with a type created using
     * {@link org.apache.calcite.rel.type.RelDataTypeFactory#createSqlType(org.apache.calcite.sql.type.SqlTypeName, int, int)}.
     */
    public Builder add(String name, SqlTypeName typeName, int precision,
        int scale) {
      add(name, typeFactory.createSqlType(typeName, precision, scale));
      return this;
    }

    /**
     * Adds a field with an interval type.
     */
    public Builder add(String name, TimeUnit startUnit, int startPrecision,
        TimeUnit endUnit, int fractionalSecondPrecision) {
      final SqlIntervalQualifier q =
          new SqlIntervalQualifier(startUnit, startPrecision, endUnit,
              fractionalSecondPrecision, SqlParserPos.ZERO);
      add(name, typeFactory.createSqlIntervalType(q));
      return this;
    }

    /**
     * Changes the nullability of the last field added.
     *
     * @throws java.lang.IndexOutOfBoundsException if no fields have been
     *                                             added
     */
    public Builder nullable(boolean nullable) {
      RelDataType lastType = types.get(types.size() - 1);
      if (lastType.isNullable() != nullable) {
        final RelDataType type =
            typeFactory.createTypeWithNullability(lastType, nullable);
        types.set(types.size() - 1, type);
      }
      return this;
    }

    /**
     * Adds a field. Field's ordinal is ignored.
     */
    public Builder add(RelDataTypeField field) {
      add(field.getName(), field.getType());
      return this;
    }

    /**
     * Adds all fields in a collection.
     */
    public Builder addAll(
        Iterable<? extends Map.Entry<String, RelDataType>> fields) {
      for (Map.Entry<String, RelDataType> field : fields) {
        add(field.getKey(), field.getValue());
      }
      return this;
    }

    public Builder kind(StructKind kind) {
      this.kind = kind;
      return this;
    }

    /** Sets whether the record type will be nullable. */
    public Builder nullableRecord(boolean nullableRecord) {
      this.nullableRecord = nullableRecord;
      return this;
    }

    /**
     * Makes sure that field names are unique.
     */
    public Builder uniquify() {
      final List<String> uniqueNames = SqlValidatorUtil.uniquify(names,
          typeFactory.getTypeSystem().isSchemaCaseSensitive());
      if (uniqueNames != names) {
        names.clear();
        names.addAll(uniqueNames);
      }
      return this;
    }

    /**
     * Creates a struct type with the current contents of this builder.
     */
    public RelDataType build() {
      return typeFactory.createTypeWithNullability(
          typeFactory.createStructType(kind, types, names),
          nullableRecord);
    }

    /** Creates a dynamic struct type with the current contents of this
     * builder. */
    public RelDataType buildDynamic() {
      final RelDataType dynamicType = new DynamicRecordTypeImpl(typeFactory);
      final RelDataType type = build();
      dynamicType.getFieldList().addAll(type.getFieldList());
      return dynamicType;
    }

    /** Returns whether a field exists with the given name. */
    public boolean nameExists(String name) {
      return names.contains(name);
    }
  }
}
