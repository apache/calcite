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

import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.checkerframework.checker.initialization.qual.UnknownInitialization;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

/**
 * RelDataTypeImpl is an abstract base for implementations of
 * {@link RelDataType}.
 *
 * <p>Identity is based upon the {@link #digest} field, which each derived class
 * should set during construction.
 */
public abstract class RelDataTypeImpl
    implements RelDataType, RelDataTypeFamily {

  /**
   * Suffix for the digests of non-nullable types.
   */
  public static final String NON_NULLABLE_SUFFIX = " NOT NULL";

  //~ Instance fields --------------------------------------------------------

  protected final @Nullable List<RelDataTypeField> fieldList;
  protected @Nullable String digest;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a RelDataTypeImpl.
   *
   * @param fieldList List of fields
   */
  protected RelDataTypeImpl(@Nullable List<? extends RelDataTypeField> fieldList) {
    if (fieldList != null) {
      // Create a defensive copy of the list.
      this.fieldList = ImmutableList.copyOf(fieldList);
    } else {
      this.fieldList = null;
    }
  }

  /**
   * Default constructor, to allow derived classes such as
   * {@link BasicSqlType} to be {@link Serializable}.
   *
   * <p>(The serialization specification says that a class can be serializable
   * even if its base class is not serializable, provided that the base class
   * has a public or protected zero-args constructor.)
   */
  protected RelDataTypeImpl() {
    this(null);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public @Nullable RelDataTypeField getField(String fieldName,
      boolean caseSensitive, boolean elideRecord) {
    if (fieldList == null) {
      throw new IllegalStateException("Trying to access field " + fieldName
          + " in a type with no fields: " + this);
    }
    final Map<String, RelDataTypeField> fieldMap = getFieldMap();
    if (caseSensitive && fieldMap != null) {
      RelDataTypeField field = fieldMap.get(fieldName);
      if (field != null) {
        return field;
      }
    } else {
      for (RelDataTypeField field : fieldList) {
        if (Util.matches(caseSensitive, field.getName(), fieldName)) {
          return field;
        }
      }
    }
    if (elideRecord) {
      final List<Slot> slots = new ArrayList<>();
      getFieldRecurse(slots, this, 0, fieldName, caseSensitive);
    loop:
      for (Slot slot : slots) {
        switch (slot.count) {
        case 0:
          break; // no match at this depth; try deeper
        case 1:
          return slot.field;
        default:
          break loop; // duplicate fields at this depth; abandon search
        }
      }
    }
    // Extra field
    if (!fieldList.isEmpty()) {
      final RelDataTypeField lastField = Iterables.getLast(fieldList);
      if (lastField.getName().equals("_extra")) {
        return new RelDataTypeFieldImpl(
            fieldName, -1, lastField.getType());
      }
    }

    // a dynamic * field will match any field name.
    if (fieldMap != null) {
      return fieldMap.get("");
    } else {
      for (RelDataTypeField field : fieldList) {
        if (field.isDynamicStar()) {
          // the requested field could be in the unresolved star
          return field;
        }
      }
    }

    return null;
  }

  /** Returns a map from field names to fields.
   *
   * <p>Matching is case-sensitive.
   *
   * <p>If several fields have the same name, the map contains the first.
   *
   * <p>A {@link RelDataTypeField#isDynamicStar() dynamic star field} is indexed
   * under its own name and "" (the empty string).
   *
   * <p>If the map is null, the type must do lookup the long way.
   */
  protected @Nullable Map<String, RelDataTypeField> getFieldMap() {
    return null;
  }

  private static void getFieldRecurse(List<Slot> slots, RelDataType type,
      int depth, String fieldName, boolean caseSensitive) {
    while (slots.size() <= depth) {
      slots.add(new Slot());
    }
    final Slot slot = slots.get(depth);
    for (RelDataTypeField field : type.getFieldList()) {
      if (Util.matches(caseSensitive, field.getName(), fieldName)) {
        slot.count++;
        slot.field = field;
      }
    }
    // No point looking to depth + 1 if there is a hit at depth.
    if (slot.count == 0) {
      for (RelDataTypeField field : type.getFieldList()) {
        if (field.getType().isStruct()) {
          getFieldRecurse(slots, field.getType(), depth + 1,
              fieldName, caseSensitive);
        }
      }
    }
  }

  @Override public List<RelDataTypeField> getFieldList() {
    if (fieldList == null) {
      throw new AssertionError("fieldList must not be null, type = " + this);
    }
    return fieldList;
  }

  @Override public List<String> getFieldNames() {
    if (fieldList == null) {
      throw new AssertionError("fieldList must not be null, type = " + this);
    }
    return Pair.left(fieldList);
  }

  @Override public int getFieldCount() {
    if (fieldList == null) {
      throw new AssertionError("fieldList must not be null, type = " + this);
    }
    return fieldList.size();
  }

  @Override public StructKind getStructKind() {
    return isStruct() ? StructKind.FULLY_QUALIFIED : StructKind.NONE;
  }

  @Override public @Nullable RelDataType getComponentType() {
    // this is not a collection type
    return null;
  }

  @Override public @Nullable RelDataType getKeyType() {
    // this is not a map type
    return null;
  }

  @Override public @Nullable RelDataType getValueType() {
    // this is not a map type
    return null;
  }

  @Override public boolean isStruct() {
    return fieldList != null;
  }

  @Override public boolean equals(@Nullable Object obj) {
    return this == obj
        || obj instanceof RelDataTypeImpl
        && Objects.equals(this.digest, ((RelDataTypeImpl) obj).digest);
  }

  @Override public int hashCode() {
    return Objects.hashCode(digest);
  }

  @Override public String getFullTypeString() {
    return requireNonNull(digest, "digest");
  }

  @Override public boolean isNullable() {
    return false;
  }

  @Override public @Nullable Charset getCharset() {
    return null;
  }

  @Override public @Nullable SqlCollation getCollation() {
    return null;
  }

  @Override public @Nullable SqlIntervalQualifier getIntervalQualifier() {
    return null;
  }

  @Override public int getPrecision() {
    return PRECISION_NOT_SPECIFIED;
  }

  @Override public int getScale() {
    return SCALE_NOT_SPECIFIED;
  }

  /**
   * Gets the {@link SqlTypeName} of this type.
   * Sub-classes must override the method to ensure the resulting value is non-nullable.
   *
   * @return SqlTypeName, never null
   */
  @Override public SqlTypeName getSqlTypeName() {
    // The implementations must provide non-null value, however, we keep this for compatibility
    return castNonNull(null);
  }

  @Override public @Nullable SqlIdentifier getSqlIdentifier() {
    SqlTypeName typeName = getSqlTypeName();
    if (typeName == null) {
      return null;
    }
    return new SqlIdentifier(
        typeName.name(),
        SqlParserPos.ZERO);
  }

  @Override public RelDataTypeFamily getFamily() {
    // by default, put each type into its own family
    return this;
  }

  /**
   * Generates a string representation of this type.
   *
   * @param sb         StringBuilder into which to generate the string
   * @param withDetail when true, all detail information needed to compute a
   *                   unique digest (and return from getFullTypeString) should
   *                   be included;
   */
  protected abstract void generateTypeString(
      StringBuilder sb,
      boolean withDetail);

  /**
   * Computes the digest field. This should be called in every non-abstract
   * subclass constructor once the type is fully defined.
   */
  @SuppressWarnings("method.invocation.invalid")
  protected void computeDigest(@UnknownInitialization RelDataTypeImpl this) {
    StringBuilder sb = new StringBuilder();
    generateTypeString(sb, true);
    if (!isNullable()) {
      sb.append(NON_NULLABLE_SUFFIX);
    }
    digest = sb.toString();
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    generateTypeString(sb, false);
    return sb.toString();
  }

  @Override public RelDataTypePrecedenceList getPrecedenceList() {
    // by default, make each type have a precedence list containing
    // only other types in the same family
    return new RelDataTypePrecedenceList() {
      @Override public boolean containsType(RelDataType type) {
        return getFamily() == type.getFamily();
      }

      @Override public int compareTypePrecedence(
          RelDataType type1,
          RelDataType type2) {
        assert containsType(type1);
        assert containsType(type2);
        return 0;
      }
    };
  }

  @Override public RelDataTypeComparability getComparability() {
    return RelDataTypeComparability.ALL;
  }

  /**
   * Returns an implementation of
   * {@link RelProtoDataType}
   * that copies a given type using the given type factory.
   */
  public static RelProtoDataType proto(final RelDataType protoType) {
    requireNonNull(protoType, "protoType");
    return typeFactory -> typeFactory.copyType(protoType);
  }

  /** Returns a {@link org.apache.calcite.rel.type.RelProtoDataType}
   * that will create a type {@code typeName}.
   *
   * <p>For example, {@code proto(SqlTypeName.DATE), false}
   * will create {@code DATE NOT NULL}.
   *
   * @param typeName Type name
   * @param nullable Whether nullable
   * @return Proto data type
   */
  public static RelProtoDataType proto(final SqlTypeName typeName,
      final boolean nullable) {
    requireNonNull(typeName, "typeName");
    return typeFactory -> {
      final RelDataType type = typeFactory.createSqlType(typeName);
      return typeFactory.createTypeWithNullability(type, nullable);
    };
  }

  /** Returns a {@link org.apache.calcite.rel.type.RelProtoDataType}
   * that will create a type {@code typeName(precision)}.
   *
   * <p>For example, {@code proto(SqlTypeName.VARCHAR, 100, false)}
   * will create {@code VARCHAR(100) NOT NULL}.
   *
   * @param typeName Type name
   * @param precision Precision
   * @param nullable Whether nullable
   * @return Proto data type
   */
  public static RelProtoDataType proto(final SqlTypeName typeName,
      final int precision, final boolean nullable) {
    requireNonNull(typeName, "typeName");
    return typeFactory -> {
      final RelDataType type = typeFactory.createSqlType(typeName, precision);
      return typeFactory.createTypeWithNullability(type, nullable);
    };
  }

  /** Returns a {@link org.apache.calcite.rel.type.RelProtoDataType}
   * that will create a type {@code typeName(precision, scale)}.
   *
   * <p>For example, {@code proto(SqlTypeName.DECIMAL, 7, 2, false)}
   * will create {@code DECIMAL(7, 2) NOT NULL}.
   *
   * @param typeName Type name
   * @param precision Precision
   * @param scale Scale
   * @param nullable Whether nullable
   * @return Proto data type
   */
  public static RelProtoDataType proto(final SqlTypeName typeName,
      final int precision, final int scale, final boolean nullable) {
    return typeFactory -> {
      final RelDataType type =
          typeFactory.createSqlType(typeName, precision, scale);
      return typeFactory.createTypeWithNullability(type, nullable);
    };
  }

  /**
   * Returns the "extra" field in a row type whose presence signals that
   * fields will come into existence just by asking for them.
   *
   * @param rowType Row type
   * @return The "extra" field, or null
   */
  public static @Nullable RelDataTypeField extra(RelDataType rowType) {
    // Even in a case-insensitive connection, the name must be precisely
    // "_extra".
    return rowType.getField("_extra", true, false);
  }

  @Override public boolean isDynamicStruct() {
    return false;
  }

  /** Work space for {@link RelDataTypeImpl#getFieldRecurse}. */
  private static class Slot {
    int count;
    @Nullable RelDataTypeField field;
  }
}
