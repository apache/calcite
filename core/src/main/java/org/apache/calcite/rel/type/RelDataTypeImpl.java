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

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * RelDataTypeImpl is an abstract base for implementations of
 * {@link RelDataType}.
 *
 * <p>Identity is based upon the {@link #digest} field, which each derived class
 * should set during construction.</p>
 */
public abstract class RelDataTypeImpl
    implements RelDataType, RelDataTypeFamily {
  //~ Instance fields --------------------------------------------------------

  protected final List<RelDataTypeField> fieldList;
  protected String digest;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a RelDataTypeImpl.
   *
   * @param fieldList List of fields
   */
  protected RelDataTypeImpl(List<? extends RelDataTypeField> fieldList) {
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

  public RelDataTypeField getField(String fieldName, boolean caseSensitive,
      boolean elideRecord) {
    for (RelDataTypeField field : fieldList) {
      if (Util.matches(caseSensitive, field.getName(), fieldName)) {
        return field;
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
    if (fieldList.size() > 0) {
      final RelDataTypeField lastField = Iterables.getLast(fieldList);
      if (lastField.getName().equals("_extra")) {
        return new RelDataTypeFieldImpl(
            fieldName, -1, lastField.getType());
      }
    }

    // a dynamic * field will match any field name.
    for (RelDataTypeField field : fieldList) {
      if (field.isDynamicStar()) {
        // the requested field could be in the unresolved star
        return field;
      }
    }

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

  public List<RelDataTypeField> getFieldList() {
    assert isStruct();
    return fieldList;
  }

  public List<String> getFieldNames() {
    return Pair.left(fieldList);
  }

  public int getFieldCount() {
    assert isStruct() : this;
    return fieldList.size();
  }

  public StructKind getStructKind() {
    return isStruct() ? StructKind.FULLY_QUALIFIED : StructKind.NONE;
  }

  public RelDataType getComponentType() {
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

  public boolean isStruct() {
    return fieldList != null;
  }

  @Override public boolean equals(Object obj) {
    if (obj instanceof RelDataTypeImpl) {
      final RelDataTypeImpl that = (RelDataTypeImpl) obj;
      return this.digest.equals(that.digest);
    }
    return false;
  }

  @Override public int hashCode() {
    return digest.hashCode();
  }

  public String getFullTypeString() {
    return digest;
  }

  public boolean isNullable() {
    return false;
  }

  public Charset getCharset() {
    return null;
  }

  public SqlCollation getCollation() {
    return null;
  }

  public SqlIntervalQualifier getIntervalQualifier() {
    return null;
  }

  public int getPrecision() {
    return PRECISION_NOT_SPECIFIED;
  }

  public int getScale() {
    return SCALE_NOT_SPECIFIED;
  }

  public SqlTypeName getSqlTypeName() {
    return null;
  }

  public SqlIdentifier getSqlIdentifier() {
    SqlTypeName typeName = getSqlTypeName();
    if (typeName == null) {
      return null;
    }
    return new SqlIdentifier(
        typeName.name(),
        SqlParserPos.ZERO);
  }

  public RelDataTypeFamily getFamily() {
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
  protected void computeDigest() {
    StringBuilder sb = new StringBuilder();
    generateTypeString(sb, true);
    if (!isNullable()) {
      sb.append(" NOT NULL");
    }
    digest = sb.toString();
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    generateTypeString(sb, false);
    return sb.toString();
  }

  public RelDataTypePrecedenceList getPrecedenceList() {
    // by default, make each type have a precedence list containing
    // only other types in the same family
    return new RelDataTypePrecedenceList() {
      public boolean containsType(RelDataType type) {
        return getFamily() == type.getFamily();
      }

      public int compareTypePrecedence(
          RelDataType type1,
          RelDataType type2) {
        assert containsType(type1);
        assert containsType(type2);
        return 0;
      }
    };
  }

  public RelDataTypeComparability getComparability() {
    return RelDataTypeComparability.ALL;
  }

  /**
   * Returns an implementation of
   * {@link RelProtoDataType}
   * that copies a given type using the given type factory.
   */
  public static RelProtoDataType proto(final RelDataType protoType) {
    assert protoType != null;
    return typeFactory -> typeFactory.copyType(protoType);
  }

  /** Returns a {@link org.apache.calcite.rel.type.RelProtoDataType}
   * that will create a type {@code typeName}.
   *
   * <p>For example, {@code proto(SqlTypeName.DATE), false}
   * will create {@code DATE NOT NULL}.</p>
   *
   * @param typeName Type name
   * @param nullable Whether nullable
   * @return Proto data type
   */
  public static RelProtoDataType proto(final SqlTypeName typeName,
      final boolean nullable) {
    assert typeName != null;
    return typeFactory -> {
      final RelDataType type = typeFactory.createSqlType(typeName);
      return typeFactory.createTypeWithNullability(type, nullable);
    };
  }

  /** Returns a {@link org.apache.calcite.rel.type.RelProtoDataType}
   * that will create a type {@code typeName(precision)}.
   *
   * <p>For example, {@code proto(SqlTypeName.VARCHAR, 100, false)}
   * will create {@code VARCHAR(100) NOT NULL}.</p>
   *
   * @param typeName Type name
   * @param precision Precision
   * @param nullable Whether nullable
   * @return Proto data type
   */
  public static RelProtoDataType proto(final SqlTypeName typeName,
      final int precision, final boolean nullable) {
    assert typeName != null;
    return typeFactory -> {
      final RelDataType type = typeFactory.createSqlType(typeName, precision);
      return typeFactory.createTypeWithNullability(type, nullable);
    };
  }

  /** Returns a {@link org.apache.calcite.rel.type.RelProtoDataType}
   * that will create a type {@code typeName(precision, scale)}.
   *
   * <p>For example, {@code proto(SqlTypeName.DECIMAL, 7, 2, false)}
   * will create {@code DECIMAL(7, 2) NOT NULL}.</p>
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
  public static RelDataTypeField extra(RelDataType rowType) {
    // Even in a case-insensitive connection, the name must be precisely
    // "_extra".
    return rowType.getField("_extra", true, false);
  }

  public boolean isDynamicStruct() {
    return false;
  }

  /** Work space for {@link RelDataTypeImpl#getFieldRecurse}. */
  private static class Slot {
    int count;
    RelDataTypeField field;
  }
}

// End RelDataTypeImpl.java
