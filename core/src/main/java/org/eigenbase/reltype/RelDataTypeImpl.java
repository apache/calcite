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

import java.io.*;
import java.nio.charset.*;
import java.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.Pair;
import org.eigenbase.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

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
   * Default constructor, to allow derived classes such as {@link
   * BasicSqlType} to be {@link Serializable}.
   *
   * <p>(The serialization specification says that a class can be serializable
   * even if its base class is not serializable, provided that the base class
   * has a public or protected zero-args constructor.)
   */
  protected RelDataTypeImpl() {
    this(null);
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelDataType
  public RelDataTypeField getField(String fieldName, boolean caseSensitive) {
    for (RelDataTypeField field : fieldList) {
      if (Util.match(caseSensitive, field.getName(), fieldName)) {
        return field;
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
    return null;
  }

  // implement RelDataType
  public List<RelDataTypeField> getFieldList() {
    assert isStruct();
    return fieldList;
  }

  public List<String> getFieldNames() {
    return Pair.left(fieldList);
  }

  // implement RelDataType
  public int getFieldCount() {
    assert isStruct() : this;
    return fieldList.size();
  }

  // implement RelDataType
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

  // implement RelDataType
  public boolean isStruct() {
    return fieldList != null;
  }

  // implement RelDataType
  public boolean equals(Object obj) {
    if (obj instanceof RelDataTypeImpl) {
      final RelDataTypeImpl that = (RelDataTypeImpl) obj;
      return this.digest.equals(that.digest);
    }
    return false;
  }

  // implement RelDataType
  public int hashCode() {
    return digest.hashCode();
  }

  // implement RelDataType
  public String getFullTypeString() {
    return digest;
  }

  // implement RelDataType
  public boolean isNullable() {
    return false;
  }

  // implement RelDataType
  public Charset getCharset() {
    return null;
  }

  // implement RelDataType
  public SqlCollation getCollation() {
    return null;
  }

  // implement RelDataType
  public SqlIntervalQualifier getIntervalQualifier() {
    return null;
  }

  // implement RelDataType
  public int getPrecision() {
    return PRECISION_NOT_SPECIFIED;
  }

  // implement RelDataType
  public int getScale() {
    return SCALE_NOT_SPECIFIED;
  }

  // implement RelDataType
  public SqlTypeName getSqlTypeName() {
    return null;
  }

  // implement RelDataType
  public SqlIdentifier getSqlIdentifier() {
    SqlTypeName typeName = getSqlTypeName();
    if (typeName == null) {
      return null;
    }
    return new SqlIdentifier(
        typeName.name(),
        SqlParserPos.ZERO);
  }

  // implement RelDataType
  public RelDataTypeFamily getFamily() {
    // by default, put each type into its own family
    return this;
  }

  /**
   * Generates a string representation of this type.
   *
   * @param sb         StringBuffer into which to generate the string
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

  // implement RelDataType
  public String toString() {
    StringBuilder sb = new StringBuilder();
    generateTypeString(sb, false);
    return sb.toString();
  }

  // implement RelDataType
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

  // implement RelDataType
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
    return new RelProtoDataType() {
      public RelDataType apply(RelDataTypeFactory typeFactory) {
        return typeFactory.copyType(protoType);
      }
    };
  }

  /** Returns a {@link org.eigenbase.reltype.RelProtoDataType} that will create
   * a type {@code typeName}.
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
    return new RelProtoDataType() {
      public RelDataType apply(RelDataTypeFactory typeFactory) {
        final RelDataType type = typeFactory.createSqlType(typeName);
        return typeFactory.createTypeWithNullability(type, nullable);
      }
    };
  }

  /** Returns a {@link org.eigenbase.reltype.RelProtoDataType} that will create
   * a type {@code typeName(precision)}.
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
    return new RelProtoDataType() {
      public RelDataType apply(RelDataTypeFactory typeFactory) {
        final RelDataType type = typeFactory.createSqlType(typeName, precision);
        return typeFactory.createTypeWithNullability(type, nullable);
      }
    };
  }

  /** Returns a {@link org.eigenbase.reltype.RelProtoDataType} that will create
   * a type {@code typeName(precision, scale)}.
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
    return new RelProtoDataType() {
      public RelDataType apply(RelDataTypeFactory typeFactory) {
        final RelDataType type =
            typeFactory.createSqlType(typeName, precision, scale);
        return typeFactory.createTypeWithNullability(type, nullable);
      }
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
    return rowType.getField("_extra", true);
  }
}

// End RelDataTypeImpl.java
