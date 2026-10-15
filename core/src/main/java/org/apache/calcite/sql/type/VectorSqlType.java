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
package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;

import static com.google.common.base.Preconditions.checkArgument;

import static java.util.Objects.requireNonNull;

/**
 * SQL vector type.
 *
 * <p>A vector is a list of a fixed number of numeric elements, such as an
 * embedding produced by a machine-learning model. The number of elements
 * (the <em>dimension</em>) and the element type are part of the type; for
 * example, {@code VECTOR(3, REAL)} is the type of vectors of 3
 * single-precision floating-point numbers. The elements of a vector are never
 * null, but a vector value as a whole may be null.
 *
 * <p>The supported element types are {@code REAL} (the default) and
 * {@code TINYINT}; see {@link #isValidElementType(SqlTypeName)}.
 *
 * <p>Vectors are not comparable: they cannot be operands of {@code =} or
 * {@code <}, nor keys of {@code ORDER BY} or {@code GROUP BY}.
 *
 * <p>At run time, a vector is represented, like an {@code ARRAY}, by a
 * {@link java.util.List} of its elements.
 *
 * @see org.apache.calcite.rel.type.RelDataTypeFactory#createVectorType
 */
public class VectorSqlType extends AbstractSqlType {
  /** Element type of a vector whose element type is not specified,
   * as in {@code VECTOR(3)}. */
  public static final SqlTypeName DEFAULT_ELEMENT_TYPE_NAME = SqlTypeName.REAL;

  private final RelDataType elementType;
  private final int dimension;

  /**
   * Creates a VectorSqlType. This constructor should only be called
   * from a factory method.
   *
   * @param elementType Element type; must be NOT NULL and have a type name
   *                    that satisfies {@link #isValidElementType}
   * @param dimension   Number of elements; must be positive
   * @param isNullable  Whether a value of this type may be null
   */
  public VectorSqlType(RelDataType elementType, int dimension,
      boolean isNullable) {
    super(SqlTypeName.VECTOR, isNullable, null);
    this.elementType = requireNonNull(elementType, "elementType");
    checkArgument(isValidElementType(elementType.getSqlTypeName()),
        "invalid element type for VECTOR: %s", elementType);
    checkArgument(!elementType.isNullable(),
        "element type of VECTOR must not be nullable: %s", elementType);
    checkArgument(dimension > 0,
        "dimension of VECTOR must be positive: %s", dimension);
    this.dimension = dimension;
    computeDigest();
  }

  /** Returns whether a type can be the element type of a vector. */
  public static boolean isValidElementType(SqlTypeName typeName) {
    switch (typeName) {
    case REAL:
    case TINYINT:
      return true;
    default:
      return false;
    }
  }

  /** Returns the number of elements of a vector of this type. */
  public int getDimension() {
    return dimension;
  }

  /** {@inheritDoc}
   *
   * <p>Returns the dimension, so that, for example, JDBC metadata reports
   * the dimension of a {@code VECTOR} column as its column size. */
  @Override public int getPrecision() {
    return dimension;
  }

  /** {@inheritDoc}
   *
   * <p>Returns the element type, which is never nullable. */
  @Override public RelDataType getComponentType() {
    return elementType;
  }

  /** {@inheritDoc}
   *
   * <p>Returns {@link RelDataTypeComparability#NONE}: vectors are not
   * comparable. */
  @Override public RelDataTypeComparability getComparability() {
    return RelDataTypeComparability.NONE;
  }

  /** {@inheritDoc}
   *
   * <p>Generates, for example, {@code VECTOR(3, REAL)}. */
  @Override protected void generateTypeString(StringBuilder sb,
      boolean withDetail) {
    sb.append("VECTOR(")
        .append(dimension)
        .append(", ")
        .append(elementType.getSqlTypeName().getName())
        .append(')');
  }
}
