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

import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Type system.
 *
 * <p>Provides behaviors concerning type limits and behaviors. For example,
 * in the default system, a DECIMAL can have maximum precision 19, but Hive
 * overrides to 38.
 *
 * <p>The default implementation is {@link #DEFAULT}.
 */
public interface RelDataTypeSystem {
  /** Default type system. */
  RelDataTypeSystem DEFAULT = new RelDataTypeSystemImpl() { };

  /** Returns the maximum scale of a given type. */
  int getMaxScale(SqlTypeName typeName);

  /**
   * Returns default precision for this type if supported, otherwise -1 if
   * precision is either unsupported or must be specified explicitly.
   *
   * @return Default precision
   */
  int getDefaultPrecision(SqlTypeName typeName);

  /**
   * Returns the maximum precision (or length) allowed for this type, or -1 if
   * precision/length are not applicable for this type.
   *
   * @return Maximum allowed precision
   */
  int getMaxPrecision(SqlTypeName typeName);

  /** Returns the maximum scale of a NUMERIC or DECIMAL type. */
  int getMaxNumericScale();

  /** Returns the maximum precision of a NUMERIC or DECIMAL type. */
  int getMaxNumericPrecision();

  /** Returns the LITERAL string for the type, either PREFIX/SUFFIX. */
  String getLiteral(SqlTypeName typeName, boolean isPrefix);

  /** Returns whether the type is case sensitive. */
  boolean isCaseSensitive(SqlTypeName typeName);

  /** Returns whether the type can be auto increment. */
  boolean isAutoincrement(SqlTypeName typeName);

  /** Returns the numeric type radix, typically 2 or 10.
   * 0 means "not applicable". */
  int getNumTypeRadix(SqlTypeName typeName);

  /**
   * Returns the return type of a call to the {@code SUM} aggregate function
   * inferred from its argument type.
   */
  RelDataType deriveSumType(RelDataTypeFactory typeFactory, RelDataType argumentType);

  /** Returns the return type of the {@code CUME_DIST} and {@code PERCENT_RANK}
   * aggregate functions. */
  RelDataType deriveFractionalRankType(RelDataTypeFactory typeFactory);

  /** Returns the return type of the {@code NTILE}, {@code RANK},
   * {@code DENSE_RANK}, and {@code ROW_NUMBER} aggregate functions. */
  RelDataType deriveRankType(RelDataTypeFactory typeFactory);

  /** Whether two record types are considered distinct if their field names
   * are the same but in different cases. */
  boolean isSchemaCaseSensitive();
}

// End RelDataTypeSystem.java
