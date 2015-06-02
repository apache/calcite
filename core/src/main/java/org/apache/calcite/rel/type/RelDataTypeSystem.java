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

  /** Returns if the type is case sensitive true or not (false) */
  boolean isCaseSensitive(SqlTypeName typeName);

  /** Returns if the type can be auto increment true or not (false) */
  boolean isAutoincrement(SqlTypeName typeName);

  /** Returns the numeric type Radix, 2 or 10. 0 represent not applicable*/
  int getNumTypeRadix(SqlTypeName typeName);
}

// End RelDataTypeSystem.java
