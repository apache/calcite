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

/** Implementation of {@link org.apache.calcite.rel.type.RelDataTypeSystem}
 * that sends all methods to an underlying object. */
public class DelegatingTypeSystem implements RelDataTypeSystem {
  private final RelDataTypeSystem typeSystem;

  /** Creates a {@code DelegatingTypeSystem}. */
  protected DelegatingTypeSystem(RelDataTypeSystem typeSystem) {
    this.typeSystem = typeSystem;
  }

  public int getMaxScale(SqlTypeName typeName) {
    return typeSystem.getMaxScale(typeName);
  }

  public int getDefaultPrecision(SqlTypeName typeName) {
    return typeSystem.getDefaultPrecision(typeName);
  }

  public int getMaxPrecision(SqlTypeName typeName) {
    return typeSystem.getMaxPrecision(typeName);
  }

  public int getMaxNumericScale() {
    return typeSystem.getMaxNumericScale();
  }

  public int getMaxNumericPrecision() {
    return typeSystem.getMaxNumericPrecision();
  }

  public String getLiteral(SqlTypeName typeName, boolean isPrefix) {
    return typeSystem.getLiteral(typeName, isPrefix);
  }

  public boolean isCaseSensitive(SqlTypeName typeName) {
    return typeSystem.isCaseSensitive(typeName);
  }

  public boolean isAutoincrement(SqlTypeName typeName) {
    return typeSystem.isAutoincrement(typeName);
  }

  public int getNumTypeRadix(SqlTypeName typeName) {
    return typeSystem.getNumTypeRadix(typeName);
  }

  public RelDataType deriveSumType(RelDataTypeFactory typeFactory,
      RelDataType argumentType) {
    return typeSystem.deriveSumType(typeFactory, argumentType);
  }

  public RelDataType deriveAvgAggType(RelDataTypeFactory typeFactory,
      RelDataType argumentType) {
    return typeSystem.deriveAvgAggType(typeFactory, argumentType);
  }

  public RelDataType deriveCovarType(RelDataTypeFactory typeFactory,
      RelDataType arg0Type, RelDataType arg1Type) {
    return typeSystem.deriveCovarType(typeFactory, arg0Type, arg1Type);
  }

  public RelDataType deriveFractionalRankType(RelDataTypeFactory typeFactory) {
    return typeSystem.deriveFractionalRankType(typeFactory);
  }

  public RelDataType deriveRankType(RelDataTypeFactory typeFactory) {
    return typeSystem.deriveRankType(typeFactory);
  }

  public boolean isSchemaCaseSensitive() {
    return typeSystem.isSchemaCaseSensitive();
  }

  public boolean shouldConvertRaggedUnionTypesToVarying() {
    return typeSystem.shouldConvertRaggedUnionTypesToVarying();
  }
}

// End DelegatingTypeSystem.java
