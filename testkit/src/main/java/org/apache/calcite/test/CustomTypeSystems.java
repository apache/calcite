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
package org.apache.calcite.test;

import org.apache.calcite.rel.type.DelegatingTypeSystem;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.math.RoundingMode;
import java.util.function.Function;

/**
 * Custom implementations of {@link RelDataTypeSystem} for testing.
 */
@SuppressWarnings("SameParameterValue")
public final class CustomTypeSystems {
  private CustomTypeSystems() {
  }

  /** Type system with rounding behavior {@link RoundingMode#HALF_UP}.
   * (The default implementation is {@link RoundingMode#DOWN}.) */
  public static final RelDataTypeSystem ROUNDING_MODE_HALF_UP =
      withRoundingMode(RelDataTypeSystem.DEFAULT, RoundingMode.HALF_UP);

  /** Type system that supports negative scale
   * and has rounding mode {@link RoundingMode#DOWN}. */
  public static final RelDataTypeSystem NEGATIVE_SCALE =
      withMinScale(RelDataTypeSystem.DEFAULT, sqlTypeName -> -1000);

  /** Type system that supports negative scale
   * and has rounding mode {@link RoundingMode#HALF_UP}. */
  public static final RelDataTypeSystem NEGATIVE_SCALE_ROUNDING_MODE_HALF_UP =
      withMinScale(ROUNDING_MODE_HALF_UP, sqlTypeName -> -1000);

  /** Type system that similar to Spark. */
  public static final RelDataTypeSystem SPARK_TYPE_SYSTEM =
      new DelegatingTypeSystem(RelDataTypeSystem.DEFAULT) {
        @Override public RelDataType deriveSumType(RelDataTypeFactory typeFactory,
            RelDataType argumentType) {
          if (argumentType instanceof BasicSqlType) {
            // For TINYINT, SMALLINT, INTEGER, BIGINT,
            // using BIGINT
            if (SqlTypeUtil.isExactNumeric(argumentType) && !SqlTypeUtil.isDecimal(argumentType)) {
              argumentType =
                  typeFactory.createTypeWithNullability(
                      typeFactory.createSqlType(SqlTypeName.BIGINT),
                      argumentType.isNullable());
            }
            // For FLOAT, REAL and DOUBLE,
            // using DOUBLE
            if (SqlTypeUtil.isApproximateNumeric(argumentType)) {
              argumentType =
                  typeFactory.createTypeWithNullability(
                      typeFactory.createSqlType(SqlTypeName.DOUBLE),
                      argumentType.isNullable());
            }
            return argumentType;
          }
          return super.deriveSumType(typeFactory, argumentType);
        }
    };

  /** Decorates a type system so that
   * {@link org.apache.calcite.rel.type.RelDataTypeSystem#roundingMode()}
   * returns a given value. */
  public static RelDataTypeSystem withRoundingMode(RelDataTypeSystem typeSystem,
      RoundingMode roundingMode) {
    return new DelegatingTypeSystem(typeSystem) {
      @Override public RoundingMode roundingMode() {
        return roundingMode;
      }
    };
  }

  /** Decorates a type system so that
   * {@link org.apache.calcite.rel.type.RelDataTypeSystem#getMaxPrecision(SqlTypeName)}
   * returns a given value. */
  public static RelDataTypeSystem withMaxPrecision(RelDataTypeSystem typeSystem,
      Function<SqlTypeName, Integer> maxPrecision) {
    return new DelegatingTypeSystem(typeSystem) {
      @Override public int getMaxPrecision(SqlTypeName typeName) {
        return maxPrecision.apply(typeName);
      }
    };
  }

  /** Decorates a type system so that
   * {@link org.apache.calcite.rel.type.RelDataTypeSystem#getMaxScale(SqlTypeName)}
   * returns a given value. */
  public static RelDataTypeSystem withMaxScale(RelDataTypeSystem typeSystem,
      Function<SqlTypeName, Integer> maxScale) {
    return new DelegatingTypeSystem(typeSystem) {
      @Override public int getMaxScale(SqlTypeName typeName) {
        return maxScale.apply(typeName);
      }
    };
  }

  /** Decorates a type system so that
   * {@link org.apache.calcite.rel.type.RelDataTypeSystem#getMinScale(SqlTypeName)}
   * returns a given value. */
  public static RelDataTypeSystem withMinScale(RelDataTypeSystem typeSystem,
      Function<SqlTypeName, Integer> minNumericScale) {
    return new DelegatingTypeSystem(typeSystem) {
      @Override public int getMinScale(SqlTypeName typeName) {
        return minNumericScale.apply(typeName);
      }
    };
  }
}
