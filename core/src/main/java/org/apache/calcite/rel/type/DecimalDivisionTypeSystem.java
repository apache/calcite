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
import org.apache.calcite.sql.type.SqlTypeUtil;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Type system that reduces decimal division scale based on the number of
 * integral digits, rather than the total precision.
 *
 * <p>Other type rules and limits are the same as {@link RelDataTypeSystem#DEFAULT}.
 * To enable it, set the {@code typeSystem} connection property to
 * {@code org.apache.calcite.rel.type.DecimalDivisionTypeSystem}.
 * For example, dividing two {@code DECIMAL(6, 2)} values yields
 * {@code DECIMAL(15, 9)}, rather than the default {@code DECIMAL(15, 6)}.
 */
public class DecimalDivisionTypeSystem extends RelDataTypeSystemImpl {
  @Override public @Nullable RelDataType deriveDecimalDivideType(
      RelDataTypeFactory typeFactory, RelDataType type1, RelDataType type2) {
    if (!SqlTypeUtil.isExactNumeric(type1)
        || !SqlTypeUtil.isExactNumeric(type2)
        || !(SqlTypeUtil.isDecimal(type1) || SqlTypeUtil.isDecimal(type2))) {
      return null;
    }

    // Java numeric types need their default decimal precision and scale.
    type1 = RelDataTypeFactoryImpl.isJavaType(type1)
        ? typeFactory.decimalOf(type1) : type1;
    type2 = RelDataTypeFactoryImpl.isJavaType(type2)
        ? typeFactory.decimalOf(type2) : type2;
    final int maxPrecision = getMaxPrecision(SqlTypeName.DECIMAL);
    final int maxScale = getMaxScale(SqlTypeName.DECIMAL);
    final int minScale = Math.min(6, maxScale);
    final int integral = type1.getPrecision() - type1.getScale() + type2.getScale();
    int scale =
        Math.min(Math.max(minScale, type1.getScale() + type2.getPrecision() + 1), maxScale);
    final int precision = Math.min(integral + scale, maxPrecision);

    // Preserve fractional digits when the integral part fits. Otherwise keep
    // at most six fractional digits, as in the default division rule.
    if (integral <= maxPrecision - minScale) {
      scale = Math.min(scale, maxPrecision - integral);
    } else {
      scale = Math.min(minScale, scale);
    }
    return typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
  }
}
