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

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;

import java.math.RoundingMode;

/**
 * Custom type system only for Quidem test.
 */

public final class CustomRelDataTypeSystem {

  private CustomRelDataTypeSystem() {
  }

  /**
   *  Specify the rounding behaviour. In the default implementation,
   *  the rounding mode is {@link RoundingMode#DOWN}, but here is {@link RoundingMode#HALF_UP}.
   */
  public static final RelDataTypeSystem ROUNDING_MODE_HALF_UP = new RelDataTypeSystemImpl() {
    @Override public RoundingMode roundingMode() {
      return RoundingMode.HALF_UP;
    }
  };

  /**
   *  Supports negative scale.
   */
  public static final RelDataTypeSystem NEGATIVE_SCALE = new RelDataTypeSystemImpl() {
    @Override public boolean supportsNegativeScale() {
      return true;
    }
  };

  /**
   *  Supports negative scale and rounding mode is {@link RoundingMode#HALF_UP}.
   */
  public static final RelDataTypeSystem NEGATIVE_SCALE_ROUNDING_MODE_HALF_UP =
      new RelDataTypeSystemImpl() {
    @Override public boolean supportsNegativeScale() {
      return true;
    }
    @Override public RoundingMode roundingMode() {
          return RoundingMode.HALF_UP;
        }
  };

}
