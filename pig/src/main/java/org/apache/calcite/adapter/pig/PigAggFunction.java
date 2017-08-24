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
package org.apache.calcite.adapter.pig;

import org.apache.calcite.sql.SqlKind;

/**
 * Supported Pig aggregate functions and their Calcite counterparts. The enum's
 * name() is the same as the function's name in Pig Latin.
 */
public enum PigAggFunction {

  COUNT(SqlKind.COUNT, false), COUNT_STAR(SqlKind.COUNT, true);

  private final SqlKind calciteFunc;
  private final boolean star; // as in COUNT(*)

  PigAggFunction(SqlKind calciteFunc) {
    this(calciteFunc, false);
  }

  PigAggFunction(SqlKind calciteFunc, boolean star) {
    this.calciteFunc = calciteFunc;
    this.star = star;
  }

  public static PigAggFunction valueOf(SqlKind calciteFunc, boolean star) {
    for (PigAggFunction pigAggFunction : values()) {
      if (pigAggFunction.calciteFunc == calciteFunc && pigAggFunction.star == star) {
        return pigAggFunction;
      }
    }
    throw new IllegalArgumentException("Pig agg func for " + calciteFunc + " is not supported");
  }
}

// End PigAggFunction.java
