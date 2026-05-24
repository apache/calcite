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
package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlConstantValueAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.Optionality;

import org.checkerframework.checker.nullness.qual.Nullable;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * <code>Avg</code> is an aggregator which returns the average of the values
 * which go into it. It has precisely one argument of numeric type
 * (<code>int</code>, <code>long</code>, <code>float</code>, <code>
 * double</code>), and the result is the same type.
 *
 * <p>For statistical functions (STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP),
 * this function implements {@link SqlConstantValueAggFunction} to support
 * optimization when applied to constant GROUP BY keys.
 */
public class SqlAvgAggFunction extends SqlAggFunction
    implements SqlConstantValueAggFunction {

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SqlAvgAggFunction.
   */
  public SqlAvgAggFunction(SqlKind kind) {
    this(kind.name(), kind);
  }

  SqlAvgAggFunction(String name, SqlKind kind) {
    super(name,
        null,
        kind,
        ReturnTypes.AVG_AGG_FUNCTION,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC,
        false,
        false,
        Optionality.FORBIDDEN);
    checkArgument(SqlKind.AVG_AGG_FUNCTIONS.contains(kind),
        "unsupported sql kind");
  }

  @Deprecated // to be removed before 2.0
  public SqlAvgAggFunction(
      RelDataType type,
      Subtype subtype) {
    this(SqlKind.valueOf(subtype.name()));
  }

    //~ Methods ----------------------------------------------------------------

  /**
   * Returns the specific function, e.g. AVG or STDDEV_POP.
   *
   * @return Subtype
   */
  @Deprecated // to be removed before 2.0
  public Subtype getSubtype() {
    return Subtype.valueOf(kind.name());
  }

  /** Sub-type of aggregate function. */
  @Deprecated // to be removed before 2.0
  public enum Subtype {
    AVG,
    STDDEV_POP,
    STDDEV_SAMP,
    VAR_POP,
    VAR_SAMP
  }

  @Override public @Nullable RexNode getConstantResult(RexBuilder rexBuilder,
      RelDataType returnType) {
    // Only statistical functions (variance and standard deviation) return 0 for constant values.
    // AVG and other functions should not be optimized through this interface.
    switch (kind) {
    case STDDEV_POP:
    case STDDEV_SAMP:
    case VAR_POP:
    case VAR_SAMP:
      return rexBuilder.makeLiteral(0, returnType, true);
    default:
      return null;
    }
  }
}
