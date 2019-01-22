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
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.Optionality;

import com.google.common.base.Preconditions;

/**
 * <code>Covar</code> is an aggregator which returns the Covariance of the
 * values which go into it. It has precisely two arguments of numeric type
 * (<code>int</code>, <code>long</code>, <code>float</code>, <code>
 * double</code>), and the result is the same type.
 */
public class SqlCovarAggFunction extends SqlAggFunction {
  //~ Instance fields --------------------------------------------------------

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SqlCovarAggFunction.
   */
  public SqlCovarAggFunction(SqlKind kind) {
    super(kind.name(),
        null,
        kind,
        kind == SqlKind.REGR_COUNT ? ReturnTypes.BIGINT : ReturnTypes.COVAR_REGR_FUNCTION,
        null,
        OperandTypes.NUMERIC_NUMERIC,
        SqlFunctionCategory.NUMERIC,
        false,
        false,
        Optionality.FORBIDDEN);
    Preconditions.checkArgument(SqlKind.COVAR_AVG_AGG_FUNCTIONS.contains(kind),
        "unsupported sql kind: " + kind);
  }

  @Deprecated // to be removed before 2.0
  public SqlCovarAggFunction(RelDataType type, Subtype subtype) {
    this(SqlKind.valueOf(subtype.name()));
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns the specific function, e.g. COVAR_POP or COVAR_SAMP.
   *
   * @return Subtype
   */
  @Deprecated // to be removed before 2.0
  public Subtype getSubtype() {
    return Subtype.valueOf(kind.name());
  }

  /**
   * Enum for defining specific types.
   */
  @Deprecated // to be removed before 2.0
  public enum Subtype {
    COVAR_POP,
    COVAR_SAMP,
    REGR_SXX,
    REGR_SYY
  }
}

// End SqlCovarAggFunction.java
