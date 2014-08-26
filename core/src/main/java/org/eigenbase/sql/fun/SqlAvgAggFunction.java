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
package org.eigenbase.sql.fun;

import java.util.List;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;

import com.google.common.collect.ImmutableList;

/**
 * <code>Avg</code> is an aggregator which returns the average of the values
 * which go into it. It has precisely one argument of numeric type
 * (<code>int</code>, <code>long</code>, <code>float</code>, <code>
 * double</code>), and the result is the same type.
 */
public class SqlAvgAggFunction extends SqlAggFunction {
  //~ Instance fields --------------------------------------------------------

  private final RelDataType type;
  private final Subtype subtype;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SqlAvgAggFunction
   *
   * @param type    Data type
   * @param subtype Specific function, e.g. AVG or STDDEV_POP
   */
  public SqlAvgAggFunction(
      RelDataType type,
      Subtype subtype) {
    super(
        subtype.name(),
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE_IF_EMPTY,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC);
    this.type = type;
    this.subtype = subtype;
  }

  //~ Methods ----------------------------------------------------------------

  public List<RelDataType> getParameterTypes(RelDataTypeFactory typeFactory) {
    return ImmutableList.of(type);
  }

  public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    return type;
  }

  /**
   * Returns the specific function, e.g. AVG or STDDEV_POP.
   *
   * @return Subtype
   */
  public Subtype getSubtype() {
    return subtype;
  }

  public enum Subtype {
    AVG,
    STDDEV_POP,
    STDDEV_SAMP,
    VAR_POP,
    VAR_SAMP
  }
}

// End SqlAvgAggFunction.java
