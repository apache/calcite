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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

import com.google.common.collect.ImmutableList;

import java.util.List;

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
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE_IF_EMPTY,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.NUMERIC,
        false,
        false);
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

  /** Sub-type of aggregate function. */
  public enum Subtype {
    AVG,
    STDDEV_POP,
    STDDEV_SAMP,
    VAR_POP,
    VAR_SAMP
  }
}

// End SqlAvgAggFunction.java
