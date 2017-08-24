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
package org.apache.calcite.schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Function that returns a table during execution time.
 *
 * <p>In contrast with {@code TableMacro}, the result of the table is not known
 * until execution.
 */
public interface TableFunction extends Function {
  /**
   * Returns the record type of the table yielded by this function when
   * applied to given arguments. Only literal arguments are passed,
   * non-literal are replaced with default values (null, 0, false, etc).
   *
   * @param typeFactory Type factory
   * @param arguments arguments of a function call (only literal arguments
   *                  are passed, nulls for non-literal ones)
   * @return row type of the table
   */
  RelDataType getRowType(RelDataTypeFactory typeFactory,
      List<Object> arguments);

  /**
   * Returns the row type of the table yielded by this function when
   * applied to given arguments. Only literal arguments are passed,
   * non-literal are replaced with default values (null, 0, false, etc).
   *
   * @param arguments arguments of a function call (only literal arguments
   *                  are passed, nulls for non-literal ones)
   * @return element type of the table (e.g. {@code Object[].class})
   */
  Type getElementType(List<Object> arguments);
}

// End TableFunction.java
