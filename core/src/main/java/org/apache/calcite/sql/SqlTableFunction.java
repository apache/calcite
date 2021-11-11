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
package org.apache.calcite.sql;

import org.apache.calcite.sql.type.SqlReturnTypeInference;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A function that returns a table.
 */
public interface SqlTableFunction {
  /**
   * Returns the record type of the table yielded by this function when
   * applied to given arguments. Only literal arguments are passed,
   * non-literal are replaced with default values (null, 0, false, etc).
   *
   * @return strategy to infer the row type of a call to this function
   */
  SqlReturnTypeInference getRowTypeInference();

  /**
   * Returns the table parameter characteristics for <code>ordinal</code>th argument to this
   * table function.
   *
   * <p>Returns <code>Optional.empty</code> if the <code>ordinal</code>th argument is not table
   * parameter.
   */
  default @Nullable TableCharacteristic tableCharacteristic(int ordinal) {
    return null;
  }
}
