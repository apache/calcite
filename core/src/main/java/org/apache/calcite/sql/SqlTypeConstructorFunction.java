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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.ExplicitOperandTypeChecker;

/**
 * Type Constructor function.
 *
 * <p>Created by the parser, then it is rewritten to proper SqlFunction by
 * the validator to a function defined in a Calcite schema.</p>
 */
public class SqlTypeConstructorFunction extends SqlFunction {

  private RelDataType type;

  /**
   * Creates a constructor function for types.
   *
   * @param identifier possibly qualified identifier for function
   * @param type type of data
   */
  public SqlTypeConstructorFunction(SqlIdentifier identifier,
      RelDataType type) {
    super(identifier,
        null,
        null,
        new ExplicitOperandTypeChecker(type),
        null,
        SqlFunctionCategory.SYSTEM);
    this.type = type;
  }

  @Override public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    return type;
  }
}
