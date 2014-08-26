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
package org.eigenbase.sql.validate;

import java.util.List;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.Util;

import net.hydromatic.optiq.Function;

/**
* User-defined scalar function.
 *
 * <p>Created by the validator, after resolving a function call to a function
 * defined in an Optiq schema.</p>
*/
public class SqlUserDefinedFunction extends SqlFunction {
  public final Function function;

  public SqlUserDefinedFunction(SqlIdentifier opName,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker,
      List<RelDataType> paramTypes,
      Function function) {
    super(Util.last(opName.names), opName, SqlKind.OTHER_FUNCTION,
        returnTypeInference, operandTypeInference, operandTypeChecker,
        paramTypes, SqlFunctionCategory.USER_DEFINED_FUNCTION);
    this.function = function;
  }

  /**
   * Returns function that implements given operator call.
   * @return function that implements given operator call
   */
  public Function getFunction() {
    return function;
  }
}

// End SqlUserDefinedFunction.java
