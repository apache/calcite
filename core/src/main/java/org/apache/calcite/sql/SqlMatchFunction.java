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
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import java.util.List;

/** Base class for all functions used in MATCH_RECOGNIZE. */
public class SqlMatchFunction extends SqlFunction {

  public SqlMatchFunction(String name, SqlKind kind, SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker, SqlFunctionCategory category) {
    super(name, kind, returnTypeInference, operandTypeInference, operandTypeChecker, category);
  }

  public SqlMatchFunction(SqlIdentifier sqlIdentifier, SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker, List<RelDataType> paramTypes,
      SqlFunctionCategory funcType) {
    super(sqlIdentifier, returnTypeInference, operandTypeInference, operandTypeChecker, paramTypes,
        funcType);
  }

  protected SqlMatchFunction(String name, SqlIdentifier sqlIdentifier, SqlKind kind,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker, List<RelDataType> paramTypes,
      SqlFunctionCategory category) {
    super(name, sqlIdentifier, kind, returnTypeInference, operandTypeInference, operandTypeChecker,
        paramTypes, category);
  }


}

// End SqlMatchFunction.java
