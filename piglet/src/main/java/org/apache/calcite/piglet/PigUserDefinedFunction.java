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
package org.apache.calcite.piglet;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

import org.apache.pig.FuncSpec;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Class to represent Pig UDF objects
 */
public class PigUserDefinedFunction extends SqlUserDefinedFunction {
  public final FuncSpec funcSpec;
  private PigUserDefinedFunction(SqlIdentifier opName,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker,
      List<RelDataType> paramTypes,
      Function function,
      FuncSpec funcSpec) {
    super(opName, returnTypeInference, operandTypeInference, operandTypeChecker, paramTypes,
        function,
        SqlFunctionCategory.USER_DEFINED_CONSTRUCTOR);
    this.funcSpec = funcSpec;
  }

  public PigUserDefinedFunction(String name,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeChecker operandTypeChecker,
      List<RelDataType> paramTypes,
      Function function,
      FuncSpec funcSpec) {
    this(new SqlIdentifier(ImmutableList.of(name), SqlParserPos.ZERO),
        returnTypeInference,
        null,
        operandTypeChecker,
        paramTypes,
        function,
        funcSpec);
  }

  public PigUserDefinedFunction(String name,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeChecker operandTypeChecker,
      List<RelDataType> paramTypes,
      Function function) {
    this(name, returnTypeInference, operandTypeChecker, paramTypes, function, null);
  }

}

// End PigUserDefinedFunction.java
