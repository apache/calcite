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
package org.eigenbase.sql;

import org.eigenbase.sql.type.*;

/**
 * Generic operator for nodes with internal syntax.
 */
public abstract class SqlInternalOperator extends SqlSpecialOperator {
  //~ Constructors -----------------------------------------------------------

  public SqlInternalOperator(
      String name,
      SqlKind kind) {
    super(name, kind, 2, true, null, null, null);
  }

  public SqlInternalOperator(
      String name,
      SqlKind kind,
      int prec) {
    super(name, kind, prec, true, null, null, null);
  }

  public SqlInternalOperator(
      String name,
      SqlKind kind,
      int prec,
      boolean isLeftAssoc,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker) {
    super(
        name,
        kind,
        prec,
        isLeftAssoc,
        returnTypeInference,
        operandTypeInference,
        operandTypeChecker);
  }

  //~ Methods ----------------------------------------------------------------

  public SqlSyntax getSyntax() {
    return SqlSyntax.INTERNAL;
  }

  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    throw new UnsupportedOperationException(
        "unparse must be implemented by SqlCall subclass");
  }
}

// End SqlInternalOperator.java
