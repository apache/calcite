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

import java.util.*;

import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;

/**
 * Generic operator for nodes with special syntax.
 */
public class SqlSpecialOperator extends SqlOperator {
  //~ Constructors -----------------------------------------------------------

  public SqlSpecialOperator(
      String name,
      SqlKind kind) {
    super(name, kind, 2, true, null, null, null);
  }

  public SqlSpecialOperator(
      String name,
      SqlKind kind,
      int prec) {
    super(name, kind, prec, true, null, null, null);
  }

  public SqlSpecialOperator(
      String name,
      SqlKind kind,
      int prec,
      boolean leftAssoc,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker) {
    super(
        name,
        kind,
        prec,
        leftAssoc,
        returnTypeInference,
        operandTypeInference,
        operandTypeChecker);
  }

  //~ Methods ----------------------------------------------------------------

  public SqlSyntax getSyntax() {
    return SqlSyntax.SPECIAL;
  }

  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    throw new UnsupportedOperationException(
        "unparse must be implemented by SqlCall subclass");
  }

  /**
   * Reduces a list of operators and arguments according to the rules of
   * precedence and associativity. Returns the ordinal of the node which
   * replaced the expression.
   *
   * <p>The default implementation throws {@link
   * UnsupportedOperationException}.
   *
   * @param ordinal indicating the ordinal of the current operator in the list
   *                on which a possible reduction can be made
   * @param list    List of alternating {@link
   *                org.eigenbase.sql.parser.SqlParserUtil.ToTreeListItem} and {@link
   *                SqlNode}
   * @return ordinal of the node which replaced the expression
   */
  public int reduceExpr(
      int ordinal,
      List<Object> list) {
    throw Util.needToImplement(this);
  }
}

// End SqlSpecialOperator.java
