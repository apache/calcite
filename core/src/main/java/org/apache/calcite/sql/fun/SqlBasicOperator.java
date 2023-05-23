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

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * Concrete implementation of {@link SqlOperator}.
 *
 * <p>The class is final, and instances are immutable.
 *
 * <p>Instances are created only by {@link SqlBasicOperator#create} and are
 * "modified" by "wither" methods such as {@link #withPrecedence} to create a new
 * instance with one property changed. Since the class is final, you can modify
 * behavior only by providing strategy objects, not by overriding methods in a
 * subclass.
 */
public final class SqlBasicOperator extends SqlOperator {
  @Override public SqlSyntax getSyntax() {
    return SqlSyntax.SPECIAL;
  }

  /** Private constructor. Use {@link #create}. */
  private SqlBasicOperator(String name, int leftPrecedence, int rightPrecedence,
      SqlCallFactory callFactory) {
    super(name, SqlKind.OTHER, leftPrecedence, rightPrecedence,
        ReturnTypes.BOOLEAN, InferTypes.RETURN_TYPE, OperandTypes.ANY, callFactory);
  }

  public static SqlBasicOperator create(String name) {
    return new SqlBasicOperator(name, 0, 0,
        SqlCallFactories.SQL_BASIC_CALL_FACTORY);
  }

  public SqlBasicOperator withPrecedence(int prec, boolean leftAssoc) {
    return new SqlBasicOperator(getName(), leftPrec(prec, leftAssoc),
        rightPrec(prec, leftAssoc), getSqlCallFactory());
  }

  public SqlBasicOperator withCallFactory(SqlCallFactory sqlCallFactory) {
    return new SqlBasicOperator(getName(), getLeftPrec(),
        getRightPrec(), sqlCallFactory);
  }
}
