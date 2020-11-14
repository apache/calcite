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

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.util.Litmus;

/**
 * Contains internal operators.
 *
 * <p>These operators are always created directly, not by looking up a function
 * or operator by name or syntax, and therefore this class does not implement
 * interface {@link SqlOperatorTable}.
 */
public abstract class SqlInternalOperators {
  private SqlInternalOperators() {
  }

  /** An IN operator for Druid.
   *
   * <p>Unlike the regular
   * {@link SqlStdOperatorTable#IN} operator it may
   * be used in {@link RexCall}. It does not require that
   * its operands have consistent types. */
  public static final SqlInOperator DRUID_IN =
      new SqlInOperator(SqlKind.DRUID_IN);

  /** A NOT IN operator for Druid, analogous to {@link #DRUID_IN}. */
  public static final SqlInOperator DRUID_NOT_IN =
      new SqlInOperator(SqlKind.DRUID_NOT_IN);

  /** A BETWEEN operator for Druid, analogous to {@link #DRUID_IN}. */
  public static final SqlBetweenOperator DRUID_BETWEEN =
      new SqlBetweenOperator(SqlBetweenOperator.Flag.SYMMETRIC, false) {
        @Override public SqlKind getKind() {
          return SqlKind.DRUID_BETWEEN;
        }

        @Override public boolean validRexOperands(int count, Litmus litmus) {
          return litmus.succeed();
        }
      };
}
