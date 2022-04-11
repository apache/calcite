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

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;

/**
 * An operator describing a GROUP BY.
 *
 * <p>Operands are:</p>
 *
 * <ul>
 * <li>0: distinct ({@link SqlLiteral})</li>
 * <li>1: groupingElementList ({@link SqlNodeList})</li>
 * </ul>
 */
public class SqlGroupByOperator extends SqlOperator {
  public static final SqlGroupByOperator INSTANCE =
      new SqlGroupByOperator();

  //~ Constructors -----------------------------------------------------------

  private SqlGroupByOperator() {
    super("GROUP BY", SqlKind.GROUP_BY, 0, true, null, null, null);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlSyntax getSyntax() {
    return SqlSyntax.POSTFIX;
  }

  @Override public SqlCall createCall(
      @Nullable SqlLiteral qualifier,
      SqlParserPos pos,
      @Nullable SqlNode... operands) {
    return new SqlGroupBy(pos,
        qualifier,
        new SqlNodeList(new ArrayList<>(ImmutableNullableList.copyOf(operands)), pos));
  }
}
