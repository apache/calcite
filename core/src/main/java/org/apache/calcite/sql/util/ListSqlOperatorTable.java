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
package org.apache.calcite.sql.util;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.validate.SqlNameMatcher;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of the {@link SqlOperatorTable} interface by using a list of
 * {@link SqlOperator operators}.
 */
public class ListSqlOperatorTable
    extends SqlOperatorTables.IndexedSqlOperatorTable
    implements SqlOperatorTable {

  //~ Instance fields --------------------------------------------------------

  private final List<SqlOperator> operatorList;

  //~ Constructors -----------------------------------------------------------

  /** Creates an empty, mutable ListSqlOperatorTable.
   *
   * @deprecated Use {@link SqlOperatorTables#of}, which creates an immutable
   * table. */
  @Deprecated // to be removed before 2.0
  public ListSqlOperatorTable() {
    this(new ArrayList<>(), false);
  }

  /** Creates a mutable ListSqlOperatorTable backed by a given list.
   *
   * @deprecated Use {@link SqlOperatorTables#of}, which creates an immutable
   * table. */
  @Deprecated // to be removed before 2.0
  public ListSqlOperatorTable(List<SqlOperator> operatorList) {
    this(operatorList, false);
  }

  // internal constructor
  ListSqlOperatorTable(List<SqlOperator> operatorList, boolean ignored) {
    super(operatorList);
    this.operatorList = operatorList;
  }

  //~ Methods ----------------------------------------------------------------

  /** Adds an operator to this table.
   *
   * @deprecated Use {@link SqlOperatorTables#of}, which creates an immutable
   * table. */
  @Deprecated // to be removed before 2.0
  public void add(SqlOperator op) {
    operatorList.add(op);
  }

  @Override public void lookupOperatorOverloads(SqlIdentifier opName,
      @Nullable SqlFunctionCategory category,
      SqlSyntax syntax,
      List<SqlOperator> operatorList,
      SqlNameMatcher nameMatcher) {
    if (!opName.isSimple()) {
      return;
    }
    final String simpleName = opName.getSimple();
    lookUpOperators(simpleName, nameMatcher.isCaseSensitive(), op -> {
      if (op.getSyntax().family != syntax) {
        return;
      }
      if (category != null
          && category != category(op)
          && !category.isUserDefinedNotSpecificFunction()) {
        return;
      }
      operatorList.add(op);
    });
  }

  protected static SqlFunctionCategory category(SqlOperator operator) {
    if (operator instanceof SqlFunction) {
      return ((SqlFunction) operator).getFunctionType();
    } else {
      return SqlFunctionCategory.SYSTEM;
    }
  }

  @Override public List<SqlOperator> getOperatorList() {
    return operatorList;
  }
}
