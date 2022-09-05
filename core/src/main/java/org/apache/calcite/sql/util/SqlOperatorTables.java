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

import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.runtime.SpatialTypeFunctions;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlSpatialTypeFunctions;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Utilities for {@link SqlOperatorTable}s.
 */
public class SqlOperatorTables {
  private SqlOperatorTables() {}

  @SuppressWarnings("FunctionalExpressionCanBeFolded")
  private static final Supplier<SqlOperatorTable> SPATIAL =
      Suppliers.memoize(SqlOperatorTables::createSpatial)::get;

  private static SqlOperatorTable createSpatial() {
    return CalciteCatalogReader.operatorTable(
        SpatialTypeFunctions.class.getName(),
        SqlSpatialTypeFunctions.class.getName());
  }

  /** Returns the Spatial operator table, creating it if necessary. */
  public static SqlOperatorTable spatialInstance() {
    return SPATIAL.get();
  }

  /** Creates a composite operator table. */
  public static SqlOperatorTable chain(Iterable<SqlOperatorTable> tables) {
    final List<SqlOperatorTable> list = new ArrayList<>();
    for (SqlOperatorTable table : tables) {
      addFlattened(list, table);
    }
    if (list.size() == 1) {
      return list.get(0);
    }
    return new ChainedSqlOperatorTable(ImmutableList.copyOf(list));
  }

  @SuppressWarnings("StatementWithEmptyBody")
  private static void addFlattened(List<SqlOperatorTable> list,
      SqlOperatorTable table) {
    if (table instanceof ChainedSqlOperatorTable) {
      ChainedSqlOperatorTable chainedTable = (ChainedSqlOperatorTable) table;
      for (SqlOperatorTable table2 : chainedTable.tableList) {
        addFlattened(list, table2);
      }
    } else if (table instanceof ImmutableListSqlOperatorTable
        && table.getOperatorList().isEmpty()) {
      // Table is empty and will remain empty; don't add it.
    } else {
      list.add(table);
    }
  }

  /** Creates a composite operator table from an array of tables. */
  public static SqlOperatorTable chain(SqlOperatorTable... tables) {
    return chain(ImmutableList.copyOf(tables));
  }

  /** Creates an operator table that contains an immutable list of operators. */
  public static SqlOperatorTable of(Iterable<? extends SqlOperator> list) {
    return new ImmutableListSqlOperatorTable(ImmutableList.copyOf(list));
  }

  /** Creates an operator table that contains the given operator or
   * operators. */
  public static SqlOperatorTable of(SqlOperator... operators) {
    return of(ImmutableList.copyOf(operators));
  }

  /** Subclass of {@link ListSqlOperatorTable} that is immutable.
   * Operators cannot be added or removed after creation. */
  private static class ImmutableListSqlOperatorTable
      extends ListSqlOperatorTable {
    ImmutableListSqlOperatorTable(ImmutableList<SqlOperator> operatorList) {
      super(operatorList, false);
    }
  }
}
