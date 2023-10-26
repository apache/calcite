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

import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSpatialTypeOperatorTable;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Utilities for {@link SqlOperatorTable}s.
 */
public class SqlOperatorTables {
  private SqlOperatorTables() {}

  private static final Supplier<SqlOperatorTable> SPATIAL =
      Suppliers.memoize(SqlSpatialTypeOperatorTable::new);

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
    ImmutableListSqlOperatorTable(Iterable<? extends SqlOperator> operators) {
      super(operators);
    }
  }

  /** Base class for implementations of {@link SqlOperatorTable} whose list of
   * operators rarely changes. */
  abstract static class IndexedSqlOperatorTable implements SqlOperatorTable {
    /** Contains all (name, operator) pairs. Effectively a sorted immutable
     * multimap.
     *
     * <p>There can be several operators with the same name (case-insensitive or
     * case-sensitive) and these operators will lie in a contiguous range which
     * we can find efficiently using binary search. */
    protected ImmutableMultimap<String, SqlOperator> operators;

    protected IndexedSqlOperatorTable(Iterable<? extends SqlOperator> list) {
      operators = buildIndex(list);
    }

    @Override public List<SqlOperator> getOperatorList() {
      return operators.values().asList();
    }

    protected void setOperators(Multimap<String, SqlOperator> operators) {
      this.operators = ImmutableMultimap.copyOf(operators);
    }

    /** Derives a value to be assigned to {@link #operators} from a given list
     * of operators. */
    protected static ImmutableMultimap<String, SqlOperator> buildIndex(
        Iterable<? extends SqlOperator> operators) {
      final ImmutableMultimap.Builder<String, SqlOperator> map =
          ImmutableMultimap.builder();
      operators.forEach(op ->
          map.put(op.getName().toUpperCase(Locale.ROOT), op));
      return map.build();
    }

    /** Looks up operators, optionally matching case-sensitively. */
    protected void lookUpOperators(String name,
        boolean caseSensitive, Consumer<SqlOperator> consumer) {
      final String upperName = name.toUpperCase(Locale.ROOT);
      if (caseSensitive) {
        operators.get(upperName)
            .forEach(operator -> {
              if (operator.getName().equals(name)) {
                consumer.accept(operator);
              }
            });
      } else {
        operators.get(upperName).forEach(consumer);
      }
    }
  }
}
