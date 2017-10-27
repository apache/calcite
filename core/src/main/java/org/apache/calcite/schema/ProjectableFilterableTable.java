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
package org.apache.calcite.schema;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Table that can be scanned, optionally applying supplied filter expressions,
 * and projecting a given list of columns,
 * without creating an intermediate relational expression.
 *
 * <p>If you wish to write a table that can apply projects but not filters,
 * simply decline all filters.</p>
 *
 * @see ScannableTable
 * @see FilterableTable
 */
public interface ProjectableFilterableTable extends Table {
  /** Returns an enumerable over the rows in this Table.
   *
   * <p>Each row is represented as an array of its column values.
   *
   * <p>The list of filters is mutable.
   * If the table can implement a particular filter, it should remove that
   * filter from the list.
   * If it cannot implement a filter, it should leave it in the list.
   * Any filters remaining will be implemented by the consuming Calcite
   * operator.
   *
   * <p>The projects are zero-based.</p>
   *
   * @param root Execution context
   * @param filters Mutable list of filters. The method should keep in the
   *                list any filters that it cannot apply.
   * @param projects List of projects. Each is the 0-based ordinal of the column
   *                 to project.
   * @return Enumerable over all rows that match the accepted filters, returning
   * for each row an array of column values, one value for each ordinal in
   * {@code projects}.
   */
  Enumerable<Object[]> scan(DataContext root, List<RexNode> filters,
      int[] projects);
}

// End ProjectableFilterableTable.java
