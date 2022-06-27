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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * A table-valued input parameter of a table function is classified by three
 * characteristics.
 * <ul>
 * <li>The first characteristic is semantics. The table has either
 * row semantics or set semantics.
 * <p>Row semantics means that the the result of the PTF is decided on a
 * row-by-row basis.
 *
 * Example of PTF with row semantics input table parameter:
 * We often need to read a CSV file, generally, the first line of the file
 * contains a list of column names, and subsequent lines of the file contain
 * data. The data in general can be treated as a large VARCHAR.
 * However, some of the fields may be numeric or datetime.
 * A PTF named CSVreader is designed to read a file of comma-separated values
 * and interpret this file as a table.
 *
 * CSVreader function has three parameters:
 * 1. The first parameter, File, is the name of a file on the query author's
 * system. This file must contain the comma-separated values that are to be
 * converted to a table. The first line of the file contains the names of
 * the resulting columns. Succeeding lines contain the data. Each line after
 * the first will result in one row of output, with column names as determined
 * by the first line of the input.
 * 2. Floats is a PTF descriptor area, which should provide a list of the
 * column names that are to be interpreted numerically.
 * These columns will be output with the data type FLOAT.
 * 3. Dates is a PTF descriptor area, which provides a list of the column
 * names that are to be interpreted as datetimes.
 * These columns will be output with the data type DATE.
 *
 * How to use this PTF in query?
 *
 * For a csv file which contents are:
 * docno,name,due_date,principle,interest
 * 123,Mary,01/01/2014,234.56,345.67
 * 234,Edgar,01/01/2014,654.32,543.21
 *
 * the query author may write a query such as the following:
 *
 * SELECT *
 * FROM TABLE (
 *   CSVreader (
 *   'abc.csv',
 *   DESCRIPTOR ("principle", "interest")
 *   DESCRIPTOR ("due_date")))
 *
 * The result will be:
 * +---------+--------+--------------+-------------+------------+
 * |  docno  |  name  |  due_date    |  principle  |  interest  |
 * +---------+--------+--------------+-------------+------------+
 * |  123    |  Mary  |  01/01/2014  |  234.56     |  345.67    |
 * +---------+--------+--------------+-------------+------------+
 * |  234    |  Edgar |  01/01/2014  |  654.32     |  543.21    |
 * +---------+--------+--------------+-------------+------------+
 *
 * <p>Set semantics means that the outcome of the function depends on how
 * the data is partitioned. Set semantics is useful to implement user-defined
 * analytics like aggregation or window functions.
 * They operate on an entire table or a logical partition of it.
 *
 * Example of PTF with set semantics input table parameter:
 * TopN takes an input table that has been sorted on a numeric column.
 * It copies the first n rows through to the output table. Any additional
 * rows are summarized in a single output row in which the sort column has
 * been summed and all other columns are null.
 *
 * TopN has two parameters:
 * 1. The first parameter, Input, is the input table. This table has set semantics, meaning that the
 * result depends on the set of data (since the last row is a summary row). In addition, the
 * table is marked as PRUNE WHEN EMPTY, meaning that the result is necessarily empty if the input
 * is empty. The query author must order this input table on a single numeric column (syntax below).
 * 2. The second parameter, Howmany, specifies how many input rows that the user wants to be copied
 * into the output table; all rows after this will contribute to the final summary row in the
 * output.
 *
 * How to use this PTF in query?
 *
 * Original records of table orders are:
 * +---------+----------+-----------+
 * | region  | product  |  sales    |
 * +---------+----------+-----------+
 * |  East   |    A     |  1234.56  |
 * +---------+----------+-----------+
 * |  East   |    B     |   987.65  |
 * +---------+----------+-----------+
 * |  East   |    C     |   876.54  |
 * +---------+----------+-----------+
 * |  East   |    D     |   765.43  |
 * +---------+----------+-----------+
 * |  East   |    E     |   654.32  |
 * +---------+----------+-----------+
 * |  West   |    E     |  2345.67  |
 * +---------+----------+-----------+
 * |  West   |    D     |  2001.33  |
 * +---------+----------+-----------+
 * |  West   |    C     |  1357.99  |
 * +---------+----------+-----------+
 * |  West   |    B     |   975.35  |
 * +---------+----------+-----------+
 * |  West   |    A     |   864,22  |
 * +---------+----------+-----------+
 *
 * the query author may write a query such as the following:
 *
 * SELECT *
 * FROM TABLE(
 *   Topn(
 *     TABLE orders PARTITION BY region ORDER BY sales desc,
 *     3))
 *
 * The result will be:
 * +---------+----------+-----------+
 * | region  | product  |  sales    |
 * +---------+----------+-----------+
 * |  East   |    A     |  1234.56  |
 * +---------+----------+-----------+
 * |  East   |    B     |   987.65  |
 * +---------+----------+-----------+
 * |  East   |    C     |   876.54  |
 * +---------+----------+-----------+
 * |  West   |    E     |  2345.67  |
 * +---------+----------+-----------+
 * |  West   |    D     |  2001.33  |
 * +---------+----------+-----------+
 * |  West   |    C     |  1357.99  |
 * +---------+----------+-----------+
 *
 * <li>The second characteristic only applies to input table with
 * set semantics. It specifies whether the table function can generate
 * a result row even if the input table is empty.
 *
 * <li>The third characteristic is whether the input table supports
 * pass-through columns or not.
 * </ul>
 */
public class TableCharacteristic {

  /**
   * Input table has either row semantics or set semantics.
   */
  public final Semantics semantics;

  /**
   * If the value is true, meaning that the DBMS can prune virtual processors
   * from the query plan if the input table is empty.
   * If the value is false, meaning that the DBMS must actually instantiate
   * a virtual processor (or more than one virtual processor in the presence
   * of other input tables).
   */
  public final boolean pruneIfEmpty;

  /**
   * If the value is true, for each input row, the PTF makes the entire input
   * row available in the output, qualified by a range variable associated
   * with the input table. Otherwise the value is false.
   */
  public final boolean passColumnsThrough;

  private TableCharacteristic(
      Semantics semantics,
      boolean pruneIfEmpty,
      boolean passColumnsThrough) {
    this.semantics = semantics;
    this.pruneIfEmpty = pruneIfEmpty;
    this.passColumnsThrough = passColumnsThrough;
  }

  public static TableCharacteristic withRowSemantic(boolean columnsPassThrough) {
    // Tables with row semantics are always effectively prune when empty.
    return new TableCharacteristic(Semantics.ROW, true, columnsPassThrough);
  }

  public static TableCharacteristic withSetSemantic(
      boolean pruneIfEmpty,
      boolean columnsPassThrough) {
    return new TableCharacteristic(Semantics.SET, pruneIfEmpty, columnsPassThrough);
  }

  @Override public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableCharacteristic that = (TableCharacteristic) o;
    return pruneIfEmpty == that.pruneIfEmpty
        && passColumnsThrough == that.passColumnsThrough
        && semantics == that.semantics;
  }

  @Override public int hashCode() {
    return Objects.hash(semantics, pruneIfEmpty, passColumnsThrough);
  }

  @Override public String toString() {
    return "TableCharacteristic{"
        + "semantics=" + semantics
        + ", pruneIfEmpty=" + pruneIfEmpty
        + ", passColumnsThrough=" + passColumnsThrough
        + '}';
  }

  /**
   * Input table has either row semantics or set semantics.
   */
  public enum Semantics {
    /**
     * Row semantics means that the the result of the Window TableFunction
     * is decided on a row-by-row basis.
     * As an extreme example, the DBMS could atomize the input table into
     * individual rows, and send each single row to a different virtual
     * processor.
     */
    ROW,

    /**
     * Set semantics means that the outcome of the Window TableFunction
     * depends on how the data is partitioned.
     * A partition may not be split across virtual processors, nor may a
     * virtual processor handle more than one partition.
     */
    SET
  }
}
