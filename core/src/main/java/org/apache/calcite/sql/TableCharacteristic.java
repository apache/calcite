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

 * <p>The first characteristic is semantics. The table has either
 * row semantics or set semantics.
 *
 * <p>Row semantics means that the result of the table function is decided
 * on a row-by-row basis.
 *
 * <p>Example of a table function with row semantics input table parameter:
 * We often need to read a CSV file, generally, the first line of the file
 * contains a list of column names, and subsequent lines of the file contain
 * data. The data in general can be treated as a large VARCHAR.
 * However, some of the fields may be numeric or datetime.
 *
 * <p>A table function named CSVReader is designed to read a file of
 * comma-separated values and interpret this file as a table.
 * The function has three parameters:
 * <ul>
 * <li>The first parameter, File, is the name of a file on the query author's
 * system. This file must contain the comma-separated values that are to be
 * converted to a table. The first line of the file contains the names of
 * the resulting columns. Succeeding lines contain the data. Each line after
 * the first will result in one row of output, with column names as determined
 * by the first line of the input.
 * <li>Floats is a descriptor area, which should provide a list of the
 * column names that are to be interpreted numerically.
 * These columns will be output with the data type FLOAT.
 * <li>Dates is a descriptor area, which provides a list of the column
 * names that are to be interpreted as datetimes.
 * These columns will be output with the data type DATE.
 * </ul>
 *
 * <p>How to use this table function in query?
 *
 * <p>For a csv file which contents are:
 * <blockquote><pre>
 * docno,name,due_date,principle,interest
 * 123,Mary,01/01/2014,234.56,345.67
 * 234,Edgar,01/01/2014,654.32,543.21
 * </pre></blockquote>
 *
 * <p>The query author may write a query such as the following:
 * <blockquote><pre>{@code
 * SELECT *
 * FROM TABLE (
 *   CSVreader (
 *   'abc.csv',
 *   DESCRIPTOR ("principle", "interest")
 *   DESCRIPTOR ("due_date")))
 * }</pre></blockquote>
 *
 * <table>
 * <caption>Results of the query</caption>
 * <tr>
 * <th>docno</th>
 * <th>name</th>
 * <th>due_date</th>
 * <th>principle</th>
 * <th>interest</th>
 * </tr>
 * <tr>
 * <th>123</th>
 * <th>Mary</th>
 * <th>01/01/2014</th>
 * <th>234.56</th>
 * <th>345.67</th>
 * </tr>
 * <tr>
 * <th>234</th>
 * <th>Edgar</th>
 * <th>01/01/2014</th>
 * <th>654.32</th>
 * <th>543.21</th>
 * </tr>
 * </table>
 *
 * <p>Set semantics means that the outcome of the function depends on how
 * the data is partitioned. Set semantics is useful to implement user-defined
 * analytics like aggregation or window functions.
 * They operate on an entire table or a logical partition of it.
 *
 * <p>Example of a table function with set semantics input table parameter:
 * TopN takes an input table that has been sorted on a numeric column.
 * It copies the first n rows through to the output table. Any additional
 * rows are summarized in a single output row in which the sort column has
 * been summed and all other columns are null.
 * TopN function has two parameters:
 * <ul>
 * <li>The first parameter, Input, is the input table. This table has set
 * semantics, meaning that the result depends on the set of data (since the
 * last row is a summary row). In addition, the table is marked as PRUNE WHEN
 * EMPTY, meaning that the result is necessarily empty if the input is empty.
 * The query author must order this input table on a single numeric column
 * (syntax below).
 * <li>The second parameter, Howmany, specifies how many input rows that the
 * user wants to be copied into the output table; all rows after this will
 * contribute to the final summary row in the output.
 * </ul>
 *
 * <p>How to use this table function in query?
 * <table>
 * <caption>Original records of table orders</caption>
 * <tr>
 * <th>region</th>
 * <th>product</th>
 * <th>sales</th>
 * </tr>
 * <tr>
 * <th>East</th>
 * <th>A</th>
 * <th>1234.56</th>
 * </tr>
 * <tr>
 * <th>East</th>
 * <th>B</th>
 * <th>987.65</th>
 * </tr>
 * <tr>
 * <th>East</th>
 * <th>C</th>
 * <th>876.54</th>
 * </tr>
 * <tr>
 * <th>East</th>
 * <th>D</th>
 * <th>765.43</th>
 * </tr>
 * <tr>
 * <th>East</th>
 * <th>E</th>
 * <th>654.32</th>
 * </tr>
 * <tr>
 * <th>West</th>
 * <th>E</th>
 * <th>2345.67</th>
 * </tr>
 * <tr>
 * <th>West</th>
 * <th>D</th>
 * <th>2001.33</th>
 * </tr>
 * <tr>
 * <th>West</th>
 * <th>C</th>
 * <th>1357.99</th>
 * </tr>
 * <tr>
 * <th>West</th>
 * <th>B</th>
 * <th>975.35</th>
 * </tr>
 * <tr>
 * <th>West</th>
 * <th>A</th>
 * <th>864,22</th>
 * </tr>
 * </table>
 *
 * <p>The query author may write a query such as the following:
 * <blockquote><pre>{@code
 * SELECT *
 * FROM TABLE(
 *   Topn(
 *     TABLE orders PARTITION BY region ORDER BY sales desc,
 *     3))
 * }</pre></blockquote>
 *
 * <p>The result will be:
 * <table>
 * <caption>Original records of table orders</caption>
 * <tr>
 * <th>region</th>
 * <th>product</th>
 * <th>sales</th>
 * </tr>
 * <tr>
 * <th>East</th>
 * <th>A</th>
 * <th>1234.56</th>
 * </tr>
 * <tr>
 * <th>East</th>
 * <th>B</th>
 * <th>987.65</th>
 * </tr>
 * <tr>
 * <th>East</th>
 * <th>C</th>
 * <th>876.54</th>
 * </tr>
 * <tr>
 * <th>West</th>
 * <th>E</th>
 * <th>2345.67</th>
 * </tr>
 * <tr>
 * <th>West</th>
 * <th>D</th>
 * <th>2001.33</th>
 * </tr>
 * <tr>
 * <th>West</th>
 * <th>C</th>
 * <th>1357.99</th>
 * </tr>
 * </table>
 *
 * <p>The second characteristic of input table parameter only applies to input
 * table with set semantics. It specifies whether the table function can
 * generate a result row even if the input table is empty.
 *
 * <p>The third characteristic is whether the input table supports
 * pass-through columns or not.
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
   * If the value is true, for each input row, the table function makes the
   * entire input row available in the output, qualified by a range variable
   * associated with the input table. Otherwise the value is false.
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

  /** Creates a builder. */
  public static TableCharacteristic.Builder builder(Semantics semantics) {
    return new Builder(semantics);
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
     * Row semantics means that the result of the Window TableFunction
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

  /**
   * Builder for {@link TableCharacteristic}.
   */
  public static class Builder {
    private final Semantics semantics;
    private boolean pruneIfEmpty = false;
    private boolean passColumnsThrough = false;

    /** Creates the semantics. */
    private Builder(Semantics semantics) {
      if (semantics == Semantics.ROW) {
        // Tables with row semantics are always effectively prune when empty.
        this.pruneIfEmpty = true;
      }
      this.semantics = semantics;
    }

    /** DBMS could prune virtual processors if the input table is empty. */
    public Builder pruneIfEmpty() {
      this.pruneIfEmpty = true;
      return this;
    }

    /** Input table supports pass-through columns. */
    public Builder passColumnsThrough() {
      this.passColumnsThrough = true;
      return this;
    }

    public TableCharacteristic build() {
      return new TableCharacteristic(semantics, pruneIfEmpty, passColumnsThrough);
    }
  }
}
