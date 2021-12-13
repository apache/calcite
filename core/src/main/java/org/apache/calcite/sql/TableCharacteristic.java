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
 * An input table of table function is classified by three characteristics.
 * <ul>
 * <li>The first characteristic is semantics. The table has either row semantics or set semantics.
 *
 * <li>The second characteristic only applies to input table with set semantics. It specifies
 * whether the table function can generate a result row even if the input table is empty.
 *
 * <li>The third characteristic is whether the input table supports pass-through columns or not.
 * </ul>
 */
public class TableCharacteristic {

  /**
   * Input table has either row semantics or set semantics.
   */
  public final Semantics semantics;

  /**
   * If the value is true, meaning that the DBMS can prune virtual processors from the query plan
   * if the input table is empty.
   * If the value is false, meaning that the DBMS must actually instantiate a virtual processor
   * (or more than one virtual processor in the presence of other input tables).
   */
  public final boolean pruneIfEmpty;

  /**
   * If the value is true, for each input row, the PTF makes the entire input row available in
   * the output, qualified by a range variable associated with the input table. Otherwise the
   * value is false.
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
     * Row semantics means that the the result of the Window TableFunction is decided on a
     * row-by-row basis.
     * As an extreme example, the DBMS could atomize the input table into individual rows, and
     * send each single row to a different virtual processor.
     */
    ROW,

    /**
     * Set semantics means that the outcome of the Window TableFunction depends on how the data
     * is partitioned.
     * A partition may not be split across virtual processors, nor may a virtual processor handle
     * more than one partition.
     */
    SET
  }
}
