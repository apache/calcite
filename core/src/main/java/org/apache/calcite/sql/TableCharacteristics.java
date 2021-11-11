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
 * Input table of table function are classified by three characteristics.
 * <ul>
 * <li>Input tables have either row semantics or set semantics.
 *
 * <li>The second characteristic only applies to input tables with set semantics. It specifies
 * whether the table function can generate a result row even if the input table is empty.
 *
 * <li>The third characteristic is whether the input table supports pass-through columns or not.
 * </ul>
 */
public class TableCharacteristics {

  private final Semantics semantics;

  private final PrunabilityWhenEmpty prunability;

  private final ColumnsPassThrough passThrough;

  private TableCharacteristics(
      Semantics semantics,
      PrunabilityWhenEmpty prunability,
      ColumnsPassThrough passThrough) {
    this.semantics = semantics;
    this.prunability = prunability;
    this.passThrough = passThrough;
  }

  public Semantics getSemantics() {
    return semantics;
  }

  public PrunabilityWhenEmpty getPrunability() {
    return prunability;
  }

  public ColumnsPassThrough getPassThrough() {
    return passThrough;
  }

  @Override public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableCharacteristics that = (TableCharacteristics) o;
    return semantics == that.semantics
        && prunability == that.prunability
        && passThrough == that.passThrough;
  }

  @Override public int hashCode() {
    return Objects.hash(semantics, prunability, passThrough);
  }

  @Override public String toString() {
    return "TableParamCharacteristics{"
        + "semantics = " + semantics
        + ", prunability = " + prunability
        + ", passThrough = " + passThrough
        + '}';
  }

  public static TableCharacteristics withRowSemantic(ColumnsPassThrough passThrough) {
    // Tables with row semantics are always effectively prune when empty.
    return new TableCharacteristics(Semantics.ROW, PrunabilityWhenEmpty.PRUNE, passThrough);
  }

  public static TableCharacteristics withSetSemantic(
      PrunabilityWhenEmpty prunability,
      ColumnsPassThrough passThrough) {
    return new TableCharacteristics(Semantics.SET, prunability, passThrough);
  }

  /**
   * Input tables have either row semantics or set semantics.
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

  /**
   * Prunability applies only to input tables with set semantics, is whether the PTF can generate
   * a result row even if the input table is empty.
   */
  public enum PrunabilityWhenEmpty {

    /**
     * If the PTF can generate a result row on empty input, the table is said to be “keep when
     * empty”, meaning that the DBMS must actually instantiate a virtual processor (or more than
     * one virtual processor in the presence of other input tables).
     */
    KEEP,

    /**
     * If the PTF can would not generate any result row on empty input, the table is said to be
     * "prune when empty", meaning that the DBMS can prune virtual processors from the query plan
     * if the input table is empty.
     */
    PRUNE
  }

  /**
   * ColumnsPassThrough is whether the input table supports pass-through columns or not.
   */
  public enum ColumnsPassThrough {

    /**
     * PASS THROUGH means that, for each input row, the PTF makes the entire input row available in
     * the output, qualified by a range variable associated with the input table.
     */
    PASS_THROUGH,

    /**
     * The alternative is called "NO PASS THROUGH".
     */
    NO_PASS_THROUGH
  }
}
