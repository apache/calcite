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
package org.apache.calcite.model;

/**
 * An aggregate function applied to a column (or columns) of a lattice.
 *
 * <p>Occurs in a {@link org.apache.calcite.model.JsonTile},
 * and there is a default list in
 * {@link org.apache.calcite.model.JsonLattice}.
 *
 * @see JsonRoot Description of schema elements
 */
public class JsonMeasure {
  /** The name of an aggregate function.
   *
   * <p>Required. Usually {@code count}, {@code sum},
   * {@code min}, {@code max}.
   */
  public String agg;

  /** Arguments to the measure.
   *
   * <p>Valid values are:
   * <ul>
   *   <li>Not specified: no arguments</li>
   *   <li>null: no arguments</li>
   *   <li>Empty list: no arguments</li>
   *   <li>String: single argument, the name of a lattice column</li>
   *   <li>List: multiple arguments, each a column name</li>
   * </ul>
   *
   * <p>Unlike lattice dimensions, measures can not be specified in qualified
   * format, {@code ["table", "column"]}. When you define a lattice, make sure
   * that each column you intend to use as a measure has a unique name within
   * the lattice (using "{@code AS alias}" if necessary).
   */
  public Object args;

  public void accept(ModelHandler modelHandler) {
    modelHandler.visit(this);
  }
}

// End JsonMeasure.java
