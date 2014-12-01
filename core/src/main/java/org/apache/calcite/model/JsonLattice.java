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

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Element that describes a star schema and provides a framework for defining,
 * recognizing, and recommending materialized views at various levels of
 * aggregation.
 *
 * @see JsonRoot Description of schema elements
 */
public class JsonLattice {
  public String name;

  /** SQL query that defines the lattice.
   *
   * <p>Must be a string or a list of strings (which are concatenated separated
   * by newlines).
   */
  public Object sql;

  /** Whether to create in-memory materialized aggregates on demand.
   *
   * <p>Default is true. */
  public boolean auto = true;

  /** Whether to use an algorithm to suggest aggregates.
   *
   * <p>Default is false. */
  public boolean algorithm = false;

  /** Maximum time to run the algorithm. Default is -1, meaning no timeout. */
  public long algorithmMaxMillis = -1;

  /** Estimated number of rows.
   *
   * <p>If null, Calcite will a query to find the real value. */
  public Double rowCountEstimate;

  /** List of materialized aggregates to create up front. */
  public final List<JsonTile> tiles = Lists.newArrayList();

  /** List of measures that a tile should have by default.
   *
   * <p>A tile can define its own measures, including measures not in this list.
   *
   * <p>The default list is just count. */
  public List<JsonMeasure> defaultMeasures;

  public void accept(ModelHandler handler) {
    handler.visit(this);
  }

  @Override public String toString() {
    return "JsonLattice(name=" + name + ", sql=" + getSql() + ")";
  }

  /** Returns the SQL query as a string, concatenating a list of lines if
   * necessary. */
  public String getSql() {
    return toString(sql);
  }

  /** Converts a string or a list of strings to a string. The list notation
   * is a convenient way of writing long multi-line strings in JSON. */
  static String toString(Object o) {
    return o == null ? null
        : o instanceof String ? (String) o
        : concatenate((List) o);
  }

  /** Converts a list of strings into a multi-line string. */
  private static String concatenate(List list) {
    final StringBuilder buf = new StringBuilder();
    for (Object o : list) {
      if (!(o instanceof String)) {
        throw new RuntimeException(
            "each element of a string list must be a string; found: " + o);
      }
      buf.append((String) o);
      buf.append("\n");
    }
    return buf.toString();
  }

  public void visitChildren(ModelHandler modelHandler) {
    for (JsonMeasure jsonMeasure : defaultMeasures) {
      jsonMeasure.accept(modelHandler);
    }
    for (JsonTile jsonTile : tiles) {
      jsonTile.accept(modelHandler);
    }
  }
}

// End JsonLattice.java
