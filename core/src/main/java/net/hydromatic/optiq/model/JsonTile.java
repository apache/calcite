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
package net.hydromatic.optiq.model;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Materialized view within a {@link net.hydromatic.optiq.model.JsonLattice}.
 *
 * <p>A tile is defined in terms of its dimensionality (the grouping columns,
 * drawn from the lattice) and measures (aggregate functions applied to
 * lattice columns).
 *
 * @see JsonRoot Description of schema elements
 */
public class JsonTile {
  /** List of grouping columns that define this tile.
   *
   * <p>Elements are either strings (column names unique within the lattice)
   * or string lists (pairs of table alias and column name). */
  public final List dimensions = Lists.newArrayList();

  /** List of measures in this tile.
   *
   * <p>If not specified, uses {@link JsonLattice#defaultMeasures}.
   */
  public List<JsonMeasure> measures;

  public void accept(ModelHandler handler) {
    handler.visit(this);
  }
}

// End JsonTile.java
