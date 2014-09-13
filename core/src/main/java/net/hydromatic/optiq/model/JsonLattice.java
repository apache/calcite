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
 * Element that describes a star schema and provides a framework for defining,
 * recognizing, and recommending materialized views at various levels of
 * aggregation.
 *
 * @see JsonRoot Description of schema elements
 */
public class JsonLattice {
  public String name;
  public String sql;

  /** Whether to create in-memory materialized aggregates on demand.
   *
   * <p>Default is true. */
  public boolean auto = true;

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
    return "JsonLattice(name=" + name + ", sql=" + sql + ")";
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
