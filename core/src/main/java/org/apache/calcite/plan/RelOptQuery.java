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
package org.eigenbase.relopt;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;

/**
 * A <code>RelOptQuery</code> represents a set of {@link RelNode relational
 * expressions} which derive from the same <code>select</code> statement.
 */
public class RelOptQuery {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * Prefix to the name of correlating variables.
   */
  public static final String CORREL_PREFIX = "$cor";

  //~ Instance fields --------------------------------------------------------

  /**
   * Maps name of correlating variable (e.g. "$cor3") to the {@link RelNode}
   * which implements it.
   */
  final Map<String, RelNode> mapCorrelToRel = new HashMap<String, RelNode>();

  private final RelOptPlanner planner;
  private int nextCorrel = 0;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a query.
   *
   * @param planner Planner
   */
  public RelOptQuery(RelOptPlanner planner) {
    this.planner = planner;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Converts a correlating variable name into an ordinal, unqiue within the
   * query.
   *
   * @param correlName Name of correlating variable
   * @return Correlating variable ordinal
   */
  public static int getCorrelOrdinal(String correlName) {
    assert correlName.startsWith(CORREL_PREFIX);
    return Integer.parseInt(correlName.substring(CORREL_PREFIX.length()));
  }

  /**
   * Creates a cluster.
   *
   * @param typeFactory Type factory
   * @param rexBuilder  Expression builder
   * @return New cluster
   */
  public RelOptCluster createCluster(
      RelDataTypeFactory typeFactory,
      RexBuilder rexBuilder) {
    return new RelOptCluster(this, planner, typeFactory, rexBuilder);
  }

  /**
   * Constructs a new name for a correlating variable. It is unique within the
   * whole query.
   */
  public String createCorrel() {
    int n = nextCorrel++;
    return CORREL_PREFIX + n;
  }

  /**
   * Returns the relational expression which populates a correlating variable.
   */
  public RelNode lookupCorrel(String name) {
    return mapCorrelToRel.get(name);
  }

  /**
   * Maps a correlating variable to a {@link RelNode}.
   */
  public void mapCorrel(
      String name,
      RelNode rel) {
    mapCorrelToRel.put(name, rel);
  }
}

// End RelOptQuery.java
