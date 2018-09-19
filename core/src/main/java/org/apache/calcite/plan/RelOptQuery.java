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
package org.apache.calcite.plan;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A <code>RelOptQuery</code> represents a set of
 * {@link RelNode relational expressions} which derive from the same
 * <code>select</code> statement.
 */
public class RelOptQuery {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * Prefix to the name of correlating variables.
   */
  public static final String CORREL_PREFIX = CorrelationId.CORREL_PREFIX;

  //~ Instance fields --------------------------------------------------------

  /**
   * Maps name of correlating variable (e.g. "$cor3") to the {@link RelNode}
   * which implements it.
   */
  final Map<String, RelNode> mapCorrelToRel;

  private final RelOptPlanner planner;
  final AtomicInteger nextCorrel;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a query.
   *
   * @param planner Planner
   */
  @Deprecated // to be removed before 2.0
  public RelOptQuery(RelOptPlanner planner) {
    this(planner, new AtomicInteger(0), new HashMap<>());
  }

  /** For use by RelOptCluster only. */
  RelOptQuery(RelOptPlanner planner, AtomicInteger nextCorrel,
      Map<String, RelNode> mapCorrelToRel) {
    this.planner = planner;
    this.nextCorrel = nextCorrel;
    this.mapCorrelToRel = mapCorrelToRel;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Converts a correlating variable name into an ordinal, unique within the
   * query.
   *
   * @param correlName Name of correlating variable
   * @return Correlating variable ordinal
   */
  @Deprecated // to be removed before 2.0
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
  @Deprecated // to be removed before 2.0
  public RelOptCluster createCluster(
      RelDataTypeFactory typeFactory,
      RexBuilder rexBuilder) {
    return new RelOptCluster(planner, typeFactory, rexBuilder, nextCorrel,
        mapCorrelToRel);
  }

  /**
   * Constructs a new name for a correlating variable. It is unique within the
   * whole query.
   *
   * @deprecated Use {@link RelOptCluster#createCorrel()}
   */
  @Deprecated // to be removed before 2.0
  public String createCorrel() {
    int n = nextCorrel.getAndIncrement();
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
