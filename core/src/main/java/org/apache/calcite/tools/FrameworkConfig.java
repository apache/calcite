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
package org.apache.calcite.tools;

import org.apache.calcite.materialize.SqlStatisticProvider;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;

import com.google.common.collect.ImmutableList;

/**
 * Interface that describes how to configure planning sessions generated
 * using the Frameworks tools.
 *
 * @see Frameworks#newConfigBuilder()
 */
public interface FrameworkConfig {
  /**
   * The configuration of SQL parser.
   */
  SqlParser.Config getParserConfig();

  /**
   * The configuration of {@link SqlToRelConverter}.
   */
  SqlToRelConverter.Config getSqlToRelConverterConfig();

  /**
   * Returns the default schema that should be checked before looking at the
   * root schema.  Returns null to only consult the root schema.
   */
  SchemaPlus getDefaultSchema();

  /**
   * Returns the executor used to evaluate constant expressions.
   */
  RexExecutor getExecutor();

  /**
   * Returns a list of one or more programs used during the course of query
   * evaluation.
   *
   * <p>The common use case is when there is a single program
   * created using {@link Programs#of(RuleSet)}
   * and {@link org.apache.calcite.tools.Planner#transform}
   * will only be called once.
   *
   * <p>However, consumers may also create programs
   * not based on rule sets, register multiple programs,
   * and do multiple repetitions
   * of {@link Planner#transform} planning cycles using different indices.
   *
   * <p>The order of programs provided here determines the zero-based indices
   * of programs elsewhere in this class.
   */
  ImmutableList<Program> getPrograms();

  /**
   * Returns operator table that should be used to
   * resolve functions and operators during query validation.
   */
  SqlOperatorTable getOperatorTable();

  /**
   * Returns the cost factory that should be used when creating the planner.
   * If null, use the default cost factory for that planner.
   */
  RelOptCostFactory getCostFactory();

  /**
   * Returns a list of trait definitions.
   *
   * <p>If the list is not null, the planner first de-registers any
   * existing {@link RelTraitDef}s, then registers the {@code RelTraitDef}s in
   * this list.</p>
   *
   * <p>The order of {@code RelTraitDef}s in the list matters if the
   * planner is VolcanoPlanner. The planner calls {@link RelTraitDef#convert} in
   * the order of this list. The most important trait comes first in the list,
   * followed by the second most important one, etc.</p>
   */
  ImmutableList<RelTraitDef> getTraitDefs();

  /**
   * Returns the convertlet table that should be used when converting from SQL
   * to row expressions
   */
  SqlRexConvertletTable getConvertletTable();

  /**
   * Returns the PlannerContext that should be made available during planning by
   * calling {@link org.apache.calcite.plan.RelOptPlanner#getContext()}.
   */
  Context getContext();

  /**
   * Returns the type system.
   */
  RelDataTypeSystem getTypeSystem();

  /**
   * Returns whether the lattice suggester should try to widen a lattice when a
   * new query arrives that doesn't quite fit, as opposed to creating a new
   * lattice.
   */
  boolean isEvolveLattice();

  /**
   * Returns the source of statistics about tables and columns to be used
   * by the lattice suggester to deduce primary keys, foreign keys, and the
   * direction of relationships.
   */
  SqlStatisticProvider getStatisticProvider();

  /**
   * Returns a view expander.
   */
  RelOptTable.ViewExpander getViewExpander();
}

// End FrameworkConfig.java
