/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.tools;

import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.config.Lex;

import org.eigenbase.relopt.Context;
import org.eigenbase.relopt.RelOptCostFactory;
import org.eigenbase.relopt.RelTraitDef;
import org.eigenbase.sql.SqlOperatorTable;
import org.eigenbase.sql.parser.SqlParserImplFactory;
import org.eigenbase.sql2rel.SqlRexConvertletTable;

import com.google.common.collect.ImmutableList;

/**
 * Interface that describes how to configure planning sessions generated
 * using the Frameworks tools.
 */
public interface FrameworkConfig {

  /**
   * The type of lexing the SqlParser should do.  Controls case rules
   * and quoted identifier syntax.
   */
  Lex getLex();


  /**
   * Provides the Parser factory that creates the SqlParser used in parsing
   * queries.
   */
  SqlParserImplFactory getParserFactory();

  /**
   * Returns the default schema that should be checked before looking at the
   * root schema.  Return null to only consult the root schema.
   */
  SchemaPlus getDefaultSchema();


  /**
   * List of of one or more programs used during the course of
   *     query evaluation. The common use case is when there is a single program
   *     created using {@link Programs#of(RuleSet)}
   *     and {@link net.hydromatic.optiq.tools.Planner#transform}
   *     will only be called once. However, consumers may also create programs
   *     not based on rule sets, register multiple programs,
   *     and do multiple repetitions
   *     of {@link Planner#transform} planning cycles using different indices.
   *     The order of programs provided here determines the zero-based indices
   *     of programs elsewhere in this class.
   */
  ImmutableList<Program> getPrograms();


  /**
   * Return the instance of SqlOperatorTable that should be used to
   * resolve Optiq operators.
   */
  SqlOperatorTable getOperatorTable();



  /**
   * Return the cost factory that should be used when creating the planner.
   * If null, use the default cost factory for that planner.
   */
  RelOptCostFactory getCostFactory();


/**
   * <p>If {@code traitDefs} is non-null, the planner first de-registers any
   * existing {@link RelTraitDef}s, then registers the {@code RelTraitDef}s in
   * this list.</p>
   *
   * <p>The order of {@code RelTraitDef}s in {@code traitDefs} matters if the
   * planner is VolcanoPlanner. The planner calls {@link RelTraitDef#convert} in
   * the order of this list. The most important trait comes first in the list,
   * followed by the second most important one, etc.</p>
   *
   * @param lex The type of lexing the SqlParser should do.  Controls case rules
   *     and quoted identifier syntax.
   * @param parserFactory
   * @param operatorTable The instance of SqlOperatorTable that be should to
   *     resolve Optiq operators.
   * @param ruleSets
   *  @param  traitDefs The list of RelTraitDef that would be registered with
   *     planner, or null.
 * @return
 */
  ImmutableList<RelTraitDef> getTraitDefs();

  /**
   * Return the convertlet table that should be used when converting from Sql
   * to row expressions
   */
  SqlRexConvertletTable getConvertletTable();

  /**
   * Returns the PlannerContext that should be made available during planning by
   * calling {@link org.eigenbase.relopt.RelOptPlanner#getPlannerContext}
   */
  Context getContext();
}
