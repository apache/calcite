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
 * An implementation of {@link FrameworkConfig} that uses standard Optiq
 * classes to provide basic planner functionality.
 */
class StdFrameworkConfig implements FrameworkConfig {

  private final Context context;
  private final SqlRexConvertletTable convertletTable;
  private final SqlOperatorTable operatorTable;
  private final ImmutableList<Program> programs;
  private final ImmutableList<RelTraitDef> traitDefs;
  private final Lex lex;
  private final SchemaPlus defaultSchema;
  private final RelOptCostFactory costFactory;
  private final SqlParserImplFactory parserFactory;


  public StdFrameworkConfig(Context context, //
      SqlRexConvertletTable convertletTable, //
      SqlOperatorTable operatorTable, //
      ImmutableList<Program> programs, //
      ImmutableList<RelTraitDef> traitDefs,
      Lex lex, //
      SchemaPlus defaultSchema, //
      RelOptCostFactory costFactory, //
      SqlParserImplFactory parserFactory) {
    super();
    this.context = context;
    this.convertletTable = convertletTable;
    this.operatorTable = operatorTable;
    this.programs = programs;
    this.traitDefs = traitDefs;
    this.lex = lex;
    this.defaultSchema = defaultSchema;
    this.costFactory = costFactory;
    this.parserFactory = parserFactory;
  }

  public Lex getLex() {
    return lex;
  }

  public SqlParserImplFactory getParserFactory() {
    return parserFactory;
  }

  public SchemaPlus getDefaultSchema() {
    return defaultSchema;
  }

  public ImmutableList<Program> getPrograms() {
    return programs;
  }

  public RelOptCostFactory getCostFactory() {
    return costFactory;
  }

  public ImmutableList<RelTraitDef> getTraitDefs() {
    return traitDefs;
  }

  public SqlRexConvertletTable getConvertletTable() {
    return convertletTable;
  }

  public Context getContext() {
    return context;
  }

  public SqlOperatorTable getOperatorTable() {
    return operatorTable;
  }


}
