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
import net.hydromatic.optiq.jdbc.OptiqConnection;
import net.hydromatic.optiq.jdbc.OptiqSchema;
import net.hydromatic.optiq.prepare.OptiqPrepareImpl;
import net.hydromatic.optiq.prepare.PlannerImpl;
import net.hydromatic.optiq.server.OptiqServerStatement;

import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.relopt.RelTraitDef;
import org.eigenbase.sql.SqlOperatorTable;
import org.eigenbase.sql.parser.SqlParserImplFactory;
import org.eigenbase.sql.parser.impl.SqlParserImpl;
import org.eigenbase.sql2rel.SqlRexConvertletTable;
import org.eigenbase.sql2rel.StandardConvertletTable;

import com.google.common.collect.ImmutableList;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

/**
 * Tools for invoking Optiq functionality without initializing a container /
 * server first.
 */
public class Frameworks {
  private Frameworks() {
  }

  /**
   * Creates an instance of {@code Planner}.
   *
   * @param lex The type of lexing the SqlParser should do.  Controls case rules
   *     and quoted identifier syntax.
   * @param defaultSchema Default schema. Must not be null.
   * @param operatorTable The instance of SqlOperatorTable that be should to
   *     resolve Optiq operators.
   * @param ruleSets An array of one or more rule sets used during the course of
   *     query evaluation. The common use case is when there is a single rule
   *     set and {@link net.hydromatic.optiq.tools.Planner#transform}
   *     will only be called once. However, consumers may also register multiple
   *     {@link net.hydromatic.optiq.tools.RuleSet}s and do multiple repetitions
   *     of {@link Planner#transform} planning cycles using different indices.
   *     The order of rule sets provided here determines the zero-based indices
   *     of rule sets elsewhere in this class.
   * @return The Planner object.
   */
  public static Planner getPlanner(Lex lex, SchemaPlus defaultSchema,
      SqlOperatorTable operatorTable, RuleSet... ruleSets) {
    return getPlanner(lex, SqlParserImpl.FACTORY, defaultSchema,
        operatorTable, null, StandardConvertletTable.INSTANCE, ruleSets);
  }

  /**
   * Creates an instance of {@code Planner}.
   *
   * <p>If {@code traitDefs} is specified, the planner first de-registers any
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
   * @param parserFactory Parser factory creates and returns the SQL parser.
   * @param operatorTable The instance of SqlOperatorTable that be should to
   *     resolve Optiq operators.
   * @param ruleSets An array of one or more rule sets used during the course of
   *     query evaluation. The common use case is when there is a single rule
   *     set and {@link net.hydromatic.optiq.tools.Planner#transform}
   *     will only be called once. However, consumers may also register multiple
   *     {@link net.hydromatic.optiq.tools.RuleSet}s and do multiple repetitions
   *     of {@link Planner#transform} planning cycles using different indices.
   *     The order of rule sets provided here determines the zero-based indices
   *     of rule sets elsewhere in this class.
   *  @param  traitDefs The list of RelTraitDef that would be registered with
   *     planner, or null.
   * @return The Planner object.
   */
  public static Planner getPlanner(Lex lex,
      SqlParserImplFactory parserFactory,
      SchemaPlus defaultSchema,
      SqlOperatorTable operatorTable,
      List<RelTraitDef> traitDefs,
      SqlRexConvertletTable convertletTable,
      RuleSet... ruleSets) {
    return new PlannerImpl(lex, parserFactory, defaultSchema,
        operatorTable, ImmutableList.copyOf(ruleSets),
        traitDefs == null ? null : ImmutableList.copyOf(traitDefs),
        convertletTable);
  }

  /** Piece of code to be run in a context where a planner is available. The
   * planner is accessible from the {@code cluster} parameter, as are several
   * other useful objects. */
  public interface PlannerAction<R> {
    R apply(RelOptCluster cluster, RelOptSchema relOptSchema,
        SchemaPlus rootSchema);
  }

  /** Piece of code to be run in a context where a planner and statement are
   * available. The planner is accessible from the {@code cluster} parameter, as
   * are several other useful objects. The connection and
   * {@link net.hydromatic.optiq.DataContext} are accessible from the
   * statement. */
  public interface PrepareAction<R> {
    R apply(RelOptCluster cluster, RelOptSchema relOptSchema,
        SchemaPlus rootSchema, OptiqServerStatement statement);
  }

  /**
   * Initializes a container then calls user-specified code with a planner.
   *
   * @param action Callback containing user-specified code
   * @return Return value from action
   */
  public static <R> R withPlanner(final PlannerAction<R> action) {
    return withPrepare(
        new Frameworks.PrepareAction<R>() {
          public R apply(RelOptCluster cluster, RelOptSchema relOptSchema,
              SchemaPlus rootSchema, OptiqServerStatement statement) {
            return action.apply(cluster, relOptSchema, rootSchema);
          }
        });
  }

  /**
   * Initializes a container then calls user-specified code with a planner
   * and statement.
   *
   * @param action Callback containing user-specified code
   * @return Return value from action
   */
  public static <R> R withPrepare(PrepareAction<R> action) {
    try {
      Class.forName("net.hydromatic.optiq.jdbc.Driver");
      Connection connection =
          DriverManager.getConnection("jdbc:optiq:");
      OptiqConnection optiqConnection =
          connection.unwrap(OptiqConnection.class);
      final OptiqServerStatement statement =
          optiqConnection.createStatement().unwrap(OptiqServerStatement.class);
      //noinspection deprecation
      return new OptiqPrepareImpl().perform(statement, action);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a root schema.
   */
  public static SchemaPlus createRootSchema() {
    return OptiqSchema.createRootSchema().plus();
  }
}

// End Frameworks.java
