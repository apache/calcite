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

import net.hydromatic.linq4j.function.Function1;

import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.jdbc.ConnectionConfig;
import net.hydromatic.optiq.jdbc.OptiqConnection;
import net.hydromatic.optiq.prepare.OptiqPrepareImpl;
import net.hydromatic.optiq.prepare.PlannerImpl;
import net.hydromatic.optiq.server.OptiqServerStatement;

import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.sql.fun.SqlStdOperatorTable;

import com.google.common.collect.ImmutableList;

import java.sql.Connection;
import java.sql.DriverManager;

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
   * @param schemaFactory Schema factory. Given a root schema, it creates and
   *                      returns the schema that should be used to execute
   *                      queries.
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
  public static Planner getPlanner(
      ConnectionConfig.Lex lex,
      Function1<SchemaPlus, Schema> schemaFactory,
      SqlStdOperatorTable operatorTable, RuleSet... ruleSets) {
    return new PlannerImpl(lex, schemaFactory, operatorTable,
        ImmutableList.copyOf(ruleSets));
  }

  /** Piece of code to be run in a context where a planner is available. The
   * planner is accessible from the {@code cluster} parameter, as are several
   * other useful objects. */
  public interface PlannerAction<R> {
    R apply(RelOptCluster cluster, RelOptSchema relOptSchema,
        SchemaPlus rootSchema);
  }

  /**
   * Initializes a container then calls user-specified code with a planner.
   *
   * @param action Callback containing user-specified code
   * @return Result of optimization
   */
  public static <R> R withPlanner(PlannerAction<R> action) {
    try {
      Class.forName("net.hydromatic.optiq.jdbc.Driver");
      Connection connection =
          DriverManager.getConnection("jdbc:optiq:");
      OptiqConnection optiqConnection =
          connection.unwrap(OptiqConnection.class);
      final OptiqServerStatement statement =
          optiqConnection.createStatement().unwrap(OptiqServerStatement.class);
      return new OptiqPrepareImpl().withPlanner(statement, action);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

// End Frameworks.java
