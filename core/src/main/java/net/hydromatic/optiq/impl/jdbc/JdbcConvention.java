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
package net.hydromatic.optiq.impl.jdbc;

import net.hydromatic.linq4j.expressions.Expression;

import org.eigenbase.rel.rules.PushFilterPastSetOpRule;
import org.eigenbase.rel.rules.RemoveTrivialProjectRule;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.sql.SqlDialect;

/**
 * Calling convention for relational operations that occur in a JDBC
 * database.
 *
 * <p>The convention is a slight misnomer. The operations occur in whatever
 * data-flow architecture the database uses internally. Nevertheless, the result
 * pops out in JDBC.</p>
 *
 * <p>This is the only convention, thus far, that is not a singleton. Each
 * instance contains a JDBC schema (and therefore a data source). If Optiq is
 * working with two different databases, it would even make sense to convert
 * from "JDBC#A" convention to "JDBC#B", even though we don't do it currently.
 * (That would involve asking database B to open a database link to database
 * A.)</p>
 *
 * <p>As a result, converter rules from and two this convention need to be
 * instantiated, at the start of planning, for each JDBC database in play.</p>
 */
public class JdbcConvention extends Convention.Impl {
  public final SqlDialect dialect;
  public final Expression expression;

  public JdbcConvention(SqlDialect dialect, Expression expression,
      String name) {
    super("JDBC." + name, JdbcRel.class);
    this.dialect = dialect;
    this.expression = expression;
  }

  public static JdbcConvention of(SqlDialect dialect, Expression expression,
      String name) {
    return new JdbcConvention(dialect, expression, name);
  }

  @Override
  public void register(RelOptPlanner planner) {
    for (RelOptRule rule : JdbcRules.rules(this)) {
      planner.addRule(rule);
    }
    planner.addRule(PushFilterPastSetOpRule.INSTANCE);
    planner.addRule(RemoveTrivialProjectRule.INSTANCE);
  }
}

// End JdbcConvention.java
