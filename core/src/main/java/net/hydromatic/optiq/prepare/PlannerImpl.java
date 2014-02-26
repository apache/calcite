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
package net.hydromatic.optiq.prepare;

import net.hydromatic.linq4j.function.Function1;

import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.Schemas;
import net.hydromatic.optiq.config.Lex;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.jdbc.OptiqSchema;
import net.hydromatic.optiq.tools.*;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlOperatorTable;
import org.eigenbase.sql.parser.SqlParseException;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.sql2rel.SqlToRelConverter;

import com.google.common.collect.ImmutableList;

/** Implementation of {@link net.hydromatic.optiq.tools.Planner}. */
public class PlannerImpl implements Planner {
  private final Function1<SchemaPlus, Schema> schemaFactory;
  private final SqlOperatorTable operatorTable;
  private final ImmutableList<RuleSet> ruleSets;
  private final Lex lex;

  // Options. TODO: allow client to set these. Maybe use a ConnectionConfig.
  private boolean caseSensitive = true;

  private State state;

  // set in STATE_1_RESET
  private boolean open;

  // set in STATE_2_READY
  private SchemaPlus rootSchema;
  private Schema defaultSchema;
  private JavaTypeFactory typeFactory;
  private RelOptPlanner planner;

  // set in STATE_4_VALIDATE
  private OptiqSqlValidator validator;
  private SqlNode validatedSqlNode;

  // set in STATE_5_CONVERT
  private SqlToRelConverter sqlToRelConverter;
  private RelNode rel;

  public PlannerImpl(Lex lex,
      Function1<SchemaPlus, Schema> schemaFactory,
      SqlOperatorTable operatorTable, ImmutableList<RuleSet> ruleSets) {
    this.schemaFactory = schemaFactory;
    this.operatorTable = operatorTable;
    this.ruleSets = ruleSets;
    this.lex = lex;
    this.state = State.STATE_0_CLOSED;
    reset();
  }

  /** Makes sure that the state is at least the given state. */
  private void ensure(State state) {
    if (state == this.state) {
      return;
    }
    if (state.ordinal() < this.state.ordinal()) {
      throw new IllegalArgumentException("cannot move to " + state + " from "
          + this.state);
    }
    state.from(this);
  }

  public RelTraitSet getEmptyTraitSet() {
    return planner.emptyTraitSet();
  }

  public void close() {
    open = false;
    rootSchema = null;
    defaultSchema = null;
    typeFactory = null;
    state = State.STATE_0_CLOSED;
  }

  public void reset() {
    ensure(State.STATE_0_CLOSED);
    open = true;
    state = State.STATE_1_RESET;
  }

  private void ready() {
    switch (state) {
    case STATE_0_CLOSED:
      reset();
    }
    ensure(State.STATE_1_RESET);
    Frameworks.withPlanner(
        new Frameworks.PlannerAction<Void>() {
          public Void apply(RelOptCluster cluster, RelOptSchema relOptSchema,
              SchemaPlus rootSchema) {
            PlannerImpl.this.rootSchema = rootSchema;
            defaultSchema =
                rootSchema.addRecursive(
                    schemaFactory.apply(PlannerImpl.this.rootSchema));
            typeFactory = (JavaTypeFactory) cluster.getTypeFactory();
            planner = cluster.getPlanner();
            return null;
          }
        });
    state = State.STATE_2_READY;
  }

  public SqlNode parse(final String sql) throws SqlParseException {
    switch (state) {
    case STATE_0_CLOSED:
    case STATE_1_RESET:
      ready();
    }
    ensure(State.STATE_2_READY);
    SqlParser parser =
        new SqlParser(sql, lex.quoting, lex.unquotedCasing, lex.quotedCasing);
    SqlNode sqlNode = parser.parseStmt();
    state = State.STATE_3_PARSED;
    return sqlNode;
  }

  public SqlNode validate(SqlNode sqlNode) throws ValidationException {
    ensure(State.STATE_3_PARSED);
    this.validator =
        new OptiqSqlValidator(
            operatorTable, createCatalogReader(), typeFactory);
    try {
      validatedSqlNode = validator.validate(sqlNode);
    } catch (RuntimeException e) {
      throw new ValidationException(e);
    }
    state = State.STATE_4_VALIDATED;
    return validatedSqlNode;
  }

  public RelNode convert(SqlNode sql) throws RelConversionException {
    ensure(State.STATE_4_VALIDATED);
    assert validatedSqlNode != null;
    this.sqlToRelConverter =
        new SqlToRelConverter(
            null, validator, createCatalogReader(), planner,
            createRexBuilder());
    sqlToRelConverter.setTrimUnusedFields(false);
    rel = sqlToRelConverter.convertQuery(validatedSqlNode, false, true);
    state = State.STATE_5_CONVERTED;
    return rel;
  }

  // OptiqCatalogReader is stateless; no need to store one
  private OptiqCatalogReader createCatalogReader() {
    return new OptiqCatalogReader(
        OptiqSchema.from(rootSchema),
        caseSensitive,
        Schemas.path(defaultSchema, null),
        typeFactory);
  }

  // RexBuilder is stateless; no need to store one
  private RexBuilder createRexBuilder() {
    return new RexBuilder(typeFactory);
  }

  public RelNode transform(int ruleSetIndex, RelTraitSet requiredOutputTraits,
      RelNode rel) throws RelConversionException {
    ensure(State.STATE_5_CONVERTED);
    RuleSet ruleSet = ruleSets.get(ruleSetIndex);
    planner.clearRules();
    planner.clear();
    for (RelOptRule rule : ruleSet) {
      planner.addRule(rule);
    }
    if (!rel.getTraitSet().equals(requiredOutputTraits)) {
      rel = planner.changeTraits(rel, requiredOutputTraits);
    }
    planner.setRoot(rel);
    return planner.findBestExp();
  }

  /** Stage of a statement in the query-preparation lifecycle. */
  private enum State {
    STATE_0_CLOSED {
      @Override
      void from(PlannerImpl planner) {
        planner.close();
      }
    },
    STATE_1_RESET {
      @Override
      void from(PlannerImpl planner) {
        planner.ensure(STATE_0_CLOSED);
        planner.reset();
      }
    },
    STATE_2_READY {
      @Override
      void from(PlannerImpl planner) {
        STATE_1_RESET.from(planner);
        planner.ready();
      }
    },
    STATE_3_PARSED,
    STATE_4_VALIDATED,
    STATE_5_CONVERTED;

    /** Moves planner's state to this state. This must be a higher state. */
    void from(PlannerImpl planner) {
      throw new IllegalArgumentException("cannot move from " + planner.state
          + " to " + this);
    }
  }
}

// End PlannerImpl.java
