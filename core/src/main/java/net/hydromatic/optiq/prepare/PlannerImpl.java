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
import org.eigenbase.sql.parser.SqlParserImplFactory;
import org.eigenbase.sql2rel.SqlRexConvertletTable;
import org.eigenbase.sql2rel.SqlToRelConverter;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/** Implementation of {@link net.hydromatic.optiq.tools.Planner}. */
public class PlannerImpl implements Planner {
  private final Function1<SchemaPlus, Schema> schemaFactory;
  private final SqlOperatorTable operatorTable;
  private final ImmutableList<RuleSet> ruleSets;

  /** Holds the trait definitions to be registered with planner. May be null. */
  private final ImmutableList<RelTraitDef> traitDefs;

  private final Lex lex;
  private final SqlParserImplFactory parserFactory;

  // Options. TODO: allow client to set these. Maybe use a ConnectionConfig.
  private boolean caseSensitive = true;

  private State state;

  // set in STATE_1_RESET
  private boolean open;

  // set in STATE_2_READY
  private SchemaPlus rootSchema;
  private SchemaPlus defaultSchema;
  private JavaTypeFactory typeFactory;
  private RelOptPlanner planner;

  // set in STATE_4_VALIDATE
  private OptiqSqlValidator validator;
  private SqlNode validatedSqlNode;

  // set in STATE_5_CONVERT
  private SqlToRelConverter sqlToRelConverter;
  private SqlRexConvertletTable convertletTable;
  private RelNode rel;

  /** Creates a planner. Not a public API; call
   * {@link net.hydromatic.optiq.tools.Frameworks#getPlanner} instead. */
  public PlannerImpl(Lex lex, SqlParserImplFactory parserFactory,
      Function1<SchemaPlus, Schema> schemaFactory,
      SqlOperatorTable operatorTable, ImmutableList<RuleSet> ruleSets,
      ImmutableList<RelTraitDef> traitDefs,
      SqlRexConvertletTable convertletTable) {
    this.schemaFactory = schemaFactory;
    this.operatorTable = operatorTable;
    this.ruleSets = ruleSets;
    this.lex = lex;
    this.parserFactory = parserFactory;
    this.state = State.STATE_0_CLOSED;
    this.traitDefs = traitDefs;
    this.convertletTable = convertletTable;
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
            final Schema schema =
                schemaFactory.apply(PlannerImpl.this.rootSchema);
            defaultSchema = rootSchema.add(getName(schema), schema);
            typeFactory = (JavaTypeFactory) cluster.getTypeFactory();
            planner = cluster.getPlanner();
            return null;
          }

          /** Temporary method while we are phasing out schema names. Once
           * <a href="https://github.com/julianhyde/optiq/issues/214">optiq-214</a>
           * is fixed, the client will pass us a {@link SchemaPlus}, and that
           * has a name. */
          private String getName(Schema schema) {
            try {
              final Method method = schema.getClass().getMethod("getName");
              return (String) method.invoke(schema);
            } catch (NoSuchMethodException e) {
              return "DUMMY";
            } catch (InvocationTargetException e) {
              return "DUMMY";
            } catch (IllegalAccessException e) {
              return "DUMMY";
            }
          }
        });
    state = State.STATE_2_READY;

    // If user specify own traitDef, instead of default default trait,
    // first, clear the default trait def registered with planner
    // then, register the trait def specified in traitDefs.
    if (this.traitDefs != null) {
      planner.clearRelTraitDefs();
      for (RelTraitDef def : this.traitDefs) {
        planner.addRelTraitDef(def);
      }
    }
  }

  public SqlNode parse(final String sql) throws SqlParseException {
    switch (state) {
    case STATE_0_CLOSED:
    case STATE_1_RESET:
      ready();
    }
    ensure(State.STATE_2_READY);
    SqlParser parser = SqlParser.create(parserFactory, sql,
        lex.quoting, lex.unquotedCasing, lex.quotedCasing);
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
            createRexBuilder(), convertletTable);
    sqlToRelConverter.setTrimUnusedFields(false);
    sqlToRelConverter.enableTableAccessConversion(false);
    rel = sqlToRelConverter.convertQuery(validatedSqlNode, false, true);
    rel = sqlToRelConverter.flattenTypes(rel, true);
    rel = sqlToRelConverter.decorrelate(validatedSqlNode, rel);
    state = State.STATE_5_CONVERTED;
    return rel;
  }

  // OptiqCatalogReader is stateless; no need to store one
  private OptiqCatalogReader createCatalogReader() {
    return new OptiqCatalogReader(
        OptiqSchema.from(rootSchema),
        caseSensitive,
        OptiqSchema.from(defaultSchema).path(null),
        typeFactory);
  }

  // RexBuilder is stateless; no need to store one
  private RexBuilder createRexBuilder() {
    return new RexBuilder(typeFactory);
  }

  public JavaTypeFactory getTypeFactory() {
    return typeFactory;
  }

  public RelNode transform(int ruleSetIndex, RelTraitSet requiredOutputTraits,
      RelNode rel) throws RelConversionException {
    ensure(State.STATE_5_CONVERTED);
    RuleSet ruleSet = ruleSets.get(ruleSetIndex);
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
