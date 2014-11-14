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
package net.hydromatic.optiq.prepare;

import net.hydromatic.optiq.SchemaPlus;
import net.hydromatic.optiq.config.Lex;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.jdbc.OptiqSchema;
import net.hydromatic.optiq.tools.*;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.*;
import org.eigenbase.relopt.RelOptTable.ViewExpander;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlOperatorTable;
import org.eigenbase.sql.parser.SqlParseException;
import org.eigenbase.sql.parser.SqlParser;
import org.eigenbase.sql.parser.SqlParserImplFactory;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql2rel.RelDecorrelator;
import org.eigenbase.sql2rel.SqlRexConvertletTable;
import org.eigenbase.sql2rel.SqlToRelConverter;
import org.eigenbase.util.Util;

import com.google.common.collect.ImmutableList;

import java.util.List;

/** Implementation of {@link net.hydromatic.optiq.tools.Planner}. */
public class PlannerImpl implements Planner {
  private final SqlOperatorTable operatorTable;
  private final ImmutableList<Program> programs;
  private final FrameworkConfig config;

  /** Holds the trait definitions to be registered with planner. May be null. */
  private final ImmutableList<RelTraitDef> traitDefs;

  private final Lex lex;
  private final SqlParserImplFactory parserFactory;
  private final SqlRexConvertletTable convertletTable;

  // Options. TODO: allow client to set these. Maybe use a ConnectionConfig.
  private boolean caseSensitive = true;

  private State state;

  // set in STATE_1_RESET
  private boolean open;

  // set in STATE_2_READY
  private SchemaPlus defaultSchema;
  private JavaTypeFactory typeFactory;
  private RelOptPlanner planner;

  // set in STATE_4_VALIDATE
  private OptiqSqlValidator validator;
  private SqlNode validatedSqlNode;

  // set in STATE_5_CONVERT
  private RelNode rel;

  /** Creates a planner. Not a public API; call
   * {@link net.hydromatic.optiq.tools.Frameworks#getPlanner} instead. */
  public PlannerImpl(FrameworkConfig config) {
    this.config = config;
    this.defaultSchema = config.getDefaultSchema();
    this.operatorTable = config.getOperatorTable();
    this.programs = config.getPrograms();
    this.lex = config.getLex();
    this.parserFactory = config.getParserFactory();
    this.state = State.STATE_0_CLOSED;
    this.traitDefs = config.getTraitDefs();
    this.convertletTable = config.getConvertletTable();
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
            Util.discard(rootSchema); // use our own defaultSchema
            typeFactory = (JavaTypeFactory) cluster.getTypeFactory();
            planner = cluster.getPlanner();
            return null;
          }
        },
        config);

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
    final SqlToRelConverter sqlToRelConverter =
        new SqlToRelConverter(
            new ViewExpanderImpl(), validator, createCatalogReader(), planner,
            createRexBuilder(), convertletTable);
    sqlToRelConverter.setTrimUnusedFields(false);
    sqlToRelConverter.enableTableAccessConversion(false);
    rel = sqlToRelConverter.convertQuery(validatedSqlNode, false, true);
    rel = sqlToRelConverter.flattenTypes(rel, true);
    rel = RelDecorrelator.decorrelateQuery(rel);
    state = State.STATE_5_CONVERTED;
    return rel;
  }

  /** Implements {@link org.eigenbase.relopt.RelOptTable.ViewExpander}
   * interface for {@link net.hydromatic.optiq.tools.Planner}. */
  public class ViewExpanderImpl implements ViewExpander {
    public RelNode expandView(RelDataType rowType, String queryString,
        List<String> schemaPath) {
      final SqlParser parser = SqlParser.create(parserFactory, queryString,
          lex.quoting, lex.unquotedCasing, lex.quotedCasing);
      SqlNode sqlNode;
      try {
        sqlNode = parser.parseQuery();
      } catch (SqlParseException e) {
        throw new RuntimeException("parse failed", e);
      }

      final OptiqCatalogReader catalogReader =
          createCatalogReader().withSchemaPath(schemaPath);
      final SqlValidator validator = new OptiqSqlValidator(operatorTable,
          catalogReader, typeFactory);
      final SqlNode validatedSqlNode = validator.validate(sqlNode);

      final SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(
          null, validator, catalogReader, planner,
          createRexBuilder(), convertletTable);
      sqlToRelConverter.setTrimUnusedFields(false);

      return sqlToRelConverter.convertQuery(validatedSqlNode, true, false);
    }
  }

  // OptiqCatalogReader is stateless; no need to store one
  private OptiqCatalogReader createCatalogReader() {
    SchemaPlus rootSchema = rootSchema(defaultSchema);
    return new OptiqCatalogReader(
        OptiqSchema.from(rootSchema),
        caseSensitive,
        OptiqSchema.from(defaultSchema).path(null),
        typeFactory);
  }

  private static SchemaPlus rootSchema(SchemaPlus schema) {
    for (;;) {
      if (schema.getParentSchema() == null) {
        return schema;
      }
      schema = schema.getParentSchema();
    }
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
    Program program = programs.get(ruleSetIndex);
    return program.run(planner, rel, requiredOutputTraits);
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
