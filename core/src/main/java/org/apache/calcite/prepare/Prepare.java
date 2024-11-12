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
package org.apache.calcite.prepare;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.CalciteSchema.LatticeEntry;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.runtime.Typed;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.ExtensibleTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.schema.impl.ModifiableViewTable;
import org.apache.calcite.schema.impl.StarTable;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.sql2rel.ConvertToChecked;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.TryThreadLocal;
import org.apache.calcite.util.trace.CalciteTimingTracer;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.calcite.linq4j.Nullness.castNonNull;
import static org.apache.calcite.sql2rel.SqlToRelConverter.DEFAULT_IN_SUB_QUERY_THRESHOLD;

import static java.util.Objects.requireNonNull;

/**
 * Abstract base for classes that implement
 * the process of preparing and executing SQL expressions.
 */
public abstract class Prepare {
  protected static final Logger LOGGER = CalciteTrace.getStatementTracer();

  protected final CalcitePrepare.Context context;
  protected final CatalogReader catalogReader;
  /**
   * Convention via which results should be returned by execution.
   */
  protected final Convention resultConvention;
  protected @Nullable CalciteTimingTracer timingTracer;
  protected @MonotonicNonNull List<@Nullable List<String>> fieldOrigins;
  protected @MonotonicNonNull RelDataType parameterRowType;

  // temporary. for testing.
  public static final TryThreadLocal<Boolean> THREAD_TRIM =
      TryThreadLocal.of(false);

  /** Temporary, until
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1045">[CALCITE-1045]
   * Decorrelate sub-queries in Project and Join</a> is fixed.
   *
   * <p>The default is false, meaning do not expand queries during sql-to-rel,
   * but a few tests override and set it to true. After CALCITE-1045
   * is fixed, remove those overrides and use false everywhere. */
  public static final TryThreadLocal<Boolean> THREAD_EXPAND =
      TryThreadLocal.of(false);

  // temporary. for testing.
  public static final TryThreadLocal<@Nullable Integer> THREAD_INSUBQUERY_THRESHOLD =
      TryThreadLocal.of(DEFAULT_IN_SUB_QUERY_THRESHOLD);

  protected Prepare(CalcitePrepare.Context context, CatalogReader catalogReader,
      Convention resultConvention) {
    this.context = requireNonNull(context, "context");
    this.catalogReader = catalogReader;
    this.resultConvention = resultConvention;
  }

  protected abstract PreparedResult createPreparedExplanation(
      @Nullable RelDataType resultType,
      RelDataType parameterRowType,
      @Nullable RelRoot root,
      SqlExplainFormat format,
      SqlExplainLevel detailLevel);

  /**
   * Optimizes a query plan.
   *
   * @param root Root of relational expression tree
   * @param materializations Tables known to be populated with a given query
   * @param lattices Lattices
   * @return an equivalent optimized relational expression
   */
  protected RelRoot optimize(RelRoot root,
      final List<Materialization> materializations,
      final List<CalciteSchema.LatticeEntry> lattices) {
    final RelOptPlanner planner = root.rel.getCluster().getPlanner();

    final DataContext dataContext = context.getDataContext();
    planner.setExecutor(new RexExecutorImpl(dataContext));

    final List<RelOptMaterialization> materializationList =
        new ArrayList<>(materializations.size());
    for (Materialization materialization : materializations) {
      List<String> qualifiedTableName = materialization.materializedTable.path();
      materializationList.add(
          new RelOptMaterialization(
              castNonNull(materialization.tableRel),
              castNonNull(materialization.queryRel),
              materialization.starRelOptTable,
              qualifiedTableName));
    }

    final List<RelOptLattice> latticeList = new ArrayList<>(lattices.size());
    for (CalciteSchema.LatticeEntry lattice : lattices) {
      final CalciteSchema.TableEntry starTable = lattice.getStarTable();
      final JavaTypeFactory typeFactory = context.getTypeFactory();
      final RelOptTableImpl starRelOptTable =
          RelOptTableImpl.create(catalogReader,
              starTable.getTable().getRowType(typeFactory), starTable, null);
      latticeList.add(
          new RelOptLattice(lattice.getLattice(), starRelOptTable));
    }

    final RelTraitSet desiredTraits = getDesiredRootTraitSet(root);

    final Program program = getProgram();
    final RelNode rootRel4 =
        program.run(planner, root.rel, desiredTraits, materializationList,
            latticeList);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Plan after physical tweaks:\n{}",
          RelOptUtil.toString(rootRel4, SqlExplainLevel.ALL_ATTRIBUTES));
    }

    return root.withRel(rootRel4);
  }

  protected Program getProgram() {
    // Allow a test to override the default program.
    final Holder<@Nullable Program> holder = Holder.empty();
    Hook.PROGRAM.run(holder);
    @Nullable Program holderValue = holder.get();
    if (holderValue != null) {
      return holderValue;
    }

    return Programs.standard();
  }

  protected RelTraitSet getDesiredRootTraitSet(RelRoot root) {
    // Make sure non-CallingConvention traits, if any, are preserved
    return root.rel.getTraitSet()
        .replace(resultConvention)
        .replace(root.collation)
        .simplify();
  }

  /**
   * Implements a physical query plan.
   *
   * @param root Root of the relational expression tree
   * @return an executable plan
   */
  protected abstract PreparedResult implement(RelRoot root);

  public PreparedResult prepareSql(
      SqlNode sqlQuery,
      Class runtimeContextClass,
      SqlValidator validator,
      boolean needsValidation) {
    return prepareSql(
        sqlQuery,
        sqlQuery,
        runtimeContextClass,
        validator,
        needsValidation);
  }

  public PreparedResult prepareSql(
      SqlNode sqlQuery,
      SqlNode sqlNodeOriginal,
      Class<?> runtimeContextClass,
      SqlValidator validator,
      boolean needsValidation) {
    init(runtimeContextClass);

    final SqlToRelConverter.Config config =
        SqlToRelConverter.config()
            .withTrimUnusedFields(true)
            .withExpand(THREAD_EXPAND.get())
            .withInSubQueryThreshold(castNonNull(THREAD_INSUBQUERY_THRESHOLD.get()))
            .withExplain(sqlQuery.getKind() == SqlKind.EXPLAIN);
    final Holder<SqlToRelConverter.Config> configHolder = Holder.of(config);
    Hook.SQL2REL_CONVERTER_CONFIG_BUILDER.run(configHolder);
    final SqlToRelConverter sqlToRelConverter =
        getSqlToRelConverter(validator, catalogReader, configHolder.get());

    SqlExplain sqlExplain = null;
    if (sqlQuery.getKind() == SqlKind.EXPLAIN) {
      // dig out the underlying SQL statement
      sqlExplain = (SqlExplain) sqlQuery;
      sqlQuery = sqlExplain.getExplicandum();
      sqlToRelConverter.setDynamicParamCountInExplain(
          sqlExplain.getDynamicParamCount());
    }

    RelRoot root =
        sqlToRelConverter.convertQuery(sqlQuery, needsValidation, true);
    if (this.context.config().conformance().checkedArithmetic()) {
      ConvertToChecked checkedConv = new ConvertToChecked(root.rel.getCluster().getRexBuilder());
      RelNode rel = checkedConv.visit(root.rel);
      root = root.withRel(rel);
    }
    Hook.CONVERTED.run(root.rel);

    if (timingTracer != null) {
      timingTracer.traceTime("end sql2rel");
    }

    final RelDataType resultType = validator.getValidatedNodeType(sqlQuery);
    fieldOrigins = validator.getFieldOrigins(sqlQuery);
    assert fieldOrigins.size() == resultType.getFieldCount();

    parameterRowType = validator.getParameterRowType(sqlQuery);

    // Display logical plans before view expansion, plugging in physical
    // storage and decorrelation
    if (sqlExplain != null) {
      SqlExplain.Depth explainDepth = sqlExplain.getDepth();
      SqlExplainFormat format = sqlExplain.getFormat();
      SqlExplainLevel detailLevel = sqlExplain.getDetailLevel();
      switch (explainDepth) {
      case TYPE:
        return createPreparedExplanation(resultType, parameterRowType, null,
            format, detailLevel);
      case LOGICAL:
        return createPreparedExplanation(null, parameterRowType, root, format,
            detailLevel);
      default:
      }
    }

    // Structured type flattening, view expansion, and plugging in physical
    // storage.
    root = root.withRel(flattenTypes(root.rel, true));

    if (this.context.config().forceDecorrelate()) {
      // Sub-query decorrelation.
      root = root.withRel(decorrelate(sqlToRelConverter, sqlQuery, root.rel));
    }

    if (configHolder.get().isTrimUnusedFields()) {
      // Trim unused fields.
      root = trimUnusedFields(root);

      Hook.TRIMMED.run(root.rel);
    }

    // Display physical plan after decorrelation.
    if (sqlExplain != null) {
      switch (sqlExplain.getDepth()) {
      case PHYSICAL:
      default:
        root = optimize(root, getMaterializations(), getLattices());
        return createPreparedExplanation(null, parameterRowType, root,
            sqlExplain.getFormat(), sqlExplain.getDetailLevel());
      }
    }

    root = optimize(root, getMaterializations(), getLattices());

    if (timingTracer != null) {
      timingTracer.traceTime("end optimization");
    }

    // For transformation from DML -> DML, use result of rewrite
    // (e.g. UPDATE -> MERGE).  For anything else (e.g. CALL -> SELECT),
    // use original kind.
    if (!root.kind.belongsTo(SqlKind.DML)) {
      root = root.withKind(sqlNodeOriginal.getKind());
    }
    return implement(root);
  }

  protected TableModify.@Nullable Operation mapTableModOp(
      boolean isDml, SqlKind sqlKind) {
    if (!isDml) {
      return null;
    }
    switch (sqlKind) {
    case INSERT:
      return TableModify.Operation.INSERT;
    case DELETE:
      return TableModify.Operation.DELETE;
    case MERGE:
      return TableModify.Operation.MERGE;
    case UPDATE:
      return TableModify.Operation.UPDATE;
    default:
      return null;
    }
  }

  /**
   * Protected method to allow subclasses to override construction of
   * SqlToRelConverter.
   */
  protected abstract SqlToRelConverter getSqlToRelConverter(
      SqlValidator validator,
      CatalogReader catalogReader,
      SqlToRelConverter.Config config);

  public abstract RelNode flattenTypes(
      RelNode rootRel,
      boolean restructure);

  protected abstract RelNode decorrelate(SqlToRelConverter sqlToRelConverter,
      SqlNode query, RelNode rootRel);

  protected abstract List<Materialization> getMaterializations();

  protected abstract List<LatticeEntry> getLattices();

  /**
   * Walks over a tree of relational expressions, replacing each
   * {@link org.apache.calcite.rel.RelNode} with a 'slimmed down' relational
   * expression that projects
   * only the columns required by its consumer.
   *
   * @param root Root of relational expression tree
   * @return Trimmed relational expression
   */
  protected RelRoot trimUnusedFields(RelRoot root) {
    final SqlToRelConverter.Config config = SqlToRelConverter.config()
        .withTrimUnusedFields(shouldTrim(root.rel))
        .withExpand(THREAD_EXPAND.get())
        .withInSubQueryThreshold(castNonNull(THREAD_INSUBQUERY_THRESHOLD.get()));
    final SqlToRelConverter converter =
        getSqlToRelConverter(getSqlValidator(), catalogReader, config);
    final boolean ordered = !root.collation.getFieldCollations().isEmpty();
    final boolean dml = SqlKind.DML.contains(root.kind);
    return root.withRel(converter.trimUnusedFields(dml || ordered, root.rel));
  }

  private static boolean shouldTrim(RelNode rootRel) {
    // For now, don't trim if there are more than 3 joins. The projects
    // near the leaves created by trim migrate past joins and seem to
    // prevent join-reordering.
    return THREAD_TRIM.get() || RelOptUtil.countJoins(rootRel) < 2;
  }

  protected abstract void init(Class runtimeContextClass);

  protected abstract SqlValidator getSqlValidator();

  /** Interface by which validator and planner can read table metadata. */
  public interface CatalogReader
      extends RelOptSchema, SqlValidatorCatalogReader, SqlOperatorTable {
    @Override @Nullable PreparingTable getTableForMember(List<String> names);

    /** Returns a catalog reader the same as this one but with a possibly
     * different schema path. */
    CatalogReader withSchemaPath(List<String> schemaPath);

    @Override @Nullable PreparingTable getTable(List<String> names);

    ThreadLocal<@Nullable CatalogReader> THREAD_LOCAL = new ThreadLocal<>();
  }

  /** Definition of a table, for the purposes of the validator and planner. */
  public interface PreparingTable
      extends RelOptTable, SqlValidatorTable {
  }

  /** Abstract implementation of {@link PreparingTable} with an implementation
   * for {@link #columnHasDefaultValue}. */
  public abstract static class AbstractPreparingTable
      implements PreparingTable {
    @SuppressWarnings("deprecation")
    @Override public boolean columnHasDefaultValue(RelDataType rowType, int ordinal,
        InitializerContext initializerContext) {
      // This method is no longer used
      final Table table = this.unwrap(Table.class);
      if (table instanceof Wrapper) {
        final InitializerExpressionFactory initializerExpressionFactory =
            ((Wrapper) table).unwrap(InitializerExpressionFactory.class);
        if (initializerExpressionFactory != null) {
          return initializerExpressionFactory
              .newColumnDefaultValue(this, ordinal, initializerContext)
              .getType().getSqlTypeName() != SqlTypeName.NULL;
        }
      }
      if (ordinal >= rowType.getFieldList().size()) {
        return true;
      }
      return !rowType.getFieldList().get(ordinal).getType().isNullable();
    }

    @Override public final RelOptTable extend(List<RelDataTypeField> extendedFields) {
      final Table table = unwrap(Table.class);

      // Get the set of extended columns that do not have the same name as a column
      // in the base table.
      final List<RelDataTypeField> baseColumns = getRowType().getFieldList();
      final List<RelDataTypeField> dedupedFields =
          RelOptUtil.deduplicateColumns(baseColumns, extendedFields);
      final List<RelDataTypeField> dedupedExtendedFields =
          dedupedFields.subList(baseColumns.size(), dedupedFields.size());

      if (table instanceof ExtensibleTable) {
        final Table extendedTable =
                ((ExtensibleTable) table).extend(dedupedExtendedFields);
        return extend(extendedTable);
      } else if (table instanceof ModifiableViewTable) {
        final ModifiableViewTable modifiableViewTable =
                (ModifiableViewTable) table;
        final ModifiableViewTable extendedView =
            modifiableViewTable.extend(dedupedExtendedFields,
                requireNonNull(
                    getRelOptSchema(),
                    () -> "relOptSchema for table " + getQualifiedName()).getTypeFactory());
        return extend(extendedView);
      }
      throw new RuntimeException("Cannot extend " + table);
    }

    /** Implementation-specific code to instantiate a new {@link RelOptTable}
     * based on a {@link Table} that has been extended. */
    protected abstract RelOptTable extend(Table extendedTable);

    @Override public List<ColumnStrategy> getColumnStrategies() {
      return RelOptTableImpl.columnStrategies(AbstractPreparingTable.this);
    }
  }

  /**
   * PreparedExplanation is a PreparedResult for an EXPLAIN PLAN statement.
   * It's always good to have an explanation prepared.
   */
  public abstract static class PreparedExplain
      implements PreparedResult {
    private final @Nullable RelDataType rowType;
    private final RelDataType parameterRowType;
    private final @Nullable RelRoot root;
    private final SqlExplainFormat format;
    private final SqlExplainLevel detailLevel;

    protected PreparedExplain(
        @Nullable RelDataType rowType,
        RelDataType parameterRowType,
        @Nullable RelRoot root,
        SqlExplainFormat format,
        SqlExplainLevel detailLevel) {
      this.rowType = rowType;
      this.parameterRowType = parameterRowType;
      this.root = root;
      this.format = format;
      this.detailLevel = detailLevel;
    }

    @Override public String getCode() {
      if (root == null) {
        return rowType == null ? "rowType is null" : RelOptUtil.dumpType(rowType);
      } else {
        return RelOptUtil.dumpPlan("", root.rel, format, detailLevel);
      }
    }

    @Override public RelDataType getParameterRowType() {
      return parameterRowType;
    }

    @Override public boolean isDml() {
      return false;
    }

    @Override public TableModify.@Nullable Operation getTableModOp() {
      return null;
    }

    @Override public List<@Nullable List<String>> getFieldOrigins() {
      return Collections.singletonList(
          Collections.nCopies(4, null));
    }
  }

  /**
   * Result of a call to {@link Prepare#prepareSql}.
   */
  public interface PreparedResult {
    /**
     * Returns the code generated by preparation.
     */
    String getCode();

    /**
     * Returns whether this result is for a DML statement, in which case the
     * result set is one row with one column containing the number of rows
     * affected.
     */
    boolean isDml();

    /**
     * Returns the table modification operation corresponding to this
     * statement if it is a table modification statement; otherwise null.
     */
    TableModify.@Nullable Operation getTableModOp();

    /**
     * Returns a list describing, for each result field, the origin of the
     * field as a 4-element list of (database, schema, table, column).
     */
    List<? extends @Nullable List<String>> getFieldOrigins();

    /**
     * Returns a record type whose fields are the parameters of this statement.
     */
    RelDataType getParameterRowType();

    /**
     * Executes the prepared result.
     *
     * @param cursorFactory How to map values into a cursor
     * @return producer of rows resulting from execution
     */
    Bindable getBindable(Meta.CursorFactory cursorFactory);
  }

  /**
   * Abstract implementation of {@link PreparedResult}.
   */
  public abstract static class PreparedResultImpl
      implements PreparedResult, Typed {
    protected final RelNode rootRel;
    protected final RelDataType parameterRowType;
    protected final RelDataType rowType;
    protected final boolean isDml;
    protected final TableModify.@Nullable Operation tableModOp;
    protected final List<? extends @Nullable List<String>> fieldOrigins;
    protected final List<RelCollation> collations;

    protected PreparedResultImpl(
        RelDataType rowType,
        RelDataType parameterRowType,
        List<? extends @Nullable List<String>> fieldOrigins,
        List<RelCollation> collations,
        RelNode rootRel,
        TableModify.@Nullable Operation tableModOp,
        boolean isDml) {
      this.rowType = requireNonNull(rowType, "rowType");
      this.parameterRowType = requireNonNull(parameterRowType, "parameterRowType");
      this.fieldOrigins = requireNonNull(fieldOrigins, "fieldOrigins");
      this.collations = ImmutableList.copyOf(collations);
      this.rootRel = requireNonNull(rootRel, "rootRel");
      this.tableModOp = tableModOp;
      this.isDml = isDml;
    }

    @Override public boolean isDml() {
      return isDml;
    }

    @Override public TableModify.@Nullable Operation getTableModOp() {
      return tableModOp;
    }

    @Override public List<? extends @Nullable List<String>> getFieldOrigins() {
      return fieldOrigins;
    }

    @Override public RelDataType getParameterRowType() {
      return parameterRowType;
    }

    /**
     * Returns the physical row type of this prepared statement. May not be
     * identical to the row type returned by the validator; for example, the
     * field names may have been made unique.
     */
    public RelDataType getPhysicalRowType() {
      return rowType;
    }

    @Override public abstract Type getElementType();

    public RelNode getRootRel() {
      return rootRel;
    }
  }

  /** Describes that a given SQL query is materialized by a given table.
   * The materialization is currently valid, and can be used in the planning
   * process. */
  public static class Materialization {
    /** The table that holds the materialized data. */
    final CalciteSchema.TableEntry materializedTable;
    /** The query that derives the data. */
    final String sql;
    /** The schema path for the query. */
    final List<String> viewSchemaPath;
    /** Relational expression for the table. Usually a
     * {@link org.apache.calcite.rel.logical.LogicalTableScan}. */
    @Nullable RelNode tableRel;
    /** Relational expression for the query to populate the table. */
    @Nullable RelNode queryRel;
    /** Star table identified. */
    private @Nullable RelOptTable starRelOptTable;

    public Materialization(CalciteSchema.TableEntry materializedTable,
        String sql, List<String> viewSchemaPath) {
      this.materializedTable =
          requireNonNull(materializedTable, "materializedTable");
      this.sql = requireNonNull(sql, "sql");
      this.viewSchemaPath = viewSchemaPath;
    }

    public void materialize(RelNode queryRel,
        RelOptTable starRelOptTable) {
      this.queryRel = queryRel;
      this.starRelOptTable = starRelOptTable;
      assert starRelOptTable.maybeUnwrap(StarTable.class).isPresent();
    }
  }
}
