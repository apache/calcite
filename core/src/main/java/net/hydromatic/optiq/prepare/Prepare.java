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

import net.hydromatic.optiq.DataContext;
import net.hydromatic.optiq.impl.StarTable;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.jdbc.OptiqPrepare;
import net.hydromatic.optiq.jdbc.OptiqSchema;
import net.hydromatic.optiq.runtime.Bindable;
import net.hydromatic.optiq.runtime.Hook;
import net.hydromatic.optiq.runtime.Typed;
import net.hydromatic.optiq.tools.Program;
import net.hydromatic.optiq.tools.Programs;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexExecutorImpl;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.sql2rel.SqlToRelConverter;
import org.eigenbase.trace.EigenbaseTimingTracer;
import org.eigenbase.trace.EigenbaseTrace;
import org.eigenbase.util.Holder;
import org.eigenbase.util.Pair;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Type;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Abstract base for classes that implement
 * the process of preparing and executing SQL expressions.
 */
public abstract class Prepare {
  protected static final Logger LOGGER = EigenbaseTrace.getStatementTracer();

  protected final OptiqPrepare.Context context;
  protected final CatalogReader catalogReader;
  protected String queryString = null;
  /**
   * Convention via which results should be returned by execution.
   */
  protected final Convention resultConvention;
  protected EigenbaseTimingTracer timingTracer;
  protected List<List<String>> fieldOrigins;
  protected RelDataType parameterRowType;

  // temporary. for testing.
  public static final ThreadLocal<Boolean> THREAD_TRIM =
      new ThreadLocal<Boolean>() {
        @Override protected Boolean initialValue() {
          return false;
        }
      };

  public Prepare(OptiqPrepare.Context context, CatalogReader catalogReader,
      Convention resultConvention) {
    assert context != null;
    this.context = context;
    this.catalogReader = catalogReader;
    this.resultConvention = resultConvention;
  }

  protected abstract PreparedResult createPreparedExplanation(
      RelDataType resultType,
      RelDataType parameterRowType,
      RelNode rootRel,
      boolean explainAsXml,
      SqlExplainLevel detailLevel);

  /**
   * Optimizes a query plan.
   *
   * @param logicalRowType logical row type of relational expression (before
   * struct fields are flattened, or field names are renamed for uniqueness)
   * @param rootRel root of a relational expression
   *
   * @param materializations Tables known to be populated with a given query
   * @param lattices Lattices
   * @return an equivalent optimized relational expression
   */
  protected RelNode optimize(RelDataType logicalRowType, final RelNode rootRel,
      final List<Materialization> materializations,
      final List<OptiqSchema.LatticeEntry> lattices) {
    final RelOptPlanner planner = rootRel.getCluster().getPlanner();

    planner.setRoot(rootRel);

    final RelTraitSet desiredTraits = getDesiredRootTraitSet(rootRel);
    final Program program = getProgram();

    final DataContext dataContext = context.getDataContext();
    planner.setExecutor(new RexExecutorImpl(dataContext));

    for (Materialization materialization : materializations) {
      planner.addMaterialization(
          new RelOptMaterialization(materialization.tableRel,
              materialization.queryRel,
              materialization.starRelOptTable));
    }

    for (OptiqSchema.LatticeEntry lattice : lattices) {
      final OptiqSchema.TableEntry starTable = lattice.getStarTable();
      final JavaTypeFactory typeFactory = context.getTypeFactory();
      final RelOptTableImpl starRelOptTable =
          RelOptTableImpl.create(catalogReader,
              starTable.getTable().getRowType(typeFactory), starTable, null);
      planner.addLattice(
          new RelOptLattice(lattice.getLattice(), starRelOptTable));
    }

    final RelNode rootRel4 = program.run(planner, rootRel, desiredTraits);
    if (LOGGER.isLoggable(Level.FINE)) {
      LOGGER.fine(
          "Plan after physical tweaks: "
          + RelOptUtil.toString(rootRel4, SqlExplainLevel.ALL_ATTRIBUTES));
    }

    return rootRel4;
  }

  private Program getProgram() {
    // Allow a test to override the planner.
    final List<Materialization> materializations = ImmutableList.of();
    final Holder<Program> holder = Holder.of(null);
    Hook.PROGRAM.run(Pair.of(materializations, holder));
    if (holder.get() != null) {
      return holder.get();
    }

    return Programs.standard();
  }

  protected RelTraitSet getDesiredRootTraitSet(RelNode rootRel) {
    // Make sure non-CallingConvention traits, if any, are preserved
    return rootRel.getTraitSet()
        .replace(resultConvention);
  }

  /**
   * Implements a physical query plan.
   *
   * @param rowType original row type returned by query validator
   * @param rootRel root of the relational expression.
   * @param sqlKind SqlKind of the original statement.
   * @return an executable plan
   */
  protected abstract PreparedResult implement(
      RelDataType rowType, RelNode rootRel, SqlKind sqlKind);

  public PreparedResult prepareSql(
      SqlNode sqlQuery,
      Class runtimeContextClass,
      SqlValidator validator,
      boolean needsValidation,
      List<Materialization> materializations,
      List<OptiqSchema.LatticeEntry> lattices) {
    return prepareSql(
        sqlQuery,
        sqlQuery,
        runtimeContextClass,
        validator,
        needsValidation,
        materializations,
        lattices);
  }

  public PreparedResult prepareSql(
      SqlNode sqlQuery,
      SqlNode sqlNodeOriginal,
      Class runtimeContextClass,
      SqlValidator validator,
      boolean needsValidation,
      List<Materialization> materializations,
      List<OptiqSchema.LatticeEntry> lattices) {
    queryString = sqlQuery.toString();

    init(runtimeContextClass);

    SqlToRelConverter sqlToRelConverter =
        getSqlToRelConverter(validator, catalogReader);

    SqlExplain sqlExplain = null;
    if (sqlQuery.getKind() == SqlKind.EXPLAIN) {
      // dig out the underlying SQL statement
      sqlExplain = (SqlExplain) sqlQuery;
      sqlQuery = sqlExplain.getExplicandum();
      sqlToRelConverter.setIsExplain(sqlExplain.getDynamicParamCount());
    }

    RelNode rootRel =
        sqlToRelConverter.convertQuery(sqlQuery, needsValidation, true);
    Hook.CONVERTED.run(rootRel);

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
      boolean explainAsXml = sqlExplain.isXml();
      SqlExplainLevel detailLevel = sqlExplain.getDetailLevel();
      switch (explainDepth) {
      case TYPE:
        return createPreparedExplanation(
            resultType, parameterRowType, null, explainAsXml, detailLevel);
      case LOGICAL:
        return createPreparedExplanation(
            null, parameterRowType, rootRel, explainAsXml, detailLevel);
      default:
      }
    }

    // Structured type flattening, view expansion, and plugging in physical
    // storage.
    rootRel = flattenTypes(rootRel, true);

    // Subquery decorrelation.
    rootRel = decorrelate(sqlToRelConverter, sqlQuery, rootRel);

    // Trim unused fields.
    rootRel = trimUnusedFields(rootRel);

    Hook.TRIMMED.run(rootRel);

    // Display physical plan after decorrelation.
    if (sqlExplain != null) {
      SqlExplain.Depth explainDepth = sqlExplain.getDepth();
      boolean explainAsXml = sqlExplain.isXml();
      SqlExplainLevel detailLevel = sqlExplain.getDetailLevel();
      switch (explainDepth) {
      case PHYSICAL:
      default:
        rootRel = optimize(rootRel.getRowType(), rootRel, materializations,
            lattices);
        return createPreparedExplanation(
            null, parameterRowType, rootRel, explainAsXml, detailLevel);
      }
    }

    rootRel = optimize(resultType, rootRel, materializations, lattices);

    if (timingTracer != null) {
      timingTracer.traceTime("end optimization");
    }

    // For transformation from DML -> DML, use result of rewrite
    // (e.g. UPDATE -> MERGE).  For anything else (e.g. CALL -> SELECT),
    // use original kind.
    SqlKind kind = sqlQuery.getKind();
    if (!kind.belongsTo(SqlKind.DML)) {
      kind = sqlNodeOriginal.getKind();
    }
    return implement(
        resultType,
        rootRel,
        kind);
  }

  protected TableModificationRel.Operation mapTableModOp(
      boolean isDml, SqlKind sqlKind) {
    if (!isDml) {
      return null;
    }
    switch (sqlKind) {
    case INSERT:
      return TableModificationRel.Operation.INSERT;
    case DELETE:
      return TableModificationRel.Operation.DELETE;
    case MERGE:
      return TableModificationRel.Operation.MERGE;
    case UPDATE:
      return TableModificationRel.Operation.UPDATE;
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
      CatalogReader catalogReader);

  /**
   * Protected method to allow subclasses to override construction of
   * RelImplementor.
   */
  protected abstract RelImplementor getRelImplementor(RexBuilder rexBuilder);

  protected abstract boolean shouldAlwaysWriteJavaFile();

  public abstract RelNode flattenTypes(
      RelNode rootRel,
      boolean restructure);

  protected abstract RelNode decorrelate(SqlToRelConverter sqlToRelConverter,
      SqlNode query, RelNode rootRel);

  /**
   * Walks over a tree of relational expressions, replacing each
   * {@link org.eigenbase.rel.RelNode} with a 'slimmed down' relational
   * expression that projects
   * only the columns required by its consumer.
   *
   * @param rootRel Relational expression that is at the root of the tree
   * @return Trimmed relational expression
   */
  protected RelNode trimUnusedFields(RelNode rootRel) {
    final SqlToRelConverter converter =
        getSqlToRelConverter(
            getSqlValidator(), catalogReader);
    converter.setTrimUnusedFields(shouldTrim(rootRel));
    return converter.trimUnusedFields(rootRel);
  }

  private boolean shouldTrim(RelNode rootRel) {
    // For now, don't trim if there are more than 3 joins. The projects
    // near the leaves created by trim migrate past joins and seem to
    // prevent join-reordering.
    return THREAD_TRIM.get() || RelOptUtil.countJoins(rootRel) < 2;
  }

  /**
   * Returns a relational expression which is to be substituted for an access
   * to a SQL view.
   *
   * @param rowType Row type of the view
   * @param queryString Body of the view
   * @param schemaPath List of schema names wherein to find referenced tables
   * @return Relational expression
   */
  public RelNode expandView(
      RelDataType rowType,
      String queryString,
      List<String> schemaPath) {
    throw new UnsupportedOperationException();
  }

  protected abstract void init(Class runtimeContextClass);

  protected abstract SqlValidator getSqlValidator();

  /** Interface by which validator and planner can read table metadata. */
  public interface CatalogReader
      extends RelOptSchema, SqlValidatorCatalogReader {
    PreparingTable getTableForMember(List<String> names);

    /** Returns a catalog reader the same as this one but with a possibly
     * different schema path. */
    CatalogReader withSchemaPath(List<String> schemaPath);

    PreparingTable getTable(List<String> names);
  }

  /** Definition of a table, for the purposes of the validator and planner. */
  public interface PreparingTable
      extends RelOptTable, SqlValidatorTable {
  }

  /**
   * PreparedExplanation is a PreparedResult for an EXPLAIN PLAN statement.
   * It's always good to have an explanation prepared.
   */
  public abstract static class PreparedExplain
      implements PreparedResult {
    private final RelDataType rowType;
    private final RelDataType parameterRowType;
    private final RelNode rel;
    private final boolean asXml;
    private final SqlExplainLevel detailLevel;

    public PreparedExplain(
        RelDataType rowType,
        RelDataType parameterRowType,
        RelNode rel,
        boolean asXml,
        SqlExplainLevel detailLevel) {
      this.rowType = rowType;
      this.parameterRowType = parameterRowType;
      this.rel = rel;
      this.asXml = asXml;
      this.detailLevel = detailLevel;
    }

    public String getCode() {
      if (rel == null) {
        return RelOptUtil.dumpType(rowType);
      } else {
        return RelOptUtil.dumpPlan("", rel, asXml, detailLevel);
      }
    }

    public RelDataType getParameterRowType() {
      return parameterRowType;
    }

    public boolean isDml() {
      return false;
    }

    public TableModificationRel.Operation getTableModOp() {
      return null;
    }

    public List<List<String>> getFieldOrigins() {
      return Collections.singletonList(Collections.<String>nCopies(
          4, null));
    }

    public RelNode getRel() {
      return rel;
    }

    public abstract Bindable getBindable();
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
    TableModificationRel.Operation getTableModOp();

    /**
     * Returns a list describing, for each result field, the origin of the
     * field as a 4-element list of (database, schema, table, column).
     */
    List<List<String>> getFieldOrigins();

    /**
     * Returns a record type whose fields are the parameters of this statement.
     */
    RelDataType getParameterRowType();

    /**
     * Executes the prepared result.
     *
     * @return producer of rows resulting from execution
     */
    Bindable getBindable();
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
    protected final TableModificationRel.Operation tableModOp;
    protected final List<List<String>> fieldOrigins;

    public PreparedResultImpl(
        RelDataType rowType,
        RelDataType parameterRowType,
        List<List<String>> fieldOrigins,
        RelNode rootRel,
        TableModificationRel.Operation tableModOp,
        boolean isDml) {
      assert rowType != null;
      assert parameterRowType != null;
      assert fieldOrigins != null;
      assert rootRel != null;
      this.rowType = rowType;
      this.parameterRowType = parameterRowType;
      this.fieldOrigins = fieldOrigins;
      this.rootRel = rootRel;
      this.tableModOp = tableModOp;
      this.isDml = isDml;
    }

    public boolean isDml() {
      return isDml;
    }

    public TableModificationRel.Operation getTableModOp() {
      return tableModOp;
    }

    public List<List<String>> getFieldOrigins() {
      return fieldOrigins;
    }

    public RelDataType getParameterRowType() {
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

    public abstract Type getElementType();

    public RelNode getRootRel() {
      return rootRel;
    }

    public abstract Bindable getBindable();
  }

  /** Describes that a given SQL query is materialized by a given table.
   * The materialization is currently valid, and can be used in the planning
   * process. */
  public static class Materialization {
    /** The table that holds the materialized data. */
    final OptiqSchema.TableEntry materializedTable;
    /** The query that derives the data. */
    final String sql;
    /** Relational expression for the table. Usually a
     * {@link TableAccessRel}. */
    RelNode tableRel;
    /** Relational expression for the query to populate the table. */
    RelNode queryRel;
    /** Star table identified. */
    private RelOptTable starRelOptTable;

    public Materialization(OptiqSchema.TableEntry materializedTable,
        String sql) {
      assert materializedTable != null;
      assert sql != null;
      this.materializedTable = materializedTable;
      this.sql = sql;
    }

    public void materialize(RelNode queryRel,
        RelOptTable starRelOptTable) {
      this.queryRel = queryRel;
      this.starRelOptTable = starRelOptTable;
      assert starRelOptTable.unwrap(StarTable.class) != null;
    }
  }
}

// End Prepare.java
