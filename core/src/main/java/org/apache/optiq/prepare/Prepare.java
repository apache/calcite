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
package org.apache.optiq.prepare;

import org.apache.optiq.DataContext;
import org.apache.optiq.impl.StarTable;
import org.apache.optiq.jdbc.OptiqPrepare;
import org.apache.optiq.jdbc.OptiqSchema;
import org.apache.optiq.impl.enumerable.JavaRules;
import org.apache.optiq.runtime.Bindable;
import org.apache.optiq.runtime.Hook;
import org.apache.optiq.runtime.Typed;
import org.apache.optiq.tools.Program;
import org.apache.optiq.tools.Programs;

import org.apache.optiq.rel.*;
import org.apache.optiq.rel.metadata.*;
import org.apache.optiq.rel.rules.*;
import org.apache.optiq.relopt.*;
import org.apache.optiq.reltype.RelDataType;
import org.apache.optiq.rex.RexBuilder;
import org.apache.optiq.rex.RexExecutorImpl;
import org.apache.optiq.sql.*;
import org.apache.optiq.sql.validate.*;
import org.apache.optiq.sql2rel.SqlToRelConverter;
import org.apache.optiq.trace.EigenbaseTimingTracer;
import org.apache.optiq.trace.EigenbaseTrace;

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

  private static final ImmutableList<RelOptRule> CALC_RULES =
      ImmutableList.of(
          JavaRules.ENUMERABLE_CALC_RULE,
          JavaRules.ENUMERABLE_FILTER_TO_CALC_RULE,
          JavaRules.ENUMERABLE_PROJECT_TO_CALC_RULE,
          MergeCalcRule.INSTANCE,
          MergeFilterOntoCalcRule.INSTANCE,
          MergeProjectOntoCalcRule.INSTANCE,
          FilterToCalcRule.INSTANCE,
          ProjectToCalcRule.INSTANCE,
          MergeCalcRule.INSTANCE,

          // REVIEW jvs 9-Apr-2006: Do we still need these two?  Doesn't the
          // combination of MergeCalcRule, FilterToCalcRule, and
          // ProjectToCalcRule have the same effect?
          MergeFilterOntoCalcRule.INSTANCE,
          MergeProjectOntoCalcRule.INSTANCE);

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
   * @return an equivalent optimized relational expression
   */
  protected RelNode optimize(RelDataType logicalRowType, final RelNode rootRel,
      final List<Materialization> materializations) {
    final RelOptPlanner planner = rootRel.getCluster().getPlanner();

    planner.setRoot(rootRel);

    final RelTraitSet desiredTraits = getDesiredRootTraitSet(rootRel);
    final Program program1 =
        new Program() {
          public RelNode run(RelOptPlanner planner, RelNode rel,
              RelTraitSet requiredOutputTraits) {
            final DataContext dataContext = context.getDataContext();
            planner.setExecutor(new RexExecutorImpl(dataContext));

            for (Materialization materialization : materializations) {
              planner.addMaterialization(
                  new RelOptMaterialization(materialization.tableRel,
                      materialization.queryRel,
                      materialization.starRelOptTable));
            }

            final RelNode rootRel2 =
                planner.changeTraits(rel, requiredOutputTraits);
            assert rootRel2 != null;

            planner.setRoot(rootRel2);
            final RelOptPlanner planner2 = planner.chooseDelegate();
            final RelNode rootRel3 = planner2.findBestExp();
            assert rootRel3 != null : "could not implement exp";
            return rootRel3;
          }
        };

    final RelNode rootRel3 = program1.run(planner, rootRel, desiredTraits);

    // Second planner pass to do physical "tweaks". This the first time that
    // EnumerableCalcRel is introduced.
    final Program program2 =
        Programs.hep(CALC_RULES, true, new DefaultRelMetadataProvider());
    final RelNode rootRel4 = program2.run(null, rootRel3, null);
    if (LOGGER.isLoggable(Level.FINE)) {
      LOGGER.fine(
          "Plan after physical tweaks: "
          + RelOptUtil.toString(rootRel4, SqlExplainLevel.ALL_ATTRIBUTES));
    }

    return rootRel4;
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
      List<Materialization> materializations) {
    return prepareSql(
        sqlQuery,
        sqlQuery,
        runtimeContextClass,
        validator,
        needsValidation,
        materializations);
  }

  public PreparedResult prepareSql(
      SqlNode sqlQuery,
      SqlNode sqlNodeOriginal,
      Class runtimeContextClass,
      SqlValidator validator,
      boolean needsValidation,
      List<Materialization> materializations) {
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
        rootRel = optimize(rootRel.getRowType(), rootRel, materializations);
        return createPreparedExplanation(
            null, parameterRowType, rootRel, explainAsXml, detailLevel);
      }
    }

    rootRel = optimize(resultType, rootRel, materializations);

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
   * {@link org.apache.optiq.rel.RelNode} with a 'slimmed down' relational
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
