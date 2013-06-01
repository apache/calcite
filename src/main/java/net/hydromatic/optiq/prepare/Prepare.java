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

import net.hydromatic.optiq.runtime.Typed;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.sql2rel.SqlToRelConverter;
import org.eigenbase.trace.EigenbaseTimingTracer;
import org.eigenbase.trace.EigenbaseTrace;

import java.lang.reflect.Type;
import java.util.*;
import java.util.logging.Logger;


/**
 * Abstract base for classes that implement
 * the process of preparing and executing SQL expressions.
 */
public abstract class Prepare {
  public static final String connectionVariable = "connection";
  protected static final Logger tracer = EigenbaseTrace.getStatementTracer();
  protected final CatalogReader catalogReader;
  protected String queryString = null;
  /**
   * Convention via which results should be returned by execution.
   */
  protected final Convention resultConvention;
  protected EigenbaseTimingTracer timingTracer;
  protected List<List<String>> fieldOrigins;

  public Prepare(CatalogReader catalogReader, Convention resultConvention) {
    this.catalogReader = catalogReader;
    this.resultConvention = resultConvention;
  }

  protected abstract PreparedResult createPreparedExplanation(
      RelDataType resultType,
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
   * @return an equivalent optimized relational expression
   */
  protected RelNode optimize(
      RelDataType logicalRowType,
      RelNode rootRel) {
    final RelOptPlanner planner = rootRel.getCluster().getPlanner();

    // Allow each rel to register its own rules.
    RelVisitor visitor = new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        planner.registerClass(node);
        super.visit(node, ordinal, parent);
      }
    };
    visitor.go(rootRel);

    planner.setRoot(rootRel);

    RelTraitSet desiredTraits = getDesiredRootTraitSet(rootRel);

    final RelNode rootRel2 = planner.changeTraits(rootRel, desiredTraits);
    assert rootRel2 != null;

    planner.setRoot(rootRel2);
    final RelOptPlanner planner2 = planner.chooseDelegate();
    final RelNode rootRel3 = planner2.findBestExp();
    assert rootRel3 != null : "could not implement exp";
    return rootRel3;
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
      Class runtimeContextClass,
      SqlValidator validator,
      boolean needsValidation) {
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

    if (timingTracer != null) {
      timingTracer.traceTime("end sql2rel");
    }

    final RelDataType resultType = validator.getValidatedNodeType(sqlQuery);
    fieldOrigins = validator.getFieldOrigins(sqlQuery);
    assert fieldOrigins.size() == resultType.getFieldCount();

    // Display logical plans before view expansion, plugging in physical
    // storage and decorrelation
    if (sqlExplain != null) {
      SqlExplain.Depth explainDepth = sqlExplain.getDepth();
      boolean explainAsXml = sqlExplain.isXml();
      SqlExplainLevel detailLevel = sqlExplain.getDetailLevel();
      switch (explainDepth) {
      case Type:
        return createPreparedExplanation(
            resultType, null, explainAsXml, detailLevel);
      case Logical:
        return createPreparedExplanation(
            null, rootRel, explainAsXml, detailLevel);
      default:
      }
    }

    // Structured type flattening, view expansion, and plugging in physical
    // storage.
    rootRel = flattenTypes(rootRel, true);

    // Subquery decorrelation.
    rootRel = decorrelate(sqlQuery, rootRel);

    // Trim unused fields.
    rootRel = trimUnusedFields(rootRel);

    // Display physical plan after decorrelation.
    if (sqlExplain != null) {
      SqlExplain.Depth explainDepth = sqlExplain.getDepth();
      boolean explainAsXml = sqlExplain.isXml();
      SqlExplainLevel detailLevel = sqlExplain.getDetailLevel();
      switch (explainDepth) {
      case Physical:
      default:
        rootRel =
            optimize(
                rootRel.getRowType(),
                rootRel);
        return createPreparedExplanation(
            null, rootRel, explainAsXml, detailLevel);
      }
    }

    rootRel = optimize(resultType, rootRel);

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

  protected abstract RelNode decorrelate(
      SqlNode query,
      RelNode rootRel);

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
    return getSqlToRelConverter(
        getSqlValidator(),
        catalogReader).trimUnusedFields(rootRel);
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

  public interface CatalogReader
      extends RelOptSchema, SqlValidatorCatalogReader {
    PreparingTable getTableForMember(String[] names);

    PreparingTable getTable(String[] names);
  }

  public interface PreparingTable
      extends RelOptTable, SqlValidatorTable {
  }

  /**
   * PreparedExplanation is a PreparedResult for an EXPLAIN PLAN statement.
   * It's always good to have an explanation prepared.
   */
  public static abstract class PreparedExplain
      implements PreparedResult {
    private final RelDataType rowType;
    private final RelNode rel;
    private final boolean asXml;
    private final SqlExplainLevel detailLevel;

    public PreparedExplain(
        RelDataType rowType,
        RelNode rel,
        boolean asXml,
        SqlExplainLevel detailLevel) {
      this.rowType = rowType;
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

    public abstract Object execute();
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
     * Executes the prepared result.
     *
     * @return producer of rows resulting from execution
     */
    Object execute();
  }

  /**
   * Abstract implementation of {@link PreparedResult}.
   */
  public static abstract class PreparedResultImpl
      implements PreparedResult, Typed {
    protected final RelNode rootRel;
    protected final RelDataType rowType;
    protected final boolean isDml;
    protected final TableModificationRel.Operation tableModOp;
    protected final List<List<String>> fieldOrigins;

    public PreparedResultImpl(
        RelDataType rowType,
        List<List<String>> fieldOrigins,
        RelNode rootRel,
        TableModificationRel.Operation tableModOp,
        boolean isDml) {
      this.rowType = rowType;
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

    public abstract Object execute();
  }
}

// End Prepare.java
