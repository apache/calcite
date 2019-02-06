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
package org.apache.calcite.jdbc;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.CyclicDefinitionException;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.RelRunner;
import org.apache.calcite.util.ImmutableIntList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;

/**
 * API for a service that prepares statements for execution.
 */
public interface CalcitePrepare {
  Function0<CalcitePrepare> DEFAULT_FACTORY = CalcitePrepareImpl::new;
  ThreadLocal<Deque<Context>> THREAD_CONTEXT_STACK =
      ThreadLocal.withInitial(ArrayDeque::new);

  ParseResult parse(Context context, String sql);

  ConvertResult convert(Context context, String sql);

  /** Executes a DDL statement.
   *
   * <p>The statement identified itself as DDL in the
   * {@link org.apache.calcite.jdbc.CalcitePrepare.ParseResult#kind} field. */
  void executeDdl(Context context, SqlNode node);

  /** Analyzes a view.
   *
   * @param context Context
   * @param sql View SQL
   * @param fail Whether to fail (and throw a descriptive error message) if the
   *             view is not modifiable
   * @return Result of analyzing the view
   */
  AnalyzeViewResult analyzeView(Context context, String sql, boolean fail);

  <T> CalciteSignature<T> prepareSql(
      Context context,
      Query<T> query,
      Type elementType,
      long maxRowCount);

  <T> CalciteSignature<T> prepareQueryable(
      Context context,
      Queryable<T> queryable);

  /** Context for preparing a statement. */
  interface Context {
    JavaTypeFactory getTypeFactory();

    /** Returns the root schema for statements that need a read-consistent
     * snapshot. */
    CalciteSchema getRootSchema();

    /** Returns the root schema for statements that need to be able to modify
     * schemas and have the results available to other statements. Viz, DDL
     * statements. */
    CalciteSchema getMutableRootSchema();

    List<String> getDefaultSchemaPath();

    CalciteConnectionConfig config();

    /** Returns the spark handler. Never null. */
    SparkHandler spark();

    DataContext getDataContext();

    /** Returns the path of the object being analyzed, or null.
     *
     * <p>The object is being analyzed is typically a view. If it is already
     * being analyzed further up the stack, the view definition can be deduced
     * to be cyclic. */
    List<String> getObjectPath();

    /** Gets a runner; it can execute a relational expression. */
    RelRunner getRelRunner();
  }

  /** Callback to register Spark as the main engine. */
  interface SparkHandler {
    RelNode flattenTypes(RelOptPlanner planner, RelNode rootRel,
        boolean restructure);

    void registerRules(RuleSetBuilder builder);

    boolean enabled();

    ArrayBindable compile(ClassDeclaration expr, String s);

    Object sparkContext();

    /** Allows Spark to declare the rules it needs. */
    interface RuleSetBuilder {
      void addRule(RelOptRule rule);
      void removeRule(RelOptRule rule);
    }
  }

  /** Namespace that allows us to define non-abstract methods inside an
   * interface. */
  class Dummy {
    private static SparkHandler sparkHandler;

    private Dummy() {}

    /** Returns a spark handler. Returns a trivial handler, for which
     * {@link SparkHandler#enabled()} returns {@code false}, if {@code enable}
     * is {@code false} or if Spark is not on the class path. Never returns
     * null. */
    public static synchronized SparkHandler getSparkHandler(boolean enable) {
      if (sparkHandler == null) {
        sparkHandler = enable ? createHandler() : new TrivialSparkHandler();
      }
      return sparkHandler;
    }

    private static SparkHandler createHandler() {
      try {
        final Class<?> clazz =
            Class.forName("org.apache.calcite.adapter.spark.SparkHandlerImpl");
        Method method = clazz.getMethod("instance");
        return (CalcitePrepare.SparkHandler) method.invoke(null);
      } catch (ClassNotFoundException e) {
        return new TrivialSparkHandler();
      } catch (IllegalAccessException
          | ClassCastException
          | InvocationTargetException
          | NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
    }

    public static void push(Context context) {
      final Deque<Context> stack = THREAD_CONTEXT_STACK.get();
      final List<String> path = context.getObjectPath();
      if (path != null) {
        for (Context context1 : stack) {
          final List<String> path1 = context1.getObjectPath();
          if (path.equals(path1)) {
            throw new CyclicDefinitionException(stack.size(), path);
          }
        }
      }
      stack.push(context);
    }

    public static Context peek() {
      return THREAD_CONTEXT_STACK.get().peek();
    }

    public static void pop(Context context) {
      Context x = THREAD_CONTEXT_STACK.get().pop();
      assert x == context;
    }

    /** Implementation of {@link SparkHandler} that either does nothing or
     * throws for each method. Use this if Spark is not installed. */
    private static class TrivialSparkHandler implements SparkHandler {
      public RelNode flattenTypes(RelOptPlanner planner, RelNode rootRel,
          boolean restructure) {
        return rootRel;
      }

      public void registerRules(RuleSetBuilder builder) {
      }

      public boolean enabled() {
        return false;
      }

      public ArrayBindable compile(ClassDeclaration expr, String s) {
        throw new UnsupportedOperationException();
      }

      public Object sparkContext() {
        throw new UnsupportedOperationException();
      }
    }

  }

  /** The result of parsing and validating a SQL query. */
  class ParseResult {
    public final CalcitePrepareImpl prepare;
    public final String sql; // for debug
    public final SqlNode sqlNode;
    public final RelDataType rowType;
    public final RelDataTypeFactory typeFactory;

    public ParseResult(CalcitePrepareImpl prepare, SqlValidator validator,
        String sql,
        SqlNode sqlNode, RelDataType rowType) {
      super();
      this.prepare = prepare;
      this.sql = sql;
      this.sqlNode = sqlNode;
      this.rowType = rowType;
      this.typeFactory = validator.getTypeFactory();
    }

    /** Returns the kind of statement.
     *
     * <p>Possibilities include:
     *
     * <ul>
     *   <li>Queries: usually {@link SqlKind#SELECT}, but
     *   other query operators such as {@link SqlKind#UNION} and
     *   {@link SqlKind#ORDER_BY} are possible
     *   <li>DML statements: {@link SqlKind#INSERT}, {@link SqlKind#UPDATE} etc.
     *   <li>Session control statements: {@link SqlKind#COMMIT}
     *   <li>DDL statements: {@link SqlKind#CREATE_TABLE},
     *   {@link SqlKind#DROP_INDEX}
     * </ul>
     *
     * @return Kind of statement, never null
     */
    public SqlKind kind() {
      return sqlNode.getKind();
    }
  }

  /** The result of parsing and validating a SQL query and converting it to
   * relational algebra. */
  class ConvertResult extends ParseResult {
    public final RelRoot root;

    public ConvertResult(CalcitePrepareImpl prepare, SqlValidator validator,
        String sql, SqlNode sqlNode, RelDataType rowType, RelRoot root) {
      super(prepare, validator, sql, sqlNode, rowType);
      this.root = root;
    }
  }

  /** The result of analyzing a view. */
  class AnalyzeViewResult extends ConvertResult {
    /** Not null if and only if the view is modifiable. */
    public final Table table;
    public final ImmutableList<String> tablePath;
    public final RexNode constraint;
    public final ImmutableIntList columnMapping;
    public final boolean modifiable;

    public AnalyzeViewResult(CalcitePrepareImpl prepare,
        SqlValidator validator, String sql, SqlNode sqlNode,
        RelDataType rowType, RelRoot root, Table table,
        ImmutableList<String> tablePath, RexNode constraint,
        ImmutableIntList columnMapping, boolean modifiable) {
      super(prepare, validator, sql, sqlNode, rowType, root);
      this.table = table;
      this.tablePath = tablePath;
      this.constraint = constraint;
      this.columnMapping = columnMapping;
      this.modifiable = modifiable;
      Preconditions.checkArgument(modifiable == (table != null));
    }
  }

  /** The result of preparing a query. It gives the Avatica driver framework
   * the information it needs to create a prepared statement, or to execute a
   * statement directly, without an explicit prepare step.
   *
   * @param <T> element type */
  class CalciteSignature<T> extends Meta.Signature {
    @JsonIgnore public final RelDataType rowType;
    @JsonIgnore public final CalciteSchema rootSchema;
    @JsonIgnore private final List<RelCollation> collationList;
    private final long maxRowCount;
    private final Bindable<T> bindable;

    @Deprecated // to be removed before 2.0
    public CalciteSignature(String sql, List<AvaticaParameter> parameterList,
        Map<String, Object> internalParameters, RelDataType rowType,
        List<ColumnMetaData> columns, Meta.CursorFactory cursorFactory,
        CalciteSchema rootSchema, List<RelCollation> collationList,
        long maxRowCount, Bindable<T> bindable) {
      this(sql, parameterList, internalParameters, rowType, columns,
          cursorFactory, rootSchema, collationList, maxRowCount, bindable,
          null);
    }

    public CalciteSignature(String sql,
        List<AvaticaParameter> parameterList,
        Map<String, Object> internalParameters,
        RelDataType rowType,
        List<ColumnMetaData> columns,
        Meta.CursorFactory cursorFactory,
        CalciteSchema rootSchema,
        List<RelCollation> collationList,
        long maxRowCount,
        Bindable<T> bindable,
        Meta.StatementType statementType) {
      super(columns, sql, parameterList, internalParameters, cursorFactory,
          statementType);
      this.rowType = rowType;
      this.rootSchema = rootSchema;
      this.collationList = collationList;
      this.maxRowCount = maxRowCount;
      this.bindable = bindable;
    }

    public Enumerable<T> enumerable(DataContext dataContext) {
      Enumerable<T> enumerable = bindable.bind(dataContext);
      if (maxRowCount >= 0) {
        // Apply limit. In JDBC 0 means "no limit". But for us, -1 means
        // "no limit", and 0 is a valid limit.
        enumerable = EnumerableDefaults.take(enumerable, maxRowCount);
      }
      return enumerable;
    }

    public List<RelCollation> getCollationList() {
      return collationList;
    }
  }

  /** A union type of the three possible ways of expressing a query: as a SQL
   * string, a {@link Queryable} or a {@link RelNode}. Exactly one must be
   * provided.
   *
   * @param <T> element type */
  class Query<T> {
    public final String sql;
    public final Queryable<T> queryable;
    public final RelNode rel;

    private Query(String sql, Queryable<T> queryable, RelNode rel) {
      this.sql = sql;
      this.queryable = queryable;
      this.rel = rel;

      assert (sql == null ? 0 : 1)
          + (queryable == null ? 0 : 1)
          + (rel == null ? 0 : 1) == 1;
    }

    public static <T> Query<T> of(String sql) {
      return new Query<>(sql, null, null);
    }

    public static <T> Query<T> of(Queryable<T> queryable) {
      return new Query<>(null, queryable, null);
    }

    public static <T> Query<T> of(RelNode rel) {
      return new Query<>(null, null, rel);
    }
  }
}

// End CalcitePrepare.java
