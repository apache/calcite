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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Stacks;

import com.fasterxml.jackson.annotation.JsonIgnore;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * API for a service that prepares statements for execution.
 */
public interface CalcitePrepare {
  Function0<CalcitePrepare> DEFAULT_FACTORY =
      new Function0<CalcitePrepare>() {
        public CalcitePrepare apply() {
          return new CalcitePrepareImpl();
        }
      };
  ThreadLocal<ArrayList<Context>> THREAD_CONTEXT_STACK =
      new ThreadLocal<ArrayList<Context>>() {
        @Override protected ArrayList<Context> initialValue() {
          return new ArrayList<>();
        }
      };

  ParseResult parse(Context context, String sql);

  ConvertResult convert(Context context, String sql);

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
      String sql,
      Queryable<T> expression,
      Type elementType,
      int maxRowCount);

  <T> CalciteSignature<T> prepareQueryable(
      Context context,
      Queryable<T> queryable);

  /** Context for preparing a statement. */
  interface Context {
    JavaTypeFactory getTypeFactory();

    CalciteRootSchema getRootSchema();

    List<String> getDefaultSchemaPath();

    CalciteConnectionConfig config();

    /** Returns the spark handler. Never null. */
    SparkHandler spark();

    DataContext getDataContext();
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
      Stacks.push(THREAD_CONTEXT_STACK.get(), context);
    }

    public static Context peek() {
      return Stacks.peek(THREAD_CONTEXT_STACK.get());
    }

    public static void pop(Context context) {
      Stacks.pop(THREAD_CONTEXT_STACK.get(), context);
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
  }

  /** The result of parsing and validating a SQL query and converting it to
   * relational algebra. */
  class ConvertResult extends ParseResult {
    public final RelNode relNode;

    public ConvertResult(CalcitePrepareImpl prepare, SqlValidator validator,
        String sql, SqlNode sqlNode, RelDataType rowType, RelNode relNode) {
      super(prepare, validator, sql, sqlNode, rowType);
      this.relNode = relNode;
    }
  }

  /** The result of analyzing a view. */
  class AnalyzeViewResult extends ConvertResult {
    /** Not null if and only if the view is modifiable. */
    public final Table table;
    public final ImmutableList<String> tablePath;
    public final RexNode constraint;
    public final ImmutableIntList columnMapping;

    public AnalyzeViewResult(CalcitePrepareImpl prepare,
        SqlValidator validator, String sql, SqlNode sqlNode,
        RelDataType rowType, RelNode relNode, Table table,
        ImmutableList<String> tablePath, RexNode constraint,
        ImmutableIntList columnMapping) {
      super(prepare, validator, sql, sqlNode, rowType, relNode);
      this.table = table;
      this.tablePath = tablePath;
      this.constraint = constraint;
      this.columnMapping = columnMapping;
    }
  }

  /** The result of preparing a query. It gives the Avatica driver framework
   * the information it needs to create a prepared statement, or to execute a
   * statement directly, without an explicit prepare step. */
  class CalciteSignature<T> extends Meta.Signature {
    @JsonIgnore public final RelDataType rowType;
    private final int maxRowCount;
    private final Bindable<T> bindable;

    public CalciteSignature(String sql,
        List<AvaticaParameter> parameterList,
        Map<String, Object> internalParameters,
        RelDataType rowType,
        List<ColumnMetaData> columns,
        Meta.CursorFactory cursorFactory,
        int maxRowCount,
        Bindable<T> bindable) {
      super(columns, sql, parameterList, internalParameters, cursorFactory);
      this.rowType = rowType;
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
  }
}

// End CalcitePrepare.java
