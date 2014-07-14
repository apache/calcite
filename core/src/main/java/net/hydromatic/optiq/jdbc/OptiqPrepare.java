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
package net.hydromatic.optiq.jdbc;

import net.hydromatic.avatica.*;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.ClassDeclaration;
import net.hydromatic.linq4j.function.Function0;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.config.OptiqConnectionConfig;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.prepare.OptiqPrepareImpl;
import net.hydromatic.optiq.runtime.*;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.util.Stacks;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.*;

/**
 * API for a service that prepares statements for execution.
 */
public interface OptiqPrepare {
  Function0<OptiqPrepare> DEFAULT_FACTORY =
      new Function0<OptiqPrepare>() {
        public OptiqPrepare apply() {
          return new OptiqPrepareImpl();
        }
      };
  ThreadLocal<ArrayList<Context>> THREAD_CONTEXT_STACK =
      new ThreadLocal<ArrayList<Context>>() {
        @Override
        protected ArrayList<Context> initialValue() {
          return new ArrayList<Context>();
        }
      };

  ParseResult parse(Context context, String sql);

  ConvertResult convert(Context context, String sql);

  <T> PrepareResult<T> prepareSql(
      Context context,
      String sql,
      Queryable<T> expression,
      Type elementType,
      int maxRowCount);

  <T> PrepareResult<T> prepareQueryable(
      Context context,
      Queryable<T> queryable);

  /** Context for preparing a statement. */
  interface Context {
    JavaTypeFactory getTypeFactory();

    OptiqRootSchema getRootSchema();

    List<String> getDefaultSchemaPath();

    OptiqConnectionConfig config();

    /** Returns the spark handler. Never null. */
    SparkHandler spark();

    DataContext getDataContext();
  }

  /** Callback to register Spark as the main engine. */
  public interface SparkHandler {
    RelNode flattenTypes(RelOptPlanner planner, RelNode rootRel,
        boolean restructure);

    void registerRules(RuleSetBuilder builder);

    boolean enabled();

    Bindable compile(ClassDeclaration expr, String s);

    Object sparkContext();

    /** Allows Spark to declare the rules it needs. */
    interface RuleSetBuilder {
      void addRule(RelOptRule rule);
      void removeRule(RelOptRule rule);
    }
  }

  /** Namespace that allows us to define non-abstract methods inside an
   * interface. */
  public static class Dummy {
    private static SparkHandler sparkHandler;

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
            Class.forName("net.hydromatic.optiq.impl.spark.SparkHandlerImpl");
        Method method = clazz.getMethod("instance");
        return (OptiqPrepare.SparkHandler) method.invoke(null);
      } catch (ClassNotFoundException e) {
        return new TrivialSparkHandler();
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      } catch (ClassCastException e) {
        throw new RuntimeException(e);
      } catch (NoSuchMethodException e) {
        throw new RuntimeException(e);
      } catch (InvocationTargetException e) {
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

      public Bindable compile(ClassDeclaration expr, String s) {
        throw new UnsupportedOperationException();
      }

      public Object sparkContext() {
        throw new UnsupportedOperationException();
      }
    }
  }

  /** The result of parsing and validating a SQL query. */
  public static class ParseResult {
    public final OptiqPrepareImpl prepare;
    public final String sql; // for debug
    public final SqlNode sqlNode;
    public final RelDataType rowType;
    public final RelDataTypeFactory typeFactory;

    public ParseResult(OptiqPrepareImpl prepare, SqlValidator validator,
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
  public static class ConvertResult extends ParseResult {
    public final RelNode relNode;

    public ConvertResult(OptiqPrepareImpl prepare, SqlValidator validator,
        String sql, SqlNode sqlNode, RelDataType rowType, RelNode relNode) {
      super(prepare, validator, sql, sqlNode, rowType);
      this.relNode = relNode;
    }
  }

  /** The result of preparing a query. It gives the Avatica driver framework
   * the information it needs to create a prepared statement, or to execute a
   * statement directly, without an explicit prepare step. */
  public static class PrepareResult<T> implements AvaticaPrepareResult {
    public final String sql; // for debug
    public final List<AvaticaParameter> parameterList;
    public final RelDataType rowType;
    public final ColumnMetaData.StructType structType;
    private final int maxRowCount;
    private final Bindable<T> bindable;
    public final Class resultClazz;

    public PrepareResult(String sql,
        List<AvaticaParameter> parameterList,
        RelDataType rowType,
        ColumnMetaData.StructType structType,
        int maxRowCount,
        Bindable<T> bindable,
        Class resultClazz) {
      super();
      this.sql = sql;
      this.parameterList = parameterList;
      this.rowType = rowType;
      this.structType = structType;
      this.maxRowCount = maxRowCount;
      this.bindable = bindable;
      this.resultClazz = resultClazz;
    }

    public Cursor createCursor(DataContext dataContext) {
      Enumerator<?> enumerator = enumerator(dataContext);
      //noinspection unchecked
      return structType.columns.size() == 1
          ? new ObjectEnumeratorCursor((Enumerator) enumerator)
          : resultClazz != null && !resultClazz.isArray()
          ? new RecordEnumeratorCursor((Enumerator) enumerator, resultClazz)
          : new ArrayEnumeratorCursor((Enumerator) enumerator);
    }

    public List<ColumnMetaData> getColumnList() {
      return structType.columns;
    }

    public List<AvaticaParameter> getParameterList() {
      return parameterList;
    }

    public String getSql() {
      return sql;
    }

    private Enumerable<T> getEnumerable(DataContext dataContext) {
      Enumerable<T> enumerable = bindable.bind(dataContext);
      if (maxRowCount >= 0) {
        // Apply limit. In JDBC 0 means "no limit". But for us, -1 means
        // "no limit", and 0 is a valid limit.
        enumerable = EnumerableDefaults.take(enumerable, maxRowCount);
      }
      return enumerable;
    }

    public Enumerator<T> enumerator(DataContext dataContext) {
      return getEnumerable(dataContext).enumerator();
    }

    public Iterator<T> iterator(DataContext dataContext) {
      return getEnumerable(dataContext).iterator();
    }
  }
}

// End OptiqPrepare.java
