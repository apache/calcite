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
package net.hydromatic.optiq.jdbc;

import net.hydromatic.avatica.*;

import net.hydromatic.linq4j.*;
import net.hydromatic.linq4j.expressions.ClassDeclaration;
import net.hydromatic.linq4j.function.Function0;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.prepare.OptiqPrepareImpl;
import net.hydromatic.optiq.runtime.*;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.volcano.VolcanoPlanner;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.SqlNode;

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

  ParseResult parse(
      Context context, String sql);

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

    Schema getRootSchema();

    List<String> getDefaultSchemaPath();

    ConnectionConfig config();

    /** Returns the spark handler. Never null. */
    SparkHandler spark();
  }

  /** Callback to register Spark as the main engine. */
  public interface SparkHandler {
    RelNode flattenTypes(RelOptPlanner planner, RelNode rootRel,
        boolean restructure);

    void registerRules(VolcanoPlanner planner);

    boolean enabled();

    Bindable compile(ClassDeclaration expr, String s);

    Object sparkContext();
  }

  public static class Dummy {
    private static SparkHandler handler;
    public static synchronized SparkHandler getSparkHandler() {
      if (handler == null) {
        handler = createHandler();
      }
      return handler;
    }

    private static SparkHandler createHandler() {
      try {
        final Class<?> clazz =
            Class.forName("net.hydromatic.optiq.impl.spark.SparkHandlerImpl");
        Method method = clazz.getMethod("INSTANCE");
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

    private static class TrivialSparkHandler implements SparkHandler {
      public RelNode flattenTypes(RelOptPlanner planner, RelNode rootRel,
          boolean restructure) {
        return rootRel;
      }

      public void registerRules(VolcanoPlanner planner) {
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

  public static class ParseResult {
    public final String sql; // for debug
    public final SqlNode sqlNode;
    public final RelDataType rowType;

    public ParseResult(String sql, SqlNode sqlNode, RelDataType rowType) {
      super();
      this.sql = sql;
      this.sqlNode = sqlNode;
      this.rowType = rowType;
    }
  }

  public static class PrepareResult<T> implements AvaticaPrepareResult {
    public final String sql; // for debug
    public final List<AvaticaParameter> parameterList;
    public final RelDataType rowType;
    public final List<ColumnMetaData> columnList;
    private final int maxRowCount;
    private final Bindable<T> bindable;
    public final Class resultClazz;

    public PrepareResult(String sql,
        List<AvaticaParameter> parameterList,
        RelDataType rowType,
        List<ColumnMetaData> columnList,
        int maxRowCount,
        Bindable<T> bindable,
        Class resultClazz) {
      super();
      this.sql = sql;
      this.parameterList = parameterList;
      this.rowType = rowType;
      this.columnList = columnList;
      this.maxRowCount = maxRowCount;
      this.bindable = bindable;
      this.resultClazz = resultClazz;
    }

    public Cursor createCursor(DataContext dataContext) {
      Enumerator<?> enumerator = enumerator(dataContext);
      //noinspection unchecked
      return columnList.size() == 1
          ? new ObjectEnumeratorCursor((Enumerator) enumerator)
          : resultClazz != null && !resultClazz.isArray()
          ? new RecordEnumeratorCursor((Enumerator) enumerator, resultClazz)
          : new ArrayEnumeratorCursor((Enumerator) enumerator);
    }

    public List<ColumnMetaData> getColumnList() {
      return columnList;
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
