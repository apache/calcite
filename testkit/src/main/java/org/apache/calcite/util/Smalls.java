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
package org.apache.calcite.util;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.BaseQueryable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Deterministic;
import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.linq4j.function.SemiStrict;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelJsonReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.schema.FunctionContext;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Holder for various classes and functions used in tests as user-defined
 * functions and so forth.
 */
public class Smalls {
  public static final Method GENERATE_STRINGS_METHOD =
      Types.lookupMethod(Smalls.class, "generateStrings", Integer.class);
  public static final Method GENERATE_STRINGS_OF_INPUT_SIZE_METHOD =
      Types.lookupMethod(Smalls.class, "generateStringsOfInputSize", List.class);
  public static final Method GENERATE_STRINGS_OF_INPUT_MAP_SIZE_METHOD =
      Types.lookupMethod(Smalls.class, "generateStringsOfInputMapSize", Map.class);
  public static final Method MAZE_METHOD =
      Types.lookupMethod(MazeTable.class, "generate", int.class, int.class,
          int.class);
  public static final Method MAZE2_METHOD =
      Types.lookupMethod(MazeTable.class, "generate2", int.class, int.class,
          Integer.class);
  public static final Method MAZE3_METHOD =
      Types.lookupMethod(MazeTable.class, "generate3", String.class);
  public static final Method MULTIPLICATION_TABLE_METHOD =
      Types.lookupMethod(Smalls.class, "multiplicationTable", int.class,
        int.class, Integer.class);
  public static final Method FIBONACCI_TABLE_METHOD =
      Types.lookupMethod(Smalls.class, "fibonacciTable");
  public static final Method FIBONACCI_LIMIT_100_TABLE_METHOD =
      Types.lookupMethod(Smalls.class, "fibonacciTableWithLimit100");
  public static final Method FIBONACCI_LIMIT_TABLE_METHOD =
      Types.lookupMethod(Smalls.class, "fibonacciTableWithLimit", long.class);
  public static final Method FIBONACCI_INSTANCE_TABLE_METHOD =
      Types.lookupMethod(Smalls.FibonacciTableFunction.class, "eval");
  public static final Method DUMMY_TABLE_METHOD_WITH_TWO_PARAMS =
      Types.lookupMethod(Smalls.class, "dummyTableFuncWithTwoParams", long.class, long.class);
  public static final Method DYNAMIC_ROW_TYPE_TABLE_METHOD =
      Types.lookupMethod(Smalls.class, "dynamicRowTypeTable", String.class,
          int.class);
  public static final Method VIEW_METHOD =
      Types.lookupMethod(Smalls.class, "view", String.class);
  public static final Method STR_METHOD =
      Types.lookupMethod(Smalls.class, "str", Object.class, Object.class);
  public static final Method STRING_UNION_METHOD =
      Types.lookupMethod(Smalls.class, "stringUnion", Queryable.class,
          Queryable.class);
  public static final Method PROCESS_CURSOR_METHOD =
      Types.lookupMethod(Smalls.class, "processCursor",
          int.class, Enumerable.class);
  public static final Method PROCESS_CURSORS_METHOD =
      Types.lookupMethod(Smalls.class, "processCursors",
          int.class, Enumerable.class, Enumerable.class);
  public static final Method MY_PLUS_EVAL_METHOD =
      Types.lookupMethod(MyPlusFunction.class, "eval", int.class, int.class);
  public static final Method MY_PLUS_INIT_EVAL_METHOD =
      Types.lookupMethod(MyPlusInitFunction.class, "eval", int.class,
          int.class);

  private Smalls() {}

  private static QueryableTable oneThreePlus(String s) {
    List<Integer> items;
    // Argument is null in case SQL contains function call with expression.
    // Then the engine calls a function with null arguments to get getRowType.
    if (s == null) {
      items = ImmutableList.of();
    } else {
      Integer latest = Integer.parseInt(s.substring(1, s.length() - 1));
      items = ImmutableList.of(1, 3, latest);
    }
    final Enumerable<Integer> enumerable = Linq4j.asEnumerable(items);
    return new AbstractQueryableTable(Integer.class) {
      @Override public <E> Queryable<E> asQueryable(
          QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        //noinspection unchecked
        return (Queryable<E>) enumerable.asQueryable();
      }

      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder().add("c", SqlTypeName.INTEGER).build();
      }
    };
  }

  public static <T> Queryable<T> stringUnion(
      Queryable<T> q0, Queryable<T> q1) {
    return q0.concat(q1);
  }

  /** A function that generates a table that generates a sequence of
   * {@link IntString} values. */
  public static QueryableTable generateStrings(final Integer count) {
    return new AbstractQueryableTable(IntString.class) {
      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.createJavaType(IntString.class);
      }

      @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
          SchemaPlus schema, String tableName) {
        BaseQueryable<IntString> queryable =
            new BaseQueryable<IntString>(null, IntString.class, null) {
              @Override public Enumerator<IntString> enumerator() {
                return new Enumerator<IntString>() {
                  static final String Z = "abcdefghijklm";

                  int i = 0;
                  int curI;
                  String curS;

                  @Override public IntString current() {
                    return new IntString(curI, curS);
                  }

                  @Override public boolean moveNext() {
                    if (i < count) {
                      curI = i;
                      curS = Z.substring(0, i % Z.length());
                      ++i;
                      return true;
                    } else {
                      return false;
                    }
                  }

                  @Override public void reset() {
                    i = 0;
                  }

                  @Override public void close() {
                  }
                };
              }
            };
        //noinspection unchecked
        return (Queryable<T>) queryable;
      }
    };
  }

  public static QueryableTable generateStringsOfInputSize(final List<Integer> list) {
    return generateStrings(list.size());
  }
  public static QueryableTable generateStringsOfInputMapSize(final Map<Integer, Integer> map) {
    return generateStrings(map.size());
  }

  /** A function that generates multiplication table of {@code ncol} columns x
   * {@code nrow} rows. */
  public static QueryableTable multiplicationTable(final int ncol,
      final int nrow, Integer offset) {
    final int offs = offset == null ? 0 : offset;
    return new AbstractQueryableTable(Object[].class) {
      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        final RelDataTypeFactory.Builder builder = typeFactory.builder();
        builder.add("row_name", typeFactory.createJavaType(String.class));
        final RelDataType int_ = typeFactory.createJavaType(int.class);
        for (int i = 1; i <= ncol; i++) {
          builder.add("c" + i, int_);
        }
        return builder.build();
      }

      @Override public Queryable<Object[]> asQueryable(QueryProvider queryProvider,
          SchemaPlus schema, String tableName) {
        final List<Object[]> table = new AbstractList<Object[]>() {
          @Override public Object[] get(int index) {
            Object[] cur = new Object[ncol + 1];
            cur[0] = "row " + index;
            for (int j = 1; j <= ncol; j++) {
              cur[j] = j * (index + 1) + offs;
            }
            return cur;
          }

          @Override public int size() {
            return nrow;
          }
        };
        return Linq4j.asEnumerable(table).asQueryable();
      }
    };
  }

  /** A function that generates the Fibonacci sequence.
   *
   * <p>Interesting because it has one column and no arguments,
   * and because it is infinite. */
  public static ScannableTable fibonacciTable() {
    return fibonacciTableWithLimit(-1L);
  }

  /** A function that generates the first 100 terms of the Fibonacci sequence.
   *
   * <p>Interesting because it has one column and no arguments. */
  public static ScannableTable fibonacciTableWithLimit100() {
    return fibonacciTableWithLimit(100L);
  }

  /** A function that takes 2 param as input. */
  public static ScannableTable dummyTableFuncWithTwoParams(final long param1, final long param2) {
    return new ScannableTable() {
      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder().add("N", SqlTypeName.BIGINT).build();
      }

      @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
        return new AbstractEnumerable<Object[]>() {
          @Override public Enumerator<Object[]> enumerator() {
            return new Enumerator<Object[]>() {
              @Override public Object[] current() {
                return new Object[] {};
              }

              @Override public boolean moveNext() {
                return false;
              }

              @Override public void reset() {
              }

              @Override public void close() {
              }
            };
          }
        };
      }

      @Override public Statistic getStatistic() {
        return Statistics.UNKNOWN;
      }

      @Override public Schema.TableType getJdbcTableType() {
        return Schema.TableType.TABLE;
      }

      @Override public boolean isRolledUp(String column) {
        return false;
      }

      @Override public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
          @Nullable SqlNode parent, @Nullable CalciteConnectionConfig config) {
        return true;
      }
    };
  }

  /** A function that generates the Fibonacci sequence.
   * Interesting because it has one column and no arguments. */
  public static ScannableTable fibonacciTableWithLimit(final long limit) {
    return new ScannableTable() {
      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder().add("N", SqlTypeName.BIGINT).build();
      }

      @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
        return new AbstractEnumerable<Object[]>() {
          @Override public Enumerator<Object[]> enumerator() {
            return new Enumerator<Object[]>() {
              private long prev = 1;
              private long current = 0;

              @Override public Object[] current() {
                return new Object[] {current};
              }

              @Override public boolean moveNext() {
                final long next = current + prev;
                if (limit >= 0 && next > limit) {
                  return false;
                }
                prev = current;
                current = next;
                return true;
              }

              @Override public void reset() {
                prev = 0;
                current = 1;
              }

              @Override public void close() {
              }
            };
          }
        };
      }

      @Override public Statistic getStatistic() {
        return Statistics.UNKNOWN;
      }

      @Override public Schema.TableType getJdbcTableType() {
        return Schema.TableType.TABLE;
      }

      @Override public boolean isRolledUp(String column) {
        return false;
      }

      @Override public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
          @Nullable SqlNode parent, @Nullable CalciteConnectionConfig config) {
        return true;
      }
    };
  }

  public static ScannableTable dynamicRowTypeTable(String jsonRowType,
      int rowCount) {
    return new DynamicRowTypeTable(jsonRowType, rowCount);
  }

  /** A table whose row type is determined by parsing a JSON argument. */
  private static class DynamicRowTypeTable extends AbstractTable
      implements ScannableTable {
    private final String jsonRowType;

    DynamicRowTypeTable(String jsonRowType, int count) {
      this.jsonRowType = jsonRowType;
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      try {
        return RelJsonReader.readType(typeFactory, jsonRowType);
      } catch (IOException e) {
        throw Util.throwAsRuntime(e);
      }
    }

    @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
      return Linq4j.emptyEnumerable();
    }
  }

  /** Table function that adds a number to the first column of input cursor. */
  public static QueryableTable processCursor(final int offset,
      final Enumerable<Object[]> a) {
    return new AbstractQueryableTable(Object[].class) {
      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("result", SqlTypeName.INTEGER)
            .build();
      }

      @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
          SchemaPlus schema, String tableName) {
        final Enumerable<Integer> enumerable =
            a.select(a0 -> offset + ((Integer) a0[0]));
        //noinspection unchecked
        return (Queryable) enumerable.asQueryable();
      }
    };
  }

  /**
   * A function that sums the second column of first input cursor, second
   * column of first input and the given int.
   */
  public static QueryableTable processCursors(final int offset,
      final Enumerable<Object[]> a, final Enumerable<IntString> b) {
    return new AbstractQueryableTable(Object[].class) {
      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("result", SqlTypeName.INTEGER)
            .build();
      }

      @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
          SchemaPlus schema, String tableName) {
        final Enumerable<Integer> enumerable =
            a.zip(b, (v0, v1) -> ((Integer) v0[1]) + v1.n + offset);
        //noinspection unchecked
        return (Queryable) enumerable.asQueryable();
      }
    };
  }

  public static TranslatableTable view(String s) {
    return new ViewTable(Object.class, typeFactory ->
        typeFactory.builder().add("c", SqlTypeName.INTEGER).build(),
        "values (1), (3), " + s, ImmutableList.of(), Arrays.asList("view"));
  }

  public static TranslatableTable strView(String s) {
    return new ViewTable(Object.class, typeFactory ->
        typeFactory.builder().add("c", SqlTypeName.VARCHAR, 100).build(),
        "values (" + CalciteSqlDialect.DEFAULT.quoteStringLiteral(s) + ")",
        ImmutableList.of(), Arrays.asList("view"));
  }

  public static TranslatableTable str(Object o, Object p) {
    assertThat(RexLiteral.validConstant(o, Litmus.THROW), is(true));
    assertThat(RexLiteral.validConstant(p, Litmus.THROW), is(true));
    return new ViewTable(Object.class, typeFactory ->
        typeFactory.builder().add("c", SqlTypeName.VARCHAR, 100).build(),
        "values " + CalciteSqlDialect.DEFAULT.quoteStringLiteral(o.toString())
            + ", " + CalciteSqlDialect.DEFAULT.quoteStringLiteral(p.toString()),
        ImmutableList.of(), Arrays.asList("view"));
  }

  /** Class with int and String fields. */
  public static class IntString {
    public final int n;
    public final String s;

    public IntString(int n, String s) {
      this.n = n;
      this.s = s;
    }

    @Override public String toString() {
      return "{n=" + n + ", s=" + s + "}";
    }
  }

  /** Example of a UDF with a non-static {@code eval} method,
   * and named parameters. */
  public static class MyPlusFunction {
    public static final ThreadLocal<AtomicInteger> INSTANCE_COUNT =
        new ThreadLocal<>().withInitial(() -> new AtomicInteger(0));

    // Note: Not marked @Deterministic
    public MyPlusFunction() {
      INSTANCE_COUNT.get().incrementAndGet();
    }

    public int eval(@Parameter(name = "x") int x,
        @Parameter(name = "y") int y) {
      return x + y;
    }
  }

  /** As {@link MyPlusFunction} but constructor has a
   *  {@link org.apache.calcite.schema.FunctionContext} parameter. */
  public static class MyPlusInitFunction {
    public static final ThreadLocal<AtomicInteger> INSTANCE_COUNT =
        new ThreadLocal<>().withInitial(() -> new AtomicInteger(0));
    public static final ThreadLocal<String> THREAD_DIGEST =
        new ThreadLocal<>();

    private final int initY;

    public MyPlusInitFunction(FunctionContext fx) {
      INSTANCE_COUNT.get().incrementAndGet();
      final StringBuilder b = new StringBuilder();
      final int parameterCount = fx.getParameterCount();
      b.append("parameterCount=").append(parameterCount);
      for (int i = 0; i < parameterCount; i++) {
        b.append("; argument ").append(i);
        if (fx.isArgumentConstant(i)) {
          b.append(" is constant and has value ")
              .append(fx.getArgumentValueAs(i, String.class));
        } else {
          b.append(" is not constant");
        }
      }
      THREAD_DIGEST.set(b.toString());
      this.initY = fx.isArgumentConstant(1)
          ? fx.getArgumentValueAs(1, Integer.class)
          : 100;
    }

    public int eval(@Parameter(name = "x") int x,
        @Parameter(name = "y") int y) {
      return x + initY;
    }
  }

  /** As {@link MyPlusFunction} but declared to be deterministic. */
  public static class MyDeterministicPlusFunction {
    public static final ThreadLocal<AtomicInteger> INSTANCE_COUNT =
        new ThreadLocal<>().withInitial(() -> new AtomicInteger(0));

    @Deterministic public MyDeterministicPlusFunction() {
      INSTANCE_COUNT.get().incrementAndGet();
    }

    public Integer eval(@Parameter(name = "x") Integer x,
        @Parameter(name = "y") Integer y) {
      if (x == null || y == null) {
        return null;
      }
      return x + y;
    }
  }

  /** Example of a UDF with named parameters. */
  public static class MyLeftFunction {
    public String eval(@Parameter(name = "s") String s,
        @Parameter(name = "n") int n) {
      return s.substring(0, n);
    }
  }

  /** Example of a UDF with named parameters, some of them optional. */
  public static class MyAbcdeFunction {
    public String eval(@Parameter(name = "A", optional = false) Integer a,
        @Parameter(name = "B", optional = true) Integer b,
        @Parameter(name = "C", optional = false) Integer c,
        @Parameter(name = "D", optional = true) Integer d,
        @Parameter(name = "E", optional = true) Integer e) {
      return "{a: " + a + ", b: " + b +  ", c: " + c +  ", d: " + d  + ", e: "
          + e + "}";
    }
  }

  /** Example of a non-strict UDF. (Does something useful when passed NULL.) */
  public static class MyToStringFunction {
    public static String eval(@Parameter(name = "o") Object o) {
      if (o == null) {
        return "<null>";
      }
      return "<" + o.toString() + ">";
    }
  }

  /** Example of a semi-strict UDF.
   * (Returns null if its parameter is null or if its length is 4.) */
  public static class Null4Function {
    @SemiStrict public static String eval(@Parameter(name = "s") String s) {
      if (s == null || s.length() == 4) {
        return null;
      }
      return s;
    }
  }

  /** Example of a picky, semi-strict UDF.
   * Throws {@link NullPointerException} if argument is null.
   * Returns null if its argument's length is 8. */
  public static class Null8Function {
    @SemiStrict public static String eval(@Parameter(name = "s") String s) {
      if (s.length() == 8) {
        return null;
      }
      return s;
    }
  }

  /** Example of a UDF with a static {@code eval} method. Class is abstract,
   * but code-generator should not need to instantiate it. */
  public abstract static class MyDoubleFunction {
    private MyDoubleFunction() {
    }

    public static int eval(int x) {
      return x * 2;
    }
  }

  /** Example of a UDF with non-default constructor.
   *
   * <p>Not used; we do not currently have a way to instantiate function
   * objects other than via their default constructor. */
  public static class FibonacciTableFunction {
    private final int limit;

    public FibonacciTableFunction(int limit) {
      this.limit = limit;
    }

    public ScannableTable eval() {
      return fibonacciTableWithLimit(limit);
    }
  }

  /** User-defined function with two arguments. */
  public static class MyIncrement {
    public float eval(int x, int y) {
      return x + x * y / 100;
    }
  }

  /** User-defined function that declares exceptions. */
  public static class MyExceptionFunction {
    public MyExceptionFunction() {}

    public static int eval(int x) throws IllegalArgumentException, IOException {
      if (x < 0) {
        throw new IllegalArgumentException("Illegal argument: " + x);
      } else if (x > 100) {
        throw new IOException("IOException when argument > 100");
      }
      return x + 10;
    }
  }

  /** Example of a UDF that has overloaded UDFs (same name, different args). */
  public abstract static class CountArgs0Function {
    private CountArgs0Function() {}

    public static int eval() {
      return 0;
    }
  }

  /** See {@link CountArgs0Function}. */
  public abstract static class CountArgs1Function {
    private CountArgs1Function() {}

    public static int eval(int x) {
      return 1;
    }
  }

  /** See {@link CountArgs0Function}. */
  public abstract static class CountArgs1NullableFunction {
    private CountArgs1NullableFunction() {}

    public static int eval(Short x) {
      return -1;
    }
  }

  /** See {@link CountArgs0Function}. */
  public abstract static class CountArgs2Function {
    private CountArgs2Function() {}

    public static int eval(int x, int y) {
      return 2;
    }
  }

  /** Example of a UDF class that needs to be instantiated but cannot be. */
  public abstract static class AwkwardFunction {
    private AwkwardFunction() {
    }

    public int eval(int x) {
      return 0;
    }
  }

  /** UDF class that has multiple methods, some overloaded. */
  public static class MultipleFunction {
    private MultipleFunction() {}

    // Three overloads
    public static String fun1(String x) {
      return x.toLowerCase(Locale.ROOT);
    }
    public static int fun1(int x) {
      return x * 2;
    }
    public static int fun1(int x, int y) {
      return x + y;
    }

    // Another method
    public static int fun2(int x) {
      return x * 3;
    }

    // Non-static method cannot be used because constructor is private
    public int nonStatic(int x) {
      return x * 3;
    }
  }

  /** UDF class that provides user-defined functions for each data type. */
  @Deterministic
  public static class AllTypesFunction {
    private AllTypesFunction() {}

    // We use SqlFunctions.toLong(Date) ratter than Date.getTime(),
    // and SqlFunctions.internalToTimestamp(long) rather than new Date(long),
    // because the contract of JDBC (also used by UDFs) is to represent
    // date-time values in the LOCAL time zone.

    public static long dateFun(java.sql.Date v) {
      return v == null ? -1L : SqlFunctions.toLong(v);
    }
    public static long timestampFun(java.sql.Timestamp v) {
      return v == null ? -1L : SqlFunctions.toLong(v);
    }
    public static long timeFun(java.sql.Time v) {
      return v == null ? -1L : SqlFunctions.toLong(v);
    }

    /** Overloaded, in a challenging way, with {@link #toDateFun(Long)}. */
    public static java.sql.Date toDateFun(int v) {
      return SqlFunctions.internalToDate(v);
    }

    public static java.sql.Date toDateFun(Long v) {
      return v == null ? null : SqlFunctions.internalToDate(v.intValue());
    }
    public static java.sql.Timestamp toTimestampFun(Long v) {
      return SqlFunctions.internalToTimestamp(v);
    }
    public static java.sql.Time toTimeFun(Long v) {
      return v == null ? null : SqlFunctions.internalToTime(v.intValue());
    }

    /** For overloaded user-defined functions that have {@code double} and
     * {@code BigDecimal} arguments will go wrong. */
    public static double toDouble(BigDecimal var) {
      return var == null ? 0.0d : var.doubleValue();
    }
    public static double toDouble(Double var) {
      return var == null ? 0.0d : var;
    }
    public static double toDouble(Float var) {
      return var == null ? 0.0d : Double.valueOf(var.toString());
    }

    public static List arrayAppendFun(List v, Integer i) {
      if (v == null || i == null) {
        return null;
      } else {
        v.add(i);
        return v;
      }
    }

    /** Overloaded functions with DATE, TIMESTAMP and TIME arguments. */
    public static long toLong(Date date) {
      return date == null ? 0 : SqlFunctions.toLong(date);
    }

    public static long toLong(Timestamp timestamp) {
      return timestamp == null ? 0 : SqlFunctions.toLong(timestamp);
    }

    public static long toLong(Time time) {
      return time == null ? 0 : SqlFunctions.toLong(time);
    }

  }

  /** Example of a user-defined aggregate function (UDAF). */
  public static class MySumFunction {
    public MySumFunction() {
    }
    public long init() {
      return 0L;
    }
    public long add(long accumulator, int v) {
      return accumulator + v;
    }
    public long merge(long accumulator0, long accumulator1) {
      return accumulator0 + accumulator1;
    }
    public long result(long accumulator) {
      return accumulator;
    }
  }

  /** A generic interface for defining user-defined aggregate functions.
   *
   * @param <A> accumulator type
   * @param <V> value type
   * @param <R> result type */
  private interface MyGenericAggFunction<A, V, R> {
    A init();

    A add(A accumulator, V val);

    A merge(A accumulator1, A accumulator2);

    R result(A accumulator);
  }

  /** Example of a user-defined aggregate function that implements a generic
   * interface. */
  public static class MySum3
      implements MyGenericAggFunction<Integer, Integer, Integer> {
    @Override public Integer init() {
      return 0;
    }

    @Override public Integer add(Integer accumulator, Integer val) {
      return accumulator + val;
    }

    @Override public Integer merge(Integer accumulator1, Integer accumulator2) {
      return accumulator1 + accumulator2;
    }

    @Override public Integer result(Integer accumulator) {
      return accumulator;
    }
  }

  /** Example of a user-defined aggregate function (UDAF), whose methods are
   * static. */
  public static class MyStaticSumFunction {
    public static long init() {
      return 0L;
    }
    public static long add(long accumulator, int v) {
      return accumulator + v;
    }
    public static long merge(long accumulator0, long accumulator1) {
      return accumulator0 + accumulator1;
    }
    public static long result(long accumulator) {
      return accumulator;
    }
  }

  /** Example of a user-defined aggregate function (UDAF) with two parameters.
   * The constructor has an initialization parameter. */
  public static class MyTwoParamsSumFunctionFilter1 {
    public MyTwoParamsSumFunctionFilter1(FunctionContext fx) {
      Objects.requireNonNull(fx, "fx");
      assert fx.getParameterCount() == 2;
    }
    public int init() {
      return 0;
    }
    public int add(int accumulator, int v1, int v2) {
      if (v1 > v2) {
        return accumulator + v1;
      }
      return accumulator;
    }
    public int merge(int accumulator0, int accumulator1) {
      return accumulator0 + accumulator1;
    }
    public int result(int accumulator) {
      return accumulator;
    }
  }

  /** Another example of a user-defined aggregate function (UDAF) with two
   * parameters. */
  public static class MyTwoParamsSumFunctionFilter2 {
    public MyTwoParamsSumFunctionFilter2() {
    }
    public long init() {
      return 0L;
    }
    public long add(long accumulator, int v1, String v2) {
      if (v2.equals("Eric")) {
        return accumulator + v1;
      }
      return accumulator;
    }
    public long merge(long accumulator0, long accumulator1) {
      return accumulator0 + accumulator1;
    }
    public long result(long accumulator) {
      return accumulator;
    }
  }

  /** Example of a user-defined aggregate function (UDAF), whose methods are
   * static. */
  public static class MyThreeParamsSumFunctionWithFilter1 {
    public static long init() {
      return 0L;
    }
    public static long add(long accumulator, int v1, String v2, String v3) {
      if (v2.equals(v3)) {
        return accumulator + v1;
      }
      return accumulator;
    }
    public static long merge(long accumulator0, long accumulator1) {
      return accumulator0 + accumulator1;
    }
    public static long result(long accumulator) {
      return accumulator;
    }
  }

  /** Example of a user-defined aggregate function (UDAF), whose methods are
   * static. Similar to {@link MyThreeParamsSumFunctionWithFilter1}, but
   * argument types are different. */
  public static class MyThreeParamsSumFunctionWithFilter2 {
    public static long init() {
      return 0L;
    }
    public static long add(long accumulator, int v1, int v2, int v3) {
      if (v3 > 250) {
        return accumulator + v1 + v2;
      }
      return accumulator;
    }
    public static long merge(long accumulator0, long accumulator1) {
      return accumulator0 + accumulator1;
    }
    public static long result(long accumulator) {
      return accumulator;
    }
  }

  /** User-defined function. */
  public static class SumFunctionBadIAdd {
    public long init() {
      return 0L;
    }
    public long add(short accumulator, int v) {
      return Math.addExact(accumulator, v);
    }
  }

  /** User-defined table-macro function. */
  public static class TableMacroFunction {
    public TranslatableTable eval(String s) {
      return view(s);
    }
  }

  /** User-defined table-macro function whose eval method is static. */
  public static class StaticTableMacroFunction {
    public static TranslatableTable eval(String s) {
      return view(s);
    }
  }

  /** User-defined table-macro function with named and optional parameters. */
  public static class TableMacroFunctionWithNamedParameters {
    public TranslatableTable eval(
        @Parameter(name = "R", optional = true) String r,
        @Parameter(name = "S") String s,
        @Parameter(name = "T", optional = true) Integer t) {
      final StringBuilder sb = new StringBuilder();
      abc(sb, r);
      abc(sb, s);
      abc(sb, t);
      return view(sb.toString());
    }

    private static void abc(StringBuilder sb, Object s) {
      if (s != null) {
        if (sb.length() > 0) {
          sb.append(", ");
        }
        sb.append('(').append(s).append(')');
      }
    }
  }

  /** User-defined table-macro function with named and optional parameters. */
  public static class AnotherTableMacroFunctionWithNamedParameters {
    public TranslatableTable eval(
        @Parameter(name = "R", optional = true) String r,
        @Parameter(name = "S") String s,
        @Parameter(name = "T", optional = true) Integer t,
        @Parameter(name = "S2", optional = true) String s2) {
      final StringBuilder sb = new StringBuilder();
      abc(sb, r);
      abc(sb, s);
      abc(sb, t);
      return view(sb.toString());
    }

    private static void abc(StringBuilder sb, Object s) {
      if (s != null) {
        if (sb.length() > 0) {
          sb.append(", ");
        }
        sb.append('(').append(s).append(')');
      }
    }
  }

  /** A table function that returns a {@link QueryableTable}. */
  public static class SimpleTableFunction {
    public QueryableTable eval(Integer s) {
      return generateStrings(s);
    }
  }

  /** A table function that returns a {@link QueryableTable}. */
  public static class MyTableFunction {
    public QueryableTable eval(String s) {
      return oneThreePlus(s);
    }
  }

  /** A table function that returns a {@link QueryableTable} via a
   * static method. */
  public static class TestStaticTableFunction {
    public static QueryableTable eval(String s) {
      return oneThreePlus(s);
    }
  }

  /** The real MazeTable may be found in example/function. This is a cut-down
   * version to support a test. */
  public static class MazeTable extends AbstractTable
      implements ScannableTable {

    private final String content;

    public MazeTable(String content) {
      this.content = content;
    }

    public static ScannableTable generate(int width, int height, int seed) {
      return new MazeTable(
          String.format(Locale.ROOT, "generate(w=%d, h=%d, s=%d)", width,
              height, seed));
    }

    public static ScannableTable generate2(
        @Parameter(name = "WIDTH") int width,
        @Parameter(name = "HEIGHT") int height,
        @Parameter(name = "SEED", optional = true) Integer seed) {
      return new MazeTable(
          String.format(Locale.ROOT, "generate2(w=%d, h=%d, s=%d)", width,
              height, seed));
    }

    public static ScannableTable generate3(
        @Parameter(name = "FOO") String foo) {
      return new MazeTable(
          String.format(Locale.ROOT, "generate3(foo=%s)", foo));
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("S", SqlTypeName.VARCHAR, 12)
          .build();
    }

    @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
      Object[][] rows = {{"abcde"}, {"xyz"}, {content}};
      return Linq4j.asEnumerable(rows);
    }
  }

  /** Schema containing a {@code prod} table with a lot of columns. */
  public static class WideSaleSchema {
    @Override public String toString() {
      return "WideSaleSchema";
    }

    @SuppressWarnings("unused")
    public final WideProductSale[] prod = {
        new WideProductSale(100, 10)
    };
  }

  /** Table with a lot of columns. */
  @SuppressWarnings("unused")
  public static class WideProductSale {
    public final int prodId;
    public final double sale0;
    public final double sale1 = 10;
    public final double sale2 = 10;
    public final double sale3 = 10;
    public final double sale4 = 10;
    public final double sale5 = 10;
    public final double sale6 = 10;
    public final double sale7 = 10;
    public final double sale8 = 10;
    public final double sale9 = 10;
    public final double sale10 = 10;
    public final double sale11 = 10;
    public final double sale12 = 10;
    public final double sale13 = 10;
    public final double sale14 = 10;
    public final double sale15 = 10;
    public final double sale16 = 10;
    public final double sale17 = 10;
    public final double sale18 = 10;
    public final double sale19 = 10;
    public final double sale20 = 10;
    public final double sale21 = 10;
    public final double sale22 = 10;
    public final double sale23 = 10;
    public final double sale24 = 10;
    public final double sale25 = 10;
    public final double sale26 = 10;
    public final double sale27 = 10;
    public final double sale28 = 10;
    public final double sale29 = 10;
    public final double sale30 = 10;
    public final double sale31 = 10;
    public final double sale32 = 10;
    public final double sale33 = 10;
    public final double sale34 = 10;
    public final double sale35 = 10;
    public final double sale36 = 10;
    public final double sale37 = 10;
    public final double sale38 = 10;
    public final double sale39 = 10;
    public final double sale40 = 10;
    public final double sale41 = 10;
    public final double sale42 = 10;
    public final double sale43 = 10;
    public final double sale44 = 10;
    public final double sale45 = 10;
    public final double sale46 = 10;
    public final double sale47 = 10;
    public final double sale48 = 10;
    public final double sale49 = 10;
    public final double sale50 = 10;
    public final double sale51 = 10;
    public final double sale52 = 10;
    public final double sale53 = 10;
    public final double sale54 = 10;
    public final double sale55 = 10;
    public final double sale56 = 10;
    public final double sale57 = 10;
    public final double sale58 = 10;
    public final double sale59 = 10;
    public final double sale60 = 10;
    public final double sale61 = 10;
    public final double sale62 = 10;
    public final double sale63 = 10;
    public final double sale64 = 10;
    public final double sale65 = 10;
    public final double sale66 = 10;
    public final double sale67 = 10;
    public final double sale68 = 10;
    public final double sale69 = 10;
    public final double sale70 = 10;
    public final double sale71 = 10;
    public final double sale72 = 10;
    public final double sale73 = 10;
    public final double sale74 = 10;
    public final double sale75 = 10;
    public final double sale76 = 10;
    public final double sale77 = 10;
    public final double sale78 = 10;
    public final double sale79 = 10;
    public final double sale80 = 10;
    public final double sale81 = 10;
    public final double sale82 = 10;
    public final double sale83 = 10;
    public final double sale84 = 10;
    public final double sale85 = 10;
    public final double sale86 = 10;
    public final double sale87 = 10;
    public final double sale88 = 10;
    public final double sale89 = 10;
    public final double sale90 = 10;
    public final double sale91 = 10;
    public final double sale92 = 10;
    public final double sale93 = 10;
    public final double sale94 = 10;
    public final double sale95 = 10;
    public final double sale96 = 10;
    public final double sale97 = 10;
    public final double sale98 = 10;
    public final double sale99 = 10;
    public final double sale100 = 10;
    public final double sale101 = 10;
    public final double sale102 = 10;
    public final double sale103 = 10;
    public final double sale104 = 10;
    public final double sale105 = 10;
    public final double sale106 = 10;
    public final double sale107 = 10;
    public final double sale108 = 10;
    public final double sale109 = 10;
    public final double sale110 = 10;
    public final double sale111 = 10;
    public final double sale112 = 10;
    public final double sale113 = 10;
    public final double sale114 = 10;
    public final double sale115 = 10;
    public final double sale116 = 10;
    public final double sale117 = 10;
    public final double sale118 = 10;
    public final double sale119 = 10;
    public final double sale120 = 10;
    public final double sale121 = 10;
    public final double sale122 = 10;
    public final double sale123 = 10;
    public final double sale124 = 10;
    public final double sale125 = 10;
    public final double sale126 = 10;
    public final double sale127 = 10;
    public final double sale128 = 10;
    public final double sale129 = 10;
    public final double sale130 = 10;
    public final double sale131 = 10;
    public final double sale132 = 10;
    public final double sale133 = 10;
    public final double sale134 = 10;
    public final double sale135 = 10;
    public final double sale136 = 10;
    public final double sale137 = 10;
    public final double sale138 = 10;
    public final double sale139 = 10;
    public final double sale140 = 10;
    public final double sale141 = 10;
    public final double sale142 = 10;
    public final double sale143 = 10;
    public final double sale144 = 10;
    public final double sale145 = 10;
    public final double sale146 = 10;
    public final double sale147 = 10;
    public final double sale148 = 10;
    public final double sale149 = 10;
    public final double sale150 = 10;
    public final double sale151 = 10;
    public final double sale152 = 10;
    public final double sale153 = 10;
    public final double sale154 = 10;
    public final double sale155 = 10;
    public final double sale156 = 10;
    public final double sale157 = 10;
    public final double sale158 = 10;
    public final double sale159 = 10;
    public final double sale160 = 10;
    public final double sale161 = 10;
    public final double sale162 = 10;
    public final double sale163 = 10;
    public final double sale164 = 10;
    public final double sale165 = 10;
    public final double sale166 = 10;
    public final double sale167 = 10;
    public final double sale168 = 10;
    public final double sale169 = 10;
    public final double sale170 = 10;
    public final double sale171 = 10;
    public final double sale172 = 10;
    public final double sale173 = 10;
    public final double sale174 = 10;
    public final double sale175 = 10;
    public final double sale176 = 10;
    public final double sale177 = 10;
    public final double sale178 = 10;
    public final double sale179 = 10;
    public final double sale180 = 10;
    public final double sale181 = 10;
    public final double sale182 = 10;
    public final double sale183 = 10;
    public final double sale184 = 10;
    public final double sale185 = 10;
    public final double sale186 = 10;
    public final double sale187 = 10;
    public final double sale188 = 10;
    public final double sale189 = 10;
    public final double sale190 = 10;
    public final double sale191 = 10;
    public final double sale192 = 10;
    public final double sale193 = 10;
    public final double sale194 = 10;
    public final double sale195 = 10;
    public final double sale196 = 10;
    public final double sale197 = 10;
    public final double sale198 = 10;
    public final double sale199 = 10;

    public WideProductSale(int prodId, double sale) {
      this.prodId = prodId;
      this.sale0 = sale;
    }
  }

  /**
   * Implementation of {@link TableMacro} interface with
   * {@link #apply} method that returns {@link Queryable} table.
   */
  public static class SimpleTableMacro implements TableMacro {

    @Override public TranslatableTable apply(List<?> arguments) {
      return new SimpleTable();
    }

    @Override public List<FunctionParameter> getParameters() {
      return Collections.emptyList();
    }
  }

  /** Table with columns (A, B). */
  public static class SimpleTable extends AbstractQueryableTable
      implements TranslatableTable {
    private final String[] columnNames = { "A", "B" };
    private final Class<?>[] columnTypes = { String.class, Integer.class };
    private final Object[][] rows = new Object[3][];

    public SimpleTable() {
      super(Object[].class);

      rows[0] = new Object[] { "foo", 5 };
      rows[1] = new Object[] { "bar", 4 };
      rows[2] = new Object[] { "foo", 3 };
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      int columnCount = columnNames.length;
      final List<Pair<String, RelDataType>> columnDesc =
          new ArrayList<>(columnCount);
      for (int i = 0; i < columnCount; i++) {
        final RelDataType colType = typeFactory
            .createJavaType(columnTypes[i]);
        columnDesc.add(Pair.of(columnNames[i], colType));
      }
      return typeFactory.createStructType(columnDesc);
    }

    public Iterator<Object[]> iterator() {
      return Linq4j.enumeratorIterator(enumerator());
    }

    public Enumerator<Object[]> enumerator() {
      return enumeratorImpl(null);
    }

    @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
        SchemaPlus schema, String tableName) {
      return new AbstractTableQueryable<T>(queryProvider, schema, this,
          tableName) {
        @Override public Enumerator<T> enumerator() {
          //noinspection unchecked
          return (Enumerator<T>) enumeratorImpl(null);
        }
      };
    }

    private Enumerator<Object[]> enumeratorImpl(final int[] fields) {
      return new Enumerator<Object[]>() {
        private Object[] current;
        private final Iterator<Object[]> iterator = Arrays.asList(rows)
            .iterator();

        @Override public Object[] current() {
          return current;
        }

        @Override public boolean moveNext() {
          if (iterator.hasNext()) {
            Object[] full = iterator.next();
            current = fields != null ? convertRow(full) : full;
            return true;
          } else {
            current = null;
            return false;
          }
        }

        @Override public void reset() {
          throw new UnsupportedOperationException();
        }

        @Override public void close() {
          // noop
        }

        private Object[] convertRow(Object[] full) {
          final Object[] objects = new Object[fields.length];
          for (int i = 0; i < fields.length; i++) {
            objects[i] = full[fields[i]];
          }
          return objects;
        }
      };
    }

    @Override public RelNode toRel(
        RelOptTable.ToRelContext context,
        RelOptTable relOptTable) {
      return EnumerableTableScan.create(context.getCluster(), relOptTable);
    }

    @Override public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
      MethodCallExpression queryableExpression =
          Expressions.call(Expressions.new_(SimpleTable.class),
              BuiltInMethod.QUERYABLE_TABLE_AS_QUERYABLE.method,
              Expressions.constant(null),
              Schemas.expression(schema),
              Expressions.constant(tableName));
      return Expressions.call(queryableExpression,
          BuiltInMethod.QUERYABLE_AS_ENUMERABLE.method);
    }
  }
}
