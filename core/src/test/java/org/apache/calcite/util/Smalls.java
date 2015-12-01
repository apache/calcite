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
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.BaseQueryable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Method;
import java.util.AbstractList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Holder for various classes and functions used in tests as user-defined
 * functions and so forth.
 */
public class Smalls {
  public static final Method GENERATE_STRINGS_METHOD =
      Types.lookupMethod(Smalls.class, "generateStrings", Integer.class);
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
      public <E> Queryable<E> asQueryable(
          QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        //noinspection unchecked
        return (Queryable<E>) enumerable.asQueryable();
      }

      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
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
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.createJavaType(IntString.class);
      }

      public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
          SchemaPlus schema, String tableName) {
        BaseQueryable<IntString> queryable =
            new BaseQueryable<IntString>(null, IntString.class, null) {
              public Enumerator<IntString> enumerator() {
                return new Enumerator<IntString>() {
                  static final String Z = "abcdefghijklm";

                  int i = 0;
                  int curI;
                  String curS;

                  public IntString current() {
                    return new IntString(curI, curS);
                  }

                  public boolean moveNext() {
                    if (i < count) {
                      curI = i;
                      curS = Z.substring(0, i % Z.length());
                      ++i;
                      return true;
                    } else {
                      return false;
                    }
                  }

                  public void reset() {
                    i = 0;
                  }

                  public void close() {
                  }
                };
              }
            };
        //noinspection unchecked
        return (Queryable<T>) queryable;
      }
    };
  }

  /** A function that generates multiplication table of {@code ncol} columns x
   * {@code nrow} rows. */
  public static QueryableTable multiplicationTable(final int ncol,
      final int nrow, Integer offset) {
    final int offs = offset == null ? 0 : offset;
    return new AbstractQueryableTable(Object[].class) {
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        final RelDataTypeFactory.FieldInfoBuilder builder =
            typeFactory.builder();
        builder.add("row_name", typeFactory.createJavaType(String.class));
        final RelDataType int_ = typeFactory.createJavaType(int.class);
        for (int i = 1; i <= ncol; i++) {
          builder.add("c" + i, int_);
        }
        return builder.build();
      }

      public Queryable<Object[]> asQueryable(QueryProvider queryProvider,
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

  /**
   * A function that adds a number to the first column of input cursor
   */
  public static QueryableTable processCursor(final int offset,
      final Enumerable<Object[]> a) {
    return new AbstractQueryableTable(Object[].class) {
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("result", SqlTypeName.INTEGER)
            .build();
      }

      public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
          SchemaPlus schema, String tableName) {
        final Enumerable<Integer> enumerable =
            a.select(new Function1<Object[], Integer>() {
              public Integer apply(Object[] a0) {
                return offset + ((Integer) a0[0]);
              }
            });
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
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("result", SqlTypeName.INTEGER)
            .build();
      }

      public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
          SchemaPlus schema, String tableName) {
        final Enumerable<Integer> enumerable =
            a.zip(b, new Function2<Object[], IntString, Integer>() {
              public Integer apply(Object[] v0, IntString v1) {
                return ((Integer) v0[1]) + v1.n + offset;
              }
            });
        //noinspection unchecked
        return (Queryable) enumerable.asQueryable();
      }
    };
  }

  public static TranslatableTable view(String s) {
    return new ViewTable(Object.class,
        new RelProtoDataType() {
          public RelDataType apply(RelDataTypeFactory typeFactory) {
            return typeFactory.builder().add("c", SqlTypeName.INTEGER)
                .build();
          }
        }, "values (1), (3), " + s, ImmutableList.<String>of());
  }

  public static TranslatableTable str(Object o, Object p) {
    assertThat(RexLiteral.validConstant(o, Litmus.THROW), is(true));
    assertThat(RexLiteral.validConstant(p, Litmus.THROW), is(true));
    return new ViewTable(Object.class,
        new RelProtoDataType() {
          public RelDataType apply(RelDataTypeFactory typeFactory) {
            return typeFactory.builder().add("c", SqlTypeName.VARCHAR, 100)
                .build();
          }
        },
        "values " + SqlDialect.CALCITE.quoteStringLiteral(o.toString())
            + ", " + SqlDialect.CALCITE.quoteStringLiteral(p.toString()),
        ImmutableList.<String>of());
  }

  /** Class with int and String fields. */
  public static class IntString {
    public final int n;
    public final String s;

    public IntString(int n, String s) {
      this.n = n;
      this.s = s;
    }

    public String toString() {
      return "{n=" + n + ", s=" + s + "}";
    }
  }

  /** Example of a UDF with a non-static {@code eval} method,
   * and named parameters. */
  public static class MyPlusFunction {
    public int eval(@Parameter(name = "x") int x, @Parameter(name = "y") int y) {
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

  /** Example of a UDF with a static {@code eval} method. Class is abstract,
   * but code-generator should not need to instantiate it. */
  public abstract static class MyDoubleFunction {
    private MyDoubleFunction() {
    }

    public static int eval(int x) {
      return x * 2;
    }
  }

  /** User-defined function with two arguments. */
  public static class MyIncrement {
    public float eval(int x, int y) {
      return x + x * y / 100;
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
    public static String fun1(String x) { return x.toLowerCase(); }
    public static int fun1(int x) { return x * 2; }
    public static int fun1(int x, int y) { return x + y; }

    // Another method
    public static int fun2(int x) { return x * 3; }

    // Non-static method cannot be used because constructor is private
    public int nonStatic(int x) { return x * 3; }
  }

  /** UDF class that provides user-defined functions for each data type. */
  public static class AllTypesFunction {
    private AllTypesFunction() {}

    public static long dateFun(java.sql.Date x) { return x == null ? -1L : x.getTime(); }
    public static long timestampFun(java.sql.Timestamp x) { return x == null ? -1L : x.getTime(); }
    public static long timeFun(java.sql.Time x) { return x == null ? -1L : x.getTime(); }
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

  /** User-defined function. */
  public static class SumFunctionBadIAdd {
    public long init() {
      return 0L;
    }
    public long add(short accumulator, int v) {
      return accumulator + v;
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

    private void abc(StringBuilder sb, Object s) {
      if (s != null) {
        if (sb.length() > 0) {
          sb.append(", ");
        }
        sb.append('(').append(s).append(')');
      }
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
      return new MazeTable(String.format("generate(w=%d, h=%d, s=%d)", width, height, seed));
    }

    public static ScannableTable generate2(
        @Parameter(name = "WIDTH") int width,
        @Parameter(name = "HEIGHT") int height,
        @Parameter(name = "SEED", optional = true) Integer seed) {
      return new MazeTable(String.format("generate2(w=%d, h=%d, s=%d)", width, height, seed));
    }

    public static ScannableTable generate3(
        @Parameter(name = "FOO") String foo) {
      return new MazeTable(String.format("generate3(foo=%s)", foo));
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder()
          .add("S", SqlTypeName.VARCHAR, 12)
          .build();
    }

    public Enumerable<Object[]> scan(DataContext root) {
      Object[][] rows = {{"abcde"}, {"xyz"}, {content}};
      return Linq4j.asEnumerable(rows);
    }
  }
}

// End Smalls.java
