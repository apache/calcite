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
package org.apache.calcite.test;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.Closer;
import org.apache.calcite.util.TryThreadLocal;
import org.apache.calcite.util.Util;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.io.PatternFilenameFilter;

import net.hydromatic.quidem.Quidem;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FilenameFilter;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Method;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.fail;

/**
 * Test that runs every Quidem file as a test.
 */
@RunWith(Parameterized.class)
public class QuidemTest {
  private final String path;
  private final Method method;

  public QuidemTest(String path) {
    this.path = path;
    this.method = findMethod(path);
  }

  /** Runs a test from the command line.
   *
   * <p>For example:
   *
   * <blockquote><code>java QuidemTest sql/dummy.iq</code></blockquote> */
  public static void main(String[] args) throws Exception {
    for (String arg : args) {
      new QuidemTest(arg).test();
    }
  }

  private Method findMethod(String path) {
    // E.g. path "sql/agg.iq" gives method "testSqlAgg"
    String methodName =
        AvaticaUtils.toCamelCase("test_" + path.replace('/', '_').replaceAll("\\.iq$", ""));
    Method m;
    try {
      m = getClass().getMethod(methodName);
    } catch (NoSuchMethodException e) {
      m = null;
    }
    return m;
  }

  /** For {@link org.junit.runners.Parameterized} runner. */
  @Parameterized.Parameters(name = "{index}: quidem({0})")
  public static Collection<Object[]> data() {
    // Start with a test file we know exists, then find the directory and list
    // its files.
    final String first = "sql/agg.iq";
    // inUrl = "file:/home/fred/calcite/core/target/test-classes/sql/agg.iq"
    final URL inUrl = JdbcTest.class.getResource("/" + first);
    String x = inUrl.getFile();
    assert x.endsWith(first);
    final String base =
        File.separatorChar == '\\'
            ? x.substring(1, x.length() - first.length())
                .replace('/', File.separatorChar)
            : x.substring(0, x.length() - first.length());
    final File firstFile = new File(x);
    final File dir = firstFile.getParentFile();
    final List<String> paths = new ArrayList<>();
    final FilenameFilter filter = new PatternFilenameFilter(".*\\.iq$");
    for (File f : Util.first(dir.listFiles(filter), new File[0])) {
      assert f.getAbsolutePath().startsWith(base)
          : "f: " + f.getAbsolutePath() + "; base: " + base;
      paths.add(f.getAbsolutePath().substring(base.length()));
    }
    return Lists.transform(paths, new Function<String, Object[]>() {
      public Object[] apply(String path) {
        return new Object[] {path};
      }
    });
  }

  private void checkRun(String path) throws Exception {
    final File inFile;
    final File outFile;
    final File f = new File(path);
    if (f.isAbsolute()) {
      // e.g. path = "/tmp/foo.iq"
      inFile = f;
      outFile = new File(path + ".out");
    } else {
      // e.g. path = "sql/outer.iq"
      // inUrl = "file:/home/fred/calcite/core/target/test-classes/sql/outer.iq"
      final URL inUrl = JdbcTest.class.getResource("/" + n2u(path));
      String x = u2n(inUrl.getFile());
      assert x.endsWith(path)
          : "x: " + x + "; path: " + path;
      x = x.substring(0, x.length() - path.length());
      assert x.endsWith(u2n("/test-classes/"));
      x = x.substring(0, x.length() - u2n("/test-classes/").length());
      final File base = new File(x);
      inFile = new File(base, u2n("/test-classes/") + path);
      outFile = new File(base, u2n("/surefire/") + path);
    }
    Util.discard(outFile.getParentFile().mkdirs());
    try (final Reader reader = Util.reader(inFile);
         final Writer writer = Util.printWriter(outFile);
         final Closer closer = new Closer()) {
      new Quidem(reader, writer, env(), new QuidemConnectionFactory())
          .withPropertyHandler(new Quidem.PropertyHandler() {
            public void onSet(String propertyName, Object value) {
              if (propertyName.equals("bindable")) {
                final boolean b = value instanceof Boolean
                    && (Boolean) value;
                closer.add(Hook.ENABLE_BINDABLE.addThread(Hook.property(b)));
              }
              if (propertyName.equals("expand")) {
                final boolean b = value instanceof Boolean
                    && (Boolean) value;
                closer.add(Prepare.THREAD_EXPAND.push(b));
              }
            }
          })
          .execute();
    }
    final String diff = DiffTestCase.diff(inFile, outFile);
    if (!diff.isEmpty()) {
      fail("Files differ: " + outFile + " " + inFile + "\n"
          + diff);
    }
  }

  /** Converts a path from Unix to native. On Windows, converts
   * forward-slashes to back-slashes; on Linux, does nothing. */
  private static String u2n(String s) {
    return File.separatorChar == '\\'
        ? s.replace('/', '\\')
        : s;
  }

  private static String n2u(String s) {
    return File.separatorChar == '\\'
        ? s.replace('\\', '/')
        : s;
  }

  private Function<String, Object> env() {
    return new Function<String, Object>() {
      public Object apply(String varName) {
        switch (varName) {
        case "jdk18":
          return System.getProperty("java.version").startsWith("1.8");
        case "fixed":
          return new Function<String, Object>() {
            public Object apply(String v) {
              switch (v) {
              case "calcite1045":
                return Bug.CALCITE_1045_FIXED;
              case "calcite1048":
                return Bug.CALCITE_1048_FIXED;
              }
              return null;
            }
          };
        default:
          return null;
        }
      }
    };
  }

  @Test public void test() throws Exception {
    if (method != null) {
      method.invoke(this);
    } else {
      checkRun(path);
    }
  }

  /** Override settings for "sql/misc.iq". */
  public void testSqlMisc() throws Exception {
    switch (CalciteAssert.DB) {
    case ORACLE:
      // There are formatting differences (e.g. "4.000" vs "4") when using
      // Oracle as the JDBC data source.
      return;
    }
    try (final TryThreadLocal.Memo ignored = Prepare.THREAD_EXPAND.push(true)) {
      checkRun(path);
    }
  }

  /** Override settings for "sql/scalar.iq". */
  public void testSqlScalar() throws Exception {
    try (final TryThreadLocal.Memo ignored = Prepare.THREAD_EXPAND.push(true)) {
      checkRun(path);
    }
  }

  /** Runs the dummy script "sql/dummy.iq", which is checked in empty but
   * which you may use as scratch space during development. */
  // Do not add disable this test; just remember not to commit changes to dummy.iq
  public void testSqlDummy() throws Exception {
    try (final TryThreadLocal.Memo ignored = Prepare.THREAD_EXPAND.push(true)) {
      checkRun(path);
    }
  }

  /** Quidem connection factory for Calcite's built-in test schemas. */
  private static class QuidemConnectionFactory
      implements Quidem.ConnectionFactory {
    public Connection connect(String name) throws Exception {
      return connect(name, false);
    }

    public Connection connect(String name, boolean reference)
        throws Exception {
      if (reference) {
        if (name.equals("foodmart")) {
          final ConnectionSpec db =
              CalciteAssert.DatabaseInstance.HSQLDB.foodmart;
          final Connection connection =
              DriverManager.getConnection(db.url, db.username,
                  db.password);
          connection.setSchema("foodmart");
          return connection;
        }
        return null;
      }
      switch (name) {
      case "hr":
        return CalciteAssert.hr()
            .connect();
      case "foodmart":
        return CalciteAssert.that()
            .with(CalciteAssert.Config.FOODMART_CLONE)
            .connect();
      case "scott":
        return CalciteAssert.that()
            .with(CalciteAssert.Config.SCOTT)
            .connect();
      case "jdbc_scott":
        return CalciteAssert.that()
            .with(CalciteAssert.Config.JDBC_SCOTT)
            .connect();
      case "post":
        return CalciteAssert.that()
            .with(CalciteAssert.Config.REGULAR)
            .with(CalciteAssert.SchemaSpec.POST)
            .withDefaultSchema("POST")
            .connect();
      case "catchall":
        return CalciteAssert.that()
            .withSchema("s",
                new ReflectiveSchema(
                    new ReflectiveSchemaTest.CatchallSchema()))
            .connect();
      case "orinoco":
        return CalciteAssert.that()
            .with(CalciteAssert.SchemaSpec.ORINOCO)
            .withDefaultSchema("ORINOCO")
            .connect();
      case "blank":
        return CalciteAssert.that()
            .with("parserFactory",
                "org.apache.calcite.sql.parser.parserextensiontesting"
                    + ".ExtensionSqlParserImpl#FACTORY")
            .with(CalciteAssert.SchemaSpec.BLANK)
            .withDefaultSchema("BLANK")
            .connect();
      case "seq":
        final Connection connection = CalciteAssert.that()
            .withSchema("s", new AbstractSchema())
            .connect();
        connection.unwrap(CalciteConnection.class).getRootSchema()
            .getSubSchema("s")
            .add("my_seq",
                new AbstractTable() {
                  public RelDataType getRowType(
                      RelDataTypeFactory typeFactory) {
                    return typeFactory.builder()
                        .add("$seq", SqlTypeName.BIGINT).build();
                  }

                  @Override public Schema.TableType getJdbcTableType() {
                    return Schema.TableType.SEQUENCE;
                  }
                });
        return connection;
      default:
        throw new RuntimeException("unknown connection '" + name + "'");
      }
    }
  }

}

// End QuidemTest.java
