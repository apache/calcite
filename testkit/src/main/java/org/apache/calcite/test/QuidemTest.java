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

import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.test.schemata.catchall.CatchallSchema;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.Closer;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.Util;

import com.google.common.io.PatternFilenameFilter;

import net.hydromatic.quidem.CommandHandler;
import net.hydromatic.quidem.Quidem;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.FilenameFilter;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.fail;

import static java.util.Objects.requireNonNull;

/**
 * Test that runs every Quidem file as a test.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class QuidemTest {

  private static final Pattern PATTERN = Pattern.compile("\\.iq$");

  private static @Nullable Object getEnv(String varName) {
    switch (varName) {
    case "jdk18":
      return System.getProperty("java.version").startsWith("1.8");
    case "fixed":
      // Quidem requires a Java 8 function
      return (Function<String, Object>) v -> {
        switch (v) {
        case "calcite1045":
          return Bug.CALCITE_1045_FIXED;
        case "calcite1048":
          return Bug.CALCITE_1048_FIXED;
        }
        return null;
      };
    case "not":
      return (Function<String, Object>) v -> {
        final Object o = getEnv(v);
        if (o instanceof Function) {
          @SuppressWarnings("unchecked") final Function<String, Object> f =
              (Function<String, Object>) o;
          return (Function<String, Object>) v2 -> !((Boolean) f.apply(v2));
        }
        return null;
      };
    default:
      return null;
    }
  }

  private @Nullable Method findMethod(String path) {
    // E.g. path "sql/agg.iq" gives method "testSqlAgg"
    final String path1 = path.replace(File.separatorChar, '_');
    final String path2 = PATTERN.matcher(path1).replaceAll("");
    String methodName = AvaticaUtils.toCamelCase("test_" + path2);
    Method m;
    try {
      m = getClass().getMethod(methodName, String.class);
    } catch (NoSuchMethodException e) {
      m = null;
    }
    return m;
  }

  @SuppressWarnings({"BetaApi", "UnstableApiUsage"})
  protected static Collection<String> data(String first) {
    // inUrl = "file:/home/fred/calcite/core/target/test-classes/sql/agg.iq"
    final URL inUrl = QuidemTest.class.getResource("/" + n2u(first));
    final File firstFile = Sources.of(requireNonNull(inUrl, "inUrl")).file();
    final int commonPrefixLength = firstFile.getAbsolutePath().length() - first.length();
    final File dir = firstFile.getParentFile();
    final List<String> paths = new ArrayList<>();
    final FilenameFilter filter = new PatternFilenameFilter(".*\\.iq$");
    for (File f : Util.first(dir.listFiles(filter), new File[0])) {
      paths.add(f.getAbsolutePath().substring(commonPrefixLength));
    }
    return paths;
  }

  protected void checkRun(String path) throws Exception {
    final File inFile;
    final File outFile;
    final File f = new File(path);
    if (f.isAbsolute()) {
      // e.g. path = "/tmp/foo.iq"
      inFile = f;
      outFile = new File(path + ".out");
    } else {
      // e.g. path = "sql/agg.iq"
      // inUrl = "file:/home/fred/calcite/core/build/resources/test/sql/agg.iq"
      // inFile = "/home/fred/calcite/core/build/resources/test/sql/agg.iq"
      // outDir = "/home/fred/calcite/core/build/quidem/test/sql"
      // outFile = "/home/fred/calcite/core/build/quidem/test/sql/agg.iq"
      final URL inUrl = QuidemTest.class.getResource("/" + n2u(path));
      inFile = Sources.of(requireNonNull(inUrl, "inUrl")).file();
      outFile = replaceDir(inFile, "resources", "quidem");
    }
    Util.discard(outFile.getParentFile().mkdirs());
    try (Reader reader = Util.reader(inFile);
         Writer writer = Util.printWriter(outFile);
         Closer closer = new Closer()) {
      final Quidem.Config config = Quidem.configBuilder()
          .withReader(reader)
          .withWriter(writer)
          .withConnectionFactory(createConnectionFactory())
          .withCommandHandler(createCommandHandler())
          .withPropertyHandler((propertyName, value) -> {
            if (propertyName.equals("bindable")) {
              final boolean b = value instanceof Boolean
                  && (Boolean) value;
              closer.add(Hook.ENABLE_BINDABLE.addThread(Hook.propertyJ(b)));
            }
            if (propertyName.equals("expand")) {
              final boolean b = value instanceof Boolean
                  && (Boolean) value;
              closer.add(Prepare.THREAD_EXPAND.push(b));
            }
            if (propertyName.equals("insubquerythreshold")) {
              int thresholdValue = ((BigDecimal) value).intValue();
              closer.add(Prepare.THREAD_INSUBQUERY_THRESHOLD.push(thresholdValue));
            }
          })
          .withEnv(QuidemTest::getEnv)
          .build();
      new Quidem(config).execute();
    }
    // Sanity check: we do not allow an empty input file, it may indicate that it was overwritten
    if (inFile.length() == 0) {
      fail("Input file was empty: " + inFile + "\n");
    }
    final String diff = DiffTestCase.diff(inFile, outFile);
    if (!diff.isEmpty()) {
      fail("Files differ: " + outFile + " " + inFile + "\n"
          + diff);
    }
  }

  /** Returns a file, replacing one directory with another.
   *
   * <p>For example, {@code replaceDir("/abc/str/astro.txt", "str", "xyz")}
   * returns "{@code "/abc/xyz/astro.txt}".
   * Note that the file name "astro.txt" does not become "axyzo.txt".
   */
  private static File replaceDir(File file, String target, String replacement) {
    return new File(
        n2u(file.getAbsolutePath()).replace(n2u('/' + target + '/'),
            n2u('/' + replacement + '/')));
  }

  /** Creates a command handler. */
  protected CommandHandler createCommandHandler() {
    return Quidem.EMPTY_COMMAND_HANDLER;
  }

  /** Creates a connection factory. */
  protected Quidem.ConnectionFactory createConnectionFactory() {
    return new QuidemConnectionFactory();
  }

  /** Converts a path from native to Unix. On Windows, converts
   * back-slashes to forward-slashes; on Linux, does nothing. */
  private static String n2u(String s) {
    return File.separatorChar == '\\'
        ? s.replace('\\', '/')
        : s;
  }

  @ParameterizedTest
  @MethodSource("getPath")
  public void test(String path) throws Exception {
    final Method method = findMethod(path);
    if (method != null) {
      try {
        method.invoke(this, path);
      } catch (InvocationTargetException e) {
        Throwable cause = e.getCause();
        if (cause instanceof Exception) {
          throw (Exception) cause;
        }
        if (cause instanceof Error) {
          throw (Error) cause;
        }
        throw e;
      }
    } else {
      checkRun(path);
    }
  }

  /** Factory method for {@link QuidemTest#test(String)} parameters. */
  protected abstract Collection<String> getPath();

  /** Quidem connection factory for Calcite's built-in test schemas. */
  protected static class QuidemConnectionFactory
      implements Quidem.ConnectionFactory {
    public Connection connect(String name) throws Exception {
      return connect(name, false);
    }

    @Override public Connection connect(String name, boolean reference)
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
      case "aux":
        return CalciteAssert.hr()
            .with(CalciteAssert.Config.AUX)
            .connect();
      case "foodmart":
        return CalciteAssert.that()
            .with(CalciteAssert.Config.FOODMART_CLONE)
            .connect();
      case "geo":
        return CalciteAssert.that()
            .with(CalciteAssert.Config.GEO)
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
            .connect();
      case "post-big-query":
        return CalciteAssert.that()
            .with(CalciteConnectionProperty.FUN, "standard,bigquery")
            .with(CalciteAssert.Config.REGULAR)
            .with(CalciteAssert.SchemaSpec.POST)
            .connect();
      case "mysqlfunc":
        return CalciteAssert.that()
            .with(CalciteConnectionProperty.FUN, "mysql")
            .with(CalciteAssert.Config.REGULAR)
            .with(CalciteAssert.SchemaSpec.POST)
            .connect();
      case "sparkfunc":
        return CalciteAssert.that()
            .with(CalciteConnectionProperty.FUN, "spark")
            .with(CalciteAssert.Config.REGULAR)
            .with(CalciteAssert.SchemaSpec.POST)
            .connect();
      case "oraclefunc":
        return CalciteAssert.that()
            .with(CalciteConnectionProperty.FUN, "oracle")
            .with(CalciteAssert.Config.REGULAR)
            .connect();
      case "mssqlfunc":
        return CalciteAssert.that()
            .with(CalciteConnectionProperty.FUN, "mssql")
            .with(CalciteAssert.Config.REGULAR)
            .connect();
      case "catchall":
        return CalciteAssert.that()
            .with(CalciteConnectionProperty.TIME_ZONE, "UTC")
            .withSchema("s",
                new ReflectiveSchemaWithoutRowCount(
                    new CatchallSchema()))
            .connect();
      case "orinoco":
        return CalciteAssert.that()
            .with(CalciteAssert.SchemaSpec.ORINOCO)
            .connect();
      case "seq":
        final Connection connection = CalciteAssert.that()
            .withSchema("s", new AbstractSchema())
            .connect();
        connection.unwrap(CalciteConnection.class).getRootSchema()
            .getSubSchema("s")
            .add("my_seq",
                new AbstractTable() {
                  @Override public RelDataType getRowType(
                      RelDataTypeFactory typeFactory) {
                    return typeFactory.builder()
                        .add("$seq", SqlTypeName.BIGINT).build();
                  }

                  @Override public Schema.TableType getJdbcTableType() {
                    return Schema.TableType.SEQUENCE;
                  }
                });
        return connection;
      case "bookstore":
        return CalciteAssert.that()
            .with(CalciteAssert.SchemaSpec.BOOKSTORE)
            .connect();
      default:
        throw new RuntimeException("unknown connection '" + name + "'");
      }
    }
  }

}
