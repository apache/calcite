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
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.Closer;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.Util;

import com.google.common.collect.Lists;
import com.google.common.io.PatternFilenameFilter;

import net.hydromatic.quidem.CommandHandler;
import net.hydromatic.quidem.Quidem;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FilenameFilter;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
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
public abstract class QuidemTest {
  protected final String path;
  protected final Method method;

  /** Creates a QuidemTest. */
  protected QuidemTest(String path) {
    this.path = path;
    this.method = findMethod(path);
  }

  private static Object getEnv(String varName) {
    switch (varName) {
    case "jdk18":
      return System.getProperty("java.version").startsWith("1.8");
    case "fixed":
      // Quidem requires a Guava function
      return (com.google.common.base.Function<String, Object>) v -> {
        switch (v) {
        case "calcite1045":
          return Bug.CALCITE_1045_FIXED;
        case "calcite1048":
          return Bug.CALCITE_1048_FIXED;
        }
        return null;
      };
    default:
      return null;
    }
  }

  private Method findMethod(String path) {
    // E.g. path "sql/agg.iq" gives method "testSqlAgg"
    String methodName =
        AvaticaUtils.toCamelCase(
            "test_" + path.replace(File.separatorChar, '_').replaceAll("\\.iq$", ""));
    Method m;
    try {
      m = getClass().getMethod(methodName);
    } catch (NoSuchMethodException e) {
      m = null;
    }
    return m;
  }

  protected static Collection<Object[]> data(String first) {
    // inUrl = "file:/home/fred/calcite/core/target/test-classes/sql/agg.iq"
    final URL inUrl = JdbcTest.class.getResource("/" + n2u(first));
    final File firstFile = Sources.of(inUrl).file();
    final int commonPrefixLength = firstFile.getAbsolutePath().length() - first.length();
    final File dir = firstFile.getParentFile();
    final List<String> paths = new ArrayList<>();
    final FilenameFilter filter = new PatternFilenameFilter(".*\\.iq$");
    for (File f : Util.first(dir.listFiles(filter), new File[0])) {
      paths.add(f.getAbsolutePath().substring(commonPrefixLength));
    }
    return Lists.transform(paths, path -> new Object[] {path});
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
      // e.g. path = "sql/outer.iq"
      // inUrl = "file:/home/fred/calcite/core/target/test-classes/sql/outer.iq"
      final URL inUrl = JdbcTest.class.getResource("/" + n2u(path));
      inFile = Sources.of(inUrl).file();
      outFile = new File(inFile.getAbsoluteFile().getParent(), u2n("surefire/") + path);
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
          })
          .withEnv(QuidemTest::getEnv)
          .build();
      new Quidem(config).execute();
    }
    final String diff = DiffTestCase.diff(inFile, outFile);
    if (!diff.isEmpty()) {
      fail("Files differ: " + outFile + " " + inFile + "\n"
          + diff);
    }
  }

  /** Creates a command handler. */
  protected CommandHandler createCommandHandler() {
    return Quidem.EMPTY_COMMAND_HANDLER;
  }

  /** Creates a connection factory. */
  protected Quidem.ConnectionFactory createConnectionFactory() {
    return new QuidemConnectionFactory();
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

  @Test public void test() throws Exception {
    if (method != null) {
      try {
        method.invoke(this);
      } catch (InvocationTargetException e) {
        Throwable cause = e.getCause();
        if (cause instanceof Exception) {
          throw (Exception) cause;
        }
        throw e;
      }
    } else {
      checkRun(path);
    }
  }

  /** Quidem connection factory for Calcite's built-in test schemas. */
  protected static class QuidemConnectionFactory
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
      case "catchall":
        return CalciteAssert.that()
            .withSchema("s",
                new ReflectiveSchema(
                    new ReflectiveSchemaTest.CatchallSchema()))
            .connect();
      case "orinoco":
        return CalciteAssert.that()
            .with(CalciteAssert.SchemaSpec.ORINOCO)
            .connect();
      case "blank":
        return CalciteAssert.that()
            .with(CalciteConnectionProperty.PARSER_FACTORY,
                "org.apache.calcite.sql.parser.parserextensiontesting"
                    + ".ExtensionSqlParserImpl#FACTORY")
            .with(CalciteAssert.SchemaSpec.BLANK)
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
