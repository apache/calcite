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
package org.apache.calcite.adapter.os;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.util.JsonBuilder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Command that executes its arguments as a SQL query
 * against Calcite's OS adapter.
 */
public class SqlShell {
  static final String MODEL = model();

  private final List<String> args;
  private final InputStreamReader in;
  private final PrintWriter out;
  private final PrintWriter err;

  SqlShell(InputStreamReader in, PrintWriter out,
      PrintWriter err, String... args) {
    this.args = ImmutableList.copyOf(args);
    this.in = Objects.requireNonNull(in);
    this.out = Objects.requireNonNull(out);
    this.err = Objects.requireNonNull(err);
  }

  private static String model() {
    final StringBuilder b = new StringBuilder();
    b.append("{\n")
        .append("  version: '1.0',\n")
        .append("  defaultSchema: 'os',\n")
        .append("   schemas: [\n")
        .append("     {\n")
        .append("       \"name\": \"os\",\n")
        .append("       \"tables\": [ {\n");
    addView(b, "du", "select *, \"size_k\" * 1024 as \"size_b\"\n"
            + "from table(\"du\"(true))");
    addView(b, "files", "select * from table(\"files\"('.'))");
    addView(b, "git_commits", "select * from table(\"git_commits\"(true))");
    addView(b, "jps", "select * from table(\"jps\"(true))");
    addView(b, "ps", "select * from table(\"ps\"(true))");
    addView(b, "stdin", "select * from table(\"stdin\"(true))");
    addView(b, "vmstat", "select * from table(\"vmstat\"(true))");
    b.append("       } ],\n")
        .append("       functions: [ {\n");
    addFunction(b, "du", DuTableFunction.class);
    addFunction(b, "files", FilesTableFunction.class);
    addFunction(b, "git_commits", GitCommitsTableFunction.class);
    addFunction(b, "jps", JpsTableFunction.class);
    addFunction(b, "ps", PsTableFunction.class);
    addFunction(b, "stdin", StdinTableFunction.class);
    addFunction(b, "vmstat", VmstatTableFunction.class);
    b.append("       } ]\n")
        .append("     }\n")
        .append("   ]\n")
        .append("}");
    return b.toString();
  }

  /** Main entry point. */
  public static void main(String[] args) {
    try (PrintWriter err =
             new PrintWriter(
                 new OutputStreamWriter(System.err, StandardCharsets.UTF_8));
         InputStreamReader in =
             new InputStreamReader(System.in, StandardCharsets.UTF_8);
         PrintWriter out =
             new PrintWriter(
                 new OutputStreamWriter(System.out, StandardCharsets.UTF_8))) {
      new SqlShell(in, out, err, args).run();
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }

  void run() throws SQLException {
    final String url = "jdbc:calcite:lex=JAVA;conformance=LENIENT"
        + ";model=inline:" + MODEL;
    final String help = "Usage: sqlsh [OPTION]... SQL\n"
        + "Execute a SQL command\n"
        + "\n"
        + "Options:\n"
        + "  -o FORMAT  Print output in FORMAT; options are 'spaced' (the "
        + "default), 'csv',\n"
        + "             'headers', 'json', 'mysql'\n"
        + "  -h --help  Print this help";
    final StringBuilder b = new StringBuilder();
    Format format = Format.SPACED;
    try (Enumerator<String> args =
             Linq4j.asEnumerable(this.args).enumerator()) {
      while (args.moveNext()) {
        if (args.current().equals("-o")) {
          if (args.moveNext()) {
            String formatString = args.current();
            try {
              format = Format.valueOf(formatString.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
              throw new RuntimeException("unknown format: " + formatString);
            }
          } else {
            throw new RuntimeException("missing format");
          }
        } else if (args.current().equals("-h")
            || args.current().equals("--help")) {
          out.println(help);
          return;
        } else {
          if (b.length() > 0) {
            b.append(' ');
          }
          b.append(args.current());
        }
      }
    }
    try (Connection connection = DriverManager.getConnection(url);
         Statement s = connection.createStatement();
         Enumerator<String> args =
             Linq4j.asEnumerable(this.args).enumerator()) {
      final ResultSet r = s.executeQuery(b.toString());
      format.output(out, r);
      r.close();
    } finally {
      out.flush();
    }
  }


  private static void addView(StringBuilder b, String name, String sql) {
    if (!name.equals("du")) { // we know that "du" is the first
      b.append("}, {\n");
    }
    b.append("         \"name\": \"")
        .append(name)
        .append("\",\n")
        .append("         \"type\": \"view\",\n")
        .append("         \"sql\": \"")
        .append(sql.replaceAll("\"", "\\\\\"")
            .replaceAll("\n", ""))
        .append("\"\n");
  }

  private static void addFunction(StringBuilder b, String name, Class c) {
    if (!name.equals("du")) { // we know that "du" is the first
      b.append("}, {\n");
    }
    b.append("         \"name\": \"")
        .append(name)
        .append("\",\n")
        .append("         \"className\": \"")
        .append(c.getName())
        .append("\"\n");
  }

  /** Output format. */
  enum Format {
    SPACED {
      protected void output(PrintWriter out, ResultSet r) throws SQLException {
        final int n = r.getMetaData().getColumnCount();
        final StringBuilder b = new StringBuilder();
        while (r.next()) {
          for (int i = 0; i < n; i++) {
            if (i > 0) {
              b.append(' ');
            }
            b.append(r.getString(i + 1));
          }
          out.println(b);
          b.setLength(0);
        }
      }
    },
    HEADERS {
      protected void output(PrintWriter out, ResultSet r) throws SQLException {
        final ResultSetMetaData m = r.getMetaData();
        final int n = m.getColumnCount();
        final StringBuilder b = new StringBuilder();
        for (int i = 0; i < n; i++) {
          if (i > 0) {
            b.append(' ');
          }
          b.append(m.getColumnLabel(i + 1));
        }
        out.println(b);
        b.setLength(0);
        SPACED.output(out, r);
      }
    },
    CSV {
      protected void output(PrintWriter out, ResultSet r) throws SQLException {
        // We aim to comply with https://tools.ietf.org/html/rfc4180.
        // It's a bug if we don't.
        final ResultSetMetaData m = r.getMetaData();
        final int n = m.getColumnCount();
        final StringBuilder b = new StringBuilder();
        for (int i = 0; i < n; i++) {
          if (i > 0) {
            b.append(',');
          }
          value(b, m.getColumnLabel(i + 1));
        }
        out.print(b);
        b.setLength(0);
        while (r.next()) {
          out.println();
          for (int i = 0; i < n; i++) {
            if (i > 0) {
              b.append(',');
            }
            value(b, r.getString(i + 1));
          }
          out.print(b);
          b.setLength(0);
        }
      }

      private void value(StringBuilder b, String s) {
        if (s == null) {
          // do nothing - unfortunately same as empty string
        } else if (s.contains("\"")) {
          b.append('"')
              .append(s.replaceAll("\"", "\"\""))
              .append('"');
        } else if (s.indexOf(',') >= 0
            || s.indexOf('\n') >= 0
            || s.indexOf('\r') >= 0) {
          b.append('"').append(s).append('"');
        } else {
          b.append(s);
        }
      }
    },
    JSON {
      protected void output(PrintWriter out, final ResultSet r)
          throws SQLException {
        final ResultSetMetaData m = r.getMetaData();
        final int n = m.getColumnCount();
        final Map<String, Integer> fieldOrdinals = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
          fieldOrdinals.put(m.getColumnLabel(i + 1),
              fieldOrdinals.size() + 1);
        }
        final Set<String> fields = fieldOrdinals.keySet();
        final JsonBuilder json = new JsonBuilder();
        final StringBuilder b = new StringBuilder();
        out.println("[");
        int i = 0;
        while (r.next()) {
          if (i++ > 0) {
            out.println(",");
          }
          json.append(b, 0,
              Maps.asMap(fields, columnLabel -> {
                try {
                  final int i1 = fieldOrdinals.get(columnLabel);
                  switch (m.getColumnType(i1)) {
                  case Types.BOOLEAN:
                    final boolean b1 = r.getBoolean(i1);
                    return !b1 && r.wasNull() ? null : b1;
                  case Types.DECIMAL:
                  case Types.FLOAT:
                  case Types.REAL:
                  case Types.DOUBLE:
                    final double d = r.getDouble(i1);
                    return d == 0D && r.wasNull() ? null : d;
                  case Types.BIGINT:
                  case Types.INTEGER:
                  case Types.SMALLINT:
                  case Types.TINYINT:
                    final long v = r.getLong(i1);
                    return v == 0L && r.wasNull() ? null : v;
                  default:
                    return r.getString(i1);
                  }
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              }));
          out.append(b);
          b.setLength(0);
        }
        if (i > 0) {
          out.println();
        }
        out.println("]");
      }
    },
    MYSQL {
      protected void output(PrintWriter out, final ResultSet r)
          throws SQLException {
        // E.g.
        // +-------+--------+
        // | EMPNO | ENAME  |
        // +-------+--------+
        // |  7369 | SMITH  |
        // |   822 | LEE    |
        // +-------+--------+

        final ResultSetMetaData m = r.getMetaData();
        final int n = m.getColumnCount();
        final List<String> values = new ArrayList<>();
        final int[] lengths = new int[n];
        final boolean[] rights = new boolean[n];
        for (int i = 0; i < n; i++) {
          final String v = m.getColumnLabel(i + 1);
          values.add(v);
          lengths[i] = v.length();
          switch (m.getColumnType(i + 1)) {
          case Types.BIGINT:
          case Types.INTEGER:
          case Types.SMALLINT:
          case Types.TINYINT:
          case Types.REAL:
          case Types.FLOAT:
          case Types.DOUBLE:
            rights[i] = true;
          }
        }
        while (r.next()) {
          for (int i = 0; i < n; i++) {
            final String v = r.getString(i + 1);
            values.add(v);
            if (v != null && v.length() > lengths[i]) {
              lengths[i] = v.length();
            }
          }
        }

        final StringBuilder b = new StringBuilder("+");
        for (int length : lengths) {
          pad(b, length + 2, '-');
          b.append('+');
        }
        final String bar = b.toString();
        out.println(bar);
        b.setLength(0);

        for (int i = 0; i < n; i++) {
          if (i == 0) {
            b.append('|');
          }
          b.append(' ');
          value(b, values.get(i), lengths[i], rights[i]);
          b.append(" |");
        }
        out.println(b);
        b.setLength(0);
        out.print(bar);

        for (int h = n; h < values.size(); h++) {
          final int i = h % n;
          if (i == 0) {
            out.println(b);
            b.setLength(0);
            b.append('|');
          }
          b.append(' ');
          value(b, values.get(h), lengths[i], rights[i]);
          b.append(" |");
        }
        out.println(b);
        out.println(bar);

        int rowCount = (values.size() / n) - 1;
        if (rowCount == 1) {
          out.println("(1 row)");
        } else {
          out.print("(");
          out.print(rowCount);
          out.println(" rows)");
        }
        out.println();
      }

      private void value(StringBuilder b, String value, int length,
          boolean right) {
        if (value == null) {
          pad(b, length, ' ');
        } else {
          final int pad = length - value.length();
          if (pad == 0) {
            b.append(value);
          } else if (right) {
            pad(b, pad, ' ');
            b.append(value);
          } else {
            b.append(value);
            pad(b, pad, ' ');
          }
        }
      }

      private void pad(StringBuilder b, int pad, char c) {
        for (int j = 0; j < pad; j++) {
          b.append(c);
        }
      }
    };

    protected abstract void output(PrintWriter out, ResultSet r)
        throws SQLException;
  }
}

// End SqlShell.java
