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
package net.hydromatic.optiq.tools;

import com.google.common.collect.ImmutableList;

import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * Runs a SQL script.
 */
public class SqlRun {
  private BufferedReader reader;
  private Writer writer;
  private PrintWriter printWriter;
  private final Map<Property, Object> map = new HashMap<Property, Object>();
  private ResultSet resultSet;
  private SQLException resultSetException;
  private final List<String> lines = new ArrayList<String>();
  private String pushedLine;
  private final StringBuilder buf = new StringBuilder();
  private Connection connection;
  private ConnectionFactory connectionFactory;
  private boolean execute = true;

  public SqlRun(BufferedReader reader, Writer writer) {
    this.reader = reader;
    this.writer = writer;
    this.map.put(Property.OUTPUTFORMAT, OutputFormat.CSV);
  }

  public static void main(String[] args) {
    final File inFile = new File(args[0]);
    final File outFile = new File(args[1]);
    final Reader reader;
    try {
      reader = new LineNumberReader(new FileReader(inFile));
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Error opening input " + inFile, e);
    }
    final Writer writer;
    try {
      writer = new FileWriter(outFile);
    } catch (IOException e) {
      throw new RuntimeException("Error opening output " + outFile, e);
    }
    final SqlRun sqlRun = new SqlRun(new BufferedReader(reader), writer);
    try {
      sqlRun.execute(
          new ConnectionFactory() {
            public Connection connect(String name) {
              throw new UnsupportedOperationException();
            }
          });
      reader.close();
      writer.close();
    } catch (Throwable e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  public void execute(ConnectionFactory connectionFactory) {
    this.connectionFactory = connectionFactory;
    this.printWriter = new PrintWriter(writer);
    try {
      Command command = new Parser().parse();
      try {
        command.execute();
      } catch (Exception e) {
        throw new RuntimeException(
            "Error while executing command " + command, e);
      }
    } finally {
      printWriter.flush();
      this.connection = null;
    }
  }

  Command of(List<Command> commands) {
    return commands.size() == 1
        ? commands.get(0)
        : new CompositeCommand(ImmutableList.copyOf(commands));
  }

  private static String pad(String s, int width, boolean right) {
    final int x = width - s.length();
    if (x <= 0) {
      return s;
    }
    final StringBuilder buf = new StringBuilder();
    if (right) {
      buf.append(chars(' ', x)).append(s);
    } else {
      buf.append(s).append(chars(' ', x));
    }
    return buf.toString();
  }

  private static CharSequence chars(final char c, final int length) {
    return new CharSequence() {
      @Override public String toString() {
        final StringBuilder buf = new StringBuilder();
        for (int i = 0; i < length; i++) {
          buf.append(c);
        }
        return buf.toString();
      }

      public int length() {
        return length;
      }

      public char charAt(int index) {
        return c;
      }

      public CharSequence subSequence(int start, int end) {
        return SqlRun.chars(c, end - start);
      }
    };
  }

  /** Parser. */
  private class Parser {
    Command parse() {
      List<Command> commands = new ArrayList<Command>();
      for (;;) {
        Command command;
        try {
          command = nextCommand();
        } catch (IOException e) {
          throw new RuntimeException("Error while reading next command", e);
        }
        if (command == null) {
          break;
        }
        commands.add(command);
      }
      return of(commands);
    }

    private Command nextCommand() throws IOException {
      lines.clear();
      String line = nextLine();
      if (line == null) {
        return null;
      }
      if (line.startsWith("#") || line.isEmpty()) {
        return new CommentCommand(lines);
      }
      if (line.startsWith("!")) {
        line = line.substring(1);
        while (line.startsWith(" ")) {
          line = line.substring(1);
        }
        if (line.startsWith("use")) {
          String[] parts = line.split(" ");
          return new UseCommand(lines, parts[1]);
        }
        if (line.startsWith("ok")) {
          return new CheckResultCommand(lines);
        }
        if (line.startsWith("set outputformat")) {
          String[] parts = line.split(" ");
          final OutputFormat outputFormat =
              OutputFormat.valueOf(parts[2].toUpperCase());
          return new SetCommand(lines, Property.OUTPUTFORMAT, outputFormat);
        }
        if (line.equals("if (false) {")) {
          List<String> ifLines = ImmutableList.copyOf(lines);
          lines.clear();
          Command command = new Parser().parse();
          return new IfCommand(ifLines, lines, command);
        }
        if (line.equals("}")) {
          return null;
        }
        throw new RuntimeException("Unknown command: " + line);
      }
      buf.setLength(0);
      for (;;) {
        boolean last = false;
        if (line.endsWith(";")) {
          last = true;
          line = line.substring(0, line.length() - 1);
        }
        buf.append(line).append("\n");
        if (last) {
          break;
        }
        line = nextLine();
        if (line == null) {
          throw new RuntimeException(
              "end of file reached before end of SQL command");
        }
      }
      final ImmutableList<String> sqlLines = ImmutableList.copyOf(lines);
      String sql = buf.toString();
      for (;;) {
        line = nextLine();
        if (line == null || line.startsWith("!") || line.startsWith("#")) {
          pushLine();
          break;
        }
      }
      final List<String> outputLines =
          lines.subList(sqlLines.size(), lines.size());
      return new SqlCommand(sqlLines, sql, outputLines);
    }


    private void pushLine() {
      if (pushedLine != null) {
        throw new AssertionError("cannot push two lines");
      }
      if (lines.size() == 0) {
        throw new AssertionError("no line has been read");
      }
      pushedLine = lines.get(lines.size() - 1);
      lines.remove(lines.size() - 1);
    }

    private String nextLine() throws IOException {
      String line;
      if (pushedLine != null) {
        line = pushedLine;
        pushedLine = null;
      } else {
        line = reader.readLine();
        if (line == null) {
          return null;
        }
      }
      lines.add(line);
      return line;
    }
  }

  /** Schemes for converting the output of a SQL statement into text. */
  enum OutputFormat {
    CSV {
      @Override
      public void format(ResultSet resultSet, SqlRun run) throws Exception {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final PrintWriter pw = run.printWriter;
        final int n = metaData.getColumnCount();
        for (int i = 0; i < n; i++) {
          if (i > 0) {
            pw.print(", ");
          }
          pw.print(metaData.getColumnLabel(i + 1));
        }
        pw.println();
        while (resultSet.next()) {
          for (int i = 0; i < n; i++) {
            if (i > 0) {
              pw.print(", ");
            }
            pw.print(resultSet.getString(i + 1));
          }
          pw.println();
        }
      }
    },

    // Example:
    //
    //  ename | deptno | gender | first_value
    // -------+--------+--------+-------------
    //  Jane  |     10 | F      | Jane
    //  Bob   |     10 | M      | Jane
    // (2 rows)
    PSQL {
      @Override
      public void format(ResultSet resultSet, SqlRun run) throws Exception {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final PrintWriter pw = run.printWriter;
        final int n = metaData.getColumnCount();
        final int[] widths = new int[n];
        final List<String[]> rows = new ArrayList<String[]>();
        final boolean[] rights = new boolean[n];
        for (int i = 0; i < n; i++) {
          widths[i] = metaData.getColumnLabel(i + 1).length();
        }
        while (resultSet.next()) {
          String[] row = new String[n];
          for (int i = 0; i < n; i++) {
            String value = resultSet.getString(i + 1);
            widths[i] = Math.max(widths[i], value.length());
            row[i] = value;
          }
          rows.add(row);
        }
        for (int i = 0; i < widths.length; i++) {
          switch (metaData.getColumnType(i + 1)) {
          case Types.INTEGER:
            rights[i] = true;
          }
        }
        for (int i = 0; i < n; i++) {
          pw.print(i > 0 ? " | " : " ");
          pw.print(metaData.getColumnLabel(i + 1));
        }
        pw.println();
        for (int i = 0; i < n; i++) {
          if (i > 0) {
            pw.print("+");
          }
          pw.print(chars('-', widths[i] + 2));
        }
        pw.println();
        for (String[] row : rows) {
          for (int i = 0; i < n; i++) {
            pw.print(i > 0 ? " | " : " ");
            // don't pad the last field if it is left-justified
            final String s = i == n - 1 && !rights[i]
                ? row[i]
                : pad(row[i], widths[i], rights[i]);
            pw.print(s);
          }
          pw.println();
        }
        pw.println(rows.size() == 1 ? "(1 row)" : "(" + rows.size() + " rows)");
        pw.println();
      }
    };

    public abstract void format(ResultSet resultSet, SqlRun run)
      throws Exception;
  }

  /** Command. */
  interface Command {
    void execute() throws Exception;
  }

  /** Base class for implementations of Command. */
  abstract class AbstractCommand implements Command {
    protected Command echo(Iterable<String> lines) {
      for (String line : lines) {
        try {
          printWriter.println(line);
        } catch (Exception e) {
          throw new RuntimeException("Error while writing output", e);
        }
      }
      return this;
    }
  }

  /** Base class for implementations of Command that have one piece of source
   * code. */
  abstract class SimpleCommand extends AbstractCommand {
    protected final ImmutableList<String> lines;

    public SimpleCommand(List<String> lines) {
      this.lines = ImmutableList.copyOf(lines);
    }
  }

  /** Command that sets the current connection. */
  class UseCommand extends SimpleCommand {
    private final String name;

    public UseCommand(List<String> lines, String name) {
      super(lines);
      this.name = name;
    }

    public void execute() throws Exception {
      echo(lines);
      if (connection != null) {
        connection.close();
      }
      connection = connectionFactory.connect(name);
    }
  }

  /** Command that executes a SQL statement and checks its result. */
  class CheckResultCommand extends SimpleCommand {
    public CheckResultCommand(List<String> lines) {
      super(lines);
    }

    public void execute() throws Exception {
      if (execute) {
        OutputFormat format = (OutputFormat) map.get(Property.OUTPUTFORMAT);
        if (resultSet != null) {
          format.format(resultSet, SqlRun.this);
          resultSet.close();
        } else if (resultSetException != null) {
          resultSetException.printStackTrace(printWriter);
        } else {
          throw new AssertionError("neither resultSet nor exception set");
        }
        resultSet = null;
        resultSetException = null;
      }
      echo(lines);
    }
  }

  /** Command that executes a SQL statement. */
  private class SqlCommand extends SimpleCommand {
    private final String sql;
    private final ImmutableList<String> output;

    protected SqlCommand(List<String> lines, String sql, List<String> output) {
      super(lines);
      this.sql = sql;
      this.output = ImmutableList.copyOf(output);
    }

    public void execute() throws Exception {
      echo(lines);
      if (execute) {
        if (connection == null) {
          throw new RuntimeException("no connection");
        }
        final Statement statement = connection.createStatement();
        if (resultSet != null) {
          throw new AssertionError("result set already present");
        }
        try {
          resultSet = statement.executeQuery(sql);
        } catch (SQLException e) {
          resultSetException = e;
        }
      } else {
        echo(output);
      }
    }
  }

  /** Creates a connection for a given name.
   * Kind of a directory service.
   * Caller must close the connection. */
  public interface ConnectionFactory {
    Connection connect(String name) throws Exception;
  }

  /** Property whose value may be set. */
  enum Property {
    OUTPUTFORMAT
  }

  /** Command that executes a SQL statement and checks its result. */
  class SetCommand extends SimpleCommand {
    private final Property property;
    private final Object value;

    public SetCommand(List<String> lines, Property property, Object value) {
      super(lines);
      this.property = property;
      this.value = value;
    }

    public void execute() throws Exception {
      echo(lines);
      map.put(property, value);
    }
  }

  /** Command that executes a comment. (Does nothing.) */
  class CommentCommand extends SimpleCommand {
    public CommentCommand(List<String> lines) {
      super(lines);
    }

    public void execute() throws Exception {
      echo(lines);
    }
  }

  /** Command that executes a comment. (Does nothing.) */
  class IfCommand extends AbstractCommand {
    private final List<String> ifLines;
    private final List<String> endLines;
    private final Command command;

    public IfCommand(List<String> ifLines,
        List<String> endLines, Command command) {
      this.ifLines = ImmutableList.copyOf(ifLines);
      this.endLines = ImmutableList.copyOf(endLines);
      this.command = command;
    }

    public void execute() throws Exception {
      echo(ifLines);
      // switch to a mode where we don't execute, just echo
      boolean oldExecute = execute;
      execute = false;
      command.execute();
      execute = oldExecute;
      echo(endLines);
    }
  }

  /** Command that executes a comment. (Does nothing.) */
  class CompositeCommand extends AbstractCommand {
    private final List<Command> commands;

    public CompositeCommand(List<Command> commands) {
      this.commands = commands;
    }

    public void execute() throws Exception {
      for (Command command : commands) {
        try {
          command.execute();
        } catch (Exception e) {
          throw new RuntimeException(
              "Error while executing command " + command, e);
        }
      }
    }
  }
}

// End SqlRun.java
