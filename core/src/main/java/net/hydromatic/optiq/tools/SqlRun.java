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
import java.util.ArrayList;
import java.util.List;

/**
 * Runs a SQL script.
 */
public class SqlRun {
  private BufferedReader reader;
  private Writer writer;
  private PrintWriter printWriter;
  private OutputFormat outputFormat = OutputFormat.CSV;
  private ResultSet resultSet;
  private final List<String> lines = new ArrayList<String>();
  private String pushedLine;
  private final StringBuilder buf = new StringBuilder();
  private Connection connection;
  private ConnectionFactory connectionFactory;

  public SqlRun(BufferedReader reader, Writer writer) {
    this.reader = reader;
    this.writer = writer;
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
        try {
          command.execute();
        } catch (Exception e) {
          throw new RuntimeException(
              "Error while executing command " + command, e);
        }
        for (String line : command.lines()) {
          try {
            printWriter.println(line);
          } catch (Exception e) {
            throw new RuntimeException("Error while writing output", e);
          }
        }
      }
    } finally {
      printWriter.flush();
      this.connection = null;
    }
  }

  private Command nextCommand() throws IOException {
    lines.clear();
    String line = nextLine();
    if (line == null) {
      return null;
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
    buf.setLength(0);
    for (;;) {
      line = nextLine();
      if (line == null || line.startsWith("!")) {
        pushLine();
        break;
      }
      buf.append(line).append("\n");
    }
    String output = buf.toString();
    return new SqlCommand(sqlLines, sql, output);
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

  /** Command. */
  interface Command {
    void execute() throws Exception;
    Iterable<String> lines();
  }

  /** Base class for implementations of Command. */
  abstract class AbstractCommand implements Command {
    private final ImmutableList<String> lines;

    public AbstractCommand(List<String> lines) {
      this.lines = ImmutableList.copyOf(lines);
    }

    public Iterable<String> lines() {
      return lines;
    }
  }

  /** Command that sets the current connection. */
  class UseCommand extends AbstractCommand {
    private final String name;

    public UseCommand(List<String> lines, String name) {
      super(lines);
      this.name = name;
    }

    public void execute() throws Exception {
      if (connection != null) {
        connection.close();
      }
      connection = connectionFactory.connect(name);
    }
  }

  /** Command that executes a SQL statement and checks its result. */
  class CheckResultCommand extends AbstractCommand {
    public CheckResultCommand(List<String> lines) {
      super(lines);
    }

    public void execute() throws Exception {
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final int n = metaData.getColumnCount();
      for (int i = 0; i < n; i++) {
        if (i > 0) {
          printWriter.print(", ");
        }
        printWriter.print(metaData.getColumnLabel(i + 1));
      }
      printWriter.println();
      while (resultSet.next()) {
        for (int i = 0; i < n; i++) {
          if (i > 0) {
            printWriter.print(", ");
          }
          printWriter.print(resultSet.getString(i + 1));
        }
        printWriter.println();
      }
      resultSet.close();
    }
  }

  /** Schemes for converting the outout of a SQL statement into text. */
  enum OutputFormat {
    CSV
  }

  /** Command that executes a SQL statement. */
  private class SqlCommand extends AbstractCommand {
    private final String sql;
    private final String output;

    protected SqlCommand(List<String> lines, String sql, String output) {
      super(lines);
      this.sql = sql;
      this.output = output;
    }

    public void execute() throws Exception {
      if (connection == null) {
        throw new RuntimeException("no connection");
      }
      final Statement statement = connection.createStatement();
      resultSet = statement.executeQuery(sql);
    }
  }

  /** Creates a connection for a given name.
   * Kind of a directory service.
   * Caller must close the connection. */
  public interface ConnectionFactory {
    Connection connect(String name) throws Exception;
  }
}

// End SqlRun.java
