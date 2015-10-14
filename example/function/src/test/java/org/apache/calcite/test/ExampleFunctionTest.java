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

import org.apache.calcite.example.maze.MazeTable;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.TableFunctionImpl;

import org.junit.Test;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Unit tests for example user-defined functions.
 */
public class ExampleFunctionTest {
  public static final Method MAZE_METHOD =
      Types.lookupMethod(MazeTable.class, "generate", int.class, int.class,
          int.class);
  public static final Method SOLVE_METHOD =
      Types.lookupMethod(MazeTable.class, "solve", int.class, int.class,
          int.class);

  /** Unit test for {@link MazeTable}. */
  @Test public void testMazeTableFunction()
      throws SQLException, ClassNotFoundException {
    final String maze = ""
        + "+--+--+--+--+--+\n"
        + "|        |     |\n"
        + "+--+  +--+--+  +\n"
        + "|     |  |     |\n"
        + "+  +--+  +--+  +\n"
        + "|              |\n"
        + "+--+--+--+--+--+\n";
    checkMazeTableFunction(false, maze);
  }

  /** Unit test for {@link MazeTable}. */
  @Test public void testMazeTableFunctionWithSolution()
      throws SQLException, ClassNotFoundException {
    final String maze = ""
        + "+--+--+--+--+--+\n"
        + "|*  *    |     |\n"
        + "+--+  +--+--+  +\n"
        + "|*  * |  |     |\n"
        + "+  +--+  +--+  +\n"
        + "|*  *  *  *  * |\n"
        + "+--+--+--+--+--+\n";
    checkMazeTableFunction(true, maze);
  }

  public void checkMazeTableFunction(Boolean solution, String maze)
      throws SQLException, ClassNotFoundException {
    Connection connection = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
    final TableFunction table = TableFunctionImpl.create(MAZE_METHOD);
    schema.add("Maze", table);
    final TableFunction table2 = TableFunctionImpl.create(SOLVE_METHOD);
    schema.add("Solve", table2);
    final String sql;
    if (solution) {
      sql = "select *\n"
          + "from table(\"s\".\"Solve\"(5, 3, 1)) as t(s)";
    } else {
      sql = "select *\n"
          + "from table(\"s\".\"Maze\"(5, 3, 1)) as t(s)";
    }
    ResultSet resultSet = connection.createStatement().executeQuery(sql);
    final StringBuilder b = new StringBuilder();
    while (resultSet.next()) {
      b.append(resultSet.getString(1)).append("\n");
    }
    assertThat(b.toString(), is(maze));
  }
}

// End ExampleFunctionTest.java
