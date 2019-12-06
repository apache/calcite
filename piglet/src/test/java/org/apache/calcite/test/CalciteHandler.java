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

import org.apache.calcite.piglet.Handler;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.PigRelBuilder;
import org.apache.calcite.tools.RelRunners;
import org.apache.calcite.util.TestUtil;

import java.io.PrintWriter;
import java.io.Writer;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Extension to {@link org.apache.calcite.piglet.Handler} that can execute
 * commands using Calcite.
 */
class CalciteHandler extends Handler {
  private final PrintWriter writer;

  CalciteHandler(PigRelBuilder builder, Writer writer) {
    super(builder);
    this.writer = new PrintWriter(writer);
  }

  @Override protected void dump(RelNode rel) {
    dump(rel, writer);
  }

  public static void dump(RelNode rel, Writer writer) {
    try (PreparedStatement preparedStatement = RelRunners.run(rel)) {
      final ResultSet resultSet = preparedStatement.executeQuery();
      dump(resultSet, true, new PrintWriter(writer));
    } catch (SQLException e) {
      throw TestUtil.rethrow(e);
    }
  }

  private static void dump(ResultSet resultSet, boolean newline, PrintWriter writer)
      throws SQLException {
    final int columnCount = resultSet.getMetaData().getColumnCount();
    int r = 0;
    while (resultSet.next()) {
      if (!newline && r++ > 0) {
        writer.print(",");
      }
      if (columnCount == 0) {
        if (newline) {
          writer.println("()");
        } else {
          writer.print("()");
        }
      } else {
        writer.print('(');
        dumpColumn(resultSet, 1, writer);
        for (int i = 2; i <= columnCount; i++) {
          writer.print(',');
          dumpColumn(resultSet, i, writer);
        }
        if (newline) {
          writer.println(')');
        } else {
          writer.print(")");
        }
      }
    }
  }

  /** Dumps a column value.
   *
   * @param i Column ordinal, 1-based
   */
  private static void dumpColumn(ResultSet resultSet, int i, PrintWriter writer)
      throws SQLException {
    final int t = resultSet.getMetaData().getColumnType(i);
    switch (t) {
    case Types.ARRAY:
      final Array array = resultSet.getArray(i);
      writer.print("{");
      if (array != null) {
        dump(array.getResultSet(), false, writer);
      }
      writer.print("}");
      return;
    case Types.REAL:
      writer.print(resultSet.getString(i));
      writer.print("F");
      return;
    default:
      writer.print(resultSet.getString(i));
    }
  }
}

// End CalciteHandler.java
