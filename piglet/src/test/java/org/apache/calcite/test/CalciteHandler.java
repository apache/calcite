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

import com.google.common.base.Throwables;

import java.io.PrintWriter;
import java.io.Writer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Extension to {@link org.apache.calcite.piglet.Handler} that can execute
 * commands using Calcite.
 */
class CalciteHandler extends Handler {
  private final PrintWriter writer;

  public CalciteHandler(PigRelBuilder builder, Writer writer) {
    super(builder);
    this.writer = new PrintWriter(writer);
  }

  @Override protected void dump(RelNode rel) {
    try (final PreparedStatement preparedStatement = RelRunners.run(rel)) {
      final ResultSet resultSet = preparedStatement.executeQuery();
      final int columnCount = resultSet.getMetaData().getColumnCount();
      while (resultSet.next()) {
        if (columnCount == 0) {
          writer.println("()");
        } else {
          writer.print('(');
          writer.print(resultSet.getObject(1));
          for (int i = 1; i < columnCount; i++) {
            writer.print(',');
            writer.print(resultSet.getString(i + 1));
          }
          writer.println(')');
        }
      }
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    }
  }
}

// End CalciteHandler.java
