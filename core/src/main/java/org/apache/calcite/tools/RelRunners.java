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
package org.apache.calcite.tools;

import org.apache.calcite.rel.RelNode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/** Implementations of {@link RelRunner}. */
public class RelRunners {
  private RelRunners() {}

  /** Runs a relational expression by creating a JDBC connection. */
  public static PreparedStatement run(RelNode rel) {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:")) {
      final RelRunner runner = connection.unwrap(RelRunner.class);
      return runner.prepare(rel);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}

// End RelRunners.java
