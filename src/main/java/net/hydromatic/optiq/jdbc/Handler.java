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
package net.hydromatic.optiq.jdbc;

import java.sql.SQLException;

/**
 * Called at various points in the JDBC lifecycle.
 *
 * <p>Most drivers will use {@link HandlerImpl}, which provides no-op
 * implementations of all methods. You only need to override methods if you
 * need to achieve special effects.</p>
 */
public interface Handler {
  /** Called when a connection is being created. If it throws, the connection
   * will not be created.
   *
   * @param connection Connection
   * @throws SQLException on error
   */
  void onConnectionInit(OptiqConnection connection) throws SQLException;

  /** Called by Optiq server when a statement is being executed.
   *
   * <p>If the session would like the statement results stored in a temporary
   * table, {@code resultSink} is not null.
   * The provider must call its {@link ResultSink#toBeCompleted}
   * method at some point during execution (not necessarily before the call to
   * this method returns).</p>
   *
   * @param statement Statement
   * @param resultSink Place to put result of query. Null if Optiq does not
   *                   want results stored to a temporary table
   * @throws RuntimeException on error
   */
  void onStatementExecute(
      OptiqStatement statement,
      ResultSink resultSink) throws RuntimeException;

  interface ResultSink {
    /** Registers a temporary table. */
    void toBeCompleted();
  }
}

// End Handler.java
