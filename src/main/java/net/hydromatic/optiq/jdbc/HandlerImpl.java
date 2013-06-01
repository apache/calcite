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
 * Implementation of {@link Handler} that does nothing for each callback.
 * It is recommended implementations of {@code Handler} use this as a base
 * class, to ensure forward compatibility.
 */
public class HandlerImpl implements Handler {
  public void onConnectionInit(OptiqConnection connection)
      throws SQLException {
    // nothing
  }

  public void onStatementExecute(
      OptiqStatement statement, ResultSink resultSink) throws RuntimeException {
    // nothing
  }
}

// End HandlerImpl.java
