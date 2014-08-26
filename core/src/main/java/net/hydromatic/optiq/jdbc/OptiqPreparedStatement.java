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
package net.hydromatic.optiq.jdbc;

import net.hydromatic.avatica.AvaticaPrepareResult;
import net.hydromatic.avatica.AvaticaPreparedStatement;

import net.hydromatic.optiq.server.OptiqServerStatement;

import java.sql.*;

/**
 * Implementation of {@link java.sql.PreparedStatement}
 * for the Optiq engine.
 *
 * <p>This class has sub-classes which implement JDBC 3.0 and JDBC 4.0 APIs;
 * it is instantiated using {@link net.hydromatic.avatica.AvaticaFactory#newPreparedStatement}.</p>
 */
abstract class OptiqPreparedStatement
    extends AvaticaPreparedStatement
    implements OptiqServerStatement {
  /**
   * Creates an OptiqPreparedStatement.
   *
   * @param connection Connection
   * @param prepareResult Result of preparing statement
   *
   * @throws SQLException if database error occurs
   */
  protected OptiqPreparedStatement(
      OptiqConnectionImpl connection,
      AvaticaPrepareResult prepareResult,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability)
      throws SQLException {
    super(
        connection, prepareResult, resultSetType, resultSetConcurrency,
        resultSetHoldability);
  }

  @Override
  public OptiqConnectionImpl getConnection() {
    return (OptiqConnectionImpl) super.getConnection();
  }

  public OptiqConnectionImpl.ContextImpl createPrepareContext() {
    return new OptiqConnectionImpl.ContextImpl(getConnection());
  }
}

// End OptiqPreparedStatement.java
