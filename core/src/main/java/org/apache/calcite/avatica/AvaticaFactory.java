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
package org.apache.calcite.avatica;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TimeZone;

/**
 * Factory for JDBC objects.
 *
 * <p>There is an implementation for each supported JDBC version.</p>
 */
public interface AvaticaFactory {
  int getJdbcMajorVersion();

  int getJdbcMinorVersion();

  AvaticaConnection newConnection(
      UnregisteredDriver driver,
      AvaticaFactory factory,
      String url,
      Properties info) throws SQLException;

  AvaticaStatement newStatement(AvaticaConnection connection,
      /*@Nullable*/ Meta.StatementHandle h, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException;

  AvaticaPreparedStatement newPreparedStatement(AvaticaConnection connection,
      /*@Nullable*/ Meta.StatementHandle h, Meta.Signature signature,
      int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException;

  /**
   * Creates a result set. You will then need to call
   * {@link AvaticaResultSet#execute()} on it.
   *
   * @param statement Statement
   * @param state The state used to create this result set
   * @param signature Prepared statement
   * @param timeZone Time zone
   * @param firstFrame Frame containing the first (or perhaps only) rows in the
   *                   result, or null if an execute/fetch is required
   * @return Result set
   */
  AvaticaResultSet newResultSet(AvaticaStatement statement, QueryState state,
      Meta.Signature signature, TimeZone timeZone, Meta.Frame firstFrame)
      throws SQLException;

  /**
   * Creates meta data for the database.
   *
   * @return Database meta data
   */
  AvaticaSpecificDatabaseMetaData newDatabaseMetaData(AvaticaConnection connection);

  /**
   * Creates meta data for a result set.
   *
   * @param statement Statement
   * @param signature Prepared statement
   * @return Result set meta data
   */
  ResultSetMetaData newResultSetMetaData(AvaticaStatement statement,
      Meta.Signature signature) throws SQLException;
}

// End AvaticaFactory.java
