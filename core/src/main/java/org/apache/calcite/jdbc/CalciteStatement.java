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
package org.apache.calcite.jdbc;

import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.server.CalciteServerStatement;

/**
 * Implementation of {@link java.sql.Statement}
 * for the Calcite engine.
 */
public abstract class CalciteStatement
    extends AvaticaStatement
    implements CalciteServerStatement {
  CalciteStatement(
      CalciteConnectionImpl connection,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) {
    super(connection, resultSetType, resultSetConcurrency,
        resultSetHoldability);
  }

  // implement Statement

  @Override public CalciteConnectionImpl getConnection() {
    return (CalciteConnectionImpl) connection;
  }

  public CalciteConnectionImpl.ContextImpl createPrepareContext() {
    return new CalciteConnectionImpl.ContextImpl(getConnection());
  }

  protected <T> CalcitePrepare.PrepareResult<T> prepare(
      Queryable<T> queryable) {
    final CalcitePrepare prepare = getConnection().prepareFactory.apply();
    return prepare.prepareQueryable(createPrepareContext(), queryable);
  }

  @Override protected void close_() {
    if (!closed) {
      closed = true;
      final CalciteConnectionImpl connection1 =
          (CalciteConnectionImpl) connection;
      connection1.server.removeStatement(this);
      if (openResultSet != null) {
        AvaticaResultSet c = openResultSet;
        openResultSet = null;
        c.close();
      }
      // If onStatementClose throws, this method will throw an exception (later
      // converted to SQLException), but this statement still gets closed.
      connection1.getDriver().handler.onStatementClose(this);
    }
  }
}

// End CalciteStatement.java
