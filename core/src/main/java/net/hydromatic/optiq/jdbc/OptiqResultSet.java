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

import net.hydromatic.avatica.*;

import net.hydromatic.linq4j.Enumerator;
import net.hydromatic.linq4j.Linq4j;

import net.hydromatic.optiq.runtime.*;

import com.google.common.collect.ImmutableList;

import java.sql.*;
import java.util.*;

/**
 * Implementation of {@link ResultSet}
 * for the Optiq engine.
 */
public class OptiqResultSet extends AvaticaResultSet {
  OptiqResultSet(
      AvaticaStatement statement,
      OptiqPrepare.PrepareResult prepareResult,
      ResultSetMetaData resultSetMetaData,
      TimeZone timeZone) {
    super(statement, prepareResult, resultSetMetaData, timeZone);
  }

  @Override protected OptiqResultSet execute() throws SQLException {
    // Call driver's callback. It is permitted to throw a RuntimeException.
    OptiqConnectionImpl connection = getOptiqConnection();
    final boolean autoTemp = connection.config().autoTemp();
    Handler.ResultSink resultSink = null;
    if (autoTemp) {
      resultSink = new Handler.ResultSink() {
        public void toBeCompleted() {
        }
      };
    }
    connection.getDriver().handler.onStatementExecute(
        statement, resultSink);

    super.execute();
    return this;
  }

  @Override public ResultSet create(ColumnMetaData.AvaticaType elementType,
      Iterable iterable) {
    final OptiqResultSet resultSet =
        new OptiqResultSet(statement,
            (OptiqPrepare.PrepareResult) prepareResult, resultSetMetaData,
            localCalendar.getTimeZone());
    final Cursor cursor = resultSet.createCursor(elementType, iterable);
    final List<ColumnMetaData> columnMetaDataList;
    if (elementType instanceof ColumnMetaData.StructType) {
      columnMetaDataList = ((ColumnMetaData.StructType) elementType).columns;
    } else {
      columnMetaDataList =
          ImmutableList.of(ColumnMetaData.dummy(elementType, false));
    }
    return resultSet.execute2(cursor, columnMetaDataList);
  }

  private Cursor createCursor(ColumnMetaData.AvaticaType elementType,
      Iterable iterable) {
    final Enumerator enumerator = Linq4j.iterableEnumerator(iterable);
    //noinspection unchecked
    return !(elementType instanceof ColumnMetaData.StructType)
        || ((ColumnMetaData.StructType) elementType).columns.size() == 1
        ? new ObjectEnumeratorCursor(enumerator)
        : new ArrayEnumeratorCursor(enumerator);
  }

  // do not make public
  OptiqPrepare.PrepareResult getPrepareResult() {
    return (OptiqPrepare.PrepareResult) prepareResult;
  }

  // do not make public
  OptiqConnectionImpl getOptiqConnection() {
    return (OptiqConnectionImpl) statement.getConnection();
  }
}

// End OptiqResultSet.java
