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
package org.apache.calcite.sql.dialect;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

/**
 * The default implementation for sequence information extraction.
 */
public class SequenceSupportImpl implements SqlDialect.SequenceSupport {
  private final String sequencesQuery;
  private final String catalogFilter;
  private final String schemaFilter;

  public SequenceSupportImpl(
      String sequencesQuery,
      String catalogFilter,
      String schemaFilter) {
    this.sequencesQuery = sequencesQuery;
    this.catalogFilter = catalogFilter;
    this.schemaFilter = schemaFilter;
  }

  @Override public Collection<SqlDialect.SequenceInformation> extract(
      Connection connection,
      String catalog,
      String schema) throws SQLException {
    PreparedStatement statement = null;
    ResultSet resultSet = null;
    try {
      String query = sequencesQuery;
      boolean filterCatalog = catalogFilter != null && catalog != null && !catalog.isEmpty();
      boolean filterSchema = schemaFilter != null && schema != null && !schema.isEmpty();
      if (filterCatalog) {
        query += catalogFilter;
      }
      if (filterSchema) {
        query += schemaFilter;
      }
      statement = connection.prepareStatement(query);
      if (filterCatalog) {
        statement.setString(1, catalog);
      }
      if (filterSchema) {
        statement.setString(filterCatalog ? 2 : 1, schema);
      }
      resultSet = statement.executeQuery();

      List<SqlDialect.SequenceInformation> sequenceInformations = new ArrayList<>();
      while (resultSet.next()) {
        String sequenceCatalog = resultSet.getString(1);
        String sequenceSchema = resultSet.getString(2);
        String sequenceName = resultSet.getString(3);
        String type = resultSet.getString(4);
        int increment = resultSet.getInt(5);
        SqlTypeName typeName;

        if (type == null || type.isEmpty()) {
          typeName = SqlTypeName.BIGINT;
        } else {
          typeName = Util.enumVal(SqlTypeName.BIGINT, type.toUpperCase(Locale.US));
        }

        sequenceInformations.add(
            new SequenceInformationImpl(
                sequenceCatalog,
                sequenceSchema,
                sequenceName,
                typeName,
                increment));
      }
      return sequenceInformations;
    } finally {
      if (resultSet != null) {
        try {
          resultSet.close();
        } catch (SQLException e) {
          // ignore
        }
      }
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException e) {
          // ignore
        }
      }
    }
  }

  @Override public void unparseSequenceVal(SqlWriter writer, SqlKind kind, SqlNode sequenceNode) {
    writer.sep(kind == SqlKind.NEXT_VALUE ? "NEXT VALUE FOR" : "CURRENT VALUE FOR");
    sequenceNode.unparse(writer, 0, 0);
  }
}

// End SequenceSupportImpl.java
