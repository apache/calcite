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

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * Implementation using the SQL Server sys sequences view from version 2012 onwards.
 */
public class MssqlSequenceSupportResolver extends SequenceSupportImpl
        implements SqlDialect.SequenceSupportResolver {
  public static final SqlDialect.SequenceSupportResolver INSTANCE =
          new MssqlSequenceSupportResolver();

  private MssqlSequenceSupportResolver() {
    super(
      "select NULL, schema_name(seq.schema_id), seq.name, "
              + "CASE WHEN PRECISION = 19 THEN 'BIGINT' ELSE 'INT', seq.increment from "
              + "sys.sequences AS seq where 1=1",
      null,
      " and schema_name(seq.schema_id) = ?"
    );
  }

  @Override public SqlDialect.SequenceSupport resolveExtractor(
      DatabaseMetaData metaData) throws SQLException {
    int databaseMajorVersion = metaData.getDatabaseMajorVersion();
    // SQL Server 2012 supports sequences which has the major version number 11
    if (databaseMajorVersion >= 11) {
      return this;
    }
    return null;
  }

  @Override public void unparseSequenceVal(SqlWriter writer, SqlKind kind, SqlNode sequenceNode) {
    if (kind == SqlKind.NEXT_VALUE) {
      writer.sep("NEXT VALUE FOR");
      sequenceNode.unparse(writer, 0, 0);
    } else {
      // Apparently there is no builtin syntax to do this
      // https://stackoverflow.com/questions/13702471/get-current-value-from-a-sql-server-sequence
      writer.sep("select current_value from sys.sequences where name = '");
      sequenceNode.unparse(writer, 0, 0);
      writer.sep("'");
    }
  }
}

// End MssqlSequenceSupportResolver.java
