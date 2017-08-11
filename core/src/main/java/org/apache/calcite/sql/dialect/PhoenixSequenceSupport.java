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
 * Implementation for the Phoenix dialect.
 */
public class PhoenixSequenceSupport extends SequenceSupportImpl
        implements SqlDialect.SequenceSupportResolver {
  public static final SqlDialect.SequenceSupportResolver INSTANCE =
          new PhoenixSequenceSupport();

  protected PhoenixSequenceSupport() {
    super(
      "select NULL, sequence_schema, sequence_name, 'BIGINT', increment_by from "
              + "information_schema.sequences where 1=1",
      null,
      " and sequence_schema = ?");
  }

  @Override public SqlDialect.SequenceSupport resolveExtractor(
          DatabaseMetaData metaData) throws SQLException {
    return this;
  }

  @Override public void unparseSequenceVal(SqlWriter writer, SqlKind kind, SqlNode sequenceNode) {
    if (kind == SqlKind.NEXT_VALUE) {
      writer.sep("NEXT VALUE FOR");
      sequenceNode.unparse(writer, 0, 0);
    } else {
      // Apparently there is no builtin syntax to do this
      // https://phoenix.apache.org/sequences.html
      writer.sep("select current_value from system.sequence where sequence_name = '");
      sequenceNode.unparse(writer, 0, 0);
      writer.sep("'");
    }
  }
}

// End PhoenixSequenceSupport.java
