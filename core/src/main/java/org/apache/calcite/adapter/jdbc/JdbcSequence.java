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
package org.apache.calcite.adapter.jdbc;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Sequence;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * An extension of the table with additional sequence information.
 */
class JdbcSequence extends JdbcTable implements Sequence {
  private final SqlTypeName sequenceType;
  private final int increment;

  JdbcSequence(JdbcSchema jdbcSchema, String jdbcCatalogName,
                      String jdbcSchemaName, String sequenceName, SqlTypeName sequenceType,
                      int increment) {
    super(jdbcSchema, jdbcCatalogName, jdbcSchemaName, sequenceName, Schema.TableType.SEQUENCE);
    this.sequenceType = sequenceType;
    this.increment = increment;
  }

  @Override public String toString() {
    return "JdbcSequence {" + getJdbcTableName() + "}";
  }

  @Override public Schema.TableType getJdbcTableType() {
    return Schema.TableType.SEQUENCE;
  }

  @Override public SqlTypeName getSequenceType() {
    return sequenceType;
  }

  @Override public int getIncrement() {
    return increment;
  }
}

// End JdbcSequence.java
