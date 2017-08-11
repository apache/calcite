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

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * Implementation using the information schema sequences table.
 */
public class InformationSchemaSequenceSupport extends SequenceSupportImpl
        implements SqlDialect.SequenceSupportResolver {
  public static final SqlDialect.SequenceSupportResolver INSTANCE =
          new InformationSchemaSequenceSupport();

  protected InformationSchemaSequenceSupport() {
    super(
      "select SEQUENCE_CATALOG, SEQUENCE_SCHEMA, SEQUENCE_NAME, DATA_TYPE, INCREMENT from "
        + "information_schema.sequences where 1=1",
      " and SEQUENCE_CATALOG = ?",
      " and SEQUENCE_SCHEMA = ?");
  }

  @Override public SqlDialect.SequenceSupport resolveExtractor(
      DatabaseMetaData metaData) throws SQLException {
    return this;
  }
}

// End InformationSchemaSequenceSupport.java
