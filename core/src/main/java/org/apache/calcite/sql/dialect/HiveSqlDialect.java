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

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

import org.apache.maven.artifact.versioning.ComparableVersion;

/**
 * A <code>SqlDialect</code> implementation for the Apache Hive database.
 */
public class HiveSqlDialect extends SqlDialect {
  public static final SqlDialect DEFAULT =
      new HiveSqlDialect(EMPTY_CONTEXT.withDatabaseProduct(DatabaseProduct.HIVE)
          .withNullCollation(NullCollation.LOW));

  private static final SqlFunction ISNULL_FUNCTION =
      new SqlFunction("ISNULL", SqlKind.OTHER_FUNCTION,
          ReturnTypes.BOOLEAN_NOT_NULL, InferTypes.FIRST_KNOWN,
          OperandTypes.ANY, SqlFunctionCategory.SYSTEM);

  /**
   * Creates a HiveSqlDialect.
   */
  public HiveSqlDialect(Context context) {
    super(context);
  }

  @Override protected boolean allowsAs() {
    return false;
  }

  @Override public boolean supportsOffsetFetch() {
    return false;
  }

  /**
   * Since 2.1.0, Hive natively supports "NULLS FIRST" and "NULLS LAST".
   * https://issues.apache.org/jira/browse/HIVE-12994*
   */
  @Override public SqlNode emulateNullDirection(SqlNode node, boolean nullsFirst) {
    if (databaseVersion == null || compareVersions("2.1.0", databaseVersion) < 0) {
      node = ISNULL_FUNCTION.createCall(SqlParserPos.ZERO, node);
      if (nullsFirst) {
        node = SqlStdOperatorTable.DESC.createCall(SqlParserPos.ZERO, node);
      }
      return node;
    }

    return null;
  }

  private static int compareVersions(String v1, String v2) {
    return new ComparableVersion(v1).compareTo(new ComparableVersion(v2));
  }
}

// End HiveSqlDialect.java
