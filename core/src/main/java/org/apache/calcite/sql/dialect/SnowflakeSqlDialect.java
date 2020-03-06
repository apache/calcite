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

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.util.ToNumberUtils;

/**
 * A <code>SqlDialect</code> implementation for the Snowflake database.
 */
public class SnowflakeSqlDialect extends SqlDialect {
  public static final SqlDialect DEFAULT =
      new SnowflakeSqlDialect(EMPTY_CONTEXT
          .withDatabaseProduct(DatabaseProduct.SNOWFLAKE)
          .withIdentifierQuoteString("\"")
          .withUnquotedCasing(Casing.TO_UPPER));

  /** Creates a SnowflakeSqlDialect. */
  public SnowflakeSqlDialect(Context context) {
    super(context);
  }

  @Override public boolean supportsAliasedValues() {
    return false;
  }

  @Override public boolean supportsCharSet() {
    return false;
  }

  @Override public void unparseCall(final SqlWriter writer, final SqlCall call, final
  int leftPrec, final int rightPrec) {
    switch (call.getKind()) {
    case SUBSTRING:
      final SqlWriter.Frame substringFrame = writer.startFunCall("SUBSTR");
      for (SqlNode operand : call.getOperandList()) {
        writer.sep(",");
        operand.unparse(writer, leftPrec, rightPrec);
      }
      writer.endFunCall(substringFrame);
      break;
    case TO_NUMBER:
      if (ToNumberUtils.needsCustomUnparsing(call)) {
        ToNumberUtils.unparseToNumberSnowFlake(writer, call, leftPrec, rightPrec);
      } else {
        super.unparseCall(writer, call, leftPrec, rightPrec);
      }
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }

}

// End SnowflakeSqlDialect.java
