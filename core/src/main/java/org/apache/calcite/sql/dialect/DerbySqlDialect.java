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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * A <code>SqlDialect</code> implementation for the Apache Derby database.
 */
public class DerbySqlDialect extends SqlDialect {
  public static final SqlDialect.Context DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
      .withDatabaseProduct(SqlDialect.DatabaseProduct.DERBY);

  public static final SqlDialect DEFAULT = new DerbySqlDialect(DEFAULT_CONTEXT);

  /** Creates a DerbySqlDialect. */
  public DerbySqlDialect(Context context) {
    super(context);
  }

  @Override public void unparseCall(final SqlWriter writer, final SqlCall call, final int leftPrec,
      final int rightPrec) {
    switch (call.getKind()) {
    case CHAR_LENGTH:
      SqlCall lengthCall = SqlLibraryOperators.LENGTH
          .createCall(SqlParserPos.ZERO, call.getOperandList());
      super.unparseCall(writer, lengthCall, leftPrec, rightPrec);
      break;
    default:
      super.unparseCall(writer, call, leftPrec, rightPrec);
    }
  }
}
