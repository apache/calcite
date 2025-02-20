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
package org.apache.calcite.rel.rel2sql;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWriter;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mock dialect for testing.
 *
 * <p>Available under {@link DialectCode#MOCK}.
 *
 * <p>If you are writing tests, feel free to add fields and modify behavior
 * for particular tests.
 */
class MockSqlDialect extends SqlDialect {
  public static final ThreadLocal<AtomicInteger> THREAD_UNPARSE_SELECT_COUNT =
      ThreadLocal.withInitial(() -> new AtomicInteger(0));

  MockSqlDialect() {
    super(SqlDialect.EMPTY_CONTEXT);
  }

  @Override public void unparseCall(SqlWriter writer, SqlCall call,
      int leftPrec, int rightPrec) {
    if (call instanceof SqlSelect) {
      THREAD_UNPARSE_SELECT_COUNT.get().incrementAndGet();
    }
    super.unparseCall(writer, call, leftPrec, rightPrec);
  }
}
