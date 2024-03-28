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
package org.apache.calcite.sql.fun;

import org.apache.calcite.linq4j.function.*;
import org.apache.calcite.sql.SqlOperator;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.*;

/** Map an operator to a list of dialect-specific operators it should be unparsed as. */
public class SqlOperatorMap {
    public static final BiMap<SqlOperator, SqlOperator> SQL_OPERATOR_MAP =
      ImmutableBiMap.<SqlOperator, SqlOperator>builder()
          .put(SqlLibraryOperators.STARTSWITH, SqlLibraryOperators.STARTS_WITH)
          .put(SqlLibraryOperators.ENDSWITH, SqlLibraryOperators.ENDSWITH)
          .put(SqlLibraryOperators.LENGTH, CHAR_LENGTH)
          .build();


    public static @Nullable SqlOperator getMatchingOperator(SqlOperator operator) {
      if (SQL_OPERATOR_MAP.containsKey(operator)) {
        return SQL_OPERATOR_MAP.get(operator);
      } else if (SQL_OPERATOR_MAP.containsValue(operator)) {
        return SQL_OPERATOR_MAP.inverse().get(operator);
      } else {
        return null;
      }
    }
}
