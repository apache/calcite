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

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

import com.google.common.base.Suppliers;

import java.util.function.Supplier;

/**
 * Operator table that contains only Oracle-specific functions and operators.
 *
 * @deprecated Use
 * {@link SqlLibraryOperatorTableFactory#getOperatorTable(SqlLibrary...)}
 * instead, passing {@link SqlLibrary#ORACLE} as argument.
 */
@Deprecated // to be removed before 2.0
public class OracleSqlOperatorTable extends ReflectiveSqlOperatorTable {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * The table of Oracle-specific operators.
   */
  private static final Supplier<OracleSqlOperatorTable> INSTANCE =
      Suppliers.memoize(() ->
          (OracleSqlOperatorTable) new OracleSqlOperatorTable().init());

  @Deprecated // to be removed before 2.0
  public static final SqlFunction DECODE = SqlLibraryOperators.DECODE;

  @Deprecated // to be removed before 2.0
  public static final SqlFunction NVL = SqlLibraryOperators.NVL;

  @Deprecated // to be removed before 2.0
  public static final SqlFunction LTRIM = SqlLibraryOperators.LTRIM;

  @Deprecated // to be removed before 2.0
  public static final SqlFunction RTRIM = SqlLibraryOperators.RTRIM;

  @Deprecated // to be removed before 2.0
  public static final SqlFunction SUBSTR = SqlLibraryOperators.SUBSTR_ORACLE;

  @Deprecated // to be removed before 2.0
  public static final SqlFunction GREATEST = SqlLibraryOperators.GREATEST;

  @Deprecated // to be removed before 2.0
  public static final SqlFunction LEAST = SqlLibraryOperators.LEAST;

  @Deprecated // to be removed before 2.0
  public static final SqlFunction TRANSLATE3 = SqlLibraryOperators.TRANSLATE3;

  /**
   * Returns the Oracle operator table, creating it if necessary.
   */
  public static OracleSqlOperatorTable instance() {
    return INSTANCE.get();
  }
}
