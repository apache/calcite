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

import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * An operator which performs set operations on multisets, such as "MULTISET
 * UNION ALL".
 *
 * <p>Not to be confused with {@link SqlMultisetValueConstructor} or
 * {@link SqlMultisetQueryConstructor}.
 *
 * <p>todo: Represent the ALL keyword to MULTISET UNION ALL etc. as a hidden
 * operand. Then we can obsolete this class.
 */
public class SqlMultisetSetOperator extends SqlBinaryOperator {
  //~ Instance fields --------------------------------------------------------

  private final boolean all;

  //~ Constructors -----------------------------------------------------------

  public SqlMultisetSetOperator(String name, int prec, boolean all) {
    super(
        name,
        SqlKind.OTHER,
        prec,
        true,
        ReturnTypes.MULTISET_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.MULTISET_MULTISET);
    this.all = all;
  }
}

// End SqlMultisetSetOperator.java
