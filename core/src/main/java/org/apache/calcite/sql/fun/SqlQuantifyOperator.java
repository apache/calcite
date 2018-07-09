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

import org.apache.calcite.sql.SqlKind;

import com.google.common.base.Preconditions;

/**
 * Definition of the SQL <code>ALL</code> and <code>SOME</code>operators.
 *
 * <p>Each is used in combination with a relational operator:
 * <code>&lt;</code>, <code>&le;</code>,
 * <code>&gt;</code>, <code>&ge;</code>,
 * <code>=</code>, <code>&lt;&gt;</code>.
 *
 * <p><code>ANY</code> is a synonym for <code>SOME</code>.
 */
public class SqlQuantifyOperator extends SqlInOperator {
  //~ Instance fields --------------------------------------------------------

  public final SqlKind comparisonKind;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SqlQuantifyOperator.
   *
   * @param kind Either ALL or SOME
   * @param comparisonKind Either <code>&lt;</code>, <code>&le;</code>,
   *   <code>&gt;</code>, <code>&ge;</code>,
   *   <code>=</code> or <code>&lt;&gt;</code>.
   */
  SqlQuantifyOperator(SqlKind kind, SqlKind comparisonKind) {
    super(comparisonKind.sql + " " + kind, kind);
    this.comparisonKind = Preconditions.checkNotNull(comparisonKind);
    Preconditions.checkArgument(comparisonKind == SqlKind.EQUALS
        || comparisonKind == SqlKind.NOT_EQUALS
        || comparisonKind == SqlKind.LESS_THAN_OR_EQUAL
        || comparisonKind == SqlKind.LESS_THAN
        || comparisonKind == SqlKind.GREATER_THAN_OR_EQUAL
        || comparisonKind == SqlKind.GREATER_THAN);
    Preconditions.checkArgument(kind == SqlKind.SOME
        || kind == SqlKind.ALL);
  }
}

// End SqlQuantifyOperator.java
