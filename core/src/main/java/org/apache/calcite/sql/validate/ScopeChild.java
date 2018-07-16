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
package org.apache.calcite.sql.validate;


/** One of the inputs of a {@link SqlValidatorScope}.
 *
 * <p>Most commonly, it is an item in a FROM clause, and consists of a namespace
 * (the columns it provides), and optional name (table alias), and ordinal
 * within the FROM clause. */
class ScopeChild {
  final int ordinal;
  final String name;
  final SqlValidatorNamespace namespace;
  final boolean nullable;

  /** Creates a ScopeChild.
   *
   * @param ordinal Ordinal of child within parent scope
   * @param name Table alias (may be null)
   * @param namespace Namespace of child
   * @param nullable Whether fields of the child are nullable when seen from the
   *   parent, due to outer joins
   */
  ScopeChild(int ordinal, String name, SqlValidatorNamespace namespace,
      boolean nullable) {
    this.ordinal = ordinal;
    this.name = name;
    this.namespace = namespace;
    this.nullable = nullable;
  }
}

// End ScopeChild.java
