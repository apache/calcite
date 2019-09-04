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
package org.apache.calcite.schema;

import org.apache.calcite.plan.RelOptTable;

/** Describes how a column gets populated.
 *
 * @see org.apache.calcite.sql2rel.InitializerExpressionFactory#generationStrategy
 * @see RelOptTable#getColumnStrategies()
 */
public enum ColumnStrategy {
  /** Column does not have a default value, but does allow null values.
   * If you don't specify it in an INSERT, it will get a NULL value. */
  NULLABLE,
  /** Column does not have a default value, and does not allow nulls.
   * You must specify it in an INSERT. */
  NOT_NULLABLE,
  /** Column has a default value.
   * If you don't specify it in an INSERT, it will get a NULL value. */
  DEFAULT,
  /** Column is computed and stored. You cannot insert into it. */
  STORED,
  /** Column is computed and not stored. You cannot insert into it. */
  VIRTUAL;

  /**
   * Returns whether you can insert into the column.
   * @return true if this column can be inserted.
   */
  public boolean canInsertInto() {
    return this != STORED && this != VIRTUAL;
  }
}

// End ColumnStrategy.java
