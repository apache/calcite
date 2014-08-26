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
package org.eigenbase.sql.validate;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.SqlNode;

/** Namespace based on a table from the catalog. */
class TableNamespace extends AbstractNamespace {
  private final SqlValidatorTable table;

  /** Creates a TableNamespace. */
  TableNamespace(SqlValidatorImpl validator, SqlValidatorTable table) {
    super(validator, null);
    this.table = table;
    assert table != null;
  }

  protected RelDataType validateImpl() {
    return table.getRowType();
  }

  public SqlNode getNode() {
    // This is the only kind of namespace not based on a node in the parse tree.
    return null;
  }

  @Override
  public SqlValidatorTable getTable() {
    return table;
  }
}

// End TableNamespace.java
