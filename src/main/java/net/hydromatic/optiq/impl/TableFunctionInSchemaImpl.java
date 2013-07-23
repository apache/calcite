/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.impl;

import net.hydromatic.optiq.*;

/**
 * Implementation of {@link net.hydromatic.optiq.Schema.TableFunctionInSchema}
 * where all properties are held in fields.
 */
public class TableFunctionInSchemaImpl extends Schema.TableFunctionInSchema {
  private final TableFunction tableFunction;

  /** Creates a TableFunctionInSchemaImpl. */
  public TableFunctionInSchemaImpl(Schema schema, String name,
      TableFunction tableFunction) {
    super(schema, name);
    this.tableFunction = tableFunction;
  }

  public TableFunction getTableFunction() {
    return tableFunction;
  }

  public boolean isMaterialization() {
    return tableFunction
        instanceof MaterializedViewTable.MaterializedViewTableFunction;
  }
}

// End TableFunctionInSchemaImpl.java
