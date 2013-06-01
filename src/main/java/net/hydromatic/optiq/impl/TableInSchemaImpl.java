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

import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.Table;

/**
 * Implementation of {@link Schema.TableInSchema} where all properties are
 * held in fields.
 */
public class TableInSchemaImpl extends Schema.TableInSchema {
  private final Table table;

  /** Creates a TableInSchemaImpl. */
  public TableInSchemaImpl(
      Schema schema, String name, Schema.TableType tableType, Table table) {
    super(schema, name, tableType);
    this.table = table;
  }

  @SuppressWarnings("unchecked")
  public <E> Table<E> getTable(Class<E> elementType) {
    return table;
  }
}

// End TableInSchemaImpl.java
