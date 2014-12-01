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
package org.apache.calcite.adapter.java;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.impl.AbstractTable;

import java.lang.reflect.Type;

/**
 * Abstract base class for implementing {@link org.apache.calcite.schema.Table}.
 */
public abstract class AbstractQueryableTable extends AbstractTable
    implements QueryableTable {
  protected final Type elementType;

  protected AbstractQueryableTable(Type elementType) {
    super();
    this.elementType = elementType;
  }

  public Type getElementType() {
    return elementType;
  }

  public Expression getExpression(SchemaPlus schema, String tableName,
      Class clazz) {
    return Schemas.tableExpression(schema, elementType, tableName, clazz);
  }
}

// End AbstractQueryableTable.java
