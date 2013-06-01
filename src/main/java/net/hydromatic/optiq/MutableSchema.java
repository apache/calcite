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
package net.hydromatic.optiq;

import net.hydromatic.linq4j.expressions.Expression;

/**
 * Schema that can be modified.
 */
public interface MutableSchema extends Schema {
  /** Defines a table-function in this schema. There can be multiple
   * table-functions with the same name; this method will not remove a
   * table-function with the same name, just define another overloading. */
  void addTableFunction(String name, TableFunction tableFunction);

  /** Defines a table within this schema. */
  void addTable(TableInSchema table);

  /** Adds a child schema of this schema. */
  void addSchema(String name, Schema schema);

  /** Returns the expression with which a sub-schema of this schema with a
   * given name and type should be accessed. */
  Expression getSubSchemaExpression(String name, Class type);
}

// End MutableSchema.java
