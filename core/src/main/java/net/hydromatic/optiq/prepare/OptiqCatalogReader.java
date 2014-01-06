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
package net.hydromatic.optiq.prepare;

import net.hydromatic.optiq.Table;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;
import net.hydromatic.optiq.jdbc.OptiqSchema;

import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.validate.SqlMoniker;

import java.util.Collections;
import java.util.List;

/**
 * Implementation of {@link net.hydromatic.optiq.prepare.Prepare.CatalogReader}.
 */
class OptiqCatalogReader implements Prepare.CatalogReader {
  final OptiqSchema rootSchema;
  final JavaTypeFactory typeFactory;
  private final List<String> defaultSchema;

  public OptiqCatalogReader(
      OptiqSchema rootSchema,
      List<String> defaultSchema,
      JavaTypeFactory typeFactory) {
    super();
    assert rootSchema != defaultSchema;
    this.rootSchema = rootSchema;
    this.defaultSchema = defaultSchema;
    this.typeFactory = typeFactory;
  }

  public OptiqCatalogReader withSchemaPath(List<String> schemaPath) {
    return new OptiqCatalogReader(rootSchema, schemaPath, typeFactory);
  }

  public OptiqPrepareImpl.RelOptTableImpl getTable(final List<String> names) {
    // First look in the default schema, if any.
    if (defaultSchema != null) {
      OptiqPrepareImpl.RelOptTableImpl table =
          getTableFrom(names, defaultSchema);
      if (table != null) {
        return table;
      }
    }
    // If not found, look in the root schema
    return getTableFrom(names, Collections.<String>emptyList());
  }

  private OptiqPrepareImpl.RelOptTableImpl getTableFrom(
      List<String> names,
      List<String> schemaNames) {
    OptiqSchema schema = rootSchema;
    for (String schemaName : schemaNames) {
      schema = schema.getSubSchema(schemaName);
      if (schema == null) {
        return null;
      }
    }
    for (int i = 0; i < names.size(); i++) {
      final String name = names.get(i);
      OptiqSchema subSchema = schema.getSubSchema(name);
      if (subSchema != null) {
        schema = subSchema;
        continue;
      }
      final Table table = schema.compositeTableMap.get(name);
      if (table != null) {
        if (i != names.size() - 1) {
          // not enough objects to match all names
          return null;
        }
        return new OptiqPrepareImpl.RelOptTableImpl(
            this,
            table.getRowType(typeFactory),
            schema.add(name, table));
      }
      return null;
    }
    return null;
  }

  public RelDataType getNamedType(SqlIdentifier typeName) {
    return null;
  }

  public List<SqlMoniker> getAllSchemaObjectNames(List<String> names) {
    return null;
  }

  public String getSchemaName() {
    return null;
  }

  public OptiqPrepareImpl.RelOptTableImpl getTableForMember(
      List<String> names) {
    return getTable(names);
  }

  public RelDataTypeFactory getTypeFactory() {
    return typeFactory;
  }

  public void registerRules(RelOptPlanner planner) throws Exception {
  }
}

// End OptiqCatalogReader.java
