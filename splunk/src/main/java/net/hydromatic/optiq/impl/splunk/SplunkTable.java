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
package net.hydromatic.optiq.impl.splunk;

import net.hydromatic.linq4j.*;

import net.hydromatic.optiq.*;
import net.hydromatic.optiq.impl.AbstractTableQueryable;
import net.hydromatic.optiq.impl.java.AbstractQueryableTable;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

/**
 * Table based on Splunk.
 */
class SplunkTable extends AbstractQueryableTable implements TranslatableTable {
  final SplunkSchema schema;

  public SplunkTable(SplunkSchema schema) {
    super(Object[].class);
    this.schema = schema;
    assert schema != null;
  }

  public String toString() {
    return "SplunkTable";
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataType stringType =
        ((JavaTypeFactory) typeFactory).createType(String.class);
    return typeFactory.builder()
        .add("source", stringType)
        .add("sourcetype", stringType)
        .add("_extra", stringType)
        .build();
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return new AbstractTableQueryable<T>(queryProvider, schema, this,
        tableName) {
      public Enumerator<T> enumerator() {
        final SplunkQuery<T> query = createQuery();
        return query.enumerator();
      }
    };
  }

  private <T> SplunkQuery<T> createQuery() {
    return new SplunkQuery<T>(
        schema.splunkConnection,
        "search",
        null,
        null,
        null);
  }

  public RelNode toRel(
      RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    return new SplunkTableAccessRel(
        context.getCluster(),
        relOptTable,
        this,
        "search",
        null,
        null,
        relOptTable.getRowType().getFieldNames());
  }
}

// End SplunkTable.java
