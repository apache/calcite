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
package org.apache.calcite.chinook;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeName;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Example Table Function for lateral join checks
 */
public class CodesFunction {

  private CodesFunction(){
  }

  public static QueryableTable getTable(String name) {

    return new AbstractQueryableTable(Object[].class) {
      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("TYPE", SqlTypeName.VARCHAR)
            .add("CODEVALUE", SqlTypeName.VARCHAR)
            .build();
      }

      @Override public Queryable<String[]> asQueryable(QueryProvider queryProvider,
                                                       SchemaPlus schema,
                                                       String tableName) {
        if (name == null) {
          return Linq4j.<String[]>emptyEnumerable().asQueryable();
        }
        return Linq4j.asEnumerable(new String[][]{
            new String[]{"HASHCODE", "" + name.hashCode()},
            new String[]{"BASE64",
                Base64.getEncoder().encodeToString(name.getBytes(StandardCharsets.UTF_8))}
        }).asQueryable();
      }
    };
  }
}

// End CodesFunction.java
