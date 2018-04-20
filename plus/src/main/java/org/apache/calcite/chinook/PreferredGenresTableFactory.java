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
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;

import java.util.Map;

/**
 * Factory for the table of genres preferred by the current user.
 */
public class PreferredGenresTableFactory implements TableFactory<AbstractQueryableTable> {
  private static final Integer[] SPECIFIC_USER_PREFERRED_GENRES =
      {1, 2, 7, 9, 15};
  private static final int FIRST_ID = 1;
  private static final int LAST_ID = 25;

  @Override public AbstractQueryableTable create(
      SchemaPlus schema,
      String name,
      Map<String, Object> operand,
      RelDataType rowType) {
    return new AbstractQueryableTable(Integer.class) {
      @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder().add("ID", SqlTypeName.INTEGER).build();
      }

      @Override public Queryable<Integer> asQueryable(
          QueryProvider qp,
          SchemaPlus sp,
          String string) {
        return fetchPreferredGenres();
      }
    };
  }

  private Queryable<Integer> fetchPreferredGenres() {
    if (EnvironmentFairy.getUser() == EnvironmentFairy.User.SPECIFIC_USER) {
      return Linq4j.asEnumerable(SPECIFIC_USER_PREFERRED_GENRES).asQueryable();
    } else {
      final ContiguousSet<Integer> set =
          ContiguousSet.create(Range.closed(FIRST_ID, LAST_ID),
              DiscreteDomain.integers());
      return Linq4j.asEnumerable(set).asQueryable();
    }
  }
}

// End PreferredGenresTableFactory.java
