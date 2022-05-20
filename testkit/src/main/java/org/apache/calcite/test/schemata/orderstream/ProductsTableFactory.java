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
package org.apache.calcite.test.schemata.orderstream;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;

/**
 * Mocks a simple relation to use for stream joining test.
 */
public class ProductsTableFactory implements TableFactory<Table> {
  @Override public Table create(SchemaPlus schema, String name,
      Map<String, Object> operand, @Nullable RelDataType rowType) {
    final Object[][] rows = {
        {"paint", 1},
        {"paper", 0},
        {"brush", 1}
    };
    return new ProductsTable(ImmutableList.copyOf(rows));
  }
}
