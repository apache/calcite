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
package org.apache.calcite.test.catalog;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;

/** Define column strategies for the "VIRTUALCOLUMNS" table. */
public class VirtualColumnsExpressionFactory extends NullInitializerExpressionFactory {
  @Override public ColumnStrategy generationStrategy(RelOptTable table, int iColumn) {
    switch (iColumn) {
    case 3:
      return ColumnStrategy.STORED;
    case 4:
      return ColumnStrategy.VIRTUAL;
    default:
      return super.generationStrategy(table, iColumn);
    }
  }

  @Override public RexNode newColumnDefaultValue(
      RelOptTable table, int iColumn, InitializerContext context) {
    if (iColumn == 4) {
      final SqlNode node = context.parseExpression(SqlParser.Config.DEFAULT, "A + 1");
      // Actually we should validate the node with physical schema,
      // here full table schema(includes the virtual columns) also works
      // because the expression "A + 1" does not reference any virtual column.
      final SqlNode validated = context.validateExpression(table.getRowType(), node);
      return context.convertExpression(validated);
    } else {
      return super.newColumnDefaultValue(table, iColumn, context);
    }
  }
}
