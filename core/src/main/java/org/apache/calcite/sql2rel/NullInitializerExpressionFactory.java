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
package org.apache.calcite.sql2rel;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;

import java.util.List;

/**
 * An implementation of {@link InitializerExpressionFactory} that always supplies NULL.
 */
public class NullInitializerExpressionFactory implements InitializerExpressionFactory {
  protected final RexBuilder rexBuilder;

  public NullInitializerExpressionFactory(RelDataTypeFactory typeFactory) {
    this.rexBuilder = new RexBuilder(typeFactory);
  }

  public boolean isGeneratedAlways(RelOptTable table, int iColumn) {
    return false;
  }

  public RexNode newColumnDefaultValue(RelOptTable table, int iColumn) {
    return rexBuilder.constantNull();
  }

  public RexNode newAttributeInitializer(RelDataType type,
      SqlFunction constructor, int iAttribute, List<RexNode> constructorArgs) {
    return rexBuilder.constantNull();
  }
}

// End NullInitializerExpressionFactory.java
