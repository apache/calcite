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

package org.apache.calcite.adapter.arrow;

import org.apache.arrow.gandiva.evaluator.Projector;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.expression.TreeBuilder;
import org.apache.arrow.gandiva.expression.TreeNode;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import org.apache.arrow.vector.ipc.ArrowFileReader;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.stream.Collectors;

public class ArrowTable extends AbstractTable implements TranslatableTable, QueryableTable {

  private final RelProtoDataType protoRowType;
  private Schema schema;
  ArrowFileReader arrowFileReader;

  public ArrowTable(RelProtoDataType protoRowType, ArrowFileReader arrowFileReader) {
    try {
      this.schema = arrowFileReader.getVectorSchemaRoot().getSchema();
    } catch (IOException e) {
      e.printStackTrace();
    }
    this.protoRowType = protoRowType;
    this.arrowFileReader = arrowFileReader;
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (this.protoRowType != null) {
      return this.protoRowType.apply(typeFactory);
    }
    return deduceRowType(this.schema, (JavaTypeFactory) typeFactory);
  }

  public Expression getExpression(SchemaPlus schema, String tableName, Class clazz) {
    return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
  }

  public Enumerable<Object> project(DataContext root, final int[] fields) {
    List<TreeNode> treeNodes = new ArrayList<>();
    List<ExpressionTree> expressionTrees = new ArrayList<>();

    for(int i = 0; i < fields.length; i++) {
      Field field = schema.getFields().get(i);
      treeNodes.add(TreeBuilder.makeField(field));
      expressionTrees.add(TreeBuilder.makeExpression(treeNodes.get(i), field));
    }

    Projector projector = null;
    try {
      projector = Projector.make(schema, expressionTrees);
    } catch (GandivaException e) {
      e.printStackTrace();
    }
    Projector finalProjector = projector;
    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        try {
          return new ArrowEnumerator(finalProjector, fields, arrowFileReader);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                      SchemaPlus schema, String tableName) {
    throw new UnsupportedOperationException();
  }

  public Type getElementType() {
    return Object[].class;
  }

  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final int fieldCount = relOptTable.getRowType().getFieldCount();
    final int[] fields = ArrowEnumerator.identityList(fieldCount);
    return new ArrowTableScan(context.getCluster(), relOptTable, this, fields);
  }

  private RelDataType deduceRowType(Schema schema, JavaTypeFactory typeFactory) {
    List<Pair<String, RelDataType>> ret = schema.getFields().stream().map(field -> {
      RelDataType relDataType = ArrowFieldType.of(field.getType()).toType(typeFactory);
      return new Pair<>(field.getName(), relDataType);
    }).collect(Collectors.toList());
    return typeFactory.createStructType(ret);
  }
}
