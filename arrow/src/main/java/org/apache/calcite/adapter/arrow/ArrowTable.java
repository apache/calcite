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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
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

import org.apache.arrow.gandiva.evaluator.Filter;
import org.apache.arrow.gandiva.evaluator.Projector;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.Condition;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.gandiva.expression.TreeBuilder;
import org.apache.arrow.gandiva.expression.TreeNode;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Arrow Table.
 */
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

  public Enumerable<Object> query(DataContext root, int[] fields, List<String> condition) {
    if (fields == null) {
      fields = new int[schema.getFields().size()];
      for (int i = 0; i < fields.length; i++) {
        fields[i] = i;
      }
    }

    Projector projector = null;
    Filter filter = null;

    if (condition.size() == 0) {
      List<ExpressionTree> expressionTrees = new ArrayList<>(fields.length);
      for (int i = 0; i < fields.length; i++) {
        Field field = schema.getFields().get(i);
        TreeNode node = TreeBuilder.makeField(field);
        expressionTrees.add(TreeBuilder.makeExpression(node, field));
      }
      try {
        projector = Projector.make(schema, expressionTrees);
      } catch (GandivaException e) {
        throw new RuntimeException(e);
      }
    } else {
      List<TreeNode> conditionNodes = new ArrayList<>(condition.size());
      for (String con: condition) {
        String[] data = con.split(" ");
        List<TreeNode> treeNodes = new ArrayList<>(2);
        treeNodes.add(TreeBuilder.makeField(schema.getFields()
            .get(schema.getFields().indexOf(schema.findField(data[0])))));
        treeNodes.add(makeLiteralNode(data[2], data[3]));
        String equality = data[1];
        conditionNodes.add(TreeBuilder.makeFunction(equality, treeNodes, new ArrowType.Bool()));
      }
      Condition filterCondition;
      if (conditionNodes.size() == 1) {
        filterCondition = TreeBuilder.makeCondition(conditionNodes.get(0));
      }
      else {
        TreeNode treeNode = TreeBuilder.makeAnd(conditionNodes);
        filterCondition = TreeBuilder.makeCondition(treeNode);
      }

      try {
        filter = Filter.make(schema, filterCondition);
      } catch (GandivaException e) {
        throw new RuntimeException(e);
      }
    }

    int[] finalFields = fields;
    Projector finalProjector = projector;
    Filter finalFilter = filter;
    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        try {
          return new ArrowEnumerator(finalProjector, finalFilter, finalFields, arrowFileReader);
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
    final RelOptCluster cluster = context.getCluster();
    return new ArrowTableScan(cluster, cluster.traitSetOf(ArrowRel.CONVENTION), relOptTable,
        this, fields);
  }

  private RelDataType deduceRowType(Schema schema, JavaTypeFactory typeFactory) {
    List<Pair<String, RelDataType>> ret = schema.getFields().stream().map(field -> {
      RelDataType relDataType = ArrowFieldType.of(field.getType()).toType(typeFactory);
      return new Pair<>(field.getName(), relDataType);
    }).collect(Collectors.toList());
    return typeFactory.createStructType(ret);
  }

  private TreeNode makeLiteralNode(String literal, String type) {
    if (type.equals("integer")) {
      return TreeBuilder.makeLiteral(Integer.parseInt(literal));
    } else if (type.equals("long")) {
      return TreeBuilder.makeLiteral(Long.parseLong(literal));
    } else if (type.equals("float")) {
      return TreeBuilder.makeLiteral(Float.parseFloat(literal));
    } else if (type.equals("double")) {
      return TreeBuilder.makeLiteral(Double.parseDouble(literal));
    } else if (type.equals("string")) {
      return TreeBuilder.makeStringLiteral(literal);
    }
    throw new AssertionError("Invalid literal");
  }
}
