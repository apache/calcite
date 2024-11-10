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
import org.apache.calcite.linq4j.Enumerable;
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
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Double.parseDouble;
import static java.lang.Float.parseFloat;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.util.Objects.requireNonNull;

/**
 * Arrow Table.
 */
public class ArrowTable extends AbstractTable
    implements TranslatableTable, QueryableTable {
  private final @Nullable RelProtoDataType protoRowType;
  /** Arrow schema. (In Calcite terminology, more like a row type than a Schema.) */
  private final Schema schema;
  private final ArrowFileReader arrowFileReader;

  ArrowTable(@Nullable RelProtoDataType protoRowType, ArrowFileReader arrowFileReader) {
    try {
      this.schema = arrowFileReader.getVectorSchemaRoot().getSchema();
    } catch (IOException e) {
      throw Util.toUnchecked(e);
    }
    this.protoRowType = protoRowType;
    this.arrowFileReader = arrowFileReader;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    if (this.protoRowType != null) {
      return this.protoRowType.apply(typeFactory);
    }
    return deduceRowType(this.schema, (JavaTypeFactory) typeFactory);
  }

  @Override public Expression getExpression(SchemaPlus schema, String tableName,
      Class clazz) {
    return Schemas.tableExpression(schema, getElementType(), tableName, clazz);
  }

  /** Called via code generation; see uses of
   * {@link org.apache.calcite.adapter.arrow.ArrowMethod#ARROW_QUERY}. */
  @SuppressWarnings("unused")
  public Enumerable<Object> query(DataContext root, ImmutableIntList fields,
      List<String> conditions) {
    requireNonNull(fields, "fields");
    final Projector projector;
    final Filter filter;

    if (conditions.isEmpty()) {
      filter = null;

      final List<ExpressionTree> expressionTrees = new ArrayList<>();
      for (int fieldOrdinal : fields) {
        Field field = schema.getFields().get(fieldOrdinal);
        TreeNode node = TreeBuilder.makeField(field);
        expressionTrees.add(TreeBuilder.makeExpression(node, field));
      }
      try {
        projector = Projector.make(schema, expressionTrees);
      } catch (GandivaException e) {
        throw Util.toUnchecked(e);
      }
    } else {
      projector = null;

      final List<TreeNode> conditionNodes = new ArrayList<>(conditions.size());
      for (String condition : conditions) {
        String[] data = condition.split(" ");
        List<TreeNode> treeNodes = new ArrayList<>(2);
        treeNodes.add(
            TreeBuilder.makeField(schema.getFields()
                .get(schema.getFields().indexOf(schema.findField(data[0])))));

        // if the split condition has more than two parts it's a binary operator
        // with an additional literal node
        if (data.length > 2) {
          treeNodes.add(makeLiteralNode(data[2], data[3]));
        }

        String operator = data[1];
        conditionNodes.add(
            TreeBuilder.makeFunction(operator, treeNodes, new ArrowType.Bool()));
      }
      final Condition filterCondition;
      if (conditionNodes.size() == 1) {
        filterCondition = TreeBuilder.makeCondition(conditionNodes.get(0));
      } else {
        TreeNode treeNode = TreeBuilder.makeAnd(conditionNodes);
        filterCondition = TreeBuilder.makeCondition(treeNode);
      }

      try {
        filter = Filter.make(schema, filterCondition);
      } catch (GandivaException e) {
        throw Util.toUnchecked(e);
      }
    }

    return new ArrowEnumerable(arrowFileReader, fields, projector, filter);
  }

  @Override public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    throw new UnsupportedOperationException();
  }

  @Override public Type getElementType() {
    return Object[].class;
  }

  @Override public RelNode toRel(RelOptTable.ToRelContext context,
      RelOptTable relOptTable) {
    final int fieldCount = relOptTable.getRowType().getFieldCount();
    final ImmutableIntList fields =
        ImmutableIntList.copyOf(Util.range(fieldCount));
    final RelOptCluster cluster = context.getCluster();
    return new ArrowTableScan(cluster, cluster.traitSetOf(ArrowRel.CONVENTION),
        relOptTable, this, fields);
  }

  private static RelDataType deduceRowType(Schema schema,
      JavaTypeFactory typeFactory) {
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (Field field : schema.getFields()) {
      builder.add(field.getName(),
          ArrowFieldType.of(field.getType()).toType(typeFactory));
    }
    return builder.build();
  }

  private static TreeNode makeLiteralNode(String literal, String type) {
    if (type.startsWith("decimal")) {
      String[] typeParts =
          type.substring(type.indexOf('(') + 1, type.indexOf(')')).split(",");
      int precision = parseInt(typeParts[0]);
      int scale = parseInt(typeParts[1]);
      return TreeBuilder.makeDecimalLiteral(literal, precision, scale);
    } else if (type.equals("integer")) {
      return TreeBuilder.makeLiteral(parseInt(literal));
    } else if (type.equals("long")) {
      return TreeBuilder.makeLiteral(parseLong(literal));
    } else if (type.equals("float")) {
      return TreeBuilder.makeLiteral(parseFloat(literal));
    } else if (type.equals("double")) {
      return TreeBuilder.makeLiteral(parseDouble(literal));
    } else if (type.equals("string")) {
      return TreeBuilder.makeStringLiteral(literal.substring(1, literal.length() - 1));
    } else {
      throw new IllegalArgumentException("Invalid literal " + literal
          + ", type " + type);
    }
  }
}
