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
package org.apache.calcite.piglet;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.PigRelBuilder;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Walks over a Piglet AST and calls the corresponding methods in a
 * {@link PigRelBuilder}.
 */
public class Handler {
  private final PigRelBuilder builder;
  private final Map<String, RelNode> map = new HashMap<>();

  public Handler(PigRelBuilder builder) {
    this.builder = builder;
  }

  /** Creates relational expressions for a given AST node. */
  public Handler handle(Ast.Node node) {
    final RelNode input;
    final List<RexNode> rexNodes;
    switch (node.op) {
    case LOAD:
      final Ast.LoadStmt load = (Ast.LoadStmt) node;
      builder.scan((String) load.name.value);
      register(load.target.value);
      return this;
    case VALUES:
      final Ast.ValuesStmt values = (Ast.ValuesStmt) node;
      final RelDataType rowType = toType(values.schema);
      builder.values(tuples(values, rowType), rowType);
      register(values.target.value);
      return this;
    case FOREACH:
      final Ast.ForeachStmt foreach = (Ast.ForeachStmt) node;
      builder.clear();
      input = map.get(foreach.source.value);
      builder.push(input);
      rexNodes = new ArrayList<>();
      for (Ast.Node exp : foreach.expList) {
        rexNodes.add(toRex(exp));
      }
      builder.project(rexNodes);
      register(foreach.target.value);
      return this;
    case FOREACH_NESTED:
      final Ast.ForeachNestedStmt foreachNested = (Ast.ForeachNestedStmt) node;
      builder.clear();
      input = map.get(foreachNested.source.value);
      builder.push(input);
      System.out.println(input.getRowType());
      for (RelDataTypeField field : input.getRowType().getFieldList()) {
        switch (field.getType().getSqlTypeName()) {
        case ARRAY:
          System.out.println(field);
        }
      }
      for (Ast.Stmt stmt : foreachNested.nestedStmtList) {
        handle(stmt);
      }
      rexNodes = new ArrayList<>();
      for (Ast.Node exp : foreachNested.expList) {
        rexNodes.add(toRex(exp));
      }
      builder.project(rexNodes);
      register(foreachNested.target.value);
      return this;
    case FILTER:
      final Ast.FilterStmt filter = (Ast.FilterStmt) node;
      builder.clear();
      input = map.get(filter.source.value);
      builder.push(input);
      final RexNode rexNode = toRex(filter.condition);
      builder.filter(rexNode);
      register(filter.target.value);
      return this;
    case DISTINCT:
      final Ast.DistinctStmt distinct = (Ast.DistinctStmt) node;
      builder.clear();
      input = map.get(distinct.source.value);
      builder.push(input);
      builder.distinct(null, -1);
      register(distinct.target.value);
      return this;
    case ORDER:
      final Ast.OrderStmt order = (Ast.OrderStmt) node;
      builder.clear();
      input = map.get(order.source.value);
      builder.push(input);
      final List<RexNode> nodes = new ArrayList<>();
      for (Pair<Ast.Identifier, Ast.Direction> field : order.fields) {
        toSortRex(nodes, field);
      }
      builder.sort(nodes);
      register(order.target.value);
      return this;
    case LIMIT:
      final Ast.LimitStmt limit = (Ast.LimitStmt) node;
      builder.clear();
      input = map.get(limit.source.value);
      final int count = ((Number) limit.count.value).intValue();
      builder.push(input);
      builder.limit(0, count);
      register(limit.target.value);
      return this;
    case GROUP:
      final Ast.GroupStmt group = (Ast.GroupStmt) node;
      builder.clear();
      input = map.get(group.source.value);
      builder.push(input).as(group.source.value);
      final List<RelBuilder.GroupKey> groupKeys = new ArrayList<>();
      final List<RexNode> keys = new ArrayList<>();
      if (group.keys != null) {
        for (Ast.Node key : group.keys) {
          keys.add(toRex(key));
        }
      }
      groupKeys.add(builder.groupKey(keys));
      builder.group(PigRelBuilder.GroupOption.COLLECTED, null, -1, groupKeys);
      register(group.target.value);
      return this;
    case PROGRAM:
      final Ast.Program program = (Ast.Program) node;
      for (Ast.Stmt stmt : program.stmtList) {
        handle(stmt);
      }
      return this;
    case DUMP:
      final Ast.DumpStmt dump = (Ast.DumpStmt) node;
      final RelNode relNode = map.get(dump.relation.value);
      dump(relNode);
      return this; // nothing to do; contains no algebra
    default:
      throw new AssertionError("unknown operation " + node.op);
    }
  }

  /** Executes a relational expression and prints the output.
   *
   * <p>The default implementation does nothing.
   *
   * @param rel Relational expression
   */
  protected void dump(RelNode rel) {
  }

  private ImmutableList<ImmutableList<RexLiteral>> tuples(
      Ast.ValuesStmt valuesStmt, RelDataType rowType) {
    final ImmutableList.Builder<ImmutableList<RexLiteral>> listBuilder =
        ImmutableList.builder();
    for (List<Ast.Node> nodeList : valuesStmt.tupleList) {
      listBuilder.add(tuple(nodeList, rowType));
    }
    return listBuilder.build();
  }

  private ImmutableList<RexLiteral> tuple(List<Ast.Node> nodeList,
      RelDataType rowType) {
    final ImmutableList.Builder<RexLiteral> listBuilder =
        ImmutableList.builder();
    for (Pair<Ast.Node, RelDataTypeField> pair
        : Pair.zip(nodeList, rowType.getFieldList())) {
      final Ast.Node node = pair.left;
      final RelDataType type = pair.right.getType();
      listBuilder.add(item(node, type));
    }
    return listBuilder.build();
  }

  private ImmutableList<RexLiteral> bag(List<Ast.Node> nodeList,
      RelDataType type) {
    final ImmutableList.Builder<RexLiteral> listBuilder =
        ImmutableList.builder();
    for (Ast.Node node : nodeList) {
      listBuilder.add(item(node, type.getComponentType()));
    }
    return listBuilder.build();
  }

  private RexLiteral item(Ast.Node node, RelDataType type) {
    final RexBuilder rexBuilder = builder.getRexBuilder();
    switch (node.op) {
    case LITERAL:
      final Ast.Literal literal = (Ast.Literal) node;
      return (RexLiteral) rexBuilder.makeLiteral(literal.value, type, false);
    case TUPLE:
      final Ast.Call tuple = (Ast.Call) node;
      final ImmutableList<RexLiteral> list = tuple(tuple.operands, type);
      return (RexLiteral) rexBuilder.makeLiteral(list, type, false);
    case BAG:
      final Ast.Call bag = (Ast.Call) node;
      final ImmutableList<RexLiteral> list2 = bag(bag.operands, type);
      return (RexLiteral) rexBuilder.makeLiteral(list2, type, false);
    default:
      throw new IllegalArgumentException("not a literal: " + node);
    }
  }


  private RelDataType toType(Ast.Schema schema) {
    final RelDataTypeFactory.Builder typeBuilder =
        builder.getTypeFactory().builder();
    for (Ast.FieldSchema fieldSchema : schema.fieldSchemaList) {
      typeBuilder.add(fieldSchema.id.value, toType(fieldSchema.type));
    }
    return typeBuilder.build();
  }

  private RelDataType toType(Ast.Type type) {
    switch (type.op) {
    case SCALAR_TYPE:
      return toType((Ast.ScalarType) type);
    case BAG_TYPE:
      return toType((Ast.BagType) type);
    case MAP_TYPE:
      return toType((Ast.MapType) type);
    case TUPLE_TYPE:
      return toType((Ast.TupleType) type);
    default:
      throw new AssertionError("unknown type " + type);
    }
  }

  private RelDataType toType(Ast.ScalarType type) {
    final RelDataTypeFactory typeFactory = builder.getTypeFactory();
    switch (type.name) {
    case "boolean":
      return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    case "int":
      return typeFactory.createSqlType(SqlTypeName.INTEGER);
    case "float":
      return typeFactory.createSqlType(SqlTypeName.REAL);
    default:
      return typeFactory.createSqlType(SqlTypeName.VARCHAR);
    }
  }

  private RelDataType toType(Ast.BagType type) {
    final RelDataTypeFactory typeFactory = builder.getTypeFactory();
    final RelDataType t = toType(type.componentType);
    return typeFactory.createMultisetType(t, -1);
  }

  private RelDataType toType(Ast.MapType type) {
    final RelDataTypeFactory typeFactory = builder.getTypeFactory();
    final RelDataType k = toType(type.keyType);
    final RelDataType v = toType(type.valueType);
    return typeFactory.createMapType(k, v);
  }

  private RelDataType toType(Ast.TupleType type) {
    final RelDataTypeFactory typeFactory = builder.getTypeFactory();
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (Ast.FieldSchema fieldSchema : type.fieldSchemaList) {
      builder.add(fieldSchema.id.value, toType(fieldSchema.type));
    }
    return builder.build();
  }

  private void toSortRex(List<RexNode> nodes,
      Pair<Ast.Identifier, Ast.Direction> pair) {
    if (pair.left.isStar()) {
      for (RexNode node : builder.fields()) {
        switch (pair.right) {
        case DESC:
          node = builder.desc(node);
        }
        nodes.add(node);
      }
    } else {
      RexNode node = toRex(pair.left);
      switch (pair.right) {
      case DESC:
        node = builder.desc(node);
      }
      nodes.add(node);
    }
  }

  private RexNode toRex(Ast.Node exp) {
    final Ast.Call call;
    switch (exp.op) {
    case LITERAL:
      return builder.literal(((Ast.Literal) exp).value);
    case IDENTIFIER:
      final String value = ((Ast.Identifier) exp).value;
      if (value.matches("^\\$[0-9]+")) {
        int i = Integer.valueOf(value.substring(1));
        return builder.field(i);
      }
      return builder.field(value);
    case DOT:
      call = (Ast.Call) exp;
      final RexNode left = toRex(call.operands.get(0));
      final Ast.Identifier right = (Ast.Identifier) call.operands.get(1);
      return builder.dot(left, right.value);
    case EQ:
    case NE:
    case GT:
    case GTE:
    case LT:
    case LTE:
    case AND:
    case OR:
    case NOT:
    case PLUS:
    case MINUS:
      call = (Ast.Call) exp;
      return builder.call(op(exp.op), toRex(call.operands));
    default:
      throw new AssertionError("unknown op " + exp.op);
    }
  }

  private static SqlOperator op(Ast.Op op) {
    switch (op) {
    case EQ:
      return SqlStdOperatorTable.EQUALS;
    case NE:
      return SqlStdOperatorTable.NOT_EQUALS;
    case GT:
      return SqlStdOperatorTable.GREATER_THAN;
    case GTE:
      return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
    case LT:
      return SqlStdOperatorTable.LESS_THAN;
    case LTE:
      return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
    case AND:
      return SqlStdOperatorTable.AND;
    case OR:
      return SqlStdOperatorTable.OR;
    case NOT:
      return SqlStdOperatorTable.NOT;
    case PLUS:
      return SqlStdOperatorTable.PLUS;
    case MINUS:
      return SqlStdOperatorTable.MINUS;
    default:
      throw new AssertionError("unknown: " + op);
    }
  }

  private ImmutableList<RexNode> toRex(Iterable<Ast.Node> operands) {
    final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
    for (Ast.Node operand : operands) {
      builder.add(toRex(operand));
    }
    return builder.build();
  }

  /** Assigns the current relational expression to a given name. */
  private void register(String name) {
    map.put(name, builder.peek());
  }
}

// End Handler.java
