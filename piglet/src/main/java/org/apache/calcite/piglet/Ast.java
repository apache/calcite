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

import org.apache.calcite.avatica.util.Spacer;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;

/** Abstract syntax tree.
 *
 * <p>Contains inner classes for various kinds of parse tree node.
 */
public class Ast {
  private Ast() {}

  public static String toString(Node x) {
    return new UnParser().append(x).buf.toString();
  }

  /** Formats a node and its children as a string. */
  public static UnParser unParse(UnParser u, Node n) {
    switch (n.op) {
    case PROGRAM:
      final Program program = (Program) n;
      return u.append("{op: PROGRAM, stmts: ").appendList(program.stmtList)
          .append("}");
    case LOAD:
      final LoadStmt load = (LoadStmt) n;
      return u.append("{op: LOAD, target: " + load.target.value + ", name: "
          + load.name.value + "}");
    case DUMP:
      final DumpStmt dump = (DumpStmt) n;
      return u.append("{op: DUMP, relation: " + dump.relation.value + "}");
    case DESCRIBE:
      final DescribeStmt describe = (DescribeStmt) n;
      return u.append("{op: DESCRIBE, relation: " + describe.relation.value
          + "}");
    case FOREACH:
      final ForeachStmt foreach = (ForeachStmt) n;
      return u.append("{op: FOREACH, target: " + foreach.target.value
          + ", source: " + foreach.source.value + ", expList: ")
          .appendList(foreach.expList)
          .append("}");
    case FOREACH_NESTED:
      final ForeachNestedStmt foreachNested = (ForeachNestedStmt) n;
      return u.append("{op: FOREACH, target: " + foreachNested.target.value
          + ", source: " + foreachNested.source.value
          + ", nestedOps: ")
          .appendList(foreachNested.nestedStmtList)
          .append(", expList: ")
          .appendList(foreachNested.expList)
          .append("}");
    case FILTER:
      final FilterStmt filter = (FilterStmt) n;
      u.append("{op: FILTER, target: " + filter.target.value + ", source: "
          + filter.source.value + ", condition: ");
      u.in().append(filter.condition).out();
      return u.append("}");
    case DISTINCT:
      final DistinctStmt distinct = (DistinctStmt) n;
      return u.append("{op: DISTINCT, target: " + distinct.target.value
          + ", source: " + distinct.source.value + "}");
    case LIMIT:
      final LimitStmt limit = (LimitStmt) n;
      return u.append("{op: LIMIT, target: ").append(limit.target.value)
          .append(", source: ").append(limit.source.value)
          .append(", count: ").append(limit.count.value.toString())
          .append("}");
    case ORDER:
      final OrderStmt order = (OrderStmt) n;
      return u.append("{op: ORDER, target: " + order.target.value
          + ", source: " + order.source.value + "}");
    case GROUP:
      final GroupStmt group = (GroupStmt) n;
      u.append("{op: GROUP, target: " + group.target.value
          + ", source: " + group.source.value);
      if (group.keys != null) {
        u.append(", keys: ").appendList(group.keys);
      }
      return u.append("}");
    case LITERAL:
      final Literal literal = (Literal) n;
      return u.append(String.valueOf(literal.value));
    case IDENTIFIER:
      final Identifier id = (Identifier) n;
      return u.append(id.value);
    default:
      throw new AssertionError("unknown op " + n.op);
    }
  }

  /** Parse tree node type. */
  public enum Op {
    PROGRAM,

    // atoms
    LITERAL, IDENTIFIER, BAG, TUPLE,

    // statements
    DESCRIBE, DISTINCT, DUMP, LOAD, FOREACH, FILTER,
    FOREACH_NESTED, LIMIT, ORDER, GROUP, VALUES,

    // types
    SCHEMA, SCALAR_TYPE, BAG_TYPE, TUPLE_TYPE, MAP_TYPE, FIELD_SCHEMA,

    // operators
    DOT, EQ, NE, GT, LT, GTE, LTE, PLUS, MINUS, AND, OR, NOT
  }

  /** Abstract base class for parse tree node. */
  public abstract static class Node {
    public final Op op;
    public final SqlParserPos pos;

    protected Node(SqlParserPos pos, Op op) {
      this.op = Objects.requireNonNull(op);
      this.pos = Objects.requireNonNull(pos);
    }
  }

  /** Abstract base class for parse tree node representing a statement. */
  public abstract static class Stmt extends Node {
    protected Stmt(SqlParserPos pos, Op op) {
      super(pos, op);
    }
  }

  /** Abstract base class for statements that assign to a named relation. */
  public abstract static class Assignment extends Stmt {
    final Identifier target;

    protected Assignment(SqlParserPos pos, Op op, Identifier target) {
      super(pos, op);
      this.target = Objects.requireNonNull(target);
    }
  }

  /** Parse tree node for LOAD statement. */
  public static class LoadStmt extends Assignment {
    final Literal name;

    public LoadStmt(SqlParserPos pos, Identifier target, Literal name) {
      super(pos, Op.LOAD, target);
      this.name = Objects.requireNonNull(name);
    }
  }

  /** Parse tree node for VALUES statement.
   *
   * <p>VALUES is an extension to Pig, inspired by SQL's VALUES clause.
   */
  public static class ValuesStmt extends Assignment {
    final List<List<Node>> tupleList;
    final Schema schema;

    public ValuesStmt(SqlParserPos pos, Identifier target, Schema schema,
        List<List<Node>> tupleList) {
      super(pos, Op.VALUES, target);
      this.schema = schema;
      this.tupleList = ImmutableList.copyOf(tupleList);
    }
  }

  /** Abstract base class for an assignment with one source relation. */
  public static class Assignment1 extends Assignment {
    final Identifier source;

    protected Assignment1(SqlParserPos pos, Op op, Identifier target,
        Identifier source) {
      super(pos, op, target);
      this.source = source;
    }
  }

  /** Parse tree node for FOREACH statement (non-nested).
   *
   * <p>Syntax:
   * <blockquote><code>
   * alias = FOREACH alias GENERATE expression [, expression]...
   * [ AS schema ];</code>
   * </blockquote>
   *
   * @see org.apache.calcite.piglet.Ast.ForeachNestedStmt
   */
  public static class ForeachStmt extends Assignment1 {
    final List<Node> expList;

    public ForeachStmt(SqlParserPos pos, Identifier target, Identifier source,
        List<Node> expList, Schema schema) {
      super(pos, Op.FOREACH, target, source);
      this.expList = expList;
      assert schema == null; // not supported yet
    }
  }

  /** Parse tree node for FOREACH statement (nested).
   *
   * <p>Syntax:
   *
   * <blockquote><code>
   * alias = FOREACH nested_alias {
   *   alias = nested_op; [alias = nested_op; ]...
   *   GENERATE expression [, expression]...
   * };<br>
   * &nbsp;
   * nested_op ::= DISTINCT, FILTER, LIMIT, ORDER, SAMPLE
   * </code>
   * </blockquote>
   *
   * @see org.apache.calcite.piglet.Ast.ForeachStmt
   */
  public static class ForeachNestedStmt extends Assignment1 {
    final List<Stmt> nestedStmtList;
    final List<Node> expList;

    public ForeachNestedStmt(SqlParserPos pos, Identifier target,
        Identifier source, List<Stmt> nestedStmtList,
        List<Node> expList, Schema schema) {
      super(pos, Op.FOREACH_NESTED, target, source);
      this.nestedStmtList = nestedStmtList;
      this.expList = expList;
      assert schema == null; // not supported yet
    }
  }

  /** Parse tree node for FILTER statement.
   *
   * <p>Syntax:
   * <blockquote><pre>alias = FILTER alias BY expression;</pre></blockquote>
   */
  public static class FilterStmt extends Assignment1 {
    final Node condition;

    public FilterStmt(SqlParserPos pos, Identifier target,
        Identifier source, Node condition) {
      super(pos, Op.FILTER, target, source);
      this.condition = condition;
    }
  }

  /** Parse tree node for DISTINCT statement.
   *
   * <p>Syntax:
   * <blockquote><pre>alias = DISTINCT alias;</pre></blockquote>
   */
  public static class DistinctStmt extends Assignment1 {
    public DistinctStmt(SqlParserPos pos, Identifier target,
        Identifier source) {
      super(pos, Op.DISTINCT, target, source);
    }
  }

  /** Parse tree node for LIMIT statement.
   *
   * <p>Syntax:
   * <blockquote><pre>alias = LIMIT alias n;</pre></blockquote>
   */
  public static class LimitStmt extends Assignment1 {
    final Literal count;

    public LimitStmt(SqlParserPos pos, Identifier target,
        Identifier source, Literal count) {
      super(pos, Op.LIMIT, target, source);
      this.count = count;
    }
  }

  /** Parse tree node for ORDER statement.
   *
   * <p>Syntax:
   * <blockquote>
   *   <code>alias = ORDER alias BY (* | field) [ASC | DESC]
   *     [, field [ASC | DESC] ]...;</code>
   * </blockquote>
   */
  public static class OrderStmt extends Assignment1 {
    final List<Pair<Identifier, Direction>> fields;

    public OrderStmt(SqlParserPos pos, Identifier target,
        Identifier source, List<Pair<Identifier, Direction>> fields) {
      super(pos, Op.ORDER, target, source);
      this.fields = fields;
    }
  }

  /** Parse tree node for GROUP statement.
   *
   * <p>Syntax:
   * <blockquote>
   *   <code>alias = GROUP alias
   *   ( ALL | BY ( exp | '(' exp [, exp]... ')' ) )</code>
   * </blockquote>
   */
  public static class GroupStmt extends Assignment1 {
    /** Grouping keys. May be null (for ALL), or a list of one or more
     * expressions. */
    final List<Node> keys;

    public GroupStmt(SqlParserPos pos, Identifier target,
        Identifier source, List<Node> keys) {
      super(pos, Op.GROUP, target, source);
      this.keys = keys;
      assert keys == null || keys.size() >= 1;
    }
  }

  /** Parse tree node for DUMP statement. */
  public static class DumpStmt extends Stmt {
    final Identifier relation;

    public DumpStmt(SqlParserPos pos, Identifier relation) {
      super(pos, Op.DUMP);
      this.relation = Objects.requireNonNull(relation);
    }
  }

  /** Parse tree node for DESCRIBE statement. */
  public static class DescribeStmt extends Stmt {
    final Identifier relation;

    public DescribeStmt(SqlParserPos pos, Identifier relation) {
      super(pos, Op.DESCRIBE);
      this.relation = Objects.requireNonNull(relation);
    }
  }

  /** Parse tree node for Literal. */
  public static class Literal extends Node {
    final Object value;

    public Literal(SqlParserPos pos, Object value) {
      super(pos, Op.LITERAL);
      this.value = Objects.requireNonNull(value);
    }

    public static NumericLiteral createExactNumeric(String s,
        SqlParserPos pos) {
      BigDecimal value;
      int prec;
      int scale;

      int i = s.indexOf('.');
      if ((i >= 0) && ((s.length() - 1) != i)) {
        value = SqlParserUtil.parseDecimal(s);
        scale = s.length() - i - 1;
        assert scale == value.scale() : s;
        prec = s.length() - 1;
      } else if ((i >= 0) && ((s.length() - 1) == i)) {
        value = SqlParserUtil.parseInteger(s.substring(0, i));
        scale = 0;
        prec = s.length() - 1;
      } else {
        value = SqlParserUtil.parseInteger(s);
        scale = 0;
        prec = s.length();
      }
      return new NumericLiteral(pos, value, prec, scale, true);
    }

  }

  /** Parse tree node for NumericLiteral. */
  public static class NumericLiteral extends Literal {
    final int prec;
    final int scale;
    final boolean exact;

    NumericLiteral(SqlParserPos pos, BigDecimal value, int prec, int scale,
        boolean exact) {
      super(pos, value);
      this.prec = prec;
      this.scale = scale;
      this.exact = exact;
    }

    public NumericLiteral negate(SqlParserPos pos) {
      BigDecimal value = (BigDecimal) this.value;
      return new NumericLiteral(pos, value.negate(), prec, scale, exact);
    }
  }

  /** Parse tree node for Identifier. */
  public static class Identifier extends Node {
    final String value;

    public Identifier(SqlParserPos pos, String value) {
      super(pos, Op.IDENTIFIER);
      this.value = Objects.requireNonNull(value);
    }

    public boolean isStar() {
      return false;
    }
  }

  /** Parse tree node for "*", a special kind of identifier. */
  public static class SpecialIdentifier extends Identifier {
    public SpecialIdentifier(SqlParserPos pos) {
      super(pos, "*");
    }

    @Override public boolean isStar() {
      return true;
    }
  }

  /** Parse tree node for a call to a function or operator. */
  public static class Call extends Node {
    final ImmutableList<Node> operands;

    private Call(SqlParserPos pos, Op op, ImmutableList<Node> operands) {
      super(pos, op);
      this.operands = ImmutableList.copyOf(operands);
    }

    public Call(SqlParserPos pos, Op op, Iterable<? extends Node> operands) {
      this(pos, op, ImmutableList.copyOf(operands));
    }

    public Call(SqlParserPos pos, Op op, Node... operands) {
      this(pos, op, ImmutableList.copyOf(operands));
    }
  }

  /** Parse tree node for a program. */
  public static class Program extends Node {
    public final List<Stmt> stmtList;

    public Program(SqlParserPos pos, List<Stmt> stmtList) {
      super(pos, Op.PROGRAM);
      this.stmtList = stmtList;
    }
  }

  /** Parse tree for field schema.
   *
   * <p>Syntax:
   * <blockquote><pre>identifier:type</pre></blockquote>
   */
  public static class FieldSchema extends Node {
    final Identifier id;
    final Type type;

    public FieldSchema(SqlParserPos pos, Identifier id, Type type) {
      super(pos, Op.FIELD_SCHEMA);
      this.id = Objects.requireNonNull(id);
      this.type = Objects.requireNonNull(type);
    }
  }

  /** Parse tree for schema.
   *
   * <p>Syntax:
   * <blockquote>
   *   <pre>AS ( identifier:type [, identifier:type]... )</pre>
   * </blockquote>
   */
  public static class Schema extends Node {
    final List<FieldSchema> fieldSchemaList;

    public Schema(SqlParserPos pos, List<FieldSchema> fieldSchemaList) {
      super(pos, Op.SCHEMA);
      this.fieldSchemaList = ImmutableList.copyOf(fieldSchemaList);
    }
  }

  /** Parse tree for type. */
  public abstract static class Type extends Node {
    protected Type(SqlParserPos pos, Op op) {
      super(pos, op);
    }
  }

  /** Parse tree for scalar type such as {@code int}. */
  public static class ScalarType extends Type {
    final String name;

    public ScalarType(SqlParserPos pos, String name) {
      super(pos, Op.SCALAR_TYPE);
      this.name = name;
    }
  }

  /** Parse tree for a bag type. */
  public static class BagType extends Type {
    final Type componentType;

    public BagType(SqlParserPos pos, Type componentType) {
      super(pos, Op.BAG_TYPE);
      this.componentType = componentType;
    }
  }

  /** Parse tree for a tuple type. */
  public static class TupleType extends Type {
    final List<FieldSchema> fieldSchemaList;

    public TupleType(SqlParserPos pos, List<FieldSchema> fieldSchemaList) {
      super(pos, Op.TUPLE_TYPE);
      this.fieldSchemaList = ImmutableList.copyOf(fieldSchemaList);
    }
  }

  /** Parse tree for a map type. */
  public static class MapType extends Type {
    final Type keyType;
    final Type valueType;

    public MapType(SqlParserPos pos) {
      super(pos, Op.MAP_TYPE);
      // REVIEW: Why does Pig's "map" type not have key and value types?
      this.keyType = new ScalarType(pos, "int");
      this.valueType = new ScalarType(pos, "int");
    }
  }

  /** Contains output and indentation level while a tree of nodes is
   * being converted to text. */
  static class UnParser {
    final StringBuilder buf = new StringBuilder();
    final Spacer spacer = new Spacer(0);

    public UnParser in() {
      spacer.add(2);
      return this;
    }

    public UnParser out() {
      spacer.subtract(2);
      return this;
    }

    public UnParser newline() {
      buf.append(Util.LINE_SEPARATOR);
      spacer.spaces(buf);
      return this;
    }

    public UnParser append(String s) {
      buf.append(s);
      return this;
    }

    public UnParser append(Node n) {
      return unParse(this, n);
    }

    public UnParser appendList(List<? extends Node> list) {
      append("[").in();
      for (Ord<Node> n : Ord.<Node>zip(list)) {
        newline().append(n.e);
        if (n.i < list.size() - 1) {
          append(",");
        }
      }
      return out().append("]");
    }
  }

  /** Sort direction. */
  public enum Direction {
    ASC,
    DESC,
    NOT_SPECIFIED
  }
}

// End Ast.java
