/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.impl.jdbc;

import net.hydromatic.linq4j.expressions.Expressions;

import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.eigenbase.rel.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.sql.type.BasicSqlType;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.validate.SqlValidatorUtil;
import org.eigenbase.util.Pair;

import java.util.*;

/**
 * State for generating a SQL statement.
 */
public class JdbcImplementor {
  public static final SqlParserPos POS = SqlParserPos.ZERO;

  final SqlDialect dialect;
    private final JavaTypeFactory typeFactory;
    private final Set<String> aliasSet = new LinkedHashSet<String>();

  public JdbcImplementor(SqlDialect dialect, JavaTypeFactory typeFactory) {
    this.dialect = dialect;
      this.typeFactory = typeFactory;
  }

  /** Creates a result based on a single relational expression. */
  public Result result(SqlNode node, Collection<Clause> clauses, RelNode rel) {
    final String alias2 = simpleAlias(node);
    final String alias3 = alias2 != null ? alias2 : "t";
    final String alias4 = SqlValidatorUtil.uniquify(alias3, aliasSet);
    final String alias5 = alias2 == null || !alias2.equals(alias4) ? alias4
        : null;
    return new Result(node, clauses, alias5,
        Collections.singletonList(Pair.of(alias4, rel.getRowType())));
  }

  /** Creates a result based on a join. (Each join could contain one or more
   * relational expressions.) */
  public Result result(SqlNode join, Result leftResult, Result rightResult) {
    final List<Pair<String, RelDataType>> list =
        new ArrayList<Pair<String, RelDataType>>();
    list.addAll(leftResult.aliases);
    list.addAll(rightResult.aliases);
    return new Result(join, Expressions.list(Clause.FROM), null, list);
  }

  public static String simpleAlias(SqlNode node) {
    return node instanceof SqlIdentifier
        ? ((SqlIdentifier) node).names[((SqlIdentifier) node).names.length - 1]
        : null;
  }

  /** Wraps a node in a SELECT statement that has no clauses:
   *  "SELECT ... FROM (node)". */
  SqlSelect wrapSelect(SqlNode node) {
    assert node instanceof SqlJoin
        || node instanceof SqlIdentifier
        || node instanceof SqlCall
        && (((SqlCall) node).getOperator() instanceof SqlSetOperator
        || ((SqlCall) node).getOperator() == SqlStdOperatorTable.asOperator)
        : node;
    return SqlStdOperatorTable.selectOperator.createCall(
        SqlNodeList.Empty, null, node, null, null, null,
        SqlNodeList.Empty, null, POS);
  }

  public Result visitChild(int i, RelNode e) {
    return ((JdbcRel) e).implement(this);
  }

  /** Context for translating a {@link RexNode} expression (within a
   * {@link RelNode}) into a {@link SqlNode} expression (within a SQL parse
   * tree). */
  public abstract class Context {
    private final int fieldCount;

    protected Context(int fieldCount) {
      this.fieldCount = fieldCount;
    }

    public abstract SqlNode field(int ordinal);

    /** Converts an expression from {@link RexNode} to {@link SqlNode}
     * format. */
    SqlNode toSql(RexProgram program, RexNode rex) {
      if (rex instanceof RexLocalRef) {
        final int index = ((RexLocalRef) rex).getIndex();
        return toSql(program, program.getExprList().get(index));
      } else if (rex instanceof RexInputRef) {
        return field(((RexInputRef) rex).getIndex());
      } else if (rex instanceof RexLiteral) {
        final RexLiteral literal = (RexLiteral) rex;
        switch (literal.getTypeName().getFamily()) {
        case CHARACTER:
          return SqlLiteral.createCharString((String) literal.getValue2(), POS);
        case NUMERIC:
        case EXACT_NUMERIC:
          return SqlLiteral.createExactNumeric(literal.getValue().toString(),
              POS);
        case APPROXIMATE_NUMERIC:
          return SqlLiteral.createApproxNumeric(
              literal.getValue().toString(), POS);
        case BOOLEAN:
          return SqlLiteral.createBoolean((Boolean) literal.getValue(), POS);
        default:
          throw new AssertionError(literal);
        }
      } else if (rex instanceof RexCall) {
        final RexCall call = (RexCall) rex;
        final SqlOperator op = call.getOperator();
        final List<SqlNode> nodeList = toSql(program, call.getOperandList());
        if (op == SqlStdOperatorTable.castFunc) {
          RelDataType type = call.getType();
          if (type.getSqlTypeName() == SqlTypeName.VARCHAR
              && dialect.getDatabaseProduct()
                 == SqlDialect.DatabaseProduct.MYSQL) {
            // MySQL doesn't have a VARCHAR type, only CHAR.
            nodeList.add(
                new SqlDataTypeSpec(new SqlIdentifier("CHAR", POS),
                    type.getPrecision(), -1, null, null, POS));
          } else {
            nodeList.add(toSql(type));
          }
        }
        if (op == SqlStdOperatorTable.caseOperator) {
          final SqlNode valueNode;
          final List<SqlNode> whenList = Expressions.list();
          final List<SqlNode> thenList = Expressions.list();
          final SqlNode elseNode;
          if (nodeList.size() % 2 == 0) {
            // switched:
            //   "case x when v1 then t1 when v2 then t2 ... else e end"
            valueNode = nodeList.get(0);
            for (int i = 1; i < nodeList.size() - 1; i += 2) {
              whenList.add(nodeList.get(i));
              thenList.add(nodeList.get(i + 1));
            }
          } else {
            // other: "case when w1 then t1 when w2 then t2 ... else e end"
            valueNode = null;
            for (int i = 0; i < nodeList.size() - 1; i += 2) {
              whenList.add(nodeList.get(i));
              thenList.add(nodeList.get(i + 1));
            }
          }
          elseNode = nodeList.get(nodeList.size() - 1);
          return op.createCall(POS, valueNode, new SqlNodeList(whenList, POS),
              new SqlNodeList(thenList, POS), elseNode);
        }
        return op.createCall(new SqlNodeList(nodeList, POS));
      } else {
        throw new AssertionError(rex);
      }
    }

    private SqlNode toSql(RelDataType type) {
      if (dialect.getDatabaseProduct() == SqlDialect.DatabaseProduct.MYSQL) {
        final SqlTypeName sqlTypeName = type.getSqlTypeName();
        switch (sqlTypeName) {
        case VARCHAR:
          // MySQL doesn't have a VARCHAR type, only CHAR.
          return new SqlDataTypeSpec(new SqlIdentifier("CHAR", POS),
              type.getPrecision(), -1, null, null, POS);
        case INTEGER:
          return new SqlDataTypeSpec(new SqlIdentifier("_UNSIGNED", POS),
              type.getPrecision(), -1, null, null, POS);
        }
      }
      if (type instanceof BasicSqlType) {
        return new SqlDataTypeSpec(
            new SqlIdentifier(type.getSqlTypeName().name(), POS),
            type.getPrecision(),
            type.getScale(),
            type.getCharset() != null
            && dialect.supportsCharSet()
                ? type.getCharset().name()
                : null,
            null,
            POS);
      }
      throw new AssertionError(type); // TODO: implement
    }

    private List<SqlNode> toSql(RexProgram program, List<RexNode> operandList) {
      final List<SqlNode> list = new ArrayList<SqlNode>();
      for (RexNode rex : operandList) {
        list.add(toSql(program, rex));
      }
      return list;
    }

    public List<SqlNode> fieldList() {
      return new AbstractList<SqlNode>() {
        public SqlNode get(int index) {
          return field(index);
        }

        public int size() {
          return fieldCount;
        }
      };
    }

    /** Converts a call to an aggregate function to an expression. */
    public SqlNode toSql(AggregateCall aggCall) {
      SqlOperator op = (SqlAggFunction) aggCall.getAggregation();
      final List<SqlNode> operands = Expressions.list();
      for (int arg : aggCall.getArgList()) {
        operands.add(field(arg));
      }
      return op.createCall(
          aggCall.isDistinct() ? SqlSelectKeyword.Distinct.symbol(POS) : null,
          POS, operands.toArray(new SqlNode[operands.size()]));
    }

    /** Converts a collation to an ORDER BY item. */
    public SqlNode toSql(RelFieldCollation collation) {
      SqlNode node = field(collation.getFieldIndex());
      switch (collation.getDirection()) {
      case Descending:
      case StrictlyDescending:
        node = SqlStdOperatorTable.descendingOperator.createCall(POS, node);
      }
      switch (collation.nullDirection) {
      case FIRST:
        node = SqlStdOperatorTable.nullsFirstOperator.createCall(POS, node);
        break;
      case LAST:
        node = SqlStdOperatorTable.nullsLastOperator.createCall(POS, node);
        break;
      }
      return node;
    }
  }

  private static int computeFieldCount(
      List<Pair<String, RelDataType>> aliases) {
    int x = 0;
    for (Pair<String, RelDataType> alias : aliases) {
      x += alias.right.getFieldCount();
    }
    return x;
  }

  /** Implementation of Context that precedes field references with their
   * "table alias" based on the current sub-query's FROM clause. */
  public class AliasContext extends Context {
    private final boolean qualified;
    private final List<Pair<String, RelDataType>> aliases;

    public AliasContext(List<Pair<String, RelDataType>> aliases,
        boolean qualified) {
      super(computeFieldCount(aliases));
      this.aliases = aliases;
      this.qualified = qualified;
    }

    public SqlNode field(int ordinal) {
      for (Pair<String, RelDataType> alias : aliases) {
        final List<RelDataTypeField> fields = alias.right.getFieldList();
        if (ordinal < fields.size()) {
          RelDataTypeField field = fields.get(ordinal);
          return new SqlIdentifier(!qualified
              ? new String[] {field.getName()}
              : new String[] {alias.left, field.getName()},
              POS);
        }
        ordinal -= fields.size();
      }
      throw new AssertionError(
          "field ordinal " + ordinal + " out of range " + aliases);
    }
  }

  public class Result {
    final SqlNode node;
    private final String neededAlias;
    private final List<Pair<String, RelDataType>> aliases;
    final Expressions.FluentList<Clause> clauses;

    private Result(SqlNode node, Collection<Clause> clauses, String neededAlias,
        List<Pair<String, RelDataType>> aliases) {
      this.node = node;
      this.neededAlias = neededAlias;
      this.aliases = aliases;
      this.clauses = Expressions.list(clauses);
    }

    /** Once you have a Result of implementing a child relational expression,
     * call this method to create a Builder to implement the current relational
     * expression by adding additional clauses to the SQL query.
     *
     * <p>You need to declare which clauses you intend to add. If the clauses
     * are "later", you can add to the same query. For example, "GROUP BY" comes
     * after "WHERE". But if they are the same or earlier, this method will
     * start a new SELECT that wraps the previous result.</p>
     *
     * <p>When you have called
     * {@link Builder#setSelect(org.eigenbase.sql.SqlNodeList)},
     * {@link Builder#setWhere(org.eigenbase.sql.SqlNode)} etc. call
     * {@link Builder#result(org.eigenbase.sql.SqlNode, java.util.Collection, org.eigenbase.rel.RelNode)}
     * to fix the new query.</p>
     *
     * @param rel Relational expression being implemented
     * @param clauses Clauses that will be generated to implement current
     *                relational expression
     * @return A builder
     */
    public Builder builder(JdbcRel rel, Clause... clauses) {
      final Clause maxClause = maxClause();
      boolean needNew = false;
      for (Clause clause : clauses) {
        if (maxClause.ordinal() >= clause.ordinal()) {
          needNew = true;
        }
      }
      SqlSelect select;
      Expressions.FluentList<Clause> clauseList = Expressions.list();
      if (needNew) {
        select = subSelect();
      } else {
        select = asSelect();
        clauseList.addAll(this.clauses);
      }
      clauseList.addAll(Arrays.asList(clauses));
      Context newContext;
      final SqlNodeList selectList = select.getSelectList();
      if (selectList != null) {
        newContext = new Context(selectList.size()) {
          @Override
          public SqlNode field(int ordinal) {
            final SqlNode selectItem = selectList.get(ordinal);
            if (selectItem instanceof SqlCall
                && ((SqlCall) selectItem).getOperator()
                   == SqlStdOperatorTable.asOperator) {
              return ((SqlCall) selectItem).operands[0];
            }
            return selectItem;
          }
        };
      } else {
        newContext = new AliasContext(aliases, aliases.size() > 1);
      }
      return new Builder(rel, clauseList, select, newContext);
    }

    // make private?
    public Clause maxClause() {
      Clause maxClause = null;
      for (Clause clause : clauses) {
        if (maxClause == null || clause.ordinal() > maxClause.ordinal()) {
          maxClause = clause;
        }
      }
      assert maxClause != null;
      return maxClause;
    }

    /** Returns a node that can be included in the FROM clause or a JOIN. It has
     * an alias that is unique within the query. The alias is implicit if it
     * can be derived using the usual rules (For example, "SELECT * FROM emp" is
     * equivalent to "SELECT * FROM emp AS emp".) */
    public SqlNode asFrom() {
      if (neededAlias != null) {
        return SqlStdOperatorTable.asOperator.createCall(POS, node,
            new SqlIdentifier(neededAlias, POS));
      }
      return node;
    }

    public SqlSelect subSelect() {
      return wrapSelect(asFrom());
    }

    /** Converts a non-query node into a SELECT node. Set operators (UNION,
     * INTERSECT, EXCEPT) remain as is. */
    SqlSelect asSelect() {
      if (node instanceof SqlSelect) {
        return (SqlSelect) node;
      }
      return wrapSelect(node);
    }

    /** Converts a non-query node into a SELECT node. Set operators (UNION,
     * INTERSECT, EXCEPT) remain as is. */
    SqlNode asQuery() {
      if (node instanceof SqlCall
          && ((SqlCall) node).getOperator() instanceof SqlSetOperator) {
        return node;
      }
      return asSelect();
    }

    /** Returns a context that always qualifies identifiers. Useful if the
     * Context deals with just one arm of a join, yet we wish to generate
     * a join condition that qualifies column names to disambiguate them. */
    public Context qualifiedContext() {
      return new AliasContext(aliases, true);
    }
  }

  public class Builder {
    private final JdbcRel rel;
    private final List<Clause> clauses;
    private final SqlSelect select;
    public final Context context;

    public Builder(JdbcRel rel, List<Clause> clauses, SqlSelect select,
        Context context) {
      this.rel = rel;
      this.clauses = clauses;
      this.select = select;
      this.context = context;
    }

    public void setSelect(SqlNodeList nodeList) {
      select.operands[SqlSelect.SELECT_OPERAND] = nodeList;
    }

    public void setWhere(SqlNode node) {
      assert clauses.contains(Clause.WHERE);
      select.operands[SqlSelect.WHERE_OPERAND] = node;
    }

    public void setGroupBy(SqlNodeList nodeList) {
      assert clauses.contains(Clause.GROUP_BY);
      select.operands[SqlSelect.GROUP_OPERAND] = nodeList;
    }

    public void setOrderBy(SqlNodeList nodeList) {
      assert clauses.contains(Clause.ORDER_BY);
      select.operands[SqlSelect.ORDER_OPERAND] = nodeList;
    }

    public Result result() {
      return JdbcImplementor.this.result(select, clauses, rel);
    }
  }

  /** Clauses in a SQL query. Ordered by evaluation order.
   * SELECT is set only when there is a NON-TRIVIAL SELECT clause. */
  enum Clause {
    FROM, WHERE, GROUP_BY, HAVING, SELECT, SET_OP, ORDER_BY
  }
}

// End JdbcImplementor.java
