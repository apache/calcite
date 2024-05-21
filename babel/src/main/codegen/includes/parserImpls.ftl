<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

JoinType LeftSemiJoin() :
{
}
{
    <LEFT> <SEMI> <JOIN> { return JoinType.LEFT_SEMI_JOIN; }
}

JoinType LeftAntiJoin() :
{
}
{
    <LEFT> <ANTI> <JOIN> { return JoinType.LEFT_ANTI_JOIN; }
}

SqlNode DatePartFunctionCall() :
{
    final Span s;
    final SqlOperator op;
    final SqlNode unit;
    final List<SqlNode> args;
    SqlNode e;
}
{
    <DATE_PART> { op = SqlLibraryOperators.DATE_PART; }
    { s = span(); }
    <LPAREN>
    (   unit = TimeUnitOrName() {
            args = startList(unit);
        }
    |   unit = Expression(ExprContext.ACCEPT_NON_QUERY) {
            args = startList(unit);
        }
    )
    <COMMA> e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
        args.add(e);
    }
    <RPAREN> {
        return op.createCall(s.end(this), args);
    }
}

SqlNode DateaddFunctionCall() :
{
    final Span s;
    final SqlOperator op;
    final SqlIntervalQualifier unit;
    final List<SqlNode> args;
    SqlNode e;
}
{
    (   <DATEADD> { op = SqlLibraryOperators.DATEADD; }
    |   <DATEDIFF> { op = SqlLibraryOperators.DATEDIFF; }
    |   <DATEPART>  { op = SqlLibraryOperators.DATEPART; }
    )
    { s = span(); }
    <LPAREN> unit = TimeUnitOrName() {
        args = startList(unit);
    }
    (
        <COMMA> e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
            args.add(e);
        }
    )*
    <RPAREN> {
        return op.createCall(s.end(this), args);
    }
}

boolean IfNotExistsOpt() :
{
}
{
    <IF> <NOT> <EXISTS> { return true; }
|
    { return false; }
}

TableCollectionType TableCollectionTypeOpt() :
{
}
{
    <MULTISET> { return TableCollectionType.MULTISET; }
|
    <SET> { return TableCollectionType.SET; }
|
    { return TableCollectionType.UNSPECIFIED; }
}

boolean VolatileOpt() :
{
}
{
    <VOLATILE> { return true; }
|
    { return false; }
}

SqlNodeList ExtendColumnList() :
{
    final Span s;
    List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    ColumnWithType(list)
    (
        <COMMA> ColumnWithType(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void ColumnWithType(List<SqlNode> list) :
{
    SqlIdentifier id;
    SqlDataTypeSpec type;
    boolean nullable = true;
    final Span s = Span.of();
}
{
    id = CompoundIdentifier()
    type = DataType()
    [
        <NOT> <NULL> {
            nullable = false;
        }
    ]
    {
        list.add(SqlDdlNodes.column(s.add(id).end(this), id,
            type.withNullable(nullable), null, null));
    }
}

SqlCreate SqlCreateTable(Span s, boolean replace) :
{
    final TableCollectionType tableCollectionType;
    final boolean volatile_;
    final boolean ifNotExists;
    final SqlIdentifier id;
    final SqlNodeList columnList;
    final SqlNode query;
}
{
    tableCollectionType = TableCollectionTypeOpt()
    volatile_ = VolatileOpt()
    <TABLE>
    ifNotExists = IfNotExistsOpt()
    id = CompoundIdentifier()
    (
        columnList = ExtendColumnList()
    |
        { columnList = null; }
    )
    (
        <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    |
        { query = null; }
    )
    {
        return new SqlBabelCreateTable(s.end(this), replace,
            tableCollectionType, volatile_, ifNotExists, id, columnList, query);
    }
}


/* Extra operators */

<DEFAULT, DQID, BTID> TOKEN :
{
    < DATE_PART: "DATE_PART" >
|   < DATEADD: "DATEADD" >
|   < DATEDIFF: "DATEDIFF" >
|   < DATEPART: "DATEPART" >
|   < NEGATE: "!" >
|   < TILDE: "~" >
}

/** Parses the infix "::" cast operator used in PostgreSQL. */
void InfixCast(List<Object> list, ExprContext exprContext, Span s) :
{
    final SqlDataTypeSpec dt;
}
{
    <INFIX_CAST> {
        checkNonQueryExpression(exprContext);
    }
    dt = DataType() {
        list.add(
            new SqlParserUtil.ToTreeListItem(SqlLibraryOperators.INFIX_CAST,
                s.pos()));
        list.add(dt);
    }
}

/** Parses the NULL-safe "<=>" equal operator used in MySQL. */
void NullSafeEqual(List<Object> list, ExprContext exprContext, Span s) :
{
}
{
    <NULL_SAFE_EQUAL> {
        checkNonQueryExpression(exprContext);
        list.add(new SqlParserUtil.ToTreeListItem(SqlLibraryOperators.NULL_SAFE_EQUAL, getPos()));
    }
    AddExpression2b(list, ExprContext.ACCEPT_SUB_QUERY)
}
