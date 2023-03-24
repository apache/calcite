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


boolean IfNotExistsOpt() :
{
}
{
    <IF> <NOT> <EXISTS> { return true; }
    |
    { return false; }
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
    final boolean volatile_;
    final boolean ifNotExists;
    final SqlIdentifier id;
    final SqlNodeList columnList;
    final SqlNode query;
}
{
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
        <LIKE> query = TableRef()
    |
        { query = null; }
    )
    {
        return new SqlBodoCreateTable(s.end(this), replace, volatile_,
            ifNotExists, id, columnList, query);
    }
}




// /** Parses the infix "::" cast operator used in PostgreSQL. */
// void InfixCast(List<Object> list, ExprContext exprContext, Span s) :
// {
//     final SqlDataTypeSpec dt;
// }
// {
//     <INFIX_CAST> {
//         checkNonQueryExpression(exprContext);
//     }
//     dt = DataType() {
//         list.add(
//             new SqlParserUtil.ToTreeListItem(SqlLibraryOperators.INFIX_CAST,
//                 s.pos()));
//         list.add(dt);
//     }
// }


/** Parses the NULL-safe "<=>" equal operator used in MySQL. */
// void NullSafeEqual(List<Object> list, ExprContext exprContext, Span s):
// {
// }
// {
//     <NULL_SAFE_EQUAL> {
//         checkNonQueryExpression(exprContext);
//         list.add(new SqlParserUtil.ToTreeListItem(SqlLibraryOperators.NULL_SAFE_EQUAL, getPos()));
//     }
//     Expression2b(ExprContext.ACCEPT_SUB_QUERY, list)
// }
