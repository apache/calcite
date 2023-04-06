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

/**
 * <blockquote><pre>
 *    SHOW ALL
 *    SHOW TIME ZONE
 *    SHOW TRANSACTION ISOLATION LEVEL
 *    SHOW SESSION AUTHORIZATION
 *    SHOW Identifier()
 * </pre></blockquote>
 *
 * @see <a href="https://www.postgresql.org/docs/current/sql-show.html">
 * PostgreSQL SHOW documentation</a>
 * @see <a href="https://github.com/postgres/postgres/blob/master/src/backend/parser/gram.y">
 * PostgreSQL grammar file (VariableShowStmt) </a>
 */
SqlNode PostgresSqlShow() :
{
    final String name;
    final Span s;
}
{
    <SHOW> { s = span(); }
    (   <ALL> { name = "all"; }
    |   <TIME> <ZONE> { name = "timezone"; }
    |   <TRANSACTION> <ISOLATION> <LEVEL> { name = "transaction_isolation"; }
    |   <SESSION> <AUTHORIZATION> { name = "session_authorization"; }
    |   name = Identifier()
    ) {
        return SqlShow.OPERATOR.createCall(null, s.end(this), new SqlIdentifier(name, getPos()));
    }
}

/**
 * <blockquote><pre>
 *    SET { SESSION | LOCAL } configuration_parameter { TO | = } { value | DEFAULT }
 *    SET { SESSION | LOCAL } TIME ZONE { value | LOCAL | DEFAULT }
 *    SET { SESSION | LOCAL } (SCHEMA | NAMES | SEED) value
 *    SET { SESSION | LOCAL } TRANSACTION transaction_mode [, ...]
 *    SET { SESSION | LOCAL } TRANSACTION SNAPSHOT snapshot_id
 *    SET { SESSION | LOCAL } SESSION CHARACTERISTICS AS TRANSACTION transaction_mode [, ...]
 *    SET { SESSION | LOCAL } SESSION AUTHORIZATION
 *    SET { SESSION | LOCAL } ROLE role_name
 *    SET { SESSION | LOCAL } ROLE NONE
 *    RESET ALL
 *    RESET SESSION AUTHORIZATION
 *    RESET TIME ZONE
 *    RESET TRANSACTION ISOLATION LEVEL
 *    RESET CompoundIdentifier()
 *
 *    value - Values can be specified as string constants, identifiers, numbers, or
 *    comma-separated lists of these
 *
 *    transaction_mode is one of:
 *         ISOLATION LEVEL { SERIALIZABLE | REPEATABLE READ | READ COMMITTED | READ UNCOMMITTED }
 *         READ WRITE | READ ONLY
 *         [ NOT ] DEFERRABLE
 * </blockquote></pre>
 *
 * @see <a href="https://www.postgresql.org/docs/current/sql-set.html">
 * PostgreSQL SET documentation</a>
 * @see <a href="https://www.postgresql.org/docs/current/sql-reset.html">
 * PostgreSQL RESET documentation</a>
 * @see <a href="https://www.postgresql.org/docs/current/sql-set-role.html">
 * PostgreSQL SET ROLE documentation</a>
 * @see <a href="https://www.postgresql.org/docs/current/sql-set-transaction.html">
 * PostgreSQL SET TRANSACTION documentation</a>
 * @see <a href="https://www.postgresql.org/docs/current/sql-set-session-authorization.html">
 * PostgreSQL SET SESSION AUTHORIZATION documentation</a>
 * @see <a href="https://github.com/postgres/postgres/blob/master/src/backend/parser/gram.y">
 * PostgreSQL grammar file (VariableSetStmt, VariableResetStmt) </a>
 */
SqlAlter PostgresSqlSetOption(Span s, String scope) :
{
    final SqlAlter node;
}
{

    { s.add(this); }
    (
        <SET>
        (   LOOKAHEAD(2) node = PostgresSqlSetParameter(s, scope)
        |   <SESSION> { scope = token.image.toUpperCase(Locale.ROOT); } node = PostgresSqlSetParameter(s, scope)
        |   <LOCAL> { scope = token.image.toUpperCase(Locale.ROOT); } node = PostgresSqlSetParameter(s, scope)
        )
    |   node = PostgresSqlResetParameter(s, scope)
    ) {
        return node;
    }
}

SqlAlter PostgresSqlSetParameter(Span s, String scope) :
{
    SqlNode name;
    final SqlNode val;
}
{
    (
        <SESSION>
        (
            <CHARACTERISTICS> <AS> <TRANSACTION> {
                name = SqlSetOptions.Names.SESSION_CHARACTERISTICS_AS_TRANSACTION.symbol(getPos());
            } val = PostgresSqlTransactionModeList()
        |
            <AUTHORIZATION> {
                name = new SqlIdentifier("session_authorization", getPos());
            } val = PostgresSqlOptionValue()
        )
    |
        <TRANSACTION>
            (
                <SNAPSHOT> val = PostgresSqlOptionValues() {
                    name = SqlSetOptions.Names.TRANSACTION_SNAPSHOT.symbol(getPos());
                }
            |
                val = PostgresSqlTransactionModeList() {
                    name = SqlSetOptions.Names.TRANSACTION.symbol(getPos());
                }
            )
    |
        <TIME> <ZONE> { name = SqlSetOptions.Names.TIME_ZONE.symbol(getPos()); }
        (   val = Default()
        |   <LOCAL> { val = SqlSetOptions.Values.LOCAL.symbol(getPos()); }
        |   val = Literal()
        )
    |
        <ROLE> { name = SqlSetOptions.Names.ROLE.symbol(getPos()); }
        (   <NONE> { val = SqlSetOptions.Values.NONE.symbol(getPos()); }
        |   val = PostgresSqlOptionValue()
        )
    |
        (   name = CompoundIdentifier() ( <EQ> | <TO> )
        |   <SCHEMA> { name = new SqlIdentifier("search_path", getPos()); }
        |   <NAMES> { name = new SqlIdentifier("client_encoding", getPos()); }
        |   <SEED> { name = new SqlIdentifier("seed", getPos()); }
        ) val = PostgresSqlOptionValues()
    ) {
        return new SqlSetOption(s.end(val), scope, name, val);
    }
}

SqlAlter PostgresSqlResetParameter(Span s, String scope) :
{
    SqlNode name;
    SqlNode value;
}
{
    <RESET> { s.add(this); }
    (   <ALL> { name = SqlSetOptions.Names.ALL.symbol(getPos()); }
    |   <SESSION> <AUTHORIZATION> { name = new SqlIdentifier("session_authorization", getPos()); }
    |   <TIME> <ZONE> { name = new SqlIdentifier("timezone", getPos()); }
    |   <TRANSACTION> <ISOLATION> <LEVEL> { name = new SqlIdentifier("transaction_isolation", getPos()); }
    |   name = CompoundIdentifier()
    ) {
        return new SqlSetOption(s.end(name), scope, name, null);
    }
}

SqlNode PostgresSqlOptionValues() :
{
    final List<SqlNode> list;
    Span s;
    SqlNode e;
}
{
    e = PostgresSqlOptionValue() { s = span(); list = startList(e); }
    ( <COMMA> e = PostgresSqlOptionValue() { list.add(e); } )*
    { return list.size() > 1 ? new SqlNodeList(list, s.end(this)) : e; }
}

SqlNode PostgresSqlOptionValue() :
{
    final SqlNode val;
}
{
    (   val = Default()
    |   val = Literal()
    |   val = SimpleIdentifier()
    |   <ON> {
            // OFF is handled by SimpleIdentifier, ON handled here.
            val = new SqlIdentifier(token.image.toUpperCase(Locale.ROOT), getPos());
        }
    ) {
        return val;
    }
}

/**
 * <blockquote><pre>
 *    DISCARD { ALL | PLANS | SEQUENCES | TEMPORARY | TEMP }
 * </blockquote></pre>
 *
 * @see <a href="https://www.postgresql.org/docs/current/sql-discard.html">
 * PostgreSQL DISCARD documentation</a>
 */
SqlNode PostgresSqlDiscard() :
{
    final Span s;
}
{
    { s = span(); }
    <DISCARD> ( <ALL> | <PLANS> | <SEQUENCES> | <TEMPORARY> | <TEMP> ) {
        return SqlDiscard.OPERATOR.createCall(s.end(this), new SqlIdentifier(
            token.image.toUpperCase(Locale.ROOT), getPos()));
    }
}

/**
 * <blockquote><pre>
 *     BEGIN [ WORK | TRANSACTION ] [ transaction_mode [, ...] ]
 *     where transaction_mode is one of:
 *         ISOLATION LEVEL { SERIALIZABLE | REPEATABLE READ | READ COMMITTED | READ UNCOMMITTED }
 *         READ WRITE | READ ONLY
 *         [ NOT ] DEFERRABLE
 * </blockquote></pre>
 *
 * @see <a href="https://www.postgresql.org/docs/current/sql-begin.html">
 * PostgreSQL BEGIN documentation</a>
 */
SqlNode PostgresSqlBegin() :
{
    final Span s;
    SqlNodeList transactionModeList = SqlNodeList.EMPTY;
}
{
    { s = span(); }
    <BEGIN> [ ( <WORK> | <TRANSACTION> ) ] [ transactionModeList = PostgresSqlTransactionModeList() ] {
        return SqlBegin.OPERATOR.createCall(s.end(this), (SqlNode) transactionModeList);
    }
}

SqlNodeList PostgresSqlTransactionModeList() :
{
    final List<SqlNode> list;
    Span s;
    SqlNode e;
}
{
    { s = span(); }
    e = PostgresSqlTransactionMode() { s = span(); list = startList(e); }
    ( <COMMA> e = PostgresSqlTransactionMode() { list.add(e); } )*
    { return new SqlNodeList(list, s.end(this)); }
}

SqlNode PostgresSqlTransactionMode() :
{
    final Span s;
    TransactionMode m;
}
{
    { s = span(); }
    (   <READ>
        (   <WRITE> { m = TransactionMode.READ_WRITE; }
        |   <ONLY>  { m = TransactionMode.READ_ONLY; }
        )
    |   <DEFERRABLE> { m = TransactionMode.DEFERRABLE; }
    |   <NOT> <DEFERRABLE> { m = TransactionMode.NOT_DEFERRABLE; }
    |   <ISOLATION> <LEVEL>
        (   <SERIALIZABLE> { m = TransactionMode.ISOLATION_LEVEL_SERIALIZABLE; }
        |   <REPEATABLE> <READ> { m = TransactionMode.ISOLATION_LEVEL_REPEATABLE_READ; }
        |   <READ>
            (   <COMMITTED> { m = TransactionMode.ISOLATION_LEVEL_READ_COMMITTED; }
            |   <UNCOMMITTED>  { m = TransactionMode.ISOLATION_LEVEL_READ_UNCOMMITTED; }
            )
        )
    ) {
        return m.symbol(s.end(this));
    }
}

/**
 * <blockquote><pre>
 *     COMMIT [ WORK | TRANSACTION ] [ AND [ NO ] CHAIN ]
 * </blockquote></pre>
 *
 * @see <a href="https://www.postgresql.org/docs/current/sql-commit.html">
 * PostgreSQL COMMIT documentation</a>
 */
SqlNode PostgresSqlCommit() :
{
    final Span s;
    TransactionChainingMode chainingMode = TransactionChainingMode.AND_NO_CHAIN;
}
{
    { s = span(); }
    <COMMIT> [ <WORK> | <TRANSACTION> ] [ chainingMode = PostgresTransactionChainingMode() ] {
        final SqlParserPos pos = s.end(this);
        return SqlCommit.OPERATOR.createCall(pos, chainingMode.symbol(pos));
    }
}

/**
 * <blockquote><pre>
 *     ROLLBACK [ WORK | TRANSACTION ] [ AND [ NO ] CHAIN ]
 * </blockquote></pre>
 *
 * @see <a href="https://www.postgresql.org/docs/current/sql-rollback.html">
 * PostgreSQL ROLLBACK documentation</a>
 */
SqlNode PostgresSqlRollback() :
{
    final Span s;
    TransactionChainingMode chainingMode = TransactionChainingMode.AND_NO_CHAIN;
}
{
    { s = span(); }
    <ROLLBACK> [ <WORK> | <TRANSACTION> ] [ chainingMode = PostgresTransactionChainingMode() ] {
        final SqlParserPos pos = s.end(this);
        return SqlRollback.OPERATOR.createCall(pos, chainingMode.symbol(pos));
    }
}

TransactionChainingMode PostgresTransactionChainingMode() :
{
    TransactionChainingMode chainingMode = TransactionChainingMode.AND_NO_CHAIN;
}
{
    (   <AND> <CHAIN> { chainingMode = TransactionChainingMode.AND_CHAIN; }
    |   <AND> <NO> <CHAIN>
    )
    { return chainingMode; }
}
