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

/** SHOW (<TRANSACTION ISOLATION LEVEL> | name) */
SqlNode PostgresqlSqlShow() :
{
    final SqlIdentifier parameter;
    final Span s;
}
{
    <SHOW> { s = span(); }
  (
      parameter = PostgresqlTransactionIsolationLevel()
|
      <IDENTIFIER> {
          parameter = new SqlIdentifier(token.image.toLowerCase(Locale.ROOT), getPos());
      }
  ) { return SqlShow.OPERATOR.createCall(null, s.end(this), parameter); }
}

SqlIdentifier PostgresqlTransactionIsolationLevel() :
{
    final Span s;
}
{
  { s = span(); } <TRANSACTION> <ISOLATION> <LEVEL> {
    return new SqlIdentifier("transaction_isolation", s.end(this));
  }
}

/**
 * SET [ SESSION | LOCAL ] configuration_parameter { TO | = } { value | 'value' | DEFAULT }
 * SET [ SESSION | LOCAL ] TIME ZONE { value | 'value' | LOCAL | DEFAULT }
 * SET [ SESSION | LOCAL ] (SCHEMA | NAMES | SEED) value
 * value - Values can be specified as string constants, identifiers, numbers, or comma-separated lists of these
 */
SqlNode PostgresqlSqlSetOption() :
{
    SqlIdentifier name;
    final SqlNode val;
    String scope = null;
    Span s;
}
{
    { s = span(); }
    (
        <SET> {
            s.add(this);
        }
        [ scope = PostgresqlOptionScope() ]
        (
          <TIME> <ZONE> { name = new SqlIdentifier("timezone", getPos()); }
          (
            val = Default()
          |
            <LOCAL> { val = SqlLiteral.createCharString("LOCAL", getPos()); }
          |
            val = Literal()
          )
        |
          (
              <SCHEMA> { name = new SqlIdentifier("search_path", getPos()); }
            |
              <NAMES> { name = new SqlIdentifier("client_encoding", getPos()); }
            |
              <SEED> { name = new SqlIdentifier("seed", getPos()); }
            |
              name = CompoundIdentifier()
              ( <EQ> | <TO> )
          )
          val = PostgresqlSqlOptionValues()
        ){
          return new SqlSetOption(s.end(val), scope, name, val);
        }

    |
        <RESET> {
            s.add(this);
        }
        (
            name = CompoundIdentifier()
        |
            <ALL> {
                name = new SqlIdentifier(token.image.toUpperCase(Locale.ROOT),
                    getPos());
            }
        )
        {
            return new SqlSetOption(s.end(name), scope, name, null);
        }
    )
}

String PostgresqlOptionScope() :
{
}
{
    ( <LOCAL> | <SESSION> ) { return token.image.toUpperCase(Locale.ROOT); }
}

SqlNode PostgresqlSqlOptionValues():
{
  final List<SqlNode> list;
  Span s;
  SqlNode e;
}
{
   e = PostgresqlSqlOptionValue() { s = span(); list = startList(e); }
   ( <COMMA> e = PostgresqlSqlOptionValue() { list.add(e); } )*
   {
      return list.size() > 1 ? new SqlNodeList(list, s.end(this)) : e;
   }
}

SqlNode PostgresqlSqlOptionValue():
{
  final SqlNode val;
}
{
  (
      val = Default()
  |
      val = Literal()
  |
      val = SimpleIdentifier()
  ) {
    return val;
  }
}

/** DISCARD { ALL | PLANS | SEQUENCES | TEMPORARY | TEMP } */
SqlNode PostgresqlSqlDiscard() :
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
 * BEGIN [ WORK | TRANSACTION ] [ transaction_mode [, ...] ]
 * where transaction_mode is one of:
 * ISOLATION LEVEL { SERIALIZABLE | REPEATABLE READ | READ COMMITTED | READ UNCOMMITTED }
 * READ WRITE | READ ONLY
 * [ NOT ] DEFERRABLE
 */
SqlNode PostgresqlSqlBegin() :
{
  final Span s;
  SqlNodeList transactionModeList = SqlNodeList.EMPTY;
}
{
  { s = span(); }
  <BEGIN> [ ( <WORK> | <TRANSACTION> ) ] [transactionModeList = PostgresqlSqlBeginTransactionModeList()] {
    return SqlBegin.OPERATOR.createCall(s.end(this), (SqlNode) transactionModeList);
  }
}

SqlNodeList PostgresqlSqlBeginTransactionModeList():
{
  final List<SqlNode> list;
  Span s;
  SqlNode e;
}
{
  { s = span(); }
  e = PostgresqlSqlBeginTransactionMode() { s = span(); list = startList(e); }
  ( <COMMA> e = PostgresqlSqlBeginTransactionMode() { list.add(e); } )*
  { return new SqlNodeList(list, s.end(this)); }
}

SqlNode PostgresqlSqlBeginTransactionMode():
{
  final Span s;
  SqlBegin.TransactionMode m;
}
{
    { s = span(); }
(
    LOOKAHEAD(2)
    <READ>
    (
      <WRITE> { m = SqlBegin.TransactionMode.READ_WRITE; }
    |
      <ONLY>  { m = SqlBegin.TransactionMode.READ_ONLY; }
    )
|
    <DEFERRABLE> { m = SqlBegin.TransactionMode.DEFERRABLE; }
|
    <NOT> <DEFERRABLE> { m = SqlBegin.TransactionMode.NOT_DEFERRABLE; }
|
    <ISOLATION> <LEVEL>
    (
        <SERIALIZABLE> { m = SqlBegin.TransactionMode.ISOLATION_LEVEL_SERIALIZABLE; }
    |
        <REPEATABLE> <READ> { m = SqlBegin.TransactionMode.ISOLATION_LEVEL_REPEATABLE_READ; }
    |
        <READ>
        (
          <COMMITTED> { m = SqlBegin.TransactionMode.ISOLATION_LEVEL_READ_COMMITTED; }
        |
          <UNCOMMITTED>  { m = SqlBegin.TransactionMode.ISOLATION_LEVEL_READ_UNCOMMITTED; }
        )
    )
) {
    return m.symbol(s.end(this));
  }
}

/** COMMIT [ WORK | TRANSACTION ] [ AND [ NO ] CHAIN ] */
SqlNode PostgresqlSqlCommit():
{
  final Span s;
  AndChain chain = AndChain.AND_NO_CHAIN;
}
{
  { s = span(); }
  <COMMIT> [ <WORK> | <TRANSACTION> ] [
  (
    <AND> <CHAIN> { chain = AndChain.AND_CHAIN; }
  |
    <AND> <NO> <CHAIN>
  )] {
    final SqlParserPos pos = s.end(this);
    return SqlCommit.OPERATOR.createCall(pos, chain.symbol(pos));
  }
}

/** ROLLBACK [ WORK | TRANSACTION ] [ AND [ NO ] CHAIN ] */
SqlNode PostgresqlSqlRollback():
{
  final Span s;
  AndChain chain = AndChain.AND_NO_CHAIN;
}
{
  { s = span(); }
  <ROLLBACK> [ <WORK> | <TRANSACTION> ] [
  (
    <AND> <CHAIN> { chain = AndChain.AND_CHAIN; }
  |
    <AND> <NO> <CHAIN>
  )] {
    final SqlParserPos pos = s.end(this);
    return SqlRollback.OPERATOR.createCall(pos, chain.symbol(pos));
  }
}
