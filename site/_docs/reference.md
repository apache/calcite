---
layout: docs
title: SQL language
permalink: /docs/reference.html
---
<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

The following functions do not need to be documented. They are listed
here to appease testAllFunctionsAreDocumented:

| Function       | Reason not documented
|:-------------- |:---------------------
| CALL           | TODO: document
| CLASSIFIER()   | Documented with MATCH_RECOGNIZE
| CUME_DIST()    | In SqlStdOperatorTable, but not fully implemented
| DESC           | Described as part of ORDER BY syntax
| EQUALS         | Documented as an period operator
| FILTER         | Documented as part of aggregateCall syntax
| FINAL          | TODO: Document with MATCH_RECOGNIZE
| FIRST()        | TODO: Documented with MATCH_RECOGNIZE
| JSON_ARRAYAGG_ABSENT_ON_NULL() | Covered by JSON_ARRAYAGG
| JSON_OBJECTAGG_NULL_ON_NULL() | Covered by JSON_OBJECTAGG
| JSON_VALUE_ANY() | Covered by JSON_VALUE
| LAST()         | TODO: document with MATCH_RECOGNIZE
| MATCH_NUMBER() | Documented with MATCH_RECOGNIZE
| NEW            | TODO: document
| NEXT()         | Documented with MATCH_RECOGNIZE
| OVERLAPS       | Documented as a period operator
| PERCENT_RANK() | In SqlStdOperatorTable, but not fully implemented
| PRECEDES       | Documented as a period operator
| PREV()         | Documented with MATCH_RECOGNIZE
| RUNNING        | TODO: document with MATCH_RECOGNIZE
| SINGLE_VALUE() | Internal (but should it be?)
| SUCCEEDS       | Documented as a period operator
| TABLE          | Documented as part of FROM syntax
| VARIANCE()     | In SqlStdOperatorTable, but not fully implemented

Dialect-specific:

| C | Function       | Reason not documented
|:--|:-------------- |:---------------------

{% endcomment %}
-->

<style>
.container {
  width: 400px;
  height: 26px;
}
.gray {
  width: 60px;
  height: 26px;
  background: gray;
  float: left;
}
.r15 {
  width: 40px;
  height: 6px;
  background: yellow;
  margin-top: 4px;
  margin-left: 10px;
}
.r12 {
  width: 10px;
  height: 6px;
  background: yellow;
  margin-top: 4px;
  margin-left: 10px;
}
.r13 {
  width: 20px;
  height: 6px;
  background: yellow;
  margin-top: 4px;
  margin-left: 10px;
}
.r2 {
  width: 2px;
  height: 6px;
  background: yellow;
  margin-top: 4px;
  margin-left: 20px;
}
.r24 {
  width: 20px;
  height: 6px;
  background: yellow;
  margin-top: 4px;
  margin-left: 20px;
}
.r35 {
  width: 20px;
  height: 6px;
  background: yellow;
  margin-top: 4px;
  margin-left: 30px;
}
</style>

The page describes the SQL dialect recognized by Calcite's default SQL parser.

## Grammar

SQL grammar in [BNF](https://en.wikipedia.org/wiki/Backus%E2%80%93Naur_Form)-like
form.

{% highlight sql %}
statement:
      setStatement
  |   resetStatement
  |   explain
  |   describe
  |   insert
  |   update
  |   merge
  |   delete
  |   query

statementList:
      statement [ ';' statement ]* [ ';' ]

setStatement:
      [ ALTER { SYSTEM | SESSION } ] SET identifier '=' expression

resetStatement:
      [ ALTER { SYSTEM | SESSION } ] RESET identifier
  |   [ ALTER { SYSTEM | SESSION } ] RESET ALL

explain:
      EXPLAIN PLAN
      [ WITH TYPE | WITH IMPLEMENTATION | WITHOUT IMPLEMENTATION ]
      [ EXCLUDING ATTRIBUTES | INCLUDING [ ALL ] ATTRIBUTES ]
      [ AS JSON | AS XML | AS DOT ]
      FOR { query | insert | update | merge | delete }

describe:
      DESCRIBE DATABASE databaseName
  |   DESCRIBE CATALOG [ databaseName . ] catalogName
  |   DESCRIBE SCHEMA [ [ databaseName . ] catalogName ] . schemaName
  |   DESCRIBE [ TABLE ] [ [ [ databaseName . ] catalogName . ] schemaName . ] tableName [ columnName ]
  |   DESCRIBE [ STATEMENT ] { query | insert | update | merge | delete }

insert:
      { INSERT | UPSERT } INTO tablePrimary
      [ '(' column [, column ]* ')' ]
      query

update:
      UPDATE tablePrimary
      SET assign [, assign ]*
      [ WHERE booleanExpression ]

assign:
      identifier '=' expression

merge:
      MERGE INTO tablePrimary [ [ AS ] alias ]
      USING tablePrimary
      ON booleanExpression
      [ WHEN MATCHED THEN UPDATE SET assign [, assign ]* ]
      [ WHEN NOT MATCHED THEN INSERT VALUES '(' value [ , value ]* ')' ]

delete:
      DELETE FROM tablePrimary [ [ AS ] alias ]
      [ WHERE booleanExpression ]

query:
      values
  |   WITH [ RECURSIVE ] withItem [ , withItem ]* query
  |   {
          select
      |   selectWithoutFrom
      |   query UNION [ ALL | DISTINCT ] query
      |   query EXCEPT [ ALL | DISTINCT ] query
      |   query MINUS [ ALL | DISTINCT ] query
      |   query INTERSECT [ ALL | DISTINCT ] query
      }
      [ ORDER BY orderItem [, orderItem ]* ]
      [ LIMIT [ start, ] { count | ALL } ]
      [ OFFSET start { ROW | ROWS } ]
      [ FETCH { FIRST | NEXT } [ count ] { ROW | ROWS } ONLY ]

withItem:
      name
      [ '(' column [, column ]* ')' ]
      AS '(' query ')'

orderItem:
      expression [ ASC | DESC ] [ NULLS FIRST | NULLS LAST ]

select:
      SELECT [ hintComment ] [ STREAM ] [ ALL | DISTINCT ]
          { * | projectItem [, projectItem ]* }
      FROM tableExpression
      [ WHERE booleanExpression ]
      [ GROUP BY [ ALL | DISTINCT ] { groupItem [, groupItem ]* } ]
      [ HAVING booleanExpression ]
      [ WINDOW windowName AS windowSpec [, windowName AS windowSpec ]* ]
      [ QUALIFY booleanExpression ]

selectWithoutFrom:
      SELECT [ ALL | DISTINCT ]
          { * | projectItem [, projectItem ]* }

projectItem:
      expression [ [ AS ] columnAlias ]
  |   tableAlias . *

tableExpression:
      tableReference [, tableReference ]*
  |   tableExpression [ NATURAL ] [ { LEFT | RIGHT | FULL } [ OUTER ] ] [ ASOF ] JOIN tableExpression [ joinCondition ]
  |   tableExpression CROSS JOIN tableExpression
  |   tableExpression [ CROSS | OUTER ] APPLY tableExpression

joinCondition:
      ON booleanExpression
  |   MATCH_CONDITION booleanExpression ON booleanExpression
  |   USING '(' column [, column ]* ')'

tableReference:
      tablePrimary
      [ FOR SYSTEM_TIME AS OF expression ]
      [ pivot ]
      [ unpivot ]
      [ matchRecognize ]
      [ [ AS ] alias [ '(' columnAlias [, columnAlias ]* ')' ] ]

tablePrimary:
      [ [ catalogName . ] schemaName . ] tableName
      '(' TABLE [ [ catalogName . ] schemaName . ] tableName ')'
  |   tablePrimary [ hintComment ] [ EXTEND ] '(' columnDecl [, columnDecl ]* ')'
  |   [ LATERAL ] '(' query ')'
  |   UNNEST '(' expression ')' [ WITH ORDINALITY ]
  |   [ LATERAL ] TABLE '(' [ SPECIFIC ] functionName '(' expression [, expression ]* ')' ')'

columnDecl:
      column type [ NOT NULL ]

hint:
      hintName
  |   hintName '(' hintOptions ')'

hintOptions:
      hintKVOption [, hintKVOption ]*
  |   optionName [, optionName ]*
  |   optionValue [, optionValue ]*

hintKVOption:
      optionName '=' stringLiteral
  |   stringLiteral '=' stringLiteral

optionValue:
      stringLiteral
  |   numericLiteral

columnOrList:
      column
  |   '(' column [, column ]* ')'

exprOrList:
      expr
  |   '(' expr [, expr ]* ')'

pivot:
      PIVOT '('
      pivotAgg [, pivotAgg ]*
      FOR pivotList
      IN '(' pivotExpr [, pivotExpr ]* ')'
      ')'

pivotAgg:
      agg '(' [ ALL | DISTINCT ] value [, value ]* ')'
      [ [ AS ] alias ]

pivotList:
      columnOrList

pivotExpr:
      exprOrList [ [ AS ] alias ]

unpivot:
      UNPIVOT [ INCLUDING NULLS | EXCLUDING NULLS ] '('
      unpivotMeasureList
      FOR unpivotAxisList
      IN '(' unpivotValue [, unpivotValue ]* ')'
      ')'

unpivotMeasureList:
      columnOrList

unpivotAxisList:
      columnOrList

unpivotValue:
      column [ AS literal ]
  |   '(' column [, column ]* ')' [ AS '(' literal [, literal ]* ')' ]

values:
      { VALUES | VALUE } expression [, expression ]*

groupItem:
      expression
  |   '(' ')'
  |   '(' expression [, expression ]* ')'
  |   CUBE '(' expression [, expression ]* ')'
  |   ROLLUP '(' expression [, expression ]* ')'
  |   GROUPING SETS '(' groupItem [, groupItem ]* ')'

window:
      windowName
  |   windowSpec

windowSpec:
      '('
      [ windowName ]
      [ ORDER BY orderItem [, orderItem ]* ]
      [ PARTITION BY expression [, expression ]* ]
      [
          RANGE numericOrIntervalExpression { PRECEDING | FOLLOWING [exclude]}
      |   ROWS numericExpression { PRECEDING | FOLLOWING [exclude] }
      ]
      ')'

exclude:
      EXCLUDE NO OTHERS
  |   EXCLUDE CURRENT ROW
  |   EXCLUDE GROUP
  |   EXCLUDE TIES

{% endhighlight %}

In *insert*, if the INSERT or UPSERT statement does not specify a
list of target columns, the query must have the same number of
columns as the target table, except in certain
[conformance levels]({{ site.apiRoot }}/org/apache/calcite/sql/validate/SqlConformance.html#isInsertSubsetColumnsAllowed--).

In *merge*, at least one of the WHEN MATCHED and WHEN NOT MATCHED clauses must
be present.

*tablePrimary* may only contain an EXTEND clause in certain
[conformance levels]({{ site.apiRoot }}/org/apache/calcite/sql/validate/SqlConformance.html#allowExtend--);
in those same conformance levels, any *column* in *insert* may be replaced by
*columnDecl*, which has a similar effect to including it in an EXTEND clause.

In *orderItem*, if *expression* is a positive integer *n*, it denotes
the <em>n</em>th item in the SELECT clause.

In *query*, *count* and *start* may each be either an unsigned integer literal
or a dynamic parameter whose value is an integer.

An aggregate query is a query that contains a GROUP BY or a HAVING
clause, or aggregate functions in the SELECT clause. In the SELECT,
HAVING and ORDER BY clauses of an aggregate query, all expressions
must be constant within the current group (that is, grouping constants
as defined by the GROUP BY clause, or constants), or aggregate
functions, or a combination of constants and aggregate
functions. Aggregate and grouping functions may only appear in an
aggregate query, and only in a SELECT, HAVING or ORDER BY clause.

A scalar sub-query is a sub-query used as an expression.
If the sub-query returns no rows, the value is NULL; if it
returns more than one row, it is an error.

IN, EXISTS, UNIQUE and scalar sub-queries can occur
in any place where an expression can occur (such as the SELECT clause,
WHERE clause, ON clause of a JOIN, or as an argument to an aggregate
function).

An IN, EXISTS, UNIQUE or scalar sub-query may be correlated; that is, it
may refer to tables in the FROM clause of an enclosing query.

GROUP BY DISTINCT removes duplicate grouping sets (for example,
"GROUP BY DISTINCT GROUPING SETS ((a), (a, b), (a))" is equivalent to
"GROUP BY GROUPING SETS ((a), (a, b))");
GROUP BY ALL is equivalent to GROUP BY.

*selectWithoutFrom* is equivalent to VALUES,
but is not standard SQL and is only allowed in certain
[conformance levels]({{ site.apiRoot }}/org/apache/calcite/sql/validate/SqlConformance.html#isFromRequired--).

MINUS is equivalent to EXCEPT,
but is not standard SQL and is only allowed in certain
[conformance levels]({{ site.apiRoot }}/org/apache/calcite/sql/validate/SqlConformance.html#isMinusAllowed--).

CROSS APPLY and OUTER APPLY are only allowed in certain
[conformance levels]({{ site.apiRoot }}/org/apache/calcite/sql/validate/SqlConformance.html#isApplyAllowed--).

"LIMIT start, count" is equivalent to "LIMIT count OFFSET start"
but is only allowed in certain
[conformance levels]({{ site.apiRoot }}/org/apache/calcite/sql/validate/SqlConformance.html#isLimitStartCountAllowed--).

"OFFSET start" may occur before "LIMIT count" in certain
[conformance levels]({{ site.apiRoot }}/org/apache/calcite/sql/validate/SqlConformance.html#isOffsetLimitAllowed--).

VALUE is equivalent to VALUES,
but is not standard SQL and is only allowed in certain
[conformance levels]({{ site.apiRoot }}/org/apache/calcite/sql/validate/SqlConformance.html#isValueAllowed--).

An "ASOF JOIN" operation combines rows from two tables based on
comparable timestamp values.  For each row in the left table, the join
finds at most a single row in the right table that has the "closest"
timestamp value. The matched row on the right side is the closest
match whose timestamp column is compared using one of the operations
&lt;, &le;, &gt;, or &ge;, as specified by the comparison operator in
the MATCH_CONDITION clause.  The comparison is performed using SQL
semantics, which returns 'false' when comparing 'NULL' values with any
other values.  Thus a 'NULL' timestamp in the left table will not
match any timestamps in the right table.

ASOF JOIN can be used in an OUTER JOIN form as LEFT ASOF JOIN.  In this case,
when there is no match for a row in the left table, the columns from
the right table are null-padded.  There are no RIGHT ASOF joins.

Example:

```SQL
SELECT *
FROM left_table LEFT ASOF JOIN right_table
MATCH_CONDITION left_table.timecol <= right_table.timecol
ON left_table.col = right_table.col
```

## Keywords

The following is a list of SQL keywords.
Reserved keywords are **bold**.

{% comment %} start {% endcomment %}
A,
**ABS**,
ABSENT,
ABSOLUTE,
ACTION,
ADA,
ADD,
ADMIN,
AFTER,
**ALL**,
**ALLOCATE**,
**ALLOW**,
**ALTER**,
ALWAYS,
**AND**,
**ANY**,
APPLY,
**ARE**,
**ARRAY**,
ARRAY_AGG,
ARRAY_CONCAT_AGG,
**ARRAY_MAX_CARDINALITY**,
**AS**,
ASC,
**ASENSITIVE**,
**ASOF**,
ASSERTION,
ASSIGNMENT,
**ASYMMETRIC**,
**AT**,
**ATOMIC**,
ATTRIBUTE,
ATTRIBUTES,
**AUTHORIZATION**,
**AVG**,
BEFORE,
**BEGIN**,
**BEGIN_FRAME**,
**BEGIN_PARTITION**,
BERNOULLI,
**BETWEEN**,
**BIGINT**,
**BINARY**,
**BIT**,
**BLOB**,
**BOOLEAN**,
**BOTH**,
BREADTH,
**BY**,
C,
**CALL**,
**CALLED**,
**CARDINALITY**,
CASCADE,
**CASCADED**,
**CASE**,
**CAST**,
CATALOG,
CATALOG_NAME,
**CEIL**,
**CEILING**,
CENTURY,
CHAIN,
**CHAR**,
**CHARACTER**,
CHARACTERISTICS,
CHARACTERS,
**CHARACTER_LENGTH**,
CHARACTER_SET_CATALOG,
CHARACTER_SET_NAME,
CHARACTER_SET_SCHEMA,
**CHAR_LENGTH**,
**CHECK**,
**CLASSIFIER**,
CLASS_ORIGIN,
**CLOB**,
**CLOSE**,
**COALESCE**,
COBOL,
**COLLATE**,
COLLATION,
COLLATION_CATALOG,
COLLATION_NAME,
COLLATION_SCHEMA,
**COLLECT**,
**COLUMN**,
COLUMN_NAME,
COMMAND_FUNCTION,
COMMAND_FUNCTION_CODE,
**COMMIT**,
COMMITTED,
**CONDITION**,
CONDITIONAL,
CONDITION_NUMBER,
**CONNECT**,
CONNECTION,
CONNECTION_NAME,
**CONSTRAINT**,
CONSTRAINTS,
CONSTRAINT_CATALOG,
CONSTRAINT_NAME,
CONSTRAINT_SCHEMA,
CONSTRUCTOR,
**CONTAINS**,
CONTAINS_SUBSTR,
CONTINUE,
**CONVERT**,
**CORR**,
**CORRESPONDING**,
**COUNT**,
**COVAR_POP**,
**COVAR_SAMP**,
**CREATE**,
**CROSS**,
**CUBE**,
**CUME_DIST**,
**CURRENT**,
**CURRENT_CATALOG**,
**CURRENT_DATE**,
**CURRENT_DEFAULT_TRANSFORM_GROUP**,
**CURRENT_PATH**,
**CURRENT_ROLE**,
**CURRENT_ROW**,
**CURRENT_SCHEMA**,
**CURRENT_TIME**,
**CURRENT_TIMESTAMP**,
**CURRENT_TRANSFORM_GROUP_FOR_TYPE**,
**CURRENT_USER**,
**CURSOR**,
CURSOR_NAME,
**CYCLE**,
DATA,
DATABASE,
**DATE**,
**DATETIME**,
DATETIME_DIFF,
DATETIME_INTERVAL_CODE,
DATETIME_INTERVAL_PRECISION,
DATETIME_TRUNC,
DATE_DIFF,
DATE_TRUNC,
**DAY**,
DAYOFWEEK,
DAYOFYEAR,
DAYS,
**DEALLOCATE**,
**DEC**,
DECADE,
**DECIMAL**,
**DECLARE**,
**DEFAULT**,
DEFAULTS,
DEFERRABLE,
DEFERRED,
**DEFINE**,
DEFINED,
DEFINER,
DEGREE,
**DELETE**,
**DENSE_RANK**,
DEPTH,
**DEREF**,
DERIVED,
DESC,
**DESCRIBE**,
DESCRIPTION,
DESCRIPTOR,
**DETERMINISTIC**,
DIAGNOSTICS,
**DISALLOW**,
**DISCONNECT**,
DISPATCH,
**DISTINCT**,
DOMAIN,
DOT,
**DOUBLE**,
DOW,
DOY,
**DROP**,
**DYNAMIC**,
DYNAMIC_FUNCTION,
DYNAMIC_FUNCTION_CODE,
**EACH**,
**ELEMENT**,
**ELSE**,
**EMPTY**,
ENCODING,
**END**,
**END-EXEC**,
**END_FRAME**,
**END_PARTITION**,
EPOCH,
**EQUALS**,
ERROR,
**ESCAPE**,
**EVERY**,
**EXCEPT**,
EXCEPTION,
EXCLUDE,
EXCLUDING,
**EXEC**,
**EXECUTE**,
**EXISTS**,
**EXP**,
**EXPLAIN**,
**EXTEND**,
**EXTERNAL**,
**EXTRACT**,
**FALSE**,
**FETCH**,
**FILTER**,
FINAL,
FIRST,
**FIRST_VALUE**,
**FLOAT**,
**FLOOR**,
FOLLOWING,
**FOR**,
**FOREIGN**,
FORMAT,
FORTRAN,
FOUND,
FRAC_SECOND,
**FRAME_ROW**,
**FREE**,
**FRIDAY**,
**FROM**,
**FULL**,
**FUNCTION**,
**FUSION**,
G,
GENERAL,
GENERATED,
GEOMETRY,
**GET**,
**GLOBAL**,
GO,
GOTO,
**GRANT**,
GRANTED,
**GROUP**,
**GROUPING**,
**GROUPS**,
GROUP_CONCAT,
**HAVING**,
HIERARCHY,
**HOLD**,
HOP,
**HOUR**,
HOURS,
**IDENTITY**,
IGNORE,
ILIKE,
IMMEDIATE,
IMMEDIATELY,
IMPLEMENTATION,
**IMPORT**,
**IN**,
INCLUDE,
INCLUDING,
INCREMENT,
**INDICATOR**,
**INITIAL**,
INITIALLY,
**INNER**,
**INOUT**,
INPUT,
**INSENSITIVE**,
**INSERT**,
INSTANCE,
INSTANTIABLE,
**INT**,
**INTEGER**,
**INTERSECT**,
**INTERSECTION**,
**INTERVAL**,
**INTO**,
INVOKER,
**IS**,
ISODOW,
ISOLATION,
ISOYEAR,
JAVA,
**JOIN**,
JSON,
**JSON_ARRAY**,
**JSON_ARRAYAGG**,
**JSON_EXISTS**,
**JSON_OBJECT**,
**JSON_OBJECTAGG**,
**JSON_QUERY**,
**JSON_SCOPE**,
**JSON_VALUE**,
K,
KEY,
KEY_MEMBER,
KEY_TYPE,
LABEL,
**LAG**,
**LANGUAGE**,
**LARGE**,
LAST,
**LAST_VALUE**,
**LATERAL**,
**LEAD**,
**LEADING**,
**LEFT**,
LENGTH,
LEVEL,
LIBRARY,
**LIKE**,
**LIKE_REGEX**,
**LIMIT**,
**LN**,
**LOCAL**,
**LOCALTIME**,
**LOCALTIMESTAMP**,
LOCATOR,
**LOWER**,
M,
MAP,
**MATCH**,
MATCHED,
**MATCHES**,
**MATCH_CONDITION**,
**MATCH_NUMBER**,
**MATCH_RECOGNIZE**,
**MAX**,
MAXVALUE,
**MEASURE**,
**MEASURES**,
**MEMBER**,
**MERGE**,
MESSAGE_LENGTH,
MESSAGE_OCTET_LENGTH,
MESSAGE_TEXT,
**METHOD**,
MICROSECOND,
MILLENNIUM,
MILLISECOND,
**MIN**,
**MINUS**,
**MINUTE**,
MINUTES,
MINVALUE,
**MOD**,
**MODIFIES**,
**MODULE**,
**MONDAY**,
**MONTH**,
MONTHS,
MORE,
**MULTISET**,
MUMPS,
NAME,
NAMES,
NANOSECOND,
**NATIONAL**,
**NATURAL**,
**NCHAR**,
**NCLOB**,
NESTING,
**NEW**,
**NEXT**,
**NO**,
**NONE**,
**NORMALIZE**,
NORMALIZED,
**NOT**,
**NTH_VALUE**,
**NTILE**,
**NULL**,
NULLABLE,
**NULLIF**,
NULLS,
NUMBER,
**NUMERIC**,
OBJECT,
**OCCURRENCES_REGEX**,
OCTETS,
**OCTET_LENGTH**,
**OF**,
**OFFSET**,
**OLD**,
**OMIT**,
**ON**,
**ONE**,
**ONLY**,
**OPEN**,
OPTION,
OPTIONS,
**OR**,
**ORDER**,
ORDERING,
**ORDINAL**,
ORDINALITY,
OTHERS,
**OUT**,
**OUTER**,
OUTPUT,
**OVER**,
**OVERLAPS**,
**OVERLAY**,
OVERRIDING,
PAD,
**PARAMETER**,
PARAMETER_MODE,
PARAMETER_NAME,
PARAMETER_ORDINAL_POSITION,
PARAMETER_SPECIFIC_CATALOG,
PARAMETER_SPECIFIC_NAME,
PARAMETER_SPECIFIC_SCHEMA,
PARTIAL,
**PARTITION**,
PASCAL,
PASSING,
PASSTHROUGH,
PAST,
PATH,
**PATTERN**,
**PER**,
**PERCENT**,
**PERCENTILE_CONT**,
**PERCENTILE_DISC**,
**PERCENT_RANK**,
**PERIOD**,
**PERMUTE**,
PIVOT,
PLACING,
PLAN,
PLI,
**PORTION**,
**POSITION**,
**POSITION_REGEX**,
**POWER**,
**PRECEDES**,
PRECEDING,
**PRECISION**,
**PREPARE**,
PRESERVE,
**PREV**,
**PRIMARY**,
PRIOR,
PRIVILEGES,
**PROCEDURE**,
PUBLIC,
**QUALIFY**,
QUARTER,
QUARTERS,
**RANGE**,
**RANK**,
READ,
**READS**,
**REAL**,
**RECURSIVE**,
**REF**,
**REFERENCES**,
**REFERENCING**,
**REGR_AVGX**,
**REGR_AVGY**,
**REGR_COUNT**,
**REGR_INTERCEPT**,
**REGR_R2**,
**REGR_SLOPE**,
**REGR_SXX**,
**REGR_SXY**,
**REGR_SYY**,
RELATIVE,
**RELEASE**,
REPEATABLE,
REPLACE,
**RESET**,
RESPECT,
RESTART,
RESTRICT,
**RESULT**,
**RETURN**,
RETURNED_CARDINALITY,
RETURNED_LENGTH,
RETURNED_OCTET_LENGTH,
RETURNED_SQLSTATE,
RETURNING,
**RETURNS**,
**REVOKE**,
**RIGHT**,
RLIKE,
ROLE,
**ROLLBACK**,
**ROLLUP**,
ROUTINE,
ROUTINE_CATALOG,
ROUTINE_NAME,
ROUTINE_SCHEMA,
**ROW**,
**ROWS**,
ROW_COUNT,
**ROW_NUMBER**,
**RUNNING**,
**SAFE_CAST**,
**SAFE_OFFSET**,
**SAFE_ORDINAL**,
**SATURDAY**,
**SAVEPOINT**,
SCALAR,
SCALE,
SCHEMA,
SCHEMA_NAME,
**SCOPE**,
SCOPE_CATALOGS,
SCOPE_NAME,
SCOPE_SCHEMA,
**SCROLL**,
**SEARCH**,
**SECOND**,
SECONDS,
SECTION,
SECURITY,
**SEEK**,
**SELECT**,
SELF,
**SENSITIVE**,
SEPARATOR,
SEQUENCE,
SERIALIZABLE,
SERVER,
SERVER_NAME,
SESSION,
**SESSION_USER**,
**SET**,
SETS,
**SHOW**,
**SIMILAR**,
SIMPLE,
SIZE,
**SKIP**,
**SMALLINT**,
**SOME**,
SOURCE,
SPACE,
**SPECIFIC**,
**SPECIFICTYPE**,
SPECIFIC_NAME,
**SQL**,
**SQLEXCEPTION**,
**SQLSTATE**,
**SQLWARNING**,
SQL_BIGINT,
SQL_BINARY,
SQL_BIT,
SQL_BLOB,
SQL_BOOLEAN,
SQL_CHAR,
SQL_CLOB,
SQL_DATE,
SQL_DECIMAL,
SQL_DOUBLE,
SQL_FLOAT,
SQL_INTEGER,
SQL_INTERVAL_DAY,
SQL_INTERVAL_DAY_TO_HOUR,
SQL_INTERVAL_DAY_TO_MINUTE,
SQL_INTERVAL_DAY_TO_SECOND,
SQL_INTERVAL_HOUR,
SQL_INTERVAL_HOUR_TO_MINUTE,
SQL_INTERVAL_HOUR_TO_SECOND,
SQL_INTERVAL_MINUTE,
SQL_INTERVAL_MINUTE_TO_SECOND,
SQL_INTERVAL_MONTH,
SQL_INTERVAL_SECOND,
SQL_INTERVAL_YEAR,
SQL_INTERVAL_YEAR_TO_MONTH,
SQL_LONGVARBINARY,
SQL_LONGVARCHAR,
SQL_LONGVARNCHAR,
SQL_NCHAR,
SQL_NCLOB,
SQL_NUMERIC,
SQL_NVARCHAR,
SQL_REAL,
SQL_SMALLINT,
SQL_TIME,
SQL_TIMESTAMP,
SQL_TINYINT,
SQL_TSI_DAY,
SQL_TSI_FRAC_SECOND,
SQL_TSI_HOUR,
SQL_TSI_MICROSECOND,
SQL_TSI_MINUTE,
SQL_TSI_MONTH,
SQL_TSI_QUARTER,
SQL_TSI_SECOND,
SQL_TSI_WEEK,
SQL_TSI_YEAR,
SQL_VARBINARY,
SQL_VARCHAR,
**SQRT**,
**START**,
STATE,
STATEMENT,
**STATIC**,
**STDDEV_POP**,
**STDDEV_SAMP**,
**STREAM**,
STRING_AGG,
STRUCTURE,
STYLE,
SUBCLASS_ORIGIN,
**SUBMULTISET**,
**SUBSET**,
SUBSTITUTE,
**SUBSTRING**,
**SUBSTRING_REGEX**,
**SUCCEEDS**,
**SUM**,
**SUNDAY**,
**SYMMETRIC**,
**SYSTEM**,
**SYSTEM_TIME**,
**SYSTEM_USER**,
**TABLE**,
**TABLESAMPLE**,
TABLE_NAME,
TEMPORARY,
**THEN**,
**THURSDAY**,
TIES,
**TIME**,
**TIMESTAMP**,
TIMESTAMPADD,
TIMESTAMPDIFF,
TIMESTAMP_DIFF,
TIMESTAMP_TRUNC,
**TIMEZONE_HOUR**,
**TIMEZONE_MINUTE**,
TIME_DIFF,
TIME_TRUNC,
**TINYINT**,
**TO**,
TOP_LEVEL_COUNT,
**TRAILING**,
TRANSACTION,
TRANSACTIONS_ACTIVE,
TRANSACTIONS_COMMITTED,
TRANSACTIONS_ROLLED_BACK,
TRANSFORM,
TRANSFORMS,
**TRANSLATE**,
**TRANSLATE_REGEX**,
**TRANSLATION**,
**TREAT**,
**TRIGGER**,
TRIGGER_CATALOG,
TRIGGER_NAME,
TRIGGER_SCHEMA,
**TRIM**,
**TRIM_ARRAY**,
**TRUE**,
**TRUNCATE**,
**TRY_CAST**,
**TUESDAY**,
TUMBLE,
TYPE,
**UESCAPE**,
UNBOUNDED,
UNCOMMITTED,
UNCONDITIONAL,
UNDER,
**UNION**,
**UNIQUE**,
**UNKNOWN**,
UNNAMED,
**UNNEST**,
UNPIVOT,
**UNSIGNED**,
**UPDATE**,
**UPPER**,
**UPSERT**,
USAGE,
**USER**,
USER_DEFINED_TYPE_CATALOG,
USER_DEFINED_TYPE_CODE,
USER_DEFINED_TYPE_NAME,
USER_DEFINED_TYPE_SCHEMA,
**USING**,
UTF16,
UTF32,
UTF8,
**UUID**,
**VALUE**,
**VALUES**,
**VALUE_OF**,
**VARBINARY**,
**VARCHAR**,
**VARIANT**,
**VARYING**,
**VAR_POP**,
**VAR_SAMP**,
VERSION,
**VERSIONING**,
VIEW,
**WEDNESDAY**,
WEEK,
WEEKS,
**WHEN**,
**WHENEVER**,
**WHERE**,
**WIDTH_BUCKET**,
**WINDOW**,
**WITH**,
**WITHIN**,
**WITHOUT**,
WORK,
WRAPPER,
WRITE,
XML,
**YEAR**,
YEARS,
ZONE.
{% comment %} end {% endcomment %}

## Identifiers

Identifiers are the names of tables, columns and other metadata
elements used in a SQL query.

Unquoted identifiers, such as emp, must start with a letter and can
only contain letters, digits, and underscores. They are implicitly
converted to upper case.

Quoted identifiers, such as `"Employee Name"`, start and end with
double quotes.  They may contain virtually any character, including
spaces and other punctuation.  If you wish to include a double quote
in an identifier, use another double quote to escape it, like this:
`"An employee called ""Fred""."`.

In Calcite, matching identifiers to the name of the referenced object is
case-sensitive.  But remember that unquoted identifiers are implicitly
converted to upper case before matching, and if the object it refers
to was created using an unquoted identifier for its name, then its
name will have been converted to upper case also.

## Data types

### Scalar types

| Data type   | Description               | Range and example literals
|:----------- |:------------------------- |:--------------------------
| BOOLEAN     | Logical values            | Values: TRUE, FALSE, UNKNOWN
| TINYINT     | 1 byte signed integer     | Range is -128 to 127
| SMALLINT    | 2 byte signed integer     | Range is -32768 to 32767
| INTEGER, INT | 4 byte signed integer    | Range is -2147483648 to 2147483647
| BIGINT      | 8 byte signed integer     | Range is -9223372036854775808 to 9223372036854775807
| TINYINT UNSIGNED  | 1 byte unsigned integer     | Range is 0 to 255
| SMALLINT UNSIGNED | 2 byte unsigned integer     | Range is 0 to 65535
| INTEGER UNSIGNED, INT UNSIGNED | 4 byte unsigned integer    | Range is 0 to 4294967295
| BIGINT UNSIGNED   | 8 byte unsigned integer     | Range is 0 to 18446744073709551615
| DECIMAL(p, s) | Fixed point             | Example: 123.45 and DECIMAL '123.45' are identical values, and have type DECIMAL(5, 2)
| NUMERIC(p, s) | Fixed point             | A synonym for DECIMAL
| REAL        | 4 byte floating point     | 6 decimal digits precision; examples: CAST(1.2 AS REAL), CAST('Infinity' AS REAL)
| DOUBLE      | 8 byte floating point     | 15 decimal digits precision; examples: 1.4E2, CAST('-Infinity' AS DOUBLE), CAST('NaN' AS DOUBLE)
| FLOAT       | 8 byte floating point     | A synonym for DOUBLE
| CHAR(n), CHARACTER(n) | Fixed-width character string | 'Hello', '' (empty string), _latin1'Hello', n'Hello', _UTF16'Hello', 'Hello' 'there' (literal split into multiple parts), e'Hello\nthere' (literal containing C-style escapes)
| VARCHAR(n), CHARACTER VARYING(n) | Variable-length character string | As CHAR(n)
| BINARY(n)   | Fixed-width binary string | x'45F0AB', x'' (empty binary string), x'AB' 'CD' (multi-part binary string literal)
| VARBINARY(n), BINARY VARYING(n) | Variable-length binary string | As BINARY(n)
| DATE        | Date                      | Example: DATE '1969-07-20'
| TIME        | Time of day               | Example: TIME '20:17:40'
| TIME WITH LOCAL TIME ZONE | Time of day with local time zone | Example: TIME WITH LOCAL TIME ZONE '20:17:40'
| TIME WITH TIME ZONE | Time of day with time zone | Example: TIME '20:17:40 GMT+08'
| TIMESTAMP [ WITHOUT TIME ZONE ] | Date and time | Example: TIMESTAMP '1969-07-20 20:17:40'
| TIMESTAMP WITH LOCAL TIME ZONE | Date and time with local time zone | Example: TIMESTAMP WITH LOCAL TIME ZONE '1969-07-20 20:17:40'
| TIMESTAMP WITH TIME ZONE | Date and time with time zone | Example: TIMESTAMP WITH TIME ZONE '1969-07-20 20:17:40 America/Los Angeles'
| UUID        | An 128-bit UUID           | Example: UUID '123e4567-e89b-12d3-a456-426655440000'
| INTERVAL timeUnit [ TO timeUnit ] | Date time interval | Examples: INTERVAL '1-5' YEAR TO MONTH, INTERVAL '45' DAY, INTERVAL '1 2:34:56.789' DAY TO SECOND
| GEOMETRY | Geometry | Examples: ST_GeomFromText('POINT (30 10)')

Where:

{% highlight sql %}
timeUnit:
  MILLENNIUM | CENTURY | DECADE | YEAR | QUARTER | MONTH | WEEK | DOY | DOW | DAY | HOUR | MINUTE | SECOND | EPOCH
{% endhighlight %}

Note:

* DATE, TIME and TIMESTAMP have no time zone. For those types, there is not
  even an implicit time zone, such as UTC (as in Java) or the local time zone.
  It is left to the user or application to supply a time zone. In turn,
  TIMESTAMP WITH LOCAL TIME ZONE does not store the time zone internally, but
  it will rely on the supplied time zone to provide correct semantics.
* GEOMETRY is allowed only in certain
  [conformance levels]({{ site.apiRoot }}/org/apache/calcite/sql/validate/SqlConformance.html#allowGeometry--).
* Interval literals may only use time units
  YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE and SECOND. In certain
  [conformance levels]({{ site.apiRoot }}/org/apache/calcite/sql/validate/SqlConformance.html#allowPluralTimeUnits--),
  we also allow their plurals, YEARS, QUARTERS, MONTHS, WEEKS, DAYS, HOURS, MINUTES and SECONDS.

### Non-scalar types

| Type     | Description                | Example type
|:-------- |:---------------------------|:---------------
| ANY      | The union of all types |
| UNKNOWN  | A value of an unknown type; used as a placeholder |
| ROW      | Row with 1 or more columns | Example: row(f0 int null, f1 varchar)
| MAP      | Collection of keys mapped to values | Example: map &lt; int, varchar &gt;
| MULTISET | Unordered collection that may contain duplicates | Example: int multiset
| ARRAY    | Ordered, contiguous collection that may contain duplicates | Example: varchar(10) array
| CURSOR   | Cursor over the result of executing a query |
| FUNCTION | A function definition that is not bound to an identifier, it is not fully supported in CAST or DDL | Example FUNCTION(INTEGER, VARCHAR(30)) -> INTEGER
| VARIANT  | Dynamically-typed value that can have at runtime a value of any other type | VARIANT

Note:

* Every `ROW` column type can have an optional [ NULL | NOT NULL ] suffix
  to indicate if this column type is nullable, default is not nullable.

### The `VARIANT` type

Values of `VARIANT` type are dynamically-typed.
Any such value holds at runtime two pieces of information:
- the data type
- the data value

Values of `VARIANT` type can be created by casting any other value to a `VARIANT`: e.g.
`SELECT CAST(x AS VARIANT)`.  Conversely, values of type `VARIANT` can be cast to any other data type
`SELECT CAST(variant AS INT)`.  A cast of a value of type `VARIANT` to target type T
will compare the runtime type with T.  If the types are identical or the types are
numeric and there is a natural conversion between the two types, the
original value is converted to the target type and returned.  Otherwise the `CAST` returns `NULL`.

Values of type `ARRAY`, `MAP`, and `ROW` type can be cast to `VARIANT`.  `VARIANT` values
also offer the following operations:

- indexing using array indexing notation `variant[index]`.  If the `VARIANT` is
  obtained from an `ARRAY` value, the indexing operation returns a `VARIANT` whose value element
  is the element at the specified index.  Otherwise, this operation returns `NULL`
- indexing using map element access notation `variant[key]`, where `key` can have
  any legal `MAP` key type.  If the `VARIANT` is obtained from a `MAP` value
  that has en element with this key, a `VARIANT` value holding the associated value in
  the `MAP` is returned.  Otherwise `NULL` is returned.  If the `VARIANT` is obtained from `ROW` value
  which has a field with the name `key`, this operation returns a `VARIANT` value holding
  the corresponding field value.  Otherwise `NULL` is returned.
- field access using the dot notation: `variant.field`.  This operation is interpreted
  as equivalent to `variant['field']`.  Note, however, that the field notation
  is subject to the capitalization rules of the SQL dialect, so for correct
  operation the field may need to be quoted: `variant."field"`

The runtime types do not need to match exactly the compile-time types.
As a compiler front-end, Calcite does not mandate exactly how the runtime types
are represented.  Calcite does include one particular implementation in
Java runtime, which is used for testing.  In this representation
the runtime types are represented as follows:

- The scalar types do not include information about precision and scale.  Thus all `DECIMAL`
  compile-time types are represented by a single run-time type.
- `CHAR(N)` and `VARCHAR` are both represented by a single runtime `VARCHAR` type.
- `BINARY(N)` and `VARBINARY` are both represented by a single runtime `VARBINARY` type.
- `FLOAT` and `DOUBLE` are both represented by the same runtime type.
- All "short interval" types (from days to seconds) are represented by a single type.
- All "long interval" types (from years to months) are represented by a single type.
- Generic types such as `INT ARRAY`, `MULTISET`, and `MAP` convert all their elements to VARIANT values

The function VARIANTNULL() can be used to create an instance
of the `VARIANT` `null` value.

### Spatial types

Spatial data is represented as character strings encoded as
[well-known text (WKT)](https://en.wikipedia.org/wiki/Well-known_text)
or binary strings encoded as
[well-known binary (WKB)](https://en.wikipedia.org/wiki/Well-known_binary).

Where you would use a literal, apply the `ST_GeomFromText` function,
for example `ST_GeomFromText('POINT (30 10)')`.

| Data type          | Type code | Examples in WKT
|:-------------------|:--------- |:---------------------
| GEOMETRY           |  0 | generalization of Point, Curve, Surface, GEOMETRYCOLLECTION
| POINT              |  1 | <code>ST_GeomFromText(&#8203;'POINT (30 10)')</code> is a point in 2D space; <code>ST_GeomFromText(&#8203;'POINT Z(30 10 2)')</code> is point in 3D space
| CURVE              | 13 | generalization of LINESTRING
| LINESTRING         |  2 | <code>ST_GeomFromText(&#8203;'LINESTRING (30 10, 10 30, 40 40)')</code>
| SURFACE            | 14 | generalization of Polygon, PolyhedralSurface
| POLYGON            |  3 | <code>ST_GeomFromText(&#8203;'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))')</code> is a pentagon; <code>ST_GeomFromText(&#8203;'POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))')</code> is a pentagon with a quadrilateral hole
| POLYHEDRALSURFACE  | 15 |
| GEOMETRYCOLLECTION |  7 | a collection of zero or more GEOMETRY instances; a generalization of MULTIPOINT, MULTILINESTRING, MULTIPOLYGON
| MULTIPOINT         |  4 | <code>ST_GeomFromText(&#8203;'MULTIPOINT ((10 40), (40 30), (20 20), (30 10))')</code> is equivalent to <code>ST_GeomFromText(&#8203;'MULTIPOINT (10 40, 40 30, 20 20, 30 10)')</code>
| MULTICURVE         |  - | generalization of MULTILINESTRING
| MULTILINESTRING    |  5 | <code>ST_GeomFromText(&#8203;'MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))')</code>
| MULTISURFACE       |  - | generalization of MULTIPOLYGON
| MULTIPOLYGON       |  6 | <code>ST_GeomFromText(&#8203;'MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))')</code>

## Operators and functions

### Operator precedence

The operator precedence and associativity, highest to lowest.

| Operator                                          | Associativity
|:------------------------------------------------- |:-------------
| .                                                 | left
| ::                                                | left
| [ ] (collection element)                          | left
| + - (unary plus, minus)                           | right
| * / % &#124;&#124;                                | left
| + -                                               | left
| BETWEEN, IN, LIKE, SIMILAR, OVERLAPS, CONTAINS etc. | -
| < > = <= >= <> != <=>                             | left
| IS NULL, IS FALSE, IS NOT TRUE etc.               | -
| NOT                                               | right
| AND                                               | left
| OR                                                | left

Note that `::`,`<=>` is dialect-specific, but is shown in this table for
completeness.

### Comparison operators

| Operator syntax                                   | Description
|:------------------------------------------------- |:-----------
| value1 = value2                                   | Equals
| value1 <> value2                                  | Not equal
| value1 != value2                                  | Not equal (only in certain [conformance levels]({{ site.apiRoot }}/org/apache/calcite/sql/validate/SqlConformance.html#isBangEqualAllowed--))
| value1 > value2                                   | Greater than
| value1 >= value2                                  | Greater than or equal
| value1 < value2                                   | Less than
| value1 <= value2                                  | Less than or equal
| value1 <=> value2                                 | Whether two values are equal, treating null values as the same
| value IS NULL                                     | Whether *value* is null
| value IS NOT NULL                                 | Whether *value* is not null
| value1 IS DISTINCT FROM value2                    | Whether two values are not equal, treating null values as the same
| value1 IS NOT DISTINCT FROM value2                | Whether two values are equal, treating null values as the same
| value1 BETWEEN value2 AND value3                  | Whether *value1* is greater than or equal to *value2* and less than or equal to *value3*
| value1 NOT BETWEEN value2 AND value3              | Whether *value1* is less than *value2* or greater than *value3*
| string1 LIKE string2 [ ESCAPE string3 ]           | Whether *string1* matches pattern *string2*
| string1 NOT LIKE string2 [ ESCAPE string3 ]       | Whether *string1* does not match pattern *string2*
| string1 SIMILAR TO string2 [ ESCAPE string3 ]     | Whether *string1* matches regular expression *string2*
| string1 NOT SIMILAR TO string2 [ ESCAPE string3 ] | Whether *string1* does not match regular expression *string2*
| value IN (value [, value ]*)                      | Whether *value* is equal to a value in a list
| value NOT IN (value [, value ]*)                  | Whether *value* is not equal to every value in a list
| value IN (sub-query)                              | Whether *value* is equal to a row returned by *sub-query*
| value NOT IN (sub-query)                          | Whether *value* is not equal to every row returned by *sub-query*
| value comparison SOME (sub-query or collection)   | Whether *value* *comparison* at least one row returned by *sub-query* or *collection*
| value comparison ANY (sub-query or collection)    | Synonym for `SOME`
| value comparison ALL (sub-query or collection)    | Whether *value* *comparison* every row returned by *sub-query* or *collection*
| EXISTS (sub-query)                                | Whether *sub-query* returns at least one row
| UNIQUE (sub-query)                                | Whether the rows returned by *sub-query* are unique (ignoring null values)

{% highlight sql %}
comp:
      =
  |   <>
  |   >
  |   >=
  |   <
  |   <=
  |   <=>
{% endhighlight %}

### Logical operators

| Operator syntax        | Description
|:---------------------- |:-----------
| boolean1 OR boolean2   | Whether *boolean1* is TRUE or *boolean2* is TRUE
| boolean1 AND boolean2  | Whether *boolean1* and *boolean2* are both TRUE
| NOT boolean            | Whether *boolean* is not TRUE; returns UNKNOWN if *boolean* is UNKNOWN
| boolean IS FALSE       | Whether *boolean* is FALSE; returns FALSE if *boolean* is UNKNOWN
| boolean IS NOT FALSE   | Whether *boolean* is not FALSE; returns TRUE if *boolean* is UNKNOWN
| boolean IS TRUE        | Whether *boolean* is TRUE; returns FALSE if *boolean* is UNKNOWN
| boolean IS NOT TRUE    | Whether *boolean* is not TRUE; returns TRUE if *boolean* is UNKNOWN
| boolean IS UNKNOWN     | Whether *boolean* is UNKNOWN
| boolean IS NOT UNKNOWN | Whether *boolean* is not UNKNOWN

### Arithmetic operators and functions

| Operator syntax           | Description
|:------------------------- |:-----------
| + numeric                 | Returns *numeric*
| - numeric                 | Returns negative *numeric*
| numeric1 + numeric2       | Returns *numeric1* plus *numeric2*
| numeric1 - numeric2       | Returns *numeric1* minus *numeric2*
| numeric1 * numeric2       | Returns *numeric1* multiplied by *numeric2*
| numeric1 / numeric2       | Returns *numeric1* divided by *numeric2*
| numeric1 % numeric2       | As *MOD(numeric1, numeric2)* (only in certain [conformance levels]({{ site.apiRoot }}/org/apache/calcite/sql/validate/SqlConformance.html#isPercentRemainderAllowed--))
| POWER(numeric1, numeric2) | Returns *numeric1* raised to the power of *numeric2*
| ABS(numeric)              | Returns the absolute value of *numeric*
| MOD(numeric1, numeric2)   | Returns the remainder (modulus) of *numeric1* divided by *numeric2*. The result is negative only if *numeric1* is negative
| SQRT(numeric)             | Returns the square root of *numeric*
| LN(numeric)               | Returns the natural logarithm (base *e*) of *numeric*
| LOG10(numeric)            | Returns the base 10 logarithm of *numeric*
| EXP(numeric)              | Returns *e* raised to the power of *numeric*
| CEIL(numeric)             | Rounds *numeric* up, returning the smallest integer that is greater than or equal to *numeric*
| FLOOR(numeric)            | Rounds *numeric* down, returning the largest integer that is less than or equal to *numeric*
| RAND([seed])              | Generates a random double between 0 and 1 inclusive, optionally initializing the random number generator with *seed*
| RAND_INTEGER([seed, ] numeric) | Generates a random integer between 0 and *numeric* - 1 inclusive, optionally initializing the random number generator with *seed*
| ACOS(numeric)             | Returns the arc cosine of *numeric*
| ASIN(numeric)             | Returns the arc sine of *numeric*
| ATAN(numeric)             | Returns the arc tangent of *numeric*
| ATAN2(numeric, numeric)   | Returns the arc tangent of the *numeric* coordinates
| CBRT(numeric)             | Returns the cube root of *numeric*
| COS(numeric)              | Returns the cosine of *numeric*
| COT(numeric)              | Returns the cotangent of *numeric*
| DEGREES(numeric)          | Converts *numeric* from radians to degrees
| PI()                      | Returns a value that is closer than any other value to *pi*
| RADIANS(numeric)          | Converts *numeric* from degrees to radians
| ROUND(numeric1 [, integer2]) | Rounds *numeric1* to optionally *integer2* (if not specified 0) places right to the decimal point
| SIGN(numeric)             | Returns the signum of *numeric*
| SIN(numeric)              | Returns the sine of *numeric*
| TAN(numeric)              | Returns the tangent of *numeric*
| TRUNCATE(numeric1 [, integer2]) | Truncates *numeric1* to optionally *integer2* (if not specified 0) places right to the decimal point

### Character string operators and functions

| Operator syntax            | Description
|:-------------------------- |:-----------
| string &#124;&#124; string | Concatenates two character strings
| CHAR_LENGTH(string)        | Returns the number of characters in a character string
| CHARACTER_LENGTH(string)   | As CHAR_LENGTH(*string*)
| UPPER(string)              | Returns a character string converted to upper case
| LOWER(string)              | Returns a character string converted to lower case
| POSITION(substring IN string) | Returns the position of the first occurrence of *substring* in *string*
| POSITION(substring IN string FROM integer) | Returns the position of the first occurrence of *substring* in *string* starting at a given point (not standard SQL)
| TRIM( { BOTH &#124; LEADING &#124; TRAILING } [[ string1 ] FROM ] string2) | Removes the longest string containing only the characters in *string1* from the start/end/both ends of *string2*.  If *string1* is missing a single space is used.
| OVERLAY(string1 PLACING string2 FROM integer [ FOR integer2 ]) | Replaces a substring of *string1* with *string2*, starting at the specified position *integer* in *string1* and optionally for a specified length *integer2*
| SUBSTRING(string FROM integer)  | Returns a substring of a character string starting at a given point. If starting point is less than 1, the returned expression will begin at the first character that is specified in expression
| SUBSTRING(string FROM integer FOR integer) | Returns a substring of a character string starting at a given point with a given length. If start point is less than 1 in this case, the number of characters that are returned is the largest value of either the start + length - 1 or 0
| INITCAP(string)            | Returns *string* with the first letter of each word converter to upper case and the rest to lower case. Words are sequences of alphanumeric characters separated by non-alphanumeric characters.

Not implemented:

* SUBSTRING(string FROM regexp FOR regexp)

### Binary string operators and functions

| Operator syntax | Description
|:--------------- |:-----------
| binary &#124;&#124; binary | Concatenates two binary strings
| OCTET_LENGTH(binary) | Returns the number of bytes in *binary*
| POSITION(binary1 IN binary2) | Returns the position of the first occurrence of *binary1* in *binary2*
| POSITION(binary1 IN binary2 FROM integer) | Returns the position of the first occurrence of *binary1* in *binary2* starting at a given point (not standard SQL)
| OVERLAY(binary1 PLACING binary2 FROM integer [ FOR integer2 ]) | Replaces a substring of *binary1* with *binary2*, starting at the specified position *integer* in *binary1* and optionally for a specified length *integer2*
| SUBSTRING(binary FROM integer) | Returns a substring of *binary* starting at a given point
| SUBSTRING(binary FROM integer FOR integer) | Returns a substring of *binary* starting at a given point with a given length

### Date/time functions

| Operator syntax           | Description
|:------------------------- |:-----------
| LOCALTIME                 | Returns the current date and time in the session time zone in a value of datatype TIME
| LOCALTIME(precision)      | Returns the current date and time in the session time zone in a value of datatype TIME, with *precision* digits of precision
| LOCALTIMESTAMP            | Returns the current date and time in the session time zone in a value of datatype TIMESTAMP
| LOCALTIMESTAMP(precision) | Returns the current date and time in the session time zone in a value of datatype TIMESTAMP, with *precision* digits of precision
| CURRENT_TIME              | Returns the current time in the session time zone, in a value of datatype TIMESTAMP WITH TIME ZONE
| CURRENT_DATE              | Returns the current date in the session time zone, in a value of datatype DATE
| CURRENT_TIMESTAMP         | Returns the current date and time in the session time zone, in a value of datatype TIMESTAMP WITH TIME ZONE
| EXTRACT(timeUnit FROM datetime) | Extracts and returns the value of a specified datetime field from a datetime value expression
| FLOOR(datetime TO timeUnit) | Rounds *datetime* down to *timeUnit*
| CEIL(datetime TO timeUnit) | Rounds *datetime* up to *timeUnit*
| YEAR(date)                | Equivalent to `EXTRACT(YEAR FROM date)`. Returns an integer.
| QUARTER(date)             | Equivalent to `EXTRACT(QUARTER FROM date)`. Returns an integer between 1 and 4.
| MONTH(date)               | Equivalent to `EXTRACT(MONTH FROM date)`. Returns an integer between 1 and 12.
| WEEK(date)                | Equivalent to `EXTRACT(WEEK FROM date)`. Returns an integer between 1 and 53.
| DAYOFYEAR(date)           | Equivalent to `EXTRACT(DOY FROM date)`. Returns an integer between 1 and 366.
| DAYOFMONTH(date)          | Equivalent to `EXTRACT(DAY FROM date)`. Returns an integer between 1 and 31.
| DAYOFWEEK(date)           | Equivalent to `EXTRACT(DOW FROM date)`. Returns an integer between 1 and 7.
| HOUR(date)                | Equivalent to `EXTRACT(HOUR FROM date)`. Returns an integer between 0 and 23.
| MINUTE(date)              | Equivalent to `EXTRACT(MINUTE FROM date)`. Returns an integer between 0 and 59.
| SECOND(date)              | Equivalent to `EXTRACT(SECOND FROM date)`. Returns an integer between 0 and 59.
| TIMESTAMPADD(timeUnit, integer, datetime) | Returns *datetime* with an interval of (signed) *integer* *timeUnit*s added. Equivalent to `datetime + INTERVAL 'integer' timeUnit`
| TIMESTAMPDIFF(timeUnit, datetime, datetime2) | Returns the (signed) number of *timeUnit* intervals between *datetime* and *datetime2*. Equivalent to `(datetime2 - datetime) timeUnit`
| LAST_DAY(date)            | Returns the date of the last day of the month in a value of datatype DATE; For example, it returns DATE'2020-02-29' for both DATE'2020-02-10' and TIMESTAMP'2020-02-10 10:10:10'

Calls to niladic functions such as `CURRENT_DATE` do not accept parentheses in
standard SQL. Calls with parentheses, such as `CURRENT_DATE()` are accepted in certain
[conformance levels]({{ site.apiRoot }}/org/apache/calcite/sql/validate/SqlConformance.html#allowNiladicParentheses--).

Not implemented:

* CEIL(interval)
* FLOOR(interval)
* \+ interval
* \- interval
* interval + interval
* interval - interval
* interval / interval

### System functions

| Operator syntax | Description
|:--------------- |:-----------
| USER            | Equivalent to CURRENT_USER
| CURRENT_USER    | User name of current execution context
| SESSION_USER    | Session user name
| SYSTEM_USER     | Returns the name of the current data store user as identified by the operating system
| CURRENT_PATH    | Returns a character string representing the current lookup scope for references to user-defined routines and types
| CURRENT_ROLE    | Returns the current active role
| CURRENT_SCHEMA  | Returns the current schema

### Conditional functions and operators

| Operator syntax | Description
|:--------------- |:-----------
| CASE value<br/>WHEN value1 [, value11 ]* THEN result1<br/>[ WHEN valueN [, valueN1 ]* THEN resultN ]*<br/>[ ELSE resultZ ]<br/> END | Simple case
| CASE<br/>WHEN condition1 THEN result1<br/>[ WHEN conditionN THEN resultN ]*<br/>[ ELSE resultZ ]<br/>END | Searched case
| NULLIF(value, value) | Returns NULL if the values are the same.<br/><br/>For example, <code>NULLIF(5, 5)</code> returns NULL; <code>NULLIF(5, 0)</code> returns 5.
| COALESCE(value, value [, value ]*) | Provides a value if the first value is null.<br/><br/>For example, <code>COALESCE(NULL, 5)</code> returns 5.

### Type conversion

Generally an expression cannot contain values of different datatypes. For example, an expression cannot multiply 5 by 10 and then add 'JULIAN'.
However, Calcite supports both implicit and explicit conversion of values from one datatype to another.

#### Implicit and Explicit Type Conversion
Calcite recommends that you specify explicit conversions, rather than rely on implicit or automatic conversions, for these reasons:

* SQL statements are easier to understand when you use explicit datatype conversion functions.
* Implicit datatype conversion can have a negative impact on performance, especially if the datatype of a column value is converted to that of a constant rather than the other way around.
* Implicit conversion depends on the context in which it occurs and may not work the same way in every case. For example, implicit conversion from a datetime value to a VARCHAR value may return an unexpected format.

Algorithms for implicit conversion are subject to change across Calcite releases. Behavior of explicit conversions is more predictable.

#### Explicit Type Conversion

| Operator syntax                         | Description
|:----------------------------------------| :----------
| CAST(value AS type)                     | Converts a value to a given type. Casts between integer types truncate towards 0
| CONVERT(string, charSet1, charSet2)     | Converts *string* from *charSet1* to *charSet2*
| CONVERT(value USING transcodingName)    | Alter *value* from one base character set to *transcodingName*
| TRANSLATE(value USING transcodingName)  | Alter *value* from one base character set to *transcodingName*
| TYPEOF(variant)                        | Returns a string that describes the runtime type of *variant*, where variant has a `VARIANT` type
| VARIANTNULL()                          | Returns an instance of the `VARIANT` null value (constructor)

Converting a string to a **BINARY** or **VARBINARY** type produces the
list of bytes of the string's encoding in the strings' charset.  A
runtime error is produced if the string's characters cannot be
represented using its charset.

Supported data types syntax:

{% highlight sql %}
type:
      typeName
      [ collectionsTypeName ]*

typeName:
      sqlTypeName
  |   rowTypeName
  |   mapTypeName
  |   compoundIdentifier

mapTypeName:
  MAP '<' type ',' type '>'

sqlTypeName:
      char [ precision ] [ charSet ]
  |   varchar [ precision ] [ charSet ]
  |   DATE
  |   time
  |   timestamp
  |   GEOMETRY
  |   decimal [ precision [, scale] ]
  |   BOOLEAN
  |   integer
  |   BINARY [ precision ]
  |   varbinary [ precision ]
  |   TINYINT [ UNSIGNED ]
  |   SMALLINT [ UNSIGNED ]
  |   BIGINT [ UNSIGNED ]
  |   REAL
  |   double
  |   FLOAT
  |   ANY [ precision [, scale] ]

collectionsTypeName:
      ARRAY | MULTISET

rowTypeName:
      ROW '('
      fieldName1 fieldType1 [ NULL | NOT NULL ]
      [ , fieldName2 fieldType2 [ NULL | NOT NULL ] ]*
      ')'

char:
      CHARACTER | CHAR

varchar:
      char VARYING | VARCHAR

decimal:
      DECIMAL | DEC | NUMERIC

integer:
      INTEGER [ UNSIGNED ] | INT [ UNSIGNED ] | UNSIGNED

varbinary:
      BINARY VARYING | VARBINARY

double:
      DOUBLE [ PRECISION ]

time:
      TIME [ precision ] [ timeZone ]

timestamp:
      TIMESTAMP [ precision ] [ timeZone ]

charSet:
      CHARACTER SET charSetName

timeZone:
      WITHOUT TIME ZONE
  |   WITH LOCAL TIME ZONE
{% endhighlight %}

#### Implicit Type Conversion

Calcite automatically converts a value from one datatype to another
when such a conversion makes sense. The table below is a matrix of
Calcite type conversions. The table shows all possible conversions,
without regard to the context in which it is made. The rules governing
these details follow the table.

| FROM - TO           | NULL | BOOLEAN | TINYINT | SMALLINT | INT | BIGINT | TINYINT UNSIGNED | SMALLINT UNSIGNED | INT UNSIGNED | BIGINT UNSIGNED | DECIMAL | FLOAT or REAL | DOUBLE | INTERVAL | DATE | TIME | TIMESTAMP | CHAR or VARCHAR | BINARY or VARBINARY | GEOMETRY | ARRAY | MAP | MULTISET | ROW | UUID |
|:--------------------|:-----|:--------|:--------|:---------|:----|:-------|:-----------------|:------------------|:-------------|:----------------|:--------|:--------------|:-------|:---------|:-----|:-----|:----------|:----------------|:--------------------|:---------|:------|:----|:---------|:----|:-----|
| NULL                | i    | i       | i       | i        | i   | i      | i                | i                 | i            | i               | i       | i             | i      | i        | i    | i    | i         | i               | i                   | i        | x     | x   | x        | x   | i    |
| BOOLEAN             | x    | i       | x       | x        | x   | x      | x                | x                 | x            | x               | x       | x             | x      | x        | x    | x    | x         | i               | x                   | x        | x     | x   | x        | x   | x    |
| TINYINT             | x    | e       | i       | i        | i   | i      | i                | i                 | i            | i               | i       | i             | i      | e        | x    | x    | e         | i               | x                   | x        | x     | x   | x        | x   | x    |
| SMALLINT            | x    | e       | i       | i        | i   | i      | i                | i                 | i            | i               | i       | i             | i      | e        | x    | x    | e         | i               | x                   | x        | x     | x   | x        | x   | x    |
| INT                 | x    | e       | i       | i        | i   | i      | i                | i                 | i            | i               | i       | i             | i      | e        | x    | x    | e         | i               | x                   | x        | x     | x   | x        | x   | x    |
| BIGINT              | x    | e       | i       | i        | i   | i      | i                | i                 | i            | i               | i       | i             | i      | e        | x    | x    | e         | i               | x                   | x        | x     | x   | x        | x   | x    |
| TINYINT UNSIGNED    | x    | e       | i       | i        | i   | i      | i                | i                 | i            | i               | i       | i             | i      | e        | x    | x    | e         | i               | x                   | x        | x     | x   | x        | x   | x    |
| SMALLINT UNSIGNED   | x    | e       | i       | i        | i   | i      | i                | i                 | i            | i               | i       | i             | i      | e        | x    | x    | e         | i               | x                   | x        | x     | x   | x        | x   | x    |
| INT UNSIGNED        | x    | e       | i       | i        | i   | i      | i                | i                 | i            | i               | i       | i             | i      | e        | x    | x    | e         | i               | x                   | x        | x     | x   | x        | x   | x    |
| BIGINT UNSIGNED     | x    | e       | i       | i        | i   | i      | i                | i                 | i            | i               | i       | i             | i      | e        | x    | x    | e         | i               | x                   | x        | x     | x   | x        | x   | x    |
| DECIMAL             | x    | e       | i       | i        | i   | i      | i                | i                 | i            | i               | i       | i             | i      | e        | x    | x    | e         | i               | x                   | x        | x     | x   | x        | x   | x    |
| FLOAT/REAL          | x    | e       | i       | i        | i   | i      | i                | i                 | i            | i               | i       | i             | i      | x        | x    | x    | e         | i               | x                   | x        | x     | x   | x        | x   | x    |
| DOUBLE              | x    | e       | i       | i        | i   | i      | i                | i                 | i            | i               | i       | i             | i      | x        | x    | x    | e         | i               | x                   | x        | x     | x   | x        | x   | x    |
| INTERVAL            | x    | x       | e       | e        | e   | e      | e                | e                 | e            | e               | e       | x             | x      | i        | x    | x    | x         | e               | x                   | x        | x     | x   | x        | x   | x    |
| DATE                | x    | x       | x       | x        | x   | x      | x                | x                 | x            | x               | x       | x             | x      | x        | i    | x    | i         | i               | x                   | x        | x     | x   | x        | x   | x    |
| TIME                | x    | x       | x       | x        | x   | x      | x                | x                 | x            | x               | x       | x             | x      | x        | x    | i    | e         | i               | x                   | x        | x     | x   | x        | x   | x    |
| TIMESTAMP           | x    | x       | e       | e        | e   | e      | e                | e                 | e            | e               | e       | e             | e      | x        | i    | e    | i         | i               | x                   | x        | x     | x   | x        | x   | x    |
| CHAR or VARCHAR     | x    | e       | i       | i        | i   | i      | i                | i                 | i            | i               | i       | i             | i      | i        | i    | i    | i         | i               | i                   | i        | i     | i   | i        | i   | i    |
| BINARY or VARBINARY | x    | x       | x       | x        | x   | x      | x                | x                 | x            | x               | x       | x             | x      | x        | e    | e    | e         | i               | i                   | x        | x     | x   | x        | x   | i    |
| GEOMETRY            | x    | x       | x       | x        | x   | x      | x                | x                 | x            | x               | x       | x             | x      | x        | x    | x    | x         | i               | x                   | i        | x     | x   | x        | x   | x    |
| ARRAY               | x    | x       | x       | x        | x   | x      | x                | x                 | x            | x               | x       | x             | x      | x        | x    | x    | x         | x               | x                   | x        | i     | x   | x        | x   | x    |
| MAP                 | x    | x       | x       | x        | x   | x      | x                | x                 | x            | x               | x       | x             | x      | x        | x    | x    | x         | x               | x                   | x        | x     | i   | x        | x   | x    |
| MULTISET            | x    | x       | x       | x        | x   | x      | x                | x                 | x            | x               | x       | x             | x      | x        | x    | x    | x         | x               | x                   | x        | x     | x   | i        | x   | x    |
| ROW                 | x    | x       | x       | x        | x   | x      | x                | x                 | x            | x               | x       | x             | x      | x        | x    | x    | x         | x               | x                   | x        | x     | x   | x        | i   | x    |
| UUID                | x    | x       | x       | x        | x   | x      | x                | x                 | x            | x               | x       | x             | x      | x        | x    | x    | x         | x               | x                   | x        | x     | x   | x        | x   | i    |

i: implicit cast / e: explicit cast / x: not allowed

##### Conversion Contexts and Strategies

* Set operation (`UNION`, `EXCEPT`, `INTERSECT`): compare every branch
  row data type and find the common type of each fields pair;
* Arithmetic operations combining signed and unsigned values will
  produce a result with the wider type; if both types have the same width,
  the result is unsigned;
* Binary arithmetic expression (`+`, `-`, `&`, `^`, `/`, `%`): promote
  string operand to data type of the other numeric operand;
* Binary comparison (`=`, `<`, `<=`, `<>`, `>`, `>=`):
  if operands are `STRING` and `TIMESTAMP`, promote to `TIMESTAMP`;
  make `1 = true` and `0 = false` always evaluate to `TRUE`;
  if there is numeric type operand, find common type for both operands.
* `IN` sub-query: compare type of LHS and RHS, and find the common type;
  if it is struct type, find wider type for every field;
* `IN` expression list: compare every expression to find the common type;
* `CASE WHEN` expression or `COALESCE`: find the common wider type of the `THEN`
  and `ELSE` operands;
* Character + `INTERVAL` or character - `INTERVAL`: promote character to
  `TIMESTAMP`;
* Built-in function: look up the type families registered in the checker,
  find the family default type if checker rules allow it;
* User-defined function (UDF): coerce based on the declared argument types
  of the `eval()` method;
* `INSERT` and `UPDATE`: coerce a source field to counterpart target table
  field's type if the two fields differ with type name or precision(scale).

Note:

Implicit type coercion of following cases are ignored:

* One of the type is `ANY`;
* Type coercion within `CHARACTER` types are always ignored,
  i.e. from `CHAR(20)` to `VARCHAR(30)`;
* Type coercion from a numeric to another with higher precedence is ignored,
  i.e. from `INT` to `LONG`.

##### Strategies for Finding Common Type

* If the operator has expected data types, just take them as the
  desired one. (e.g. the UDF would have `eval()` method which has
  reflection argument types);
* If there is no expected data type but the data type families are
  registered, try to coerce the arguments to the family's default data
  type, i.e. the String family will have a `VARCHAR` type;
* If neither expected data type nor families are specified, try to
  find the tightest common type of the node types, i.e. `INTEGER` and
  `DOUBLE` will return `DOUBLE`, the numeric precision does not lose
  for this case;
* If no tightest common type is found, try to find a wider type,
  i.e. `VARCHAR` and `INTEGER` will return `INTEGER`,
  we allow some precision loss when widening decimal to fractional,
  or promote to `VARCHAR` type.

### Value constructors

| Operator syntax | Description
|:--------------- |:-----------
| ROW (value [, value ]*)  | Creates a row from a list of values.
| (value [, value ]* )     | Creates a row from a list of values.
| row '[' index ']'        | Returns the element at a particular location in a row (1-based index).
| row '[' name ']'         | Returns the element of a row with a particular name.
| map '[' key ']'     | Returns the element of a map with a particular key.
| array '[' index ']' | Returns the element at a particular location in an array (1-based index).
| ARRAY '[' value [, value ]* ']' | Creates an array from a list of values.
| MAP '[' key, value [, key, value ]* ']' | Creates a map from a list of key-value pairs.

### Value constructors by query

| Operator syntax                    | Description |
|:-----------------------------------|:------------|
| ARRAY (sub-query)                  | Creates an array from the result of a sub-query. Example: `ARRAY(SELECT empno FROM emp ORDER BY empno)` |
| MAP (sub-query)                    | Creates a map from the result of a key-value pair sub-query. Example: `MAP(SELECT empno, deptno FROM emp)` |
| MULTISET (sub-query)               | Creates a multiset from the result of a sub-query. Example: `MULTISET(SELECT empno FROM emp)` |

### Collection functions

| Operator syntax | Description
|:--------------- |:-----------
| ELEMENT(value)  | Returns the sole element of an array or multiset; null if the collection is empty; throws if it has more than one element.
| CARDINALITY(value) | Returns the number of elements in an array or multiset.
| value MEMBER OF multiset | Returns whether the *value* is a member of *multiset*.
| multiset IS A SET | Whether *multiset* is a set (has no duplicates).
| multiset IS NOT A SET | Whether *multiset* is not a set (has duplicates).
| multiset IS EMPTY | Whether *multiset* contains zero elements.
| multiset IS NOT EMPTY | Whether *multiset* contains one or more elements.
| multiset SUBMULTISET OF multiset2 | Whether *multiset* is a submultiset of *multiset2*.
| multiset NOT SUBMULTISET OF multiset2 | Whether *multiset* is not a submultiset of *multiset2*.
| multiset MULTISET UNION [ ALL &#124; DISTINCT ] multiset2 | Returns the union *multiset* and *multiset2*, eliminating duplicates if DISTINCT is specified (ALL is the default).
| multiset MULTISET INTERSECT [ ALL &#124; DISTINCT ] multiset2 | Returns the intersection of *multiset* and *multiset2*, eliminating duplicates if DISTINCT is specified (ALL is the default).
| multiset MULTISET EXCEPT [ ALL &#124; DISTINCT ] multiset2 | Returns the difference of *multiset* and *multiset2*, eliminating duplicates if DISTINCT is specified (ALL is the default).

See also: the UNNEST relational operator converts a collection to a relation.

### Period predicates

<table>
  <tr>
    <th>Operator syntax</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>period1 CONTAINS datetime</td>
    <td>
      <div class="container">
        <div class="gray"><div class="r15"></div><div class="r2"></div></div>
      </div>
    </td>
  </tr>
  <tr>
    <td>period1 CONTAINS period2</td>
    <td>
      <div class="container">
        <div class="gray"><div class="r15"></div><div class="r24"></div></div>
        <div class="gray"><div class="r15"></div><div class="r13"></div></div>
        <div class="gray"><div class="r15"></div><div class="r35"></div></div>
        <div class="gray"><div class="r15"></div><div class="r15"></div></div>
      </div>
    </td>
  </tr>
  <tr>
    <td>period1 OVERLAPS period2</td>
    <td>
      <div class="container">
        <div class="gray"><div class="r15"></div><div class="r24"></div></div>
        <div class="gray"><div class="r15"></div><div class="r13"></div></div>
        <div class="gray"><div class="r15"></div><div class="r35"></div></div>
        <div class="gray"><div class="r15"></div><div class="r15"></div></div>
        <div class="gray"><div class="r24"></div><div class="r15"></div></div>
        <div class="gray"><div class="r13"></div><div class="r15"></div></div>
        <div class="gray"><div class="r35"></div><div class="r15"></div></div>
        <div class="gray"><div class="r24"></div><div class="r13"></div></div>
        <div class="gray"><div class="r13"></div><div class="r24"></div></div>
      </div>
    </td>
  </tr>
  <tr>
    <td>period1 EQUALS period2</td>
    <td>
      <div class="container">
        <div class="gray"><div class="r15"></div><div class="r15"></div></div>
      </div>
    </td>
  </tr>
  <tr>
    <td>period1 PRECEDES period2</td>
    <td>
      <div class="container">
        <div class="gray"><div class="r12"></div><div class="r35"></div></div>
        <div class="gray"><div class="r13"></div><div class="r35"></div></div>
      </div>
    </td>
  </tr>
  <tr>
    <td>period1 IMMEDIATELY PRECEDES period2</td>
    <td>
      <div class="container">
        <div class="gray"><div class="r13"></div><div class="r35"></div></div>
      </div>
    </td>
  </tr>
  <tr>
    <td>period1 SUCCEEDS period2</td>
    <td>
      <div class="container">
        <div class="gray"><div class="r35"></div><div class="r12"></div></div>
        <div class="gray"><div class="r35"></div><div class="r13"></div></div>
      </div>
    </td>
  </tr>
  <tr>
    <td>period1 IMMEDIATELY SUCCEEDS period2</td>
    <td>
      <div class="container">
        <div class="gray"><div class="r35"></div><div class="r13"></div></div>
      </div>
    </td>
  </tr>
</table>

Where *period1* and *period2* are period expressions:

{% highlight sql %}
period:
      (datetime, datetime)
  |   (datetime, interval)
  |   PERIOD (datetime, datetime)
  |   PERIOD (datetime, interval)
{% endhighlight %}

### JDBC function escape

#### Numeric

| Operator syntax                   | Description
|:--------------------------------- |:-----------
| {fn ABS(numeric)}                 | Returns the absolute value of *numeric*
| {fn ACOS(numeric)}                | Returns the arc cosine of *numeric*
| {fn ASIN(numeric)}                | Returns the arc sine of *numeric*
| {fn ATAN(numeric)}                | Returns the arc tangent of *numeric*
| {fn ATAN2(numeric, numeric)}      | Returns the arc tangent of the *numeric* coordinates
| {fn CBRT(numeric)}                | Returns the cube root of *numeric*
| {fn CEILING(numeric)}             | Rounds *numeric* up, and returns the smallest number that is greater than or equal to *numeric*
| {fn COS(numeric)}                 | Returns the cosine of *numeric*
| {fn COT(numeric)}                 | Returns the cotangent of *numeric*
| {fn DEGREES(numeric)}             | Converts *numeric* from radians to degrees
| {fn EXP(numeric)}                 | Returns *e* raised to the power of *numeric*
| {fn FLOOR(numeric)}               | Rounds *numeric* down, and returns the largest number that is less than or equal to *numeric*
| {fn LOG(numeric)}                 | Returns the natural logarithm (base *e*) of *numeric*
| {fn LOG10(numeric)}               | Returns the base-10 logarithm of *numeric*
| {fn MOD(numeric1, numeric2)}      | Returns the remainder (modulus) of *numeric1* divided by *numeric2*. The result is negative only if *numeric1* is negative
| {fn PI()}                         | Returns a value that is closer than any other value to *pi*
| {fn POWER(numeric1, numeric2)}    | Returns *numeric1* raised to the power of *numeric2*
| {fn RADIANS(numeric)}             | Converts *numeric* from degrees to radians
| {fn RAND(numeric)}                | Returns a random double using *numeric* as the seed value
| {fn ROUND(numeric1, integer2)}    | Rounds *numeric1* to *integer2* places right to the decimal point
| {fn SIGN(numeric)}                | Returns the signum of *numeric*
| {fn SIN(numeric)}                 | Returns the sine of *numeric*
| {fn SQRT(numeric)}                | Returns the square root of *numeric*
| {fn TAN(numeric)}                 | Returns the tangent of *numeric*
| {fn TRUNCATE(numeric1, integer2)} | Truncates *numeric1* to *integer2* places right to the decimal point

#### String

| Operator syntax | Description
|:--------------- |:-----------
| {fn ASCII(string)} | Returns the ASCII code of the first character of *string*; if the first character is a non-ASCII character, returns its Unicode code point; returns 0 if *string* is empty
| {fn CHAR(integer)} | Returns the character whose ASCII code is *integer* % 256, or null if *integer* &lt; 0
| {fn CONCAT(character, character)} | Returns the concatenation of character strings
| {fn INSERT(string1, start, length, string2)} | Inserts *string2* into a slot in *string1*
| {fn LCASE(string)} | Returns a string in which all alphabetic characters in *string* have been converted to lower case
| {fn LENGTH(string)} | Returns the number of characters in a string
| {fn LOCATE(string1, string2 [, integer])} | Returns the position in *string2* of the first occurrence of *string1*. Searches from the beginning of *string2*, unless *integer* is specified.
| {fn LEFT(string, length)} | Returns the leftmost *length* characters from *string*
| {fn LTRIM(string)} | Returns *string* with leading space characters removed
| {fn REPLACE(string, search, replacement)} | Returns a string in which all the occurrences of *search* in *string* are replaced with *replacement*; returns unchanged *string* if *search* is an empty string(''); if *replacement* is the empty string, the occurrences of *search* are removed. Matching between *search* and *string* is case-insensitive under SQL Server semantics
| {fn REVERSE(string)} | Returns *string* with the order of the characters reversed
| {fn RIGHT(string, length)} | Returns the rightmost *length* characters from *string*
| {fn RTRIM(string)} | Returns *string* with trailing space characters removed
| {fn SUBSTRING(string, offset, length)} | Returns a character string that consists of *length* characters from *string* starting at the *offset* position
| {fn UCASE(string)} | Returns a string in which all alphabetic characters in *string* have been converted to upper case

#### Date/time

| Operator syntax | Description
|:--------------- |:-----------
| {fn CURDATE()}  | Equivalent to `CURRENT_DATE`
| {fn CURTIME()}  | Equivalent to `LOCALTIME`
| {fn NOW()}      | Equivalent to `LOCALTIMESTAMP`
| {fn YEAR(date)} | Equivalent to `EXTRACT(YEAR FROM date)`. Returns an integer.
| {fn QUARTER(date)} | Equivalent to `EXTRACT(QUARTER FROM date)`. Returns an integer between 1 and 4.
| {fn MONTH(date)} | Equivalent to `EXTRACT(MONTH FROM date)`. Returns an integer between 1 and 12.
| {fn WEEK(date)} | Equivalent to `EXTRACT(WEEK FROM date)`. Returns an integer between 1 and 53.
| {fn DAYOFYEAR(date)} | Equivalent to `EXTRACT(DOY FROM date)`. Returns an integer between 1 and 366.
| {fn DAYOFMONTH(date)} | Equivalent to `EXTRACT(DAY FROM date)`. Returns an integer between 1 and 31.
| {fn DAYOFWEEK(date)} | Equivalent to `EXTRACT(DOW FROM date)`. Returns an integer between 1 and 7.
| {fn HOUR(date)} | Equivalent to `EXTRACT(HOUR FROM date)`. Returns an integer between 0 and 23.
| {fn MINUTE(date)} | Equivalent to `EXTRACT(MINUTE FROM date)`. Returns an integer between 0 and 59.
| {fn SECOND(date)} | Equivalent to `EXTRACT(SECOND FROM date)`. Returns an integer between 0 and 59.
| {fn TIMESTAMPADD(timeUnit, count, datetime)} | Adds an interval of *count* *timeUnit*s to a datetime
| {fn TIMESTAMPDIFF(timeUnit, timestamp1, timestamp2)} | Subtracts *timestamp1* from *timestamp2* and returns the result in *timeUnit*s


#### System

| Operator syntax | Description
|:--------------- |:-----------
| {fn DATABASE()} | Equivalent to `CURRENT_CATALOG`
| {fn IFNULL(value1, value2)} | Returns value2 if value1 is null
| {fn USER()}     | Equivalent to `CURRENT_USER`

#### Conversion

| Operator syntax | Description
|:--------------- |:-----------
| {fn CONVERT(value, type)} | Cast *value* into *type*

### Aggregate functions

Syntax:

{% highlight sql %}
aggregateCall:
      agg '(' [ ALL | DISTINCT ] value [, value ]* ')'
      [ WITHIN DISTINCT '(' expression [, expression ]* ')' ]
      [ WITHIN GROUP '(' ORDER BY orderItem [, orderItem ]* ')' ]
      [ FILTER '(' WHERE condition ')' ]
  |   agg '(' '*' ')' [ FILTER (WHERE condition) ]
{% endhighlight %}

where *agg* is one of the operators in the following table, or a user-defined
aggregate function.

If `FILTER` is present, the aggregate function only considers rows for which
*condition* evaluates to TRUE.

If `DISTINCT` is present, duplicate argument values are eliminated before being
passed to the aggregate function.

If `WITHIN DISTINCT` is present, argument values are made distinct within
each value of specified keys before being passed to the aggregate function.

If `WITHIN GROUP` is present, the aggregate function sorts the input rows
according to the `ORDER BY` clause inside `WITHIN GROUP` before aggregating
values. `WITHIN GROUP` is only allowed for hypothetical set functions (`RANK`,
`DENSE_RANK`, `PERCENT_RANK` and `CUME_DIST`), inverse distribution functions
(`PERCENTILE_CONT` and `PERCENTILE_DISC`) and collection functions (`COLLECT`
and `LISTAGG`).

| Operator syntax                    | Description
|:---------------------------------- |:-----------
| ANY_VALUE( [ ALL &#124; DISTINCT ] value)     | Returns one of the values of *value* across all input values; this is NOT specified in the SQL standard
| ARG_MAX(value, comp)                          | Returns *value* for the maximum value of *comp* in the group
| ARG_MIN(value, comp)                          | Returns *value* for the minimum value of *comp* in the group
| APPROX_COUNT_DISTINCT(value [, value ]*)      | Returns the approximate number of distinct values of *value*; the database is allowed to use an approximation but is not required to
| AVG( [ ALL &#124; DISTINCT ] numeric)         | Returns the average (arithmetic mean) of *numeric* across all input values
| BIT_AND( [ ALL &#124; DISTINCT ] value)       | Returns the bitwise AND of all non-null input values, or null if none; integer and binary types are supported
| BIT_OR( [ ALL &#124; DISTINCT ] value)        | Returns the bitwise OR of all non-null input values, or null if none; integer and binary types are supported
| BIT_XOR( [ ALL &#124; DISTINCT ] value)       | Returns the bitwise XOR of all non-null input values, or null if none; integer and binary types are supported
| COLLECT( [ ALL &#124; DISTINCT ] value)       | Returns a multiset of the values
| COUNT(*)                                      | Returns the number of input rows
| COUNT( [ ALL &#124; DISTINCT ] value [, value ]*) | Returns the number of input rows for which *value* is not null (wholly not null if *value* is composite)
| COVAR_POP(numeric1, numeric2)                 | Returns the population covariance of the pair (*numeric1*, *numeric2*) across all input values
| COVAR_SAMP(numeric1, numeric2)                | Returns the sample covariance of the pair (*numeric1*, *numeric2*) across all input values
| EVERY(condition)                              | Returns TRUE if all of the values of *condition* are TRUE
| FUSION(multiset)                              | Returns the multiset union of *multiset* across all input values
| INTERSECTION(multiset)                        | Returns the multiset intersection of *multiset* across all input values
| LISTAGG( [ ALL &#124; DISTINCT ] value [, separator]) | Returns values concatenated into a string, delimited by separator (default ',')
| MAX( [ ALL &#124; DISTINCT ] value)           | Returns the maximum value of *value* across all input values
| MIN( [ ALL &#124; DISTINCT ] value)           | Returns the minimum value of *value* across all input values
| MODE(value)                                   | Returns the most frequent value of *value* across all input values
| REGR_COUNT(numeric1, numeric2)                | Returns the number of rows where both dependent and independent expressions are not null
| REGR_SXX(numeric1, numeric2)                  | Returns the sum of squares of the dependent expression in a linear regression model
| REGR_SYY(numeric1, numeric2)                  | Returns the sum of squares of the independent expression in a linear regression model
| SOME(condition)                               | Returns TRUE if one or more of the values of *condition* is TRUE
| STDDEV( [ ALL &#124; DISTINCT ] numeric)      | Synonym for `STDDEV_SAMP`
| STDDEV_POP( [ ALL &#124; DISTINCT ] numeric)  | Returns the population standard deviation of *numeric* across all input values
| STDDEV_SAMP( [ ALL &#124; DISTINCT ] numeric) | Returns the sample standard deviation of *numeric* across all input values
| SUM( [ ALL &#124; DISTINCT ] numeric)         | Returns the sum of *numeric* across all input values
| VAR_POP( [ ALL &#124; DISTINCT ] value)       | Returns the population variance (square of the population standard deviation) of *numeric* across all input values
| VAR_SAMP( [ ALL &#124; DISTINCT ] numeric)    | Returns the sample variance (square of the sample standard deviation) of *numeric* across all input values

Not implemented:

* REGR_AVGX(numeric1, numeric2)
* REGR_AVGY(numeric1, numeric2)
* REGR_INTERCEPT(numeric1, numeric2)
* REGR_R2(numeric1, numeric2)
* REGR_SLOPE(numeric1, numeric2)
* REGR_SXY(numeric1, numeric2)

#### Ordered-Set Aggregate Functions

The syntax is as for *aggregateCall*, except that `WITHIN GROUP` is
required.

In the following:

* *fraction* is a numeric literal between 0 and 1, inclusive, and
  represents a percentage

| Operator syntax                    | Description
|:---------------------------------- |:-----------
| PERCENTILE_CONT(fraction) WITHIN GROUP (ORDER BY orderItem) | Returns a percentile based on a continuous distribution of the column values, interpolating between adjacent input items if needed
| PERCENTILE_DISC(fraction) WITHIN GROUP (ORDER BY orderItem [, orderItem ]*) | Returns a percentile based on a discrete distribution of the column values returning the first input value whose position in the ordering equals or exceeds the specified fraction

### Window functions

Syntax:

{% highlight sql %}
windowedAggregateCall:
      agg '(' [ ALL | DISTINCT ] value [, value ]* ')'
      [ RESPECT NULLS | IGNORE NULLS ]
      [ WITHIN GROUP '(' ORDER BY orderItem [, orderItem ]* ')' ]
      OVER window
  |   agg '(' '*' ')'
      OVER window
{% endhighlight %}

where *agg* is one of the operators in the following table, or a user-defined
aggregate function.

The *exclude* clause can be one of:
- EXCLUDE NO OTHER: Does not exclude any row from the frame. This is the default.
- EXCLUDE CURRENT ROW: Exclude the current row from the frame.
- EXCLUDE GROUP: Exclude the current row and its ordering peers from the frame.
- EXCLUDE TIES: Exclude all the ordering peers of the current row, but not the current row itself.

`DISTINCT`, `FILTER` and `WITHIN GROUP` are as described for aggregate
functions.

| Operator syntax                           | Description
|:----------------------------------------- |:-----------
| COUNT(value [, value ]*) OVER window      | Returns the number of rows in *window* for which *value* is not null (wholly not null if *value* is composite)
| COUNT(*) OVER window                      | Returns the number of rows in *window*
| AVG(numeric) OVER window                  | Returns the average (arithmetic mean) of *numeric* across all values in *window*
| SUM(numeric) OVER window                  | Returns the sum of *numeric* across all values in *window*
| MAX(value) OVER window                    | Returns the maximum value of *value* across all values in *window*
| MIN(value) OVER window                    | Returns the minimum value of *value* across all values in *window*
| RANK() OVER window                        | Returns the rank of the current row with gaps; same as ROW_NUMBER of its first peer
| DENSE_RANK() OVER window                  | Returns the rank of the current row without gaps; this function counts peer groups
| ROW_NUMBER() OVER window                  | Returns the number of the current row within its partition, counting from 1
| FIRST_VALUE(value) OVER window            | Returns *value* evaluated at the row that is the first row of the window frame
| LAST_VALUE(value) OVER window             | Returns *value* evaluated at the row that is the last row of the window frame
| LEAD(value, offset, default) OVER window  | Returns *value* evaluated at the row that is *offset* rows after the current row within the partition; if there is no such row, instead returns *default*. Both *offset* and *default* are evaluated with respect to the current row. If omitted, *offset* defaults to 1 and *default* to NULL
| LAG(value, offset, default) OVER window   | Returns *value* evaluated at the row that is *offset* rows before the current row within the partition; if there is no such row, instead returns *default*. Both *offset* and *default* are evaluated with respect to the current row. If omitted, *offset* defaults to 1 and *default* to NULL
| NTH_VALUE(value, nth) OVER window         | Returns *value* evaluated at the row that is the *n*th row of the window frame
| NTILE(value) OVER window                  | Returns an integer ranging from 1 to *value*, dividing the partition as equally as possible

Note:

* You may specify null treatment (`IGNORE NULLS`, `RESPECT NULLS`) for
  `FIRST_VALUE`, `LAST_VALUE`, `NTH_VALUE`, `LEAD` and `LAG` functions. The
  syntax handled by the parser, but only `RESPECT NULLS` is implemented at
  runtime.

Not implemented:

* COUNT(DISTINCT value [, value ]*) OVER window
* APPROX_COUNT_DISTINCT(value [, value ]*) OVER window
* PERCENT_RANK(value) OVER window
* CUME_DIST(value) OVER window

### Grouping functions

| Operator syntax      | Description
|:-------------------- |:-----------
| GROUPING(expression [, expression ]*) | Returns a bit vector of the given grouping expressions
| GROUP_ID()           | Returns an integer that uniquely identifies the combination of grouping keys
| GROUPING_ID(expression [, expression ]*) | Synonym for `GROUPING`

### DESCRIPTOR

| Operator syntax      | Description
|:-------------------- |:-----------
| DESCRIPTOR(name [, name ]*) | DESCRIPTOR appears as an argument in a function to indicate a list of names. The interpretation of names is left to the function.

### Table functions

Table functions occur in the `FROM` clause.

Table functions may have generic table parameters (i.e., no row type is
declared when the table function is created), and the row type of the result
 might depend on the row type(s) of the input tables.
Besides, input tables are classified by three characteristics.
The first characteristic is semantics. Input tables have either row semantics
or set semantics, as follows:
* Row semantics means that the result of the table function depends on a
row-by-row basis.
* Set semantics means that the outcome of the function depends on how the
data is partitioned.

The second characteristic, which applies only to input tables with
set semantics, is whether the table function can generate a result row
even if the input table is empty.
* If the table function can generate a result row on empty input,
the table is said to be "keep when empty".
* The alternative is called "prune when empty", meaning that
the result would be pruned out if the input table is empty.

The third characteristic is whether the input table supports
pass-through columns or not. Pass-through columns is a mechanism
enabling the table function to copy every column of an input row
into columns of an output row.

The input tables with set semantics may be partitioned on one or more columns.
The input tables with set semantics may be ordered on one or more columns.

Note:
* The input tables with row semantics may not be partitioned or ordered.
* A polymorphic table function may have multiple input tables. However,
at most one input table could have row semantics.

#### TUMBLE

In streaming queries, TUMBLE assigns a window for each row of a relation based
on a timestamp column. An assigned window is specified by its beginning and
ending. All assigned windows have the same length, and that's why tumbling
sometimes is named as "fixed windowing".
The first parameter of the TUMBLE table function is a generic table parameter.
The input table has row semantics and supports pass-through columns.

| Operator syntax      | Description
|:-------------------- |:-----------
| TUMBLE(data, DESCRIPTOR(timecol), size [, offset ]) | Indicates a tumbling window of *size* interval for *timecol*, optionally aligned at *offset*.

Here is an example:

{% highlight sql %}
SELECT * FROM TABLE(
  TUMBLE(
    TABLE orders,
    DESCRIPTOR(rowtime),
    INTERVAL '1' MINUTE));

-- or with the named params
-- note: the DATA param must be the first
SELECT * FROM TABLE(
  TUMBLE(
    DATA => TABLE orders,
    TIMECOL => DESCRIPTOR(rowtime),
    SIZE => INTERVAL '1' MINUTE));
{% endhighlight %}

applies a tumbling window with a one minute range to rows from the `orders`
table. `rowtime` is the watermarked column of the `orders` table that informs
whether data is complete.

#### HOP

In streaming queries, HOP assigns windows that cover rows within the interval of *size* and shifting every *slide* based
on a timestamp column. Windows assigned could have overlapping so hopping sometime is named as "sliding windowing".
The first parameter of the HOP table function is a generic table parameter.
The input table has row semantics and supports pass-through columns.

| Operator syntax      | Description
|:-------------------- |:-----------
| HOP(data, DESCRIPTOR(timecol), slide, size [, offset ]) | Indicates a hopping window for *timecol*, covering rows within the interval of *size*, shifting every *slide* and optionally aligned at *offset*.

Here is an example:

{% highlight sql %}
SELECT * FROM TABLE(
  HOP(
    TABLE orders,
    DESCRIPTOR(rowtime),
    INTERVAL '2' MINUTE,
    INTERVAL '5' MINUTE));

-- or with the named params
-- note: the DATA param must be the first
SELECT * FROM TABLE(
  HOP(
    DATA => TABLE orders,
    TIMECOL => DESCRIPTOR(rowtime),
    SLIDE => INTERVAL '2' MINUTE,
    SIZE => INTERVAL '5' MINUTE));
{% endhighlight %}

applies hopping with 5-minute interval size on rows from table `orders`
and shifting every 2 minutes. `rowtime` is the watermarked column of table
orders that tells data completeness.

#### SESSION

In streaming queries, SESSION assigns windows that cover rows based on *datetime*. Within a session window, distances
of rows are less than *interval*. Session window is applied per *key*.
The first parameter of the SESSION table function is a generic table parameter.
The input table has set semantics and supports pass-through columns.
Besides, the SESSION table function would not generate a result row
if the input table is empty.

| Operator syntax      | Description
|:-------------------- |:-----------
| session(data, DESCRIPTOR(timecol), DESCRIPTOR(key), size) | Indicates a session window of *size* interval for *timecol*. Session window is applied per *key*.

Here is an example:

{% highlight sql %}
SELECT * FROM TABLE(
  SESSION(
    TABLE orders PARTITION BY product,
    DESCRIPTOR(rowtime),
    INTERVAL '20' MINUTE));

-- or with the named params
-- note: the DATA param must be the first
SELECT * FROM TABLE(
  SESSION(
    DATA => TABLE orders PARTITION BY product,
    TIMECOL => DESCRIPTOR(rowtime),
    SIZE => INTERVAL '20' MINUTE));
{% endhighlight %}

applies a session with 20-minute inactive gap on rows from table `orders`.
`rowtime` is the watermarked column of table orders that tells data
completeness. Session is applied per product.

**Note**: The `Tumble`, `Hop` and `Session` window table functions assign
each row in the original table to a window. The output table has all
the same columns as the original table plus two additional columns `window_start`
and `window_end`, which represent the start and end of the window interval, respectively.

### Grouped window functions
**warning**: grouped window functions are deprecated.

Grouped window functions occur in the `GROUP BY` clause and define a key value
that represents a window containing several rows.

In some window functions, a row may belong to more than one window.
For example, if a query is grouped using
`HOP(t, INTERVAL '2' HOUR, INTERVAL '1' HOUR)`, a row with timestamp '10:15:00'
 will occur in both the 10:00 - 11:00 and 11:00 - 12:00 totals.

| Operator syntax      | Description
|:-------------------- |:-----------
| HOP(datetime, slide, size [, time ]) | Indicates a hopping window for *datetime*, covering rows within the interval of *size*, shifting every *slide*, and optionally aligned at *time*
| SESSION(datetime, interval [, time ]) | Indicates a session window of *interval* for *datetime*, optionally aligned at *time*
| TUMBLE(datetime, interval [, time ]) | Indicates a tumbling window of *interval* for *datetime*, optionally aligned at *time*

### Grouped auxiliary functions

Grouped auxiliary functions allow you to access properties of a window defined
by a grouped window function.

| Operator syntax      | Description
|:-------------------- |:-----------
| HOP_END(expression, slide, size [, time ]) | Returns the value of *expression* at the end of the window defined by a `HOP` function call
| HOP_START(expression, slide, size [, time ]) | Returns the value of *expression* at the beginning of the window defined by a `HOP` function call
| SESSION_END(expression, interval [, time]) | Returns the value of *expression* at the end of the window defined by a `SESSION` function call
| SESSION_START(expression, interval [, time]) | Returns the value of *expression* at the beginning of the window defined by a `SESSION` function call
| TUMBLE_END(expression, interval [, time ]) | Returns the value of *expression* at the end of the window defined by a `TUMBLE` function call
| TUMBLE_START(expression, interval [, time ]) | Returns the value of *expression* at the beginning of the window defined by a `TUMBLE` function call

### Spatial functions

In the following:

* *geom* is a GEOMETRY;
* *geomCollection* is a GEOMETRYCOLLECTION;
* *point* is a POINT;
* *lineString* is a LINESTRING;
* *iMatrix* is a [DE-9IM intersection matrix](https://en.wikipedia.org/wiki/DE-9IM);
* *distance*, *tolerance*, *segmentLengthFraction*, *offsetDistance* are of type double;
* *dimension*, *quadSegs*, *srid*, *zoom* are of type integer;
* *layerType* is a character string;
* *gml* is a character string containing [Geography Markup Language (GML)](https://en.wikipedia.org/wiki/Geography_Markup_Language);
* *wkt* is a character string containing [well-known text (WKT)](https://en.wikipedia.org/wiki/Well-known_text);
* *wkb* is a binary string containing [well-known binary (WKB)](https://en.wikipedia.org/wiki/Well-known_binary).

In the "C" (for "compatibility") column, "o" indicates that the function
implements the OpenGIS Simple Features Implementation Specification for SQL,
[version 1.2.1](https://www.opengeospatial.org/standards/sfs);
"p" indicates that the function is a
[PostGIS](https://www.postgis.net/docs/reference.html) extension to OpenGIS;
"h" indicates that the function is an
[H2GIS](http://www.h2gis.org/docs/dev/functions/) extension.

#### Geometry conversion functions (2D)

| C | Operator syntax      | Description
|:- |:-------------------- |:-----------
| p | ST_AsBinary(geom) | Synonym for `ST_AsWKB`
| p | ST_AsEWKB(geom) | Synonym for `ST_AsWKB`
| p | ST_AsEWKT(geom) | Converts GEOMETRY → EWKT
| p | ST_AsGeoJSON(geom) | Converts GEOMETRY → GeoJSON
| p | ST_AsGML(geom) | Converts GEOMETRY → GML
| p | ST_AsText(geom) | Synonym for `ST_AsWKT`
| o | ST_AsWKB(geom) | Converts GEOMETRY → WKB
| o | ST_AsWKT(geom) | Converts GEOMETRY → WKT
| o | ST_Force2D(geom) | 3D GEOMETRY → 2D GEOMETRY
| o | ST_GeomFromEWKB(wkb [, srid ]) | Synonym for `ST_GeomFromWKB`
| o | ST_GeomFromEWKT(wkb [, srid ]) | Converts EWKT → GEOMETRY
| o | ST_GeomFromGeoJSON(json) | Converts GeoJSON → GEOMETRY
| o | ST_GeomFromGML(wkb [, srid ]) | Converts GML → GEOMETRY
| o | ST_GeomFromText(wkt [, srid ]) | Synonym for `ST_GeomFromWKT`
| o | ST_GeomFromWKB(wkb [, srid ]) | Converts WKB → GEOMETRY
| o | ST_GeomFromWKT(wkb [, srid ]) | Converts WKT → GEOMETRY
| o | ST_LineFromText(wkt [, srid ]) | Converts WKT → LINESTRING
| o | ST_LineFromWKB(wkt [, srid ]) | Converts WKT → LINESTRING
| o | ST_MLineFromText(wkt [, srid ]) | Converts WKT → MULTILINESTRING
| o | ST_MPointFromText(wkt [, srid ]) | Converts WKT → MULTIPOINT
| o | ST_MPolyFromText(wkt [, srid ]) Converts WKT → MULTIPOLYGON
| o | ST_PointFromText(wkt [, srid ]) | Converts WKT → POINT
| o | ST_PointFromWKB(wkt [, srid ]) | Converts WKB → POINT
| o | ST_PolyFromText(wkt [, srid ]) | Converts WKT → POLYGON
| o | ST_PolyFromWKB(wkt [, srid ]) | Converts WKB → POLYGON
| p | ST_ReducePrecision(geom, gridSize) | Reduces the precision of a *geom* to the provided *gridSize*
| h | ST_ToMultiPoint(geom) | Converts the coordinates of *geom* (which may be a GEOMETRYCOLLECTION) into a MULTIPOINT
| h | ST_ToMultiLine(geom) | Converts the coordinates of *geom* (which may be a GEOMETRYCOLLECTION) into a MULTILINESTRING
| h | ST_ToMultiSegments(geom) | Converts *geom* (which may be a GEOMETRYCOLLECTION) into a set of distinct segments stored in a MULTILINESTRING

Not implemented:

* ST_GoogleMapLink(geom [, layerType [, zoom ]]) GEOMETRY → Google map link
* ST_OSMMapLink(geom [, marker ]) GEOMETRY → OSM map link

#### Geometry conversion functions (3D)

| C | Operator syntax      | Description
|:- |:-------------------- |:-----------
| o | ST_Force3D(geom) | 2D GEOMETRY → 3D GEOMETRY

#### Geometry creation functions (2D)

| C | Operator syntax      | Description
|:- |:-------------------- |:-----------
| h | ST_BoundingCircle(geom) | Returns the minimum bounding circle of *geom*
| h | ST_Expand(geom, distance) | Expands *geom*'s envelope
| h | ST_Expand(geom, deltaX, deltaY) | Expands *geom*'s envelope
| h | ST_MakeEllipse(point, width, height) | Constructs an ellipse
| p | ST_MakeEnvelope(xMin, yMin, xMax, yMax  [, srid ]) | Creates a rectangular POLYGON
| h | ST_MakeGrid(geom, deltaX, deltaY) | Calculates a regular grid of POLYGONs based on *geom*
| h | ST_MakeGridPoints(geom, deltaX, deltaY) | Calculates a regular grid of points based on *geom*
| o | ST_MakeLine(point1 [, point ]*) | Creates a line-string from the given POINTs (or MULTIPOINTs)
| p | ST_MakePoint(x, y [, z ]) | Synonym for `ST_Point`
| p | ST_MakePolygon(lineString [, hole ]*)| Creates a POLYGON from *lineString* with the given holes (which are required to be closed LINESTRINGs)
| h | ST_MinimumDiameter(geom) | Returns the minimum diameter of *geom*
| h | ST_MinimumRectangle(geom) | Returns the minimum rectangle enclosing *geom*
| h | ST_OctogonalEnvelope(geom) | Returns the octogonal envelope of *geom*
| o | ST_Point(x, y [, z ]) | Constructs a point from two or three coordinates

Not implemented:

* ST_RingBuffer(geom, distance, bufferCount [, endCapStyle [, doDifference]]) Returns a MULTIPOLYGON of buffers centered at *geom* and of increasing buffer size

### Geometry creation functions (3D)

Not implemented:

* ST_Extrude(geom, height [, flag]) Extrudes a GEOMETRY
* ST_GeometryShadow(geom, point, height) Computes the shadow footprint of *geom*
* ST_GeometryShadow(geom, azimuth, altitude, height [, unify ]) Computes the shadow footprint of *geom*

#### Geometry properties (2D)

| C | Operator syntax      | Description
|:- |:-------------------- |:-----------
| o | ST_Boundary(geom [, srid ]) | Returns the boundary of *geom*
| o | ST_Centroid(geom) | Returns the centroid of *geom*
| o | ST_CoordDim(geom) | Returns the dimension of the coordinates of *geom*
| o | ST_Dimension(geom) | Returns the dimension of *geom*
| o | ST_Distance(geom1, geom2) | Returns the distance between *geom1* and *geom2*
| h | ST_ExteriorRing(geom) | Returns the exterior ring of *geom*, or null if *geom* is not a polygon
| o | ST_GeometryType(geom) | Returns the type of *geom*
| o | ST_GeometryTypeCode(geom) | Returns the OGC SFS type code of *geom*
| p | ST_EndPoint(lineString) | Returns the last coordinate of *geom*
| o | ST_Envelope(geom [, srid ]) | Returns the envelope of *geom* (which may be a GEOMETRYCOLLECTION) as a GEOMETRY
| o | ST_Extent(geom) | Returns the minimum bounding box of *geom* (which may be a GEOMETRYCOLLECTION)
| h | ST_GeometryN(geomCollection, n) | Returns the *n*th GEOMETRY of *geomCollection*
| h | ST_InteriorRingN(geom) | Returns the nth interior ring of *geom*, or null if *geom* is not a polygon
| h | ST_IsClosed(geom) | Returns whether *geom* is a closed LINESTRING or MULTILINESTRING
| o | ST_IsEmpty(geom) | Returns whether *geom* is empty
| o | ST_IsRectangle(geom) | Returns whether *geom* is a rectangle
| h | ST_IsRing(geom) | Returns whether *geom* is a closed and simple line-string or MULTILINESTRING
| o | ST_IsSimple(geom) | Returns whether *geom* is simple
| o | ST_IsValid(geom) | Returns whether *geom* is valid
| h | ST_NPoints(geom)  | Returns the number of points in *geom*
| h | ST_NumGeometries(geom) | Returns the number of geometries in *geom* (1 if it is not a GEOMETRYCOLLECTION)
| h | ST_NumInteriorRing(geom) | Synonym for `ST_NumInteriorRings`
| h | ST_NumInteriorRings(geom) | Returns the number of interior rings of *geom*
| h | ST_NumPoints(geom) | Returns the number of points in *geom*
| p | ST_PointN(geom, n) | Returns the *n*th point of a *geom*
| p | ST_PointOnSurface(geom) | Returns an interior or boundary point of *geom*
| o | ST_SRID(geom) | Returns SRID value of *geom* or 0 if it does not have one
| p | ST_StartPoint(geom) | Returns the first point of *geom*
| o | ST_X(geom) | Returns the x-value of the first coordinate of *geom*
| o | ST_XMax(geom) | Returns the maximum x-value of *geom*
| o | ST_XMin(geom) | Returns the minimum x-value of *geom*
| o | ST_Y(geom) | Returns the y-value of the first coordinate of *geom*
| o | ST_YMax(geom) | Returns the maximum y-value of *geom*
| o | ST_YMin(geom) | Returns the minimum y-value of *geom*

Not implemented:

* ST_CompactnessRatio(polygon) Returns the square root of *polygon*'s area divided by the area of the circle with circumference equal to its perimeter
* ST_Explode(query [, fieldName]) Explodes the GEOMETRYCOLLECTIONs in the *fieldName* column of a query into multiple geometries
* ST_IsValidDetail(geom [, selfTouchValid ]) Returns a valid detail as an array of objects
* ST_IsValidReason(geom [, selfTouchValid ]) Returns text stating whether *geom* is valid, and if not valid, a reason why


#### Geometry properties (3D)

| C | Operator syntax      | Description
|:- |:-------------------- |:-----------
| p | ST_Is3D(s) | Returns whether *geom* has at least one z-coordinate
| o | ST_Z(geom) | Returns the z-value of the first coordinate of *geom*
| o | ST_ZMax(geom) | Returns the maximum z-value of *geom*
| o | ST_ZMin(geom) | Returns the minimum z-value of *geom*

### Geometry predicates

| C | Operator syntax      | Description
|:- |:-------------------- |:-----------
| o | ST_Contains(geom1, geom2) | Returns whether *geom1* contains *geom2*
| p | ST_ContainsProperly(geom1, geom2) | Returns whether *geom1* contains *geom2* but does not intersect its boundary
| p | ST_CoveredBy(geom1, geom2) | Returns whether no point in *geom1* is outside *geom2*.
| p | ST_Covers(geom1, geom2) | Returns whether no point in *geom2* is outside *geom1*
| o | ST_Crosses(geom1, geom2) | Returns whether *geom1* crosses *geom2*
| o | ST_Disjoint(geom1, geom2) | Returns whether *geom1* and *geom2* are disjoint
| p | ST_DWithin(geom1, geom2, distance) | Returns whether *geom1* and *geom* are within *distance* of one another
| o | ST_EnvelopesIntersect(geom1, geom2) | Returns whether the envelope of *geom1* intersects the envelope of *geom2*
| o | ST_Equals(geom1, geom2) | Returns whether *geom1* equals *geom2*
| o | ST_Intersects(geom1, geom2) | Returns whether *geom1* intersects *geom2*
| o | ST_Overlaps(geom1, geom2) | Returns whether *geom1* overlaps *geom2*
| o | ST_Relate(geom1, geom2) | Returns the DE-9IM intersection matrix of *geom1* and *geom2*
| o | ST_Relate(geom1, geom2, iMatrix) | Returns whether *geom1* and *geom2* are related by the given intersection matrix *iMatrix*
| o | ST_Touches(geom1, geom2) | Returns whether *geom1* touches *geom2*
| o | ST_Within(geom1, geom2) | Returns whether *geom1* is within *geom2*

Not implemented:

* ST_OrderingEquals(geom1, geom2) Returns whether *geom1* equals *geom2* and their coordinates and component Geometries are listed in the same order

#### Geometry operators (2D)

The following functions combine 2D geometries.

| C | Operator syntax      | Description
|:- |:-------------------- |:-----------
| p | ST_Buffer(geom, distance [, quadSegs, endCapStyle ]) | Computes a buffer around *geom*
| p | ST_Buffer(geom, distance [, bufferStyle ]) | Computes a buffer around *geom*
| o | ST_ConvexHull(geom) | Computes the smallest convex polygon that contains all the points in *geom*
| o | ST_Difference(geom1, geom2) | Computes the difference between two geometries
| o | ST_SymDifference(geom1, geom2) | Computes the symmetric difference between two geometries
| o | ST_Intersection(geom1, geom2) | Computes the intersection of *geom1* and *geom2*
| p | ST_OffsetCurve(geom, distance, bufferStyle) | Computes an offset line for *linestring*
| o | ST_Union(geom1, geom2) | Computes the union of *geom1* and *geom2*
| o | ST_Union(geomCollection) | Computes the union of the geometries in *geomCollection*

See also: the `ST_Union` aggregate function.

#### Affine transformation functions (3D and 2D)

The following functions transform 2D geometries.

| C | Operator syntax      | Description
|:- |:-------------------- |:-----------
| o | ST_Rotate(geom, angle [, origin \| x, y]) | Rotates a *geom* counter-clockwise by *angle* (in radians) about *origin* (or the point (*x*, *y*))
| o | ST_Scale(geom, xFactor, yFactor) | Scales *geom* by multiplying the ordinates by the indicated scale factors
| o | ST_Translate(geom, x, y) | Translates *geom* by the vector (x, y)

Not implemented:

* ST_Scale(geom, xFactor, yFactor [, zFactor ]) Scales *geom* by multiplying the ordinates by the indicated scale factors
* ST_Translate(geom, x, y, [, z]) Translates *geom*

#### Geometry editing functions (2D)

The following functions modify 2D geometries.

| C | Operator syntax      | Description
|:- |:-------------------- |:-----------
| p | ST_AddPoint(linestring, point [, index]) | Adds *point* to *linestring* at a given *index* (or at the end if *index* is not specified)
| h | ST_Densify(geom, tolerance) | Densifies a *geom* by inserting extra vertices along the line segments
| h | ST_FlipCoordinates(geom) | Flips the X and Y coordinates of the *geom*
| h | ST_Holes(geom) | Returns the holes in the *geom* (which may be a GEOMETRYCOLLECTION)
| h | ST_Normalize(geom) | Converts the *geom* to normal form
| p | ST_RemoveRepeatedPoints(geom [, tolerance]) | Removes duplicated coordinates from the *geom*
| h | ST_RemoveHoles(geom) | Removes the holes of the *geom*
| p | ST_RemovePoint(linestring, index) | Remove *point* at given *index* in *linestring*
| h | ST_Reverse(geom) | Reverses the order of the coordinates of the *geom*

Not implemented:

* ST_CollectionExtract(geom, dimension) Filters *geom*, returning a multi-geometry of those members with a given *dimension* (1 = point, 2 = line-string, 3 = polygon)

#### Geometry editing functions (3D)

The following functions modify 3D geometries.


| C | Operator syntax      | Description
|:- |:-------------------- |:-----------
| h | ST_AddZ(geom, zToAdd) | Adds *zToAdd* to the z-coordinate of *geom*

Not implemented:

* ST_Interpolate3DLine(geom) Returns *geom* with an interpolation of z values, or null if it is not a line-string or MULTILINESTRING
* ST_MultiplyZ(geom, zFactor) Returns *geom* with its z-values multiplied by *zFactor*
* ST_Reverse3DLine(geom [, sortOrder ]) Potentially reverses *geom* according to the z-values of its first and last coordinates
* ST_UpdateZ(geom, newZ [, updateCondition ]) Updates the z-values of *geom*
* ST_ZUpdateLineExtremities(geom, startZ, endZ [, interpolate ]) Updates the start and end z-values of *geom*

#### Geometry measurement functions (2D)

The following functions measure geometries.

| C | Operator syntax      | Description
|:- |:-------------------- |:-----------
| o | ST_Area(geom) | Returns the area of *geom* (which may be a GEOMETRYCOLLECTION)
| h | ST_ClosestCoordinate(point, geom) | Returns the coordinate(s) of *geom* closest to *point*
| h | ST_ClosestPoint(geom1, geom2) | Returns the point of *geom1* closest to *geom2*
| h | ST_FurthestCoordinate(geom, point) | Returns the coordinate(s) of *geom* that are furthest from *point*
| h | ST_Length(geom) | Returns the length of *geom*
| h | ST_LocateAlong(geom, segmentLengthFraction, offsetDistance) | Returns a MULTIPOINT containing points along the line segments of *geom* at *segmentLengthFraction* and *offsetDistance*
| h | ST_LongestLine(geom1, geom2) | Returns the 2-dimensional longest line-string between the points of *geom1* and *geom2*
| h | ST_MaxDistance(geom1, geom2) | Computes the maximum distance between *geom1* and *geom2*
| h | ST_Perimeter(polygon) | Returns the length of the perimeter of *polygon* (which may be a MULTIPOLYGON)
| h | ST_ProjectPoint(point, lineString) | Projects *point* onto a *lineString* (which may be a MULTILINESTRING)

#### Geometry measurement functions (3D)

Not implemented:

* ST_3DArea(geom) Return a polygon's 3D area
* ST_3DLength(geom) Returns the 3D length of a line-string
* ST_3DPerimeter(geom) Returns the 3D perimeter of a polygon or MULTIPOLYGON
* ST_SunPosition(point [, timestamp ]) Computes the sun position at *point* and *timestamp* (now by default)

#### Geometry processing functions (2D)

The following functions process geometries.

| C | Operator syntax      | Description
|:- |:-------------------- |:-----------
| o | ST_LineMerge(geom)  | Merges a collection of linear components to form a line-string of maximal length
| o | ST_MakeValid(geom)  | Makes a valid geometry of a given invalid geometry
| o | ST_Polygonize(geom)  | Creates a MULTIPOLYGON from edges of *geom*
| o | ST_PrecisionReducer(geom, n) | Reduces *geom*'s precision to *n* decimal places
| o | ST_Simplify(geom, distance)  | Simplifies *geom* using the [Douglas-Peuker algorithm](https://en.wikipedia.org/wiki/Ramer%E2%80%93Douglas%E2%80%93Peucker_algorithm) with a *distance* tolerance
| o | ST_SimplifyPreserveTopology(geom, distance) | Simplifies *geom*, preserving its topology
| o | ST_Snap(geom1, geom2, tolerance) | Snaps *geom1* and *geom2* together
| p | ST_Split(geom, blade) | Splits *geom* by *blade*

Not implemented:

* ST_LineIntersector(geom1, geom2) Splits *geom1* (a line-string) with *geom2*
* ST_LineMerge(geom) Merges a collection of linear components to form a line-string of maximal length
* ST_MakeValid(geom [, preserveGeomDim [, preserveDuplicateCoord [, preserveCoordDim]]]) Makes *geom* valid
* ST_RingSideBuffer(geom, distance, bufferCount [, endCapStyle [, doDifference]]) Computes a ring buffer on one side
* ST_SideBuffer(geom, distance [, bufferStyle ]) Compute a single buffer on one side

#### Geometry projection functions

The EPSG dataset is released separately from Proj4J due
to its restrictive [terms of use](https://epsg.org/terms-of-use.html).
In order to use the projection functions in Apache Calcite,
users must include the EPSG dataset in their dependencies.

| C | Operator syntax      | Description
|:- |:-------------------- |:-----------
| o | ST_SetSRID(geom, srid) | Returns a copy of *geom* with a new SRID
| o | ST_Transform(geom, srid) | Transforms *geom* from one coordinate reference system (CRS) to the CRS specified by *srid*

#### Trigonometry functions

Not implemented:

* ST_Azimuth(point1, point2) Return the azimuth of the segment from *point1* to *point2*

#### Topography functions

Not implemented:

* ST_TriangleAspect(geom) Returns the aspect of a triangle
* ST_TriangleContouring(query \[, z1, z2, z3 ]\[, varArgs ]*) Splits triangles into smaller triangles according to classes
* ST_TriangleDirection(geom) Computes the direction of steepest ascent of a triangle and returns it as a line-string
* ST_TriangleSlope(geom) Computes the slope of a triangle as a percentage
* ST_Voronoi(geom [, outDimension [, envelopePolygon ]]) Creates a Voronoi diagram

#### Triangulation functions

| C | Operator syntax      | Description
|:- |:-------------------- |:-----------
| h | ST_ConstrainedDelaunay(geom [, flag]) | Computes a constrained Delaunay triangulation based on *geom*
| h | ST_Delaunay(geom [, flag]) | Computes a Delaunay triangulation based on points in *geom*

Not implemented:

* ST_Tessellate(polygon) Tessellates *polygon* (may be MULTIPOLYGON) with adaptive triangles

#### Geometry aggregate functions

| C | Operator syntax      | Description
|:- |:-------------------- |:-----------
| h | ST_Accum(geom) | Accumulates *geom* into an array
| h | ST_Collect(geom) | Collects *geom* into a GeometryCollection
| h | ST_Union(geom) | Computes the union of the geometries in *geom*

### JSON Functions

In the following:

* *jsonValue* is a character string containing a JSON value;
* *path* is a character string containing a JSON path expression; mode flag `strict` or `lax` should be specified in the beginning of *path*.

#### Query Functions

| Operator syntax        | Description
|:---------------------- |:-----------
| JSON_EXISTS(jsonValue, path [ { TRUE &#124; FALSE &#124; UNKNOWN &#124; ERROR } ON ERROR ] ) | Whether a *jsonValue* satisfies a search criterion described using JSON path expression *path*
| JSON_VALUE(jsonValue, path [ RETURNING type ] [ { ERROR &#124; NULL &#124; DEFAULT expr } ON EMPTY ] [ { ERROR &#124; NULL &#124; DEFAULT expr } ON ERROR ] ) | Extract an SQL scalar from a *jsonValue* using JSON path expression *path*
| JSON_QUERY(jsonValue, path [ { WITHOUT [ ARRAY ] &#124; WITH [ CONDITIONAL &#124; UNCONDITIONAL ] [ ARRAY ] } WRAPPER ] [ { ERROR &#124; NULL &#124; EMPTY ARRAY &#124; EMPTY OBJECT } ON EMPTY ] [ { ERROR &#124; NULL &#124; EMPTY ARRAY &#124; EMPTY OBJECT } ON ERROR ] ) | Extract a JSON object or JSON array from *jsonValue* using the *path* JSON path expression

Note:

* The `ON ERROR` and `ON EMPTY` clauses define the fallback
  behavior of the function when an error is thrown or a null value
  is about to be returned.
* The `ARRAY WRAPPER` clause defines how to represent a JSON array result
  in `JSON_QUERY` function. The following examples compare the wrapper
  behaviors.

Example Data:

{% highlight json %}
{"a": "[1,2]", "b": [1,2], "c": "hi"}
{% endhighlight json %}

Comparison:

|Operator                                    |$.a          |$.b          |$.c
|:-------------------------------------------|:------------|:------------|:------------
|JSON_VALUE                                  | [1, 2]      | error       | hi
|JSON QUERY WITHOUT ARRAY WRAPPER            | error       | [1, 2]      | error
|JSON QUERY WITH UNCONDITIONAL ARRAY WRAPPER | [ "[1,2]" ] | [ [1,2] ]   | [ "hi" ]
|JSON QUERY WITH CONDITIONAL ARRAY WRAPPER   | [ "[1,2]" ] | [1,2]       | [ "hi" ]

Not implemented:

* JSON_TABLE

#### Constructor Functions

| Operator syntax        | Description
|:---------------------- |:-----------
| JSON_OBJECT( jsonKeyVal [, jsonKeyVal ]* [ nullBehavior ] ) | Construct JSON object using a series of key-value pairs
| JSON_OBJECTAGG( jsonKeyVal [ nullBehavior ] ) | Aggregate function to construct a JSON object using a key-value pair
| JSON_ARRAY( [ jsonVal [, jsonVal ]* ] [ nullBehavior ] ) | Construct a JSON array using a series of values
| JSON_ARRAYAGG( jsonVal [ ORDER BY orderItem [, orderItem ]* ] [ nullBehavior ] ) | Aggregate function to construct a JSON array using a value

{% highlight sql %}
jsonKeyVal:
      [ KEY ] name VALUE value [ FORMAT JSON ]
  |   name : value [ FORMAT JSON ]

jsonVal:
      value [ FORMAT JSON ]

nullBehavior:
      NULL ON NULL
  |   ABSENT ON NULL
{% endhighlight %}

Note:

* The flag `FORMAT JSON` indicates the value is formatted as JSON
  character string. When `FORMAT JSON` is used, the value should be
  de-parse from JSON character string to a SQL structured value.
* `ON NULL` clause defines how the JSON output represents null
  values. The default null behavior of `JSON_OBJECT` and
  `JSON_OBJECTAGG` is `NULL ON NULL`, and for `JSON_ARRAY` and
  `JSON_ARRAYAGG` it is `ABSENT ON NULL`.
* If `ORDER BY` clause is provided, `JSON_ARRAYAGG` sorts the
  input rows into the specified order before performing aggregation.

#### Comparison Operators

| Operator syntax                   | Description
|:--------------------------------- |:-----------
| jsonValue IS JSON [ VALUE ]       | Whether *jsonValue* is a JSON value
| jsonValue IS NOT JSON [ VALUE ]   | Whether *jsonValue* is not a JSON value
| jsonValue IS JSON SCALAR          | Whether *jsonValue* is a JSON scalar value
| jsonValue IS NOT JSON SCALAR      | Whether *jsonValue* is not a JSON scalar value
| jsonValue IS JSON OBJECT          | Whether *jsonValue* is a JSON object
| jsonValue IS NOT JSON OBJECT      | Whether *jsonValue* is not a JSON object
| jsonValue IS JSON ARRAY           | Whether *jsonValue* is a JSON array
| jsonValue IS NOT JSON ARRAY       | Whether *jsonValue* is not a JSON array

Note:

* If the *jsonValue* is `NULL`, the function will return `NULL`.

### Dialect-specific Operators

The following operators are not in the SQL standard, and are not enabled in
Calcite's default operator table. They are only available for use in queries
if your session has enabled an extra operator table.

To enable an operator table, set the
[fun]({{ site.baseurl }}/docs/adapter.html#jdbc-connect-string-parameters)
connect string parameter.

The 'C' (compatibility) column contains value:
* '*' for all libraries,
* 'b' for Google BigQuery ('fun=bigquery' in the connect string),
* 'c' for Apache Calcite ('fun=calcite' in the connect string),
* 'f' for Snowflake ('fun=snowflake' in the connect string),
* 'h' for Apache Hive ('fun=hive' in the connect string),
* 'm' for MySQL ('fun=mysql' in the connect string),
* 'q' for Microsoft SQL Server ('fun=mssql' in the connect string),
* 'o' for Oracle ('fun=oracle' in the connect string),
* 'p' for PostgreSQL ('fun=postgresql' in the connect string),
* 'r' for Amazon RedShift ('fun=redshift' in the connect string),
* 's' for Apache Spark ('fun=spark' in the connect string).
* 'i' for ClickHouse('fun=clickhouse' in the connect string).

One operator name may correspond to multiple SQL dialects, but with different
semantics.

BigQuery's type system uses confusingly different names for types and functions:
* BigQuery's `DATETIME` type represents a local date time, and corresponds to
  Calcite's `TIMESTAMP` type;
* BigQuery's `TIMESTAMP` type represents an instant, and corresponds to
  Calcite's `TIMESTAMP WITH LOCAL TIME ZONE` type;
* The *timestampLtz* parameter, for instance in `DATE(timestampLtz)`, has
  Calcite type `TIMESTAMP WITH LOCAL TIME ZONE`;
* The `TIMESTAMP(string)` function, designed to be compatible the BigQuery
  function, return a Calcite `TIMESTAMP WITH LOCAL TIME ZONE`;
* Similarly, `DATETIME(string)` returns a Calcite `TIMESTAMP`.

In the following:
* *func* is a lambda argument.

| C | Operator syntax                                | Description
|:- |:-----------------------------------------------|:-----------
| p | expr :: type                                   | Casts *expr* to *type*
| m | expr1 <=> expr2                                | Whether two values are equal, treating null values as the same, and it's similar to `IS NOT DISTINCT FROM`
| p | ACOSD(numeric)                                 | Returns the inverse cosine of *numeric* in degrees as a double. Returns NaN if *numeric* is NaN. Fails if *numeric* is less than -1.0 or greater than 1.0.
| * | ACOSH(numeric)                                 | Returns the inverse hyperbolic cosine of *numeric*
| o s | ADD_MONTHS(date, numMonths)                  | Returns the date that is *numMonths* after *date*
| h s | ARRAY([expr [, expr ]*])                     | Construct an array in Apache Spark. The function allows users to use `ARRAY()` to create an empty array
| s | ARRAY_APPEND(array, element)                   | Appends an *element* to the end of the *array* and returns the result. Type of *element* should be similar to type of the elements of the *array*. If the *array* is null, the function will return null. If an *element* that is null, the null *element* will be added to the end of the *array*
| s | ARRAY_COMPACT(array)                           | Removes null values from the *array*
| b | ARRAY_CONCAT(array [, array ]*)                | Concatenates one or more arrays. If any input argument is `NULL` the function returns `NULL`
| s | ARRAY_CONTAINS(array, element)                 | Returns true if the *array* contains the *element*
| h s | ARRAY_DISTINCT(array)                        | Removes duplicate values from the *array* that keeps ordering of elements
| h s | ARRAY_EXCEPT(array1, array2)                 | Returns an array of the elements in *array1* but not in *array2*, without duplicates
| s | ARRAY_INSERT(array, pos, element)              | Places *element* into index *pos* of *array*. Array index start at 1, or start from the end if index is negative. Index above array size appends the array, or prepends the array if index is negative, with `NULL` elements.
| h s | ARRAY_INTERSECT(array1, array2)              | Returns an array of the elements in the intersection of *array1* and *array2*, without duplicates
| h s | ARRAY_JOIN(array, delimiter [, nullText ])   | Synonym for `ARRAY_TO_STRING`
| b | ARRAY_LENGTH(array)                            | Synonym for `CARDINALITY`
| h s | ARRAY_MAX(array)                             | Returns the maximum value in the *array*
| h s | ARRAY_MIN(array)                             | Returns the minimum value in the *array*
| s | ARRAY_POSITION(array, element)                 | Returns the (1-based) index of the first *element* of the *array* as long
| h s | ARRAY_REMOVE(array, element)                 | Remove all elements that equal to *element* from the *array*
| s | ARRAY_PREPEND(array, element)                  | Appends an *element* to the beginning of the *array* and returns the result. Type of *element* should be similar to type of the elements of the *array*. If the *array* is null, the function will return null. If an *element* that is null, the null *element* will be added to the beginning of the *array*
| s | ARRAY_REPEAT(element, count)                   | Returns the array containing element count times.
| b | ARRAY_REVERSE(array)                           | Reverses elements of *array*
| s | ARRAY_SIZE(array)                              | Synonym for `CARDINALITY`
| h | ARRAY_SLICE(array, start, length)              | Returns the subset or range of elements.
| b | ARRAY_TO_STRING(array, delimiter [, nullText ])| Returns a concatenation of the elements in *array* as a STRING and take *delimiter* as the delimiter. If the *nullText* parameter is used, the function replaces any `NULL` values in the array with the value of *nullText*. If the *nullText* parameter is not used, the function omits the `NULL` value and its preceding delimiter. Returns `NULL` if any argument is `NULL`
| h s | ARRAY_UNION(array1, array2)                  | Returns an array of the elements in the union of *array1* and *array2*, without duplicates
| s | ARRAYS_OVERLAP(array1, array2)                 | Returns true if *array1 contains at least a non-null element present also in *array2*. If the arrays have no common element and they are both non-empty and either of them contains a null element null is returned, false otherwise
| s | ARRAYS_ZIP(array [, array ]*)                  | Returns a merged *array* of structs in which the N-th struct contains all N-th values of input arrays
| s | SORT_ARRAY(array [, ascendingOrder])           | Sorts the *array* in ascending or descending order according to the natural ordering of the array elements. The default order is ascending if *ascendingOrder* is not specified. Null elements will be placed at the beginning of the returned array in ascending order or at the end of the returned array in descending order
| p | ASIND(numeric)                                 | Returns the inverse sine of *numeric* in degrees as a double. Returns NaN if *numeric* is NaN. Fails if *numeric* is less than -1.0 or greater than 1.0.
| * | ASINH(numeric)                                 | Returns the inverse hyperbolic sine of *numeric*
| p | ATAND(numeric)                                 | Returns the inverse tangent of *numeric* in degrees as a double. Returns NaN if *numeric* is NaN.
| * | ATANH(numeric)                                 | Returns the inverse hyperbolic tangent of *numeric*
| * | BITAND(value1, value2)                         | Returns the bitwise AND of *value1* and *value2*. *value1* and *value2* must both be integer or binary values. Binary values must be of the same length.
| * | BITOR(value1, value2)                          | Returns the bitwise OR of *value1* and *value2*. *value1* and *value2* must both be integer or binary values. Binary values must be of the same length.
| * | BITXOR(value1, value2)                         | Returns the bitwise XOR of *value1* and *value2*. *value1* and *value2* must both be integer or binary values. Binary values must be of the same length.
| * | BITNOT(value)                                  | Returns the bitwise NOT of *value*. *value* must be either an integer type or a binary value.
| f | BITAND_AGG(value)                              | Equivalent to `BIT_AND(value)`
| f | BITOR_AGG(value)                               | Equivalent to `BIT_OR(value)`
| * | BITCOUNT(value)                                | Returns the bitwise COUNT of *value* or NULL if *value* is NULL. *value* must be and integer or binary value.
| b s | BIT_COUNT(integer)                           | Returns the bitwise COUNT of *integer* or NULL if *integer* is NULL
| m | BIT_COUNT(numeric)                             | Returns the bitwise COUNT of the integer portion of *numeric* or NULL if *numeric* is NULL
| b m s | BIT_COUNT(binary)                          | Returns the bitwise COUNT of *binary* or NULL if *binary* is NULL
| s | BIT_LENGTH(binary)                             | Returns the bit length of *binary*
| s | BIT_LENGTH(string)                             | Returns the bit length of *string*
| s | BIT_GET(value, position)                       | Returns the bit (0 or 1) value at the specified *position* of numeric *value*. The positions are numbered from right to left, starting at zero. The *position* argument cannot be negative
| b | CEIL(value)                                    | Similar to standard `CEIL(value)` except if *value* is an integer type, the return type is a double
| m s | CHAR(integer)                                | Returns the character whose ASCII code is *integer* % 256, or null if *integer* &lt; 0
| b o p r | CHR(integer)                             | Returns the character whose UTF-8 code is *integer*
| b | CODE_POINTS_TO_BYTES(integers)                 | Converts *integers*, an array of integers between 0 and 255 inclusive, into bytes; throws error if any element is out of range
| b | CODE_POINTS_TO_STRING(integers)                | Converts *integers*, an array of integers between 0 and 0xD7FF or between 0xE000 and 0x10FFFF inclusive, into string; throws error if any element is out of range
| o r | CONCAT(string, string)                       | Concatenates two strings, returns null only when both string arguments are null, otherwise treats null as empty string
| b m | CONCAT(string [, string ]*)                  | Concatenates one or more strings, returns null if any of the arguments is null
| p q | CONCAT(string [, string ]*)                  | Concatenates one or more strings, null is treated as empty string
| m | CONCAT_WS(separator, str1 [, string ]*)        | Concatenates one or more strings, returns null only when separator is null, otherwise treats null arguments as empty strings
| p | CONCAT_WS(separator, any [, any ]*)            | Concatenates all but the first argument, returns null only when separator is null, otherwise treats null arguments as empty strings
| q | CONCAT_WS(separator, str1, str2 [, string ]*)  | Concatenates two or more strings, requires at least 3 arguments (up to 254), treats null arguments as empty strings
| s | CONCAT_WS(separator [, string \| array(string)]*)  | Concatenates one or more strings or arrays. Besides the separator, other arguments can include strings or string arrays. returns null only when separator is null, treats other null arguments as empty strings
| m | COMPRESS(string)                               | Compresses a string using zlib compression and returns the result as a binary string
| b | CONTAINS_SUBSTR(expression, string [ , json_scope =&gt; json_scope_value ]) | Returns whether *string* exists as a substring in *expression*. Optional *json_scope* argument specifies what scope to search if *expression* is in JSON format. Returns NULL if a NULL exists in *expression* that does not result in a match
| q | CONVERT(type, expression [ , style ])          | Equivalent to `CAST(expression AS type)`; ignores the *style* operand
| o | CONVERT(string, destCharSet[, srcCharSet])     | Converts *string* from *srcCharSet* to *destCharSet*. If the *srcCharSet* parameter is not specified, then it uses the default CharSet
| r | CONVERT_TIMEZONE(tz1, tz2, datetime)           | Converts the timezone of *datetime* from *tz1* to *tz2*
| p | COSD(numeric)                                  | Returns the cosine of *numeric* in degrees as a double. Returns NaN if *numeric* is NaN. Fails if *numeric* is greater than the maximum double value.
| * | COSH(numeric)                                  | Returns the hyperbolic cosine of *numeric*
| * | COTH(numeric)                                  | Returns the hyperbolic cotangent of *numeric*
| s h | CRC32(string)                                | Calculates a cyclic redundancy check value for string or binary argument and returns bigint value
| * | CSC(numeric)                                   | Returns the cosecant of *numeric* in radians
| * | CSCH(numeric)                                  | Returns the hyperbolic cosecant of *numeric*
| b | CURRENT_DATETIME([ timeZone ])                 | Returns the current time as a TIMESTAMP from *timezone*
| m | DAYNAME(datetime)                              | Returns the name, in the connection's locale, of the weekday in *datetime*; for example, for a locale of en, it will return 'Sunday' for both DATE '2020-02-10' and TIMESTAMP '2020-02-10 10:10:10', and for a locale of zh, it will return '星期日'
| b | DATE(timestamp)                                | Extracts the DATE from a *timestamp*
| b | DATE(timestampLtz)                             | Extracts the DATE from *timestampLtz* (an instant; BigQuery's TIMESTAMP type), assuming UTC
| b | DATE(timestampLtz, timeZone)                   | Extracts the DATE from *timestampLtz* (an instant; BigQuery's TIMESTAMP type) in *timeZone*
| b | DATE(string)                                   | Equivalent to `CAST(string AS DATE)`
| b | DATE(year, month, day)                         | Returns a DATE value for *year*, *month*, and *day* (all of type INTEGER)
| q r f | DATEADD(timeUnit, integer, datetime)       | Equivalent to `TIMESTAMPADD(timeUnit, integer, datetime)`
| q r f | DATEDIFF(timeUnit, datetime, datetime2)    | Equivalent to `TIMESTAMPDIFF(timeUnit, datetime, datetime2)`
| q | DATEPART(timeUnit, datetime)                   | Equivalent to `EXTRACT(timeUnit FROM  datetime)`
| b | DATETIME(date, time)                           | Converts *date* and *time* to a TIMESTAMP
| b | DATETIME(date)                                 | Converts *date* to a TIMESTAMP value (at midnight)
| b | DATETIME(date, timeZone)                       | Converts *date* to a TIMESTAMP value (at midnight), in *timeZone*
| b | DATETIME(year, month, day, hour, minute, second) | Creates a TIMESTAMP for *year*, *month*, *day*, *hour*, *minute*, *second* (all of type INTEGER)
| b | DATETIME_ADD(timestamp, interval)              | Returns the TIMESTAMP value that occurs *interval* after *timestamp*
| b | DATETIME_DIFF(timestamp, timestamp2, timeUnit) | Returns the whole number of *timeUnit* between *timestamp* and *timestamp2*
| b | DATETIME_SUB(timestamp, interval)              | Returns the TIMESTAMP that occurs *interval* before *timestamp*
| b | DATETIME_TRUNC(timestamp, timeUnit)            | Truncates *timestamp* to the granularity of *timeUnit*, rounding to the beginning of the unit
| b s | DATE_FROM_UNIX_DATE(integer)                 | Returns the DATE that is *integer* days after 1970-01-01
| p r | DATE_PART(timeUnit, datetime)                | Equivalent to `EXTRACT(timeUnit FROM  datetime)`
| b | DATE_ADD(date, interval)                       | Returns the DATE value that occurs *interval* after *date*
| s h | DATE_ADD(date, numDays)                      | Returns the DATE that is *numDays* after *date*
| b | DATE_DIFF(date, date2, timeUnit)               | Returns the whole number of *timeUnit* between *date* and *date2*
| b | DATE_SUB(date, interval)                       | Returns the DATE value that occurs *interval* before *date*
| s h | DATE_SUB(date, numDays)                      | Returns the DATE that is *numDays* before *date*
| b | DATE_TRUNC(date, timeUnit)                     | Truncates *date* to the granularity of *timeUnit*, rounding to the beginning of the unit
| o r s h | DECODE(value, value1, result1 [, valueN, resultN ]* [, default ]) | Compares *value* to each *valueN* value one by one; if *value* is equal to a *valueN*, returns the corresponding *resultN*, else returns *default*, or NULL if *default* is not specified
| p r | DIFFERENCE(string, string)                   | Returns a measure of the similarity of two strings, namely the number of character positions that their `SOUNDEX` values have in common: 4 if the `SOUNDEX` values are same and 0 if the `SOUNDEX` values are totally different
| f s | ENDSWITH(string1, string2)                   | Returns whether *string2* is a suffix of *string1*
| b | ENDS_WITH(string1, string2)                    | Equivalent to `ENDSWITH(string1, string2)`
| s | EXISTS(array, func)                            | Returns whether a predicate *func* holds for one or more elements in the *array*
| o | EXISTSNODE(xml, xpath, [, namespaces ])        | Determines whether traversal of a XML document using a specified xpath results in any nodes. Returns 0 if no nodes remain after applying the XPath traversal on the document fragment of the element or elements matched by the XPath expression. Returns 1 if any nodes remain. The optional namespace value that specifies a default mapping or namespace mapping for prefixes, which is used when evaluating the XPath expression.
| o | EXTRACT(xml, xpath, [, namespaces ])           | Returns the XML fragment of the element or elements matched by the XPath expression. The optional namespace value that specifies a default mapping or namespace mapping for prefixes, which is used when evaluating the XPath expression
| m | EXTRACTVALUE(xml, xpathExpr))                  | Returns the text of the first text node which is a child of the element or elements matched by the XPath expression.
| h s | FACTORIAL(integer)                           | Returns the factorial of *integer*, the range of *integer* is [0, 20]. Otherwise, returns NULL
| h s | FIND_IN_SET(matchStr, textStr)               | Returns the index (1-based) of the given *matchStr* in the comma-delimited *textStr*. Returns 0, if the given *matchStr* is not found or if the *matchStr* contains a comma. For example, FIND_IN_SET('bc', 'a,bc,def') returns 2
| b | FLOOR(value)                                   | Similar to standard `FLOOR(value)` except if *value* is an integer type, the return type is a double
| b | FORMAT_DATE(string, date)                      | Formats *date* according to the specified format *string*
| b | FORMAT_DATETIME(string, timestamp)             | Formats *timestamp* according to the specified format *string*
| h s | FORMAT_NUMBER(value, decimalVal)             | Formats the number *value* like '#,###,###.##', rounded to decimal places *decimalVal*. If *decimalVal* is 0, the result has no decimal point or fractional part
| h s | FORMAT_NUMBER(value, format)                 | Formats the number *value* to MySQL's FORMAT *format*, like '#,###,###.##0.00'
| b | FORMAT_TIME(string, time)                      | Formats *time* according to the specified format *string*
| b | FORMAT_TIMESTAMP(string timestamp)             | Formats *timestamp* according to the specified format *string*
| s | GETBIT(value, position)                        | Equivalent to `BIT_GET(value, position)`
| b o p r s h | GREATEST(expr [, expr ]*)            | Returns the greatest of the expressions
| b h s | IF(condition, value1, value2)              | Returns *value1* if *condition* is TRUE, *value2* otherwise
| b s | IFNULL(value1, value2)                       | Equivalent to `NVL(value1, value2)`
| p | string1 ILIKE string2 [ ESCAPE string3 ]       | Whether *string1* matches pattern *string2*, ignoring case (similar to `LIKE`)
| p | string1 NOT ILIKE string2 [ ESCAPE string3 ]   | Whether *string1* does not match pattern *string2*, ignoring case (similar to `NOT LIKE`)
| b h o | INSTR(string, substring [, from [, occurrence ] ]) | Returns the position of *substring* in *string*, searching starting at *from* (default 1), and until locating the nth *occurrence* (default 1) of *substring*
| b h m o | INSTR(string, substring)                   | Equivalent to `POSITION(substring IN string)`
| b | IS_INF(value)                                  | Returns whether *value* is infinite
| b | IS_NAN(value)                                  | Returns whether *value* is NaN
| m | JSON_TYPE(jsonValue)                           | Returns a string value indicating the type of *jsonValue*
| m | JSON_DEPTH(jsonValue)                          | Returns an integer value indicating the depth of *jsonValue*
| m | JSON_PRETTY(jsonValue)                         | Returns a pretty-printing of *jsonValue*
| m | JSON_LENGTH(jsonValue [, path ])               | Returns a integer indicating the length of *jsonValue*
| m | JSON_INSERT(jsonValue, path, val [, path, val ]*) | Returns a JSON document insert a data of *jsonValue*, *path*, *val*
| m | JSON_KEYS(jsonValue [, path ])                 | Returns a string indicating the keys of a JSON *jsonValue*
| m | JSON_REMOVE(jsonValue, path [, path ])         | Removes data from *jsonValue* using a series of *path* expressions and returns the result
| m | JSON_REPLACE(jsonValue, path, val [, path, val ]*)  | Returns a JSON document replace a data of *jsonValue*, *path*, *val*
| m | JSON_SET(jsonValue, path, val [, path, val ]*) | Returns a JSON document set a data of *jsonValue*, *path*, *val*
| m | JSON_STORAGE_SIZE(jsonValue)                   | Returns the number of bytes used to store the binary representation of *jsonValue*
| b o p r s h | LEAST(expr [, expr ]* )                | Returns the least of the expressions
| b m p r s | LEFT(string, length)                   | Returns the leftmost *length* characters from the *string*
| f r s | LEN(string)                                | Equivalent to `CHAR_LENGTH(string)`
| b f h p r s | LENGTH(string)                       | Equivalent to `CHAR_LENGTH(string)`
| h s | LEVENSHTEIN(string1, string2)                | Returns the Levenshtein distance between *string1* and *string2*
| b | LOG(numeric1 [, base ])                        | Returns the logarithm of *numeric1* to base *base*, or base e if *base* is not present, or error if *numeric1* is 0 or negative
| m s h | LOG([, base ], numeric1)                   | Returns the logarithm of *numeric1* to base *base*, or base e if *base* is not present, or null if *numeric1* is 0 or negative
| p | LOG([, base ], numeric1 )                      | Returns the logarithm of *numeric1* to base *base*, or base 10 if *numeric1* is not present, or error if *numeric1* is 0 or negative
| m s | LOG2(numeric)                                | Returns the base 2 logarithm of *numeric*
| s | LOG1P(numeric)                                 | Returns the natural logarithm of 1 plus *numeric*
| b o p r s h | LPAD(string, length [, pattern ])    | Returns a string or bytes value that consists of *string* prepended to *length* with *pattern*
| b | TO_BASE32(string)                              | Converts the *string* to base-32 encoded form and returns an encoded string
| b | FROM_BASE32(string)                            | Returns the decoded result of a base-32 *string* as a string
| m | TO_BASE64(string)                              | Converts the *string* to base-64 encoded form and returns a encoded string
| b m | FROM_BASE64(string)                          | Returns the decoded result of a base-64 *string* as a string. If the input argument is an invalid base-64 *string* the function returns `NULL`
| h | BASE64(string)                                 | Converts the *string* to base-64 encoded form and returns a encoded string
| h | UNBASE64(string)                               | Returns the decoded result of a base-64 *string* as a string. If the input argument is an invalid base-64 *string* the function returns `NULL`
| h s | HEX(string)                                  | Converts *string* into a hexadecimal varchar
| b | TO_HEX(binary)                                 | Converts *binary* into a hexadecimal varchar
| b | FROM_HEX(varchar)                              | Converts a hexadecimal-encoded *varchar* into bytes
| s h | BIN(BIGINT)                                  | Converts a *bigint* into bytes string
| b o p r s h | LTRIM(string)                        | Returns *string* with all blanks removed from the start
| s | MAP()                                          | Returns an empty map
| s | MAP(key, value [, key, value]*)                | Returns a map with the given *key*/*value* pairs
| s | MAP_CONCAT(map [, map]*)                       | Concatenates one or more maps. If any input argument is `NULL` the function returns `NULL`. Note that calcite is using the LAST_WIN strategy
| s | MAP_CONTAINS_KEY(map, key)                     | Returns whether *map* contains *key*
| s | MAP_ENTRIES(map)                               | Returns the entries of the *map* as an array, the order of the entries is not defined
| s | MAP_KEYS(map)                                  | Returns the keys of the *map* as an array, the order of the entries is not defined
| s | MAP_VALUES(map)                                | Returns the values of the *map* as an array, the order of the entries is not defined
| s | MAP_FROM_ARRAYS(array1, array2)                | Returns a map created from an *array1* and *array2*. Note that the lengths of two arrays should be the same and calcite is using the LAST_WIN strategy
| s | MAP_FROM_ENTRIES(arrayOfRows)                  | Returns a map created from an arrays of row with two fields. Note that the number of fields in a row must be 2. Note that calcite is using the LAST_WIN strategy
| s | STR_TO_MAP(string [, stringDelimiter [, keyValueDelimiter]]) | Returns a map after splitting the *string* into key/value pairs using delimiters. Default delimiters are ',' for *stringDelimiter* and ':' for *keyValueDelimiter*. Note that calcite is using the LAST_WIN strategy
| s | SUBSTRING_INDEX(string, delim, count)          | Returns the substring from *string* before *count* occurrences of the delimiter *delim*. If *count* is positive, everything to the left of the final delimiter (counting from the left) is returned. If *count* is negative, everything to the right of the final delimiter (counting from the right) is returned. The function substring_index performs a case-sensitive match when searching for *delim*.
| p r | STRING_TO_ARRAY(string, delimiter [, nullString ]) | Returns a one-dimensional string[] array by splitting the input string value into subvalues using the specified string value as the "delimiter". Optionally, allows a specified string value to be interpreted as NULL.
| b m p r s h | MD5(string)                          | Calculates an MD5 128-bit checksum of *string* and returns it as a hex string
| m | MONTHNAME(date)                                | Returns the name, in the connection's locale, of the month in *datetime*; for example, for a locale of en, it will return 'February' for both DATE '2020-02-10' and TIMESTAMP '2020-02-10 10:10:10', and for a locale of zh, it will return '二月'
| o r s | NVL(value1, value2)                        | Returns *value1* if *value1* is not null, otherwise *value2*
| o r s | NVL2(value1, value2, value3)               | Returns *value2* if *value1* is not null, otherwise *value3*
| b | OFFSET(index)                                  | When indexing an array, wrapping *index* in `OFFSET` returns the value at the 0-based *index*; throws error if *index* is out of bounds
| b | ORDINAL(index)                                 | Similar to `OFFSET` except *index* begins at 1
| b | PARSE_DATE(format, string)                     | Uses format specified by *format* to convert *string* representation of date to a DATE value
| b | PARSE_DATETIME(format, string)                 | Uses format specified by *format* to convert *string* representation of datetime to a TIMESTAMP value
| b | PARSE_TIME(format, string)                     | Uses format specified by *format* to convert *string* representation of time to a TIME value
| b | PARSE_TIMESTAMP(format, string[, timeZone])    | Uses format specified by *format* to convert *string* representation of timestamp to a TIMESTAMP WITH LOCAL TIME ZONE value in *timeZone*
| h s | PARSE_URL(urlString, partToExtract [, keyToExtract] ) | Returns the specified *partToExtract* from the *urlString*. Valid values for *partToExtract* include HOST, PATH, QUERY, REF, PROTOCOL, AUTHORITY, FILE, and USERINFO. *keyToExtract* specifies which query to extract. If the first argument is an invalid url *string* the function returns `NULL`
| b s | POW(numeric1, numeric2)                      | Returns *numeric1* raised to the power *numeric2*
| b c h q m o f s p r | POWER(numeric1, numeric2) | Returns *numeric1* raised to the power of *numeric2*
| p r | RANDOM()                                     | Generates a random double between 0 and 1 inclusive
| s | REGEXP(string, regexp)                         | Equivalent to `string1 RLIKE string2`
| b | REGEXP_CONTAINS(string, regexp)                | Returns whether *string* is a partial match for the *regexp*
| b | REGEXP_EXTRACT(string, regexp [, position [, occurrence]]) | Returns the substring in *string* that matches the *regexp*, starting search at *position* (default 1), and until locating the nth *occurrence* (default 1). Returns NULL if there is no match
| b | REGEXP_EXTRACT_ALL(string, regexp)             | Returns an array of all substrings in *string* that matches the *regexp*. Returns an empty array if there is no match
| b | REGEXP_INSTR(string, regexp [, position [, occurrence [, occurrence_position]]]) | Returns the lowest 1-based position of the substring in *string* that matches the *regexp*, starting search at *position* (default 1), and until locating the nth *occurrence* (default 1). Setting occurrence_position (default 0) to 1 returns the end position of substring + 1. Returns 0 if there is no match
| m o p r s | REGEXP_LIKE(string, regexp [, flags])  | Equivalent to `string1 RLIKE string2` with an optional parameter for search flags. Supported flags are: <ul><li>i: case-insensitive matching</li><li>c: case-sensitive matching</li><li>n: newline-sensitive matching</li><li>s: non-newline-sensitive matching</li><li>m: multi-line</li></ul>
| r | REGEXP_REPLACE(string, regexp)                 | Replaces all substrings of *string* that match *regexp* with the empty string
| b m o r h | REGEXP_REPLACE(string, regexp, rep [, pos [, occurrence [, matchType]]]) | Replaces all substrings of *string* that match *regexp* with *rep* at the starting *pos* in expr (if omitted, the default is 1), *occurrence* specifies which occurrence of a match to search for (if omitted, the default is 1), *matchType* specifies how to perform matching
| p | REGEXP_REPLACE(string, regexp, rep [, matchType]) | Replaces substrings of *string* that match *regexp* with *rep* at the starting *pos* in expr, *matchType* specifies how to perform matching and whether to only replace first match or all
| b | REGEXP_SUBSTR(string, regexp [, position [, occurrence]]) | Synonym for REGEXP_EXTRACT
| b m p r s h | REPEAT(string, integer)                | Returns a string consisting of *string* repeated of *integer* times; returns an empty string if *integer* is less than 1
| b m | REVERSE(string)                              | Returns *string* with the order of the characters reversed
| s | REVERSE(string \| array)                        | Returns *string* with the characters in reverse order or array with elements in reverse order
| b m p r s | RIGHT(string, length)                  | Returns the rightmost *length* characters from the *string*
| h m s | string1 RLIKE string2                      | Whether *string1* matches regex pattern *string2* (similar to `LIKE`, but uses Java regex)
| h m s | string1 NOT RLIKE string2                  | Whether *string1* does not match regex pattern *string2* (similar to `NOT LIKE`, but uses Java regex)
| b o p r s h | RPAD(string, length[, pattern ])       | Returns a string or bytes value that consists of *string* appended to *length* with *pattern*
| b o p r s h | RTRIM(string)                          | Returns *string* with all blanks removed from the end
| b | SAFE_ADD(numeric1, numeric2)                   | Returns *numeric1* + *numeric2*, or NULL on overflow.  Arguments are implicitly cast to one of the types BIGINT, DOUBLE, or DECIMAL
| b | SAFE_CAST(value AS type)                       | Converts *value* to *type*, returning NULL if conversion fails
| b | SAFE_DIVIDE(numeric1, numeric2)                | Returns *numeric1* / *numeric2*, or NULL on overflow or if *numeric2* is zero.  Arguments implicitly are cast to one of the types BIGINT, DOUBLE, or DECIMAL
| b | SAFE_MULTIPLY(numeric1, numeric2)              | Returns *numeric1* * *numeric2*, or NULL on overflow.  Arguments are implicitly cast to one of the types BIGINT, DOUBLE, or DECIMAL
| b | SAFE_NEGATE(numeric)                           | Returns *numeric* * -1, or NULL on overflow.  Arguments are implicitly cast to one of the types BIGINT, DOUBLE, or DECIMAL
| b | SAFE_OFFSET(index)                             | Similar to `OFFSET` except null is returned if *index* is out of bounds
| b | SAFE_ORDINAL(index)                            | Similar to `OFFSET` except *index* begins at 1 and null is returned if *index* is out of bounds
| b | SAFE_SUBTRACT(numeric1, numeric2)              | Returns *numeric1* - *numeric2*, or NULL on overflow.  Arguments are implicitly cast to one of the types BIGINT, DOUBLE, or DECIMAL
| * | SEC(numeric)                                   | Returns the secant of *numeric* in radians
| * | SECH(numeric)                                  | Returns the hyperbolic secant of *numeric*
| b m p r s h | SHA1(string)                           | Calculates a SHA-1 hash value of *string* and returns it as a hex string
| b p | SHA256(string)                               | Calculates a SHA-256 hash value of *string* and returns it as a hex string
| b p | SHA512(string)                               | Calculates a SHA-512 hash value of *string* and returns it as a hex string
| p | SIND(numeric)                                  | Returns the sine of *numeric* in degrees as a double. Returns NaN if *numeric* is NaN. Fails if *numeric* is greater than the maximum double value.
| * | SINH(numeric)                                  | Returns the hyperbolic sine of *numeric*
| b m o p r h | SOUNDEX(string)                        | Returns the phonetic representation of *string*; throws if *string* is encoded with multi-byte encoding such as UTF-8
| s | SOUNDEX(string)                                | Returns the phonetic representation of *string*; return original *string* if *string* is encoded with multi-byte encoding such as UTF-8
| m s h | SPACE(integer)                               | Returns a string of *integer* spaces; returns an empty string if *integer* is less than 1
| b | SPLIT(string [, delimiter ])                   | Returns the string array of *string* split at *delimiter* (if omitted, default is comma).  If the *string* is empty it returns an empty array, otherwise, if the *delimiter* is empty, it returns an array containing the original *string*.
| p | SPLIT_PART(string, delimiter, n)               | Returns the *n*th field in *string* using *delimiter*; returns empty string if *n* is less than 1 or greater than the number of fields, and the n can be negative to count from the end.
| f s | STARTSWITH(string1, string2)                 | Returns whether *string2* is a prefix of *string1*
| b p | STARTS_WITH(string1, string2)                | Equivalent to `STARTSWITH(string1, string2)`
| m | STRCMP(string, string)                         | Returns 0 if both of the strings are same and returns -1 when the first argument is smaller than the second and 1 when the second one is smaller than the first one
| b r p | STRPOS(string, substring)                  | Equivalent to `POSITION(substring IN string)`
| b m o p r | SUBSTR(string, position [, substringLength ]) | Returns a portion of *string*, beginning at character *position*, *substringLength* characters long. SUBSTR calculates lengths using characters as defined by the input character set
| o | SYSDATE                                        | Returns the current date in the operating system time zone of the database server, in a value of datatype DATE.
| o | SYSTIMESTAMP                                   | Returns the current date and time in the operating system time zone of the database server, in a value of datatype TIMESTAMP WITH TIME ZONE.
| p | TAND(numeric)                                  | Returns the tangent of *numeric* in degrees as a double. Returns NaN if *numeric* is NaN. Fails if *numeric is greater than the maximum double value.
| * | TANH(numeric)                                  | Returns the hyperbolic tangent of *numeric*
| b | TIME(hour, minute, second)                     | Returns a TIME value *hour*, *minute*, *second* (all of type INTEGER)
| b | TIME(timestamp)                                | Extracts the TIME from *timestamp* (a local time; BigQuery's DATETIME type)
| b | TIME(instant)                                  | Extracts the TIME from *timestampLtz* (an instant; BigQuery's TIMESTAMP type), assuming UTC
| b | TIME(instant, timeZone)                        | Extracts the time from *timestampLtz* (an instant; BigQuery's TIMESTAMP type), in *timeZone*
| b | TIMESTAMP(string)                              | Equivalent to `CAST(string AS TIMESTAMP WITH LOCAL TIME ZONE)`
| b | TIMESTAMP(string, timeZone)                    | Equivalent to `CAST(string AS TIMESTAMP WITH LOCAL TIME ZONE)`, converted to *timeZone*
| b | TIMESTAMP(date)                                | Converts *date* to a TIMESTAMP WITH LOCAL TIME ZONE value (at midnight)
| b | TIMESTAMP(date, timeZone)                      | Converts *date* to a TIMESTAMP WITH LOCAL TIME ZONE value (at midnight), in *timeZone*
| b | TIMESTAMP(timestamp)                           | Converts *timestamp* to a TIMESTAMP WITH LOCAL TIME ZONE, assuming a UTC
| b | TIMESTAMP(timestamp, timeZone)                 | Converts *timestamp* to a TIMESTAMP WITH LOCAL TIME ZONE, in *timeZone*
| b | TIMESTAMP_ADD(timestamp, interval)             | Returns the TIMESTAMP value that occurs *interval* after *timestamp*
| b | TIMESTAMP_DIFF(timestamp, timestamp2, timeUnit) | Returns the whole number of *timeUnit* between *timestamp* and *timestamp2*. Equivalent to `TIMESTAMPDIFF(timeUnit, timestamp2, timestamp)` and `(timestamp - timestamp2) timeUnit`
| b s | TIMESTAMP_MICROS(integer)                    | Returns the TIMESTAMP that is *integer* microseconds after 1970-01-01 00:00:00
| b s | TIMESTAMP_MILLIS(integer)                    | Returns the TIMESTAMP that is *integer* milliseconds after 1970-01-01 00:00:00
| b s | TIMESTAMP_SECONDS(integer)                   | Returns the TIMESTAMP that is *integer* seconds after 1970-01-01 00:00:00
| b | TIMESTAMP_SUB(timestamp, interval)             | Returns the TIMESTAMP value that is *interval* before *timestamp*
| b | TIMESTAMP_TRUNC(timestamp, timeUnit)           | Truncates *timestamp* to the granularity of *timeUnit*, rounding to the beginning of the unit
| b | TIME_ADD(time, interval)                       | Adds *interval* to *time*, independent of any time zone
| b | TIME_DIFF(time, time2, timeUnit)               | Returns the whole number of *timeUnit* between *time* and *time2*
| b | TIME_SUB(time, interval)                       | Returns the TIME value that is *interval* before *time*
| b | TIME_TRUNC(time, timeUnit)                     | Truncates *time* to the granularity of *timeUnit*, rounding to the beginning of the unit
| m o p r | TO_CHAR(timestamp, format)               | Converts *timestamp* to a string using the format *format*
| b | TO_CODE_POINTS(string)                         | Converts *string* to an array of integers that represent code points or extended ASCII character values
| o p r h | TO_DATE(string, format)                    | Converts *string* to a date using the format *format*
| o p r | TO_TIMESTAMP(string, format)               | Converts *string* to a timestamp using the format *format*
| b o p r s | TRANSLATE(expr, fromString, toString)  | Returns *expr* with all occurrences of each character in *fromString* replaced by its corresponding character in *toString*. Characters in *expr* that are not in *fromString* are not replaced
| b | TRUNC(numeric1 [, integer2 ])                  | Truncates *numeric1* to optionally *integer2* (if not specified 0) places right to the decimal point
| q | TRY_CAST(value AS type)                        | Converts *value* to *type*, returning NULL if conversion fails
| b s | UNIX_MICROS(timestamp)                       | Returns the number of microseconds since 1970-01-01 00:00:00
| b s | UNIX_MILLIS(timestamp)                       | Returns the number of milliseconds since 1970-01-01 00:00:00
| b s | UNIX_SECONDS(timestamp)                      | Returns the number of seconds since 1970-01-01 00:00:00
| b s | UNIX_DATE(date)                              | Returns the number of days since 1970-01-01
| s | URL_DECODE(string)                             | Decodes a *string* in 'application/x-www-form-urlencoded' format using a specific encoding scheme, returns original *string* when decoded error
| s | URL_ENCODE(string)                             | Translates a *string* into 'application/x-www-form-urlencoded' format using a specific encoding scheme
| o | XMLTRANSFORM(xml, xslt)                        | Applies XSLT transform *xslt* to XML string *xml* and returns the result

Note:

* Functions `DATEADD`, `DATEDIFF`, `DATE_PART` require the Babel parser
* `JSON_TYPE` / `JSON_DEPTH` / `JSON_PRETTY` / `JSON_STORAGE_SIZE` return null if the argument is null
* `JSON_LENGTH` / `JSON_KEYS` / `JSON_REMOVE` return null if the first argument is null
* `JSON_TYPE` generally returns an upper-case string flag indicating the type of the JSON input. Currently supported supported type flags are:
  * INTEGER
  * STRING
  * FLOAT
  * DOUBLE
  * LONG
  * BOOLEAN
  * DATE
  * OBJECT
  * ARRAY
  * NULL
* `JSON_DEPTH` defines a JSON value's depth as follows:
  * An empty array, empty object, or scalar value has depth 1;
  * A non-empty array containing only elements of depth 1 or non-empty object containing only member values of depth 1 has depth 2;
  * Otherwise, a JSON document has depth greater than 2.
* `JSON_LENGTH` defines a JSON value's length as follows:
  * A scalar value has length 1;
  * The length of array or object is the number of elements is contains.

Dialect-specific aggregate functions.

| C | Operator syntax                                | Description
|:- |:-----------------------------------------------|:-----------
| c | AGGREGATE(m)                                   | Computes measure *m* in the context of the current GROUP BY key
| b p | ARRAY_AGG( [ ALL &#124; DISTINCT ] value [ RESPECT NULLS &#124; IGNORE NULLS ] [ ORDER BY orderItem [, orderItem ]* ] ) | Gathers values into arrays
| b p | ARRAY_CONCAT_AGG( [ ALL &#124; DISTINCT ] value [ ORDER BY orderItem [, orderItem ]* ] ) | Concatenates arrays into arrays
| p r s | BOOL_AND(condition)                        | Synonym for `EVERY`
| p r s | BOOL_OR(condition)                         | Synonym for `SOME`
| f | BOOLAND_AGG(condition)                         | Synonym for `EVERY`
| f | BOOLOR_AGG(condition)                          | Synonym for `SOME`
| b | COUNTIF(condition)                             | Returns the number of rows for which *condition* is TRUE; equivalent to `COUNT(*) FILTER (WHERE condition)`
| m | GROUP_CONCAT( [ ALL &#124; DISTINCT ] value [, value ]* [ ORDER BY orderItem [, orderItem ]* ] [ SEPARATOR separator ] ) | MySQL-specific variant of `LISTAGG`
| b | LOGICAL_AND(condition)                         | Synonym for `EVERY`
| b | LOGICAL_OR(condition)                          | Synonym for `SOME`
| s | MAX_BY(value, comp)                            | Synonym for `ARG_MAX`
| s | MIN_BY(value, comp)                            | Synonym for `ARG_MIN`
| b | PERCENTILE_CONT(value, fraction [ RESPECT NULLS &#124; IGNORE NULLS ] ) OVER windowSpec | Synonym for standard `PERCENTILE_CONT` where `PERCENTILE_CONT(value, fraction) OVER (ORDER BY value)` is equivalent to standard `PERCENTILE_CONT(fraction) WITHIN GROUP (ORDER BY value)`
| b | PERCENTILE_DISC(value, fraction [ RESPECT NULLS &#124; IGNORE NULLS ] ) OVER windowSpec | Synonym for standard `PERCENTILE_DISC` where `PERCENTILE_DISC(value, fraction) OVER (ORDER BY value)` is equivalent to standard `PERCENTILE_DISC(fraction) WITHIN GROUP (ORDER BY value)`
| b p | STRING_AGG( [ ALL &#124; DISTINCT ] value [, separator] [ ORDER BY orderItem [, orderItem ]* ] ) | Synonym for `LISTAGG`

Usage Examples:

##### JSON_TYPE example

SQL

{% highlight sql %}
SELECT JSON_TYPE(v) AS c1,
  JSON_TYPE(JSON_VALUE(v, 'lax $.b' ERROR ON ERROR)) AS c2,
  JSON_TYPE(JSON_VALUE(v, 'strict $.a[0]' ERROR ON ERROR)) AS c3,
  JSON_TYPE(JSON_VALUE(v, 'strict $.a[1]' ERROR ON ERROR)) AS c4
FROM (VALUES ('{"a": [10, true],"b": "[10, true]"}')) AS t(v)
LIMIT 10;
{% endhighlight %}

Result

| c1     | c2    | c3      | c4      |
|:------:|:-----:|:-------:|:-------:|
| OBJECT | ARRAY | INTEGER | BOOLEAN |

##### JSON_DEPTH example

SQL

{% highlight sql %}
SELECT JSON_DEPTH(v) AS c1,
  JSON_DEPTH(JSON_VALUE(v, 'lax $.b' ERROR ON ERROR)) AS c2,
  JSON_DEPTH(JSON_VALUE(v, 'strict $.a[0]' ERROR ON ERROR)) AS c3,
  JSON_DEPTH(JSON_VALUE(v, 'strict $.a[1]' ERROR ON ERROR)) AS c4
FROM (VALUES ('{"a": [10, true],"b": "[10, true]"}')) AS t(v)
LIMIT 10;
{% endhighlight %}

Result

| c1     | c2    | c3      | c4      |
|:------:|:-----:|:-------:|:-------:|
| 3      | 2     | 1       | 1       |

##### JSON_LENGTH example

SQL

{% highlight sql %}
SELECT JSON_LENGTH(v) AS c1,
  JSON_LENGTH(v, 'lax $.a') AS c2,
  JSON_LENGTH(v, 'strict $.a[0]') AS c3,
  JSON_LENGTH(v, 'strict $.a[1]') AS c4
FROM (VALUES ('{"a": [10, true]}')) AS t(v)
LIMIT 10;
{% endhighlight %}

Result

| c1     | c2    | c3      | c4      |
|:------:|:-----:|:-------:|:-------:|
| 1      | 2     | 1       | 1       |

##### JSON_INSERT example

SQL

```SQL
SELECT JSON_INSERT(v, '$.a', 10, '$.c', '[1]') AS c1,
  JSON_INSERT(v, '$', 10, '$.c', '[1]') AS c2
FROM (VALUES ('{"a": [10, true]}')) AS t(v)
LIMIT 10;
```

Result

| c1                             | c2                            |
| ------------------------------ | ----------------------------- |
| {"a":1 , "b":[2] , "c":"[1]"}  | {"a":1 , "b":[2] , "c":"[1]"} |

##### JSON_KEYS example

SQL

{% highlight sql %}
SELECT JSON_KEYS(v) AS c1,
  JSON_KEYS(v, 'lax $.a') AS c2,
  JSON_KEYS(v, 'lax $.b') AS c2,
  JSON_KEYS(v, 'strict $.a[0]') AS c3,
  JSON_KEYS(v, 'strict $.a[1]') AS c4
FROM (VALUES ('{"a": [10, true],"b": {"c": 30}}')) AS t(v)
LIMIT 10;
{% endhighlight %}

 Result

| c1         | c2   | c3    | c4   | c5   |
|:----------:|:----:|:-----:|:----:|:----:|
| ["a", "b"] | NULL | ["c"] | NULL | NULL |

##### JSON_REMOVE example

SQL

{% highlight sql %}
SELECT JSON_REMOVE(v, '$[1]') AS c1
FROM (VALUES ('["a", ["b", "c"], "d"]')) AS t(v)
LIMIT 10;
{% endhighlight %}

 Result

| c1         |
|:----------:|
| ["a", "d"] |

##### JSON_REPLACE example

SQL

 ```SQL
SELECT
JSON_REPLACE(v, '$.a', 10, '$.c', '[1]') AS c1,
JSON_REPLACE(v, '$', 10, '$.c', '[1]') AS c2
FROM (VALUES ('{\"a\": 1,\"b\":[2]}')) AS t(v)
limit 10;
```

 Result

| c1                             | c2                              |
| ------------------------------ | ------------------------------- |
| {"a":1 , "b":[2] , "c":"[1]"}  | {"a":1 , "b":[2] , "c":"[1]"}") |

##### JSON_SET example

SQL

 ```SQL
SELECT
JSON_SET(v, '$.a', 10, '$.c', '[1]') AS c1,
JSON_SET(v, '$', 10, '$.c', '[1]') AS c2
FROM (VALUES ('{\"a\": 1,\"b\":[2]}')) AS t(v)
limit 10;
```

 Result

| c1                 | c2 |
| -------------------| -- |
| {"a":10, "b":[2]}  | 10 |

##### JSON_STORAGE_SIZE example

SQL

{% highlight sql %}
SELECT
JSON_STORAGE_SIZE('[100, \"sakila\", [1, 3, 5], 425.05]') AS c1,
JSON_STORAGE_SIZE('{\"a\": 10, \"b\": \"a\", \"c\": \"[1, 3, 5, 7]\"}') AS c2,
JSON_STORAGE_SIZE('{\"a\": 10, \"b\": \"xyz\", \"c\": \"[1, 3, 5, 7]\"}') AS c3,
JSON_STORAGE_SIZE('[100, \"json\", [[10, 20, 30], 3, 5], 425.05]') AS c4
limit 10;
{% endhighlight %}

 Result

| c1 | c2 | c3 | c4 |
|:--:|:---:|:---:|:--:|
| 29 | 35 | 37 | 36 |


#### DECODE example

SQL

{% highlight sql %}
SELECT DECODE(f1, 1, 'aa', 2, 'bb', 3, 'cc', 4, 'dd', 'ee') as c1,
  DECODE(f2, 1, 'aa', 2, 'bb', 3, 'cc', 4, 'dd', 'ee') as c2,
  DECODE(f3, 1, 'aa', 2, 'bb', 3, 'cc', 4, 'dd', 'ee') as c3,
  DECODE(f4, 1, 'aa', 2, 'bb', 3, 'cc', 4, 'dd', 'ee') as c4,
  DECODE(f5, 1, 'aa', 2, 'bb', 3, 'cc', 4, 'dd', 'ee') as c5
FROM (VALUES (1, 2, 3, 4, 5)) AS t(f1, f2, f3, f4, f5);
{% endhighlight %}

 Result

| c1          | c2          | c3          | c4          | c5          |
|:-----------:|:-----------:|:-----------:|:-----------:|:-----------:|
| aa          | bb          | cc          | dd          | ee          |

#### TRANSLATE example

SQL

{% highlight sql %}
SELECT TRANSLATE('Aa*Bb*Cc''D*d', ' */''%', '_') as c1,
  TRANSLATE('Aa/Bb/Cc''D/d', ' */''%', '_') as c2,
  TRANSLATE('Aa Bb Cc''D d', ' */''%', '_') as c3,
  TRANSLATE('Aa%Bb%Cc''D%d', ' */''%', '_') as c4
FROM (VALUES (true)) AS t(f0);
{% endhighlight %}

Result

| c1          | c2          | c3          | c4          |
|:-----------:|:-----------:|:-----------:|:-----------:|
| Aa_Bb_CcD_d | Aa_Bb_CcD_d | Aa_Bb_CcD_d | Aa_Bb_CcD_d |

### Higher-order Functions

A higher-order function takes one or more lambda expressions as arguments.

Lambda Expression Syntax:
{% highlight sql %}
lambdaExpression:
      parameters '->' expression

parameters:
      '(' [ identifier [, identifier ] ] ')'
  |   identifier
{% endhighlight %}

Higher-order functions are not included in the SQL standard, so all the functions will be listed in the
[Dialect-specific OperatorsPermalink]({{ site.baseurl }}/docs/reference.html#dialect-specific-operators)
as well.

Examples of functions with a lambda argument are *EXISTS*.

## User-defined functions

Calcite is extensible. You can define each kind of function using user code.
For each kind of function there are often several ways to define a function,
varying from convenient to efficient.

To implement a *scalar function*, there are 3 options:

* Create a class with a public static `eval` method,
  and register the class;
* Create a class with a public non-static `eval` method,
  and a public constructor with no arguments,
  and register the class;
* Create a class with one or more public static methods,
  and register each class/method combination.

To implement an *aggregate function*, there are 2 options:

* Create a class with public static `init`, `add` and `result` methods,
  and register the class;
* Create a class with public non-static `init`, `add` and `result` methods,
  and a  public constructor with no arguments,
  and register the class.

Optionally, add a public `merge` method to the class; this allows Calcite to
generate code that merges sub-totals.

Optionally, make your class implement the
[SqlSplittableAggFunction]({{ site.apiRoot }}/org/apache/calcite/sql/SqlSplittableAggFunction.html)
interface; this allows Calcite to decompose the function across several stages
of aggregation, roll up from summary tables, and push it through joins.

To implement a *table function*, there are 3 options:

* Create a class with a static `eval` method that returns
  [ScannableTable]({{ site.apiRoot }}/org/apache/calcite/schema/ScannableTable.html)
  or
  [QueryableTable]({{ site.apiRoot }}/org/apache/calcite/schema/QueryableTable.html),
  and register the class;
* Create a class with a non-static `eval` method that returns
  [ScannableTable]({{ site.apiRoot }}/org/apache/calcite/schema/ScannableTable.html)
  or
  [QueryableTable]({{ site.apiRoot }}/org/apache/calcite/schema/QueryableTable.html),
  and register the class;
* Create a class with one or more public static methods that return
  [ScannableTable]({{ site.apiRoot }}/org/apache/calcite/schema/ScannableTable.html)
  or
  [QueryableTable]({{ site.apiRoot }}/org/apache/calcite/schema/QueryableTable.html),
  and register each class/method combination.

To implement a *table macro*, there are 3 options:

* Create a class with a static `eval` method that returns
  [TranslatableTable]({{ site.apiRoot }}/org/apache/calcite/schema/TranslatableTable.html),
  and register the class;
* Create a class with a non-static `eval` method that returns
  [TranslatableTable]({{ site.apiRoot }}/org/apache/calcite/schema/TranslatableTable.html),
  and register the class;
* Create a class with one or more public static methods that return
  [TranslatableTable]({{ site.apiRoot }}/org/apache/calcite/schema/TranslatableTable.html),
  and register each class/method combination.

Calcite deduces the parameter types and result type of a function from the
parameter and return types of the Java method that implements it. Further, you
can specify the name and optionality of each parameter using the
[Parameter]({{ site.apiRoot }}/org/apache/calcite/linq4j/function/Parameter.html)
annotation.

### Calling functions with named and optional parameters

Usually when you call a function, you need to specify all of its parameters,
in order. But that can be a problem if a function has a lot of parameters,
and especially if you want to add more parameters over time.

To solve this problem, the SQL standard allows you to pass parameters by name,
and to define parameters which are optional (that is, have a default value
that is used if they are not specified).

Suppose you have a function `f`, declared as in the following pseudo syntax:

{% highlight sql %}
FUNCTION f(
  INTEGER a,
  INTEGER b DEFAULT NULL,
  INTEGER c,
  INTEGER d DEFAULT NULL,
  INTEGER e DEFAULT NULL) RETURNS INTEGER
{% endhighlight %}

All of the function's parameters have names, and parameters `b`, `d` and `e`
have a default value of `NULL` and are therefore optional.
(In Calcite, `NULL` is the only allowable default value for optional parameters;
this may change
[in future](https://issues.apache.org/jira/browse/CALCITE-947).)

When calling a function with optional parameters,
you can omit optional arguments at the end of the list, or use the `DEFAULT`
keyword for any optional arguments.
Here are some examples:

* `f(1, 2, 3, 4, 5)` provides a value to each parameter, in order;
* `f(1, 2, 3, 4)` omits `e`, which gets its default value, `NULL`;
* `f(1, DEFAULT, 3)` omits `d` and `e`,
  and specifies to use the default value of `b`;
* `f(1, DEFAULT, 3, DEFAULT, DEFAULT)` has the same effect as the previous
  example;
* `f(1, 2)` is not legal, because `c` is not optional;
* `f(1, 2, DEFAULT, 4)` is not legal, because `c` is not optional.

You can specify arguments by name using the `=>` syntax.
If one argument is named, they all must be.
Arguments may be in any other, but must not specify any argument more than once,
and you need to provide a value for every parameter which is not optional.
Here are some examples:

* `f(c => 3, d => 1, a => 0)` is equivalent to `f(0, NULL, 3, 1, NULL)`;
* `f(c => 3, d => 1)` is not legal, because you have not specified a value for
  `a` and `a` is not optional.

### SQL Hints

A hint is an instruction to the optimizer. When writing SQL, you may know information about
the data unknown to the optimizer. Hints enable you to make decisions normally made by the optimizer.

* Planner enforcers: there's no perfect planner, so it makes sense to implement hints to
allow user better control the execution. For instance: "never merge this subquery with others" (`/*+ no_merge */`);
“treat those tables as leading ones" (`/*+ leading */`) to affect join ordering, etc;
* Append meta data/statistics: some statistics like “table index for scan” or “skew info of some shuffle keys”
are somehow dynamic for the query, it would be very convenient to config them with hints because
our planning metadata from the planner is very often not very accurate;
* Operator resource constraints: for many cases, we would give a default resource configuration
for the execution operators,
i.e. min parallelism, memory (resource consuming UDF), special resource requirement (GPU or SSD disk) ...
It would be very flexible to profile the resource with hints per query (not the Job).

#### Syntax

Calcite supports hints in two locations:

* Query Hint: right after the `SELECT` keyword;
* Table Hint: right after the referenced table name.

For example:
{% highlight sql %}
SELECT /*+ hint1, hint2(a=1, b=2) */
...
FROM
  tableName /*+ hint3(5, 'x') */
JOIN
  tableName /*+ hint4(c=id), hint5 */
...
{% endhighlight %}

The syntax is as follows:

{% highlight sql %}
hintComment:
      '/*+' hint [, hint ]* '*/'

hint:
      hintName
  |   hintName '(' optionKey '=' optionVal [, optionKey '=' optionVal ]* ')'
  |   hintName '(' hintOption [, hintOption ]* ')'

optionKey:
      simpleIdentifier
  |   stringLiteral

optionVal:
      stringLiteral

hintOption:
      simpleIdentifier
   |  numericLiteral
   |  stringLiteral
{% endhighlight %}

It is experimental in Calcite, and yet not fully implemented, what we have implemented are:

* The parser support for the syntax above;
* `RelHint` to represent a hint item;
* Mechanism to propagate the hints, during sql-to-rel conversion and planner planning.

We do not add any builtin hint items yet, would introduce more if we think the hints is stable enough.

### MATCH_RECOGNIZE

`MATCH_RECOGNIZE` is a SQL extension for recognizing sequences of
events in complex event processing (CEP).

It is experimental in Calcite, and yet not fully implemented.

#### Syntax

{% highlight sql %}
matchRecognize:
      MATCH_RECOGNIZE '('
      [ PARTITION BY expression [, expression ]* ]
      [ ORDER BY orderItem [, orderItem ]* ]
      [ MEASURES measureColumn [, measureColumn ]* ]
      [ ONE ROW PER MATCH | ALL ROWS PER MATCH ]
      [ AFTER MATCH skip ]
      PATTERN '(' pattern ')'
      [ WITHIN intervalLiteral ]
      [ SUBSET subsetItem [, subsetItem ]* ]
      DEFINE variable AS condition [, variable AS condition ]*
      ')'

skip:
      SKIP TO NEXT ROW
  |   SKIP PAST LAST ROW
  |   SKIP TO FIRST variable
  |   SKIP TO LAST variable
  |   SKIP TO variable

subsetItem:
      variable = '(' variable [, variable ]* ')'

measureColumn:
      expression AS alias

pattern:
      patternTerm [ '|' patternTerm ]*

patternTerm:
      patternFactor [ patternFactor ]*

patternFactor:
      patternPrimary [ patternQuantifier ]

patternPrimary:
      variable
  |   '$'
  |   '^'
  |   '(' [ pattern ] ')'
  |   '{-' pattern '-}'
  |   PERMUTE '(' pattern [, pattern ]* ')'

patternQuantifier:
      '*'
  |   '*?'
  |   '+'
  |   '+?'
  |   '?'
  |   '??'
  |   '{' { [ minRepeat ], [ maxRepeat ] } '}' ['?']
  |   '{' repeat '}'

intervalLiteral:
      INTERVAL 'string' timeUnit [ TO timeUnit ]
{% endhighlight %}

In *patternQuantifier*, *repeat* is a positive integer,
and *minRepeat* and *maxRepeat* are non-negative integers.

### DDL Extensions

DDL extensions are only available in the calcite-server module.
To enable, include `calcite-server.jar` in your class path, and add
`parserFactory=org.apache.calcite.server.ServerDdlExecutor#PARSER_FACTORY`
to the JDBC connect string (see connect string property
[parserFactory]({{ site.apiRoot }}/org/apache/calcite/config/CalciteConnectionProperty.html#PARSER_FACTORY)).

{% highlight sql %}
ddlStatement:
      createSchemaStatement
  |   createForeignSchemaStatement
  |   createTableStatement
  |   createTableLikeStatement
  |   createViewStatement
  |   createMaterializedViewStatement
  |   createTypeStatement
  |   createFunctionStatement
  |   dropSchemaStatement
  |   dropForeignSchemaStatement
  |   dropTableStatement
  |   dropViewStatement
  |   dropMaterializedViewStatement
  |   dropTypeStatement
  |   dropFunctionStatement

createSchemaStatement:
      CREATE [ OR REPLACE ] SCHEMA [ IF NOT EXISTS ] name

createForeignSchemaStatement:
      CREATE [ OR REPLACE ] FOREIGN SCHEMA [ IF NOT EXISTS ] name
      (
          TYPE 'type'
      |   LIBRARY 'com.example.calcite.ExampleSchemaFactory'
      )
      [ OPTIONS '(' option [, option ]* ')' ]

option:
      name literal

createTableStatement:
      CREATE TABLE [ IF NOT EXISTS ] name
      [ '(' tableElement [, tableElement ]* ')' ]
      [ AS query ]

createTableLikeStatement:
      CREATE TABLE [ IF NOT EXISTS ] name LIKE sourceTable
      [ likeOption [, likeOption ]* ]

likeOption:
      { INCLUDING | EXCLUDING } { DEFAULTS | GENERATED | ALL }

createTypeStatement:
      CREATE [ OR REPLACE ] TYPE name AS
      {
          baseType
      |   '(' attributeDef [, attributeDef ]* ')'
      }

attributeDef:
      attributeName type
      [ COLLATE collation ]
      [ NULL | NOT NULL ]
      [ DEFAULT expression ]

tableElement:
      columnName type [ columnGenerator ] [ columnConstraint ]
  |   columnName
  |   tableConstraint

columnGenerator:
      DEFAULT expression
  |   [ GENERATED ALWAYS ] AS '(' expression ')'
      { VIRTUAL | STORED }

columnConstraint:
      [ CONSTRAINT name ]
      [ NOT ] NULL

tableConstraint:
      [ CONSTRAINT name ]
      {
          CHECK '(' expression ')'
      |   PRIMARY KEY '(' columnName [, columnName ]* ')'
      |   UNIQUE '(' columnName [, columnName ]* ')'
      }

createViewStatement:
      CREATE [ OR REPLACE ] VIEW name
      [ '(' columnName [, columnName ]* ')' ]
      AS query

createMaterializedViewStatement:
      CREATE MATERIALIZED VIEW [ IF NOT EXISTS ] name
      [ '(' columnName [, columnName ]* ')' ]
      AS query

createFunctionStatement:
      CREATE [ OR REPLACE ] FUNCTION [ IF NOT EXISTS ] name
      AS classNameLiteral
      [ USING  usingFile [, usingFile ]* ]

usingFile:
      { JAR | FILE | ARCHIVE } filePathLiteral

dropSchemaStatement:
      DROP SCHEMA [ IF EXISTS ] name

dropForeignSchemaStatement:
      DROP FOREIGN SCHEMA [ IF EXISTS ] name

dropTableStatement:
      DROP TABLE [ IF EXISTS ] name

dropViewStatement:
      DROP VIEW [ IF EXISTS ] name

dropMaterializedViewStatement:
      DROP MATERIALIZED VIEW [ IF EXISTS ] name

dropTypeStatement:
      DROP TYPE [ IF EXISTS ] name

dropFunctionStatement:
      DROP FUNCTION [ IF EXISTS ] name

truncateTableStatement:
      TRUNCATE TABLE name
      [ CONTINUE IDENTITY | RESTART IDENTITY ]
{% endhighlight %}

In *createTableStatement*, if you specify *AS query*, you may omit the list of
*tableElement*s, or you can omit the data type of any *tableElement*, in which
case it just renames the underlying column.

In *columnGenerator*, if you do not specify `VIRTUAL` or `STORED` for a
generated column, `VIRTUAL` is the default.

In *createFunctionStatement* and *usingFile*, *classNameLiteral*
and *filePathLiteral* are character literals.


#### Declaring objects for user-defined types

After an object type is defined and installed in the schema, you can use it to
declare objects in any SQL block. For example, you can use the object type to
specify the datatype of an attribute, column, variable, bind variable, record
field, table element, formal parameter, or function result. At run time,
instances of the object type are created; that is, objects of that type are
instantiated. Each object can hold different values.

For example, we can declare types `address_typ` and `employee_typ`:

{% highlight sql %}
CREATE TYPE address_typ AS (
   street          VARCHAR(30),
   city            VARCHAR(20),
   state           CHAR(2),
   postal_code     VARCHAR(6));

CREATE TYPE employee_typ AS (
  employee_id       DECIMAL(6),
  first_name        VARCHAR(20),
  last_name         VARCHAR(25),
  email             VARCHAR(25),
  phone_number      VARCHAR(20),
  hire_date         DATE,
  job_id            VARCHAR(10),
  salary            DECIMAL(8,2),
  commission_pct    DECIMAL(2,2),
  manager_id        DECIMAL(6),
  department_id     DECIMAL(4),
  address           address_typ);
{% endhighlight %}

Using these types, you can instantiate objects as follows:

{% highlight sql %}
employee_typ(315, 'Francis', 'Logan', 'FLOGAN',
    '555.777.2222', DATE '2004-05-01', 'SA_MAN', 11000, .15, 101, 110,
     address_typ('376 Mission', 'San Francisco', 'CA', '94222'))
{% endhighlight %}
