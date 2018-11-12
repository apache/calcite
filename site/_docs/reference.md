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

setStatement:
      [ ALTER ( SYSTEM | SESSION ) ] SET identifier '=' expression

resetStatement:
      [ ALTER ( SYSTEM | SESSION ) ] RESET identifier
  |   [ ALTER ( SYSTEM | SESSION ) ] RESET ALL

explain:
      EXPLAIN PLAN
      [ WITH TYPE | WITH IMPLEMENTATION | WITHOUT IMPLEMENTATION ]
      [ EXCLUDING ATTRIBUTES | INCLUDING [ ALL ] ATTRIBUTES ]
      [ AS JSON | AS XML ]
      FOR ( query | insert | update | merge | delete )

describe:
      DESCRIBE DATABASE databaseName
   |  DESCRIBE CATALOG [ databaseName . ] catalogName
   |  DESCRIBE SCHEMA [ [ databaseName . ] catalogName ] . schemaName
   |  DESCRIBE [ TABLE ] [ [ [ databaseName . ] catalogName . ] schemaName . ] tableName [ columnName ]
   |  DESCRIBE [ STATEMENT ] ( query | insert | update | merge | delete )

insert:
      ( INSERT | UPSERT ) INTO tablePrimary
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
  |   WITH withItem [ , withItem ]* query
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
      SELECT [ STREAM ] [ ALL | DISTINCT ]
          { * | projectItem [, projectItem ]* }
      FROM tableExpression
      [ WHERE booleanExpression ]
      [ GROUP BY { groupItem [, groupItem ]* } ]
      [ HAVING booleanExpression ]
      [ WINDOW windowName AS windowSpec [, windowName AS windowSpec ]* ]

selectWithoutFrom:
      SELECT [ ALL | DISTINCT ]
          { * | projectItem [, projectItem ]* }

projectItem:
      expression [ [ AS ] columnAlias ]
  |   tableAlias . *

tableExpression:
      tableReference [, tableReference ]*
  |   tableExpression [ NATURAL ] [ ( LEFT | RIGHT | FULL ) [ OUTER ] ] JOIN tableExpression [ joinCondition ]
  |   tableExpression CROSS JOIN tableExpression
  |   tableExpression [ CROSS | OUTER ] APPLY tableExpression

joinCondition:
      ON booleanExpression
  |   USING '(' column [, column ]* ')'

tableReference:
      tablePrimary
      [ matchRecognize ]
      [ [ AS ] alias [ '(' columnAlias [, columnAlias ]* ')' ] ]

tablePrimary:
      [ [ catalogName . ] schemaName . ] tableName
      '(' TABLE [ [ catalogName . ] schemaName . ] tableName ')'
  |   tablePrimary [ EXTEND ] '(' columnDecl [, columnDecl ]* ')'
  |   [ LATERAL ] '(' query ')'
  |   UNNEST '(' expression ')' [ WITH ORDINALITY ]
  |   [ LATERAL ] TABLE '(' [ SPECIFIC ] functionName '(' expression [, expression ]* ')' ')'

columnDecl:
      column type [ NOT NULL ]

values:
      VALUES expression [, expression ]*

groupItem:
      expression
  |   '(' ')'
  |   '(' expression [, expression ]* ')'
  |   CUBE '(' expression [, expression ]* ')'
  |   ROLLUP '(' expression [, expression ]* ')'
  |   GROUPING SETS '(' groupItem [, groupItem ]* ')'

windowRef:
      windowName
  |   windowSpec

windowSpec:
      [ windowName ]
      '('
      [ ORDER BY orderItem [, orderItem ]* ]
      [ PARTITION BY expression [, expression ]* ]
      [
          RANGE numericOrIntervalExpression { PRECEDING | FOLLOWING }
      |   ROWS numericExpression { PRECEDING | FOLLOWING }
      ]
      ')'
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

IN, EXISTS and scalar sub-queries can occur
in any place where an expression can occur (such as the SELECT clause,
WHERE clause, ON clause of a JOIN, or as an argument to an aggregate
function).

An IN, EXISTS or scalar sub-query may be correlated; that is, it
may refer to tables in the FROM clause of an enclosing query.

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
**ARRAY_MAX_CARDINALITY**,
**AS**,
ASC,
**ASENSITIVE**,
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
DATETIME_INTERVAL_CODE,
DATETIME_INTERVAL_PRECISION,
**DAY**,
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
**HAVING**,
HIERARCHY,
**HOLD**,
**HOUR**,
**IDENTITY**,
IMMEDIATE,
IMMEDIATELY,
IMPLEMENTATION,
**IMPORT**,
**IN**,
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
**MATCH_NUMBER**,
**MATCH_RECOGNIZE**,
**MAX**,
MAXVALUE,
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
MINVALUE,
**MOD**,
**MODIFIES**,
**MODULE**,
**MONTH**,
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
QUARTER,
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
SECTION,
SECURITY,
**SEEK**,
**SELECT**,
SELF,
**SENSITIVE**,
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
**SYMMETRIC**,
**SYSTEM**,
**SYSTEM_TIME**,
**SYSTEM_USER**,
**TABLE**,
**TABLESAMPLE**,
TABLE_NAME,
TEMPORARY,
**THEN**,
TIES,
**TIME**,
**TIMESTAMP**,
TIMESTAMPADD,
TIMESTAMPDIFF,
**TIMEZONE_HOUR**,
**TIMEZONE_MINUTE**,
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
**VALUE**,
**VALUES**,
**VALUE_OF**,
**VARBINARY**,
**VARCHAR**,
**VARYING**,
**VAR_POP**,
**VAR_SAMP**,
VERSION,
**VERSIONING**,
VIEW,
WEEK,
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
| DECIMAL(p, s) | Fixed point             | Example: 123.45 is a DECIMAL(5, 2) value.
| NUMERIC     | Fixed point               |
| REAL, FLOAT | 4 byte floating point     | 6 decimal digits precision
| DOUBLE      | 8 byte floating point     | 15 decimal digits precision
| CHAR(n), CHARACTER(n) | Fixed-width character string | 'Hello', '' (empty string), _latin1'Hello', n'Hello', _UTF16'Hello', 'Hello' 'there' (literal split into multiple parts)
| VARCHAR(n), CHARACTER VARYING(n) | Variable-length character string | As CHAR(n)
| BINARY(n)   | Fixed-width binary string | x'45F0AB', x'' (empty binary string), x'AB' 'CD' (multi-part binary string literal)
| VARBINARY(n), BINARY VARYING(n) | Variable-length binary string | As BINARY(n)
| DATE        | Date                      | Example: DATE '1969-07-20'
| TIME        | Time of day               | Example: TIME '20:17:40'
| TIMESTAMP [ WITHOUT TIME ZONE ] | Date and time | Example: TIMESTAMP '1969-07-20 20:17:40'
| TIMESTAMP WITH LOCAL TIME ZONE | Date and time with local time zone | Example: TIMESTAMP '1969-07-20 20:17:40 America/Los Angeles'
| TIMESTAMP WITH TIME ZONE | Date and time with time zone | Example: TIMESTAMP '1969-07-20 20:17:40 America/Los Angeles'
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

### Non-scalar types

| Type     | Description
|:-------- |:-----------------------------------------------------------
| ANY      | A value of an unknown type
| ROW      | Row with 1 or more columns
| MAP      | Collection of keys mapped to values
| MULTISET | Unordered collection that may contain duplicates
| ARRAY    | Ordered, contiguous collection that may contain duplicates
| CURSOR   | Cursor over the result of executing a query

### Spatial types

Spatial data is represented as character strings encoded as
[well-known text (WKT)](https://en.wikipedia.org/wiki/Well-known_text)
or binary strings encoded as
[well-known binary (WKB)](https://en.wikipedia.org/wiki/Well-known_binary).

Where you would use a literal, apply the `ST_GeomFromText` function,
for example `ST_GeomFromText('POINT (30 10)')`.

| Data type   | Type code | Examples in WKT
|:----------- |:--------- |:---------------------
| GEOMETRY           |  0 | generalization of Point, Curve, Surface, GEOMETRYCOLLECTION
| POINT              |  1 | <code>ST_GeomFromText(&#8203;'POINT (30 10)')</code> is a point in 2D space; <code>ST_GeomFromText(&#8203;'POINT Z(30 10 2)')</code> is point in 3D space
| CURVE            | 13 | generalization of LINESTRING
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
| [ ] (array element)                               | left
| + - (unary plus, minus)                           | right
| * / %                                             | left
| + -                                               | left
| BETWEEN, IN, LIKE, SIMILAR, OVERLAPS, CONTAINS etc. | -
| < > = <= >= <> !=                                 | left
| IS NULL, IS FALSE, IS NOT TRUE etc.               | -
| NOT                                               | right
| AND                                               | left
| OR                                                | left

### Comparison operators

| Operator syntax                                   | Description
|:------------------------------------------------- |:-----------
| value1 = value2                                   | Equals
| value1 <> value2                                  | Not equal
| value1 != value2                                  | Not equal (only available at some conformance levels)
| value1 > value2                                   | Greater than
| value1 >= value2                                  | Greater than or equal
| value1 < value2                                   | Less than
| value1 <= value2                                  | Less than or equal
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
| value IN (value [, value]*)                       | Whether *value* is equal to a value in a list
| value NOT IN (value [, value]*)                   | Whether *value* is not equal to every value in a list
| value IN (sub-query)                              | Whether *value* is equal to a row returned by *sub-query*
| value NOT IN (sub-query)                          | Whether *value* is not equal to every row returned by *sub-query*
| value comparison SOME (sub-query)                 | Whether *value* *comparison* at least one row returned by *sub-query*
| value comparison ANY (sub-query)                  | Synonym for SOME
| value comparison ALL (sub-query)                  | Whether *value* *comparison* every row returned by *sub-query*
| EXISTS (sub-query)                                | Whether *sub-query* returns at least one row

{% highlight sql %}
comp:
      =
  |   <>
  |   >
  |   >=
  |   <
  |   <=
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
| COS(numeric)              | Returns the cosine of *numeric*
| COT(numeric)              | Returns the cotangent of *numeric*
| DEGREES(numeric)          | Converts *numeric* from radians to degrees
| PI()                      | Returns a value that is closer than any other value to *pi*
| RADIANS(numeric)          | Converts *numeric* from degrees to radians
| ROUND(numeric1 [, numeric2]) | Rounds *numeric1* to optionally *numeric2* (if not specified 0) places right to the decimal point
| SIGN(numeric)             | Returns the signum of *numeric*
| SIN(numeric)              | Returns the sine of *numeric*
| TAN(numeric)              | Returns the tangent of *numeric*
| TRUNCATE(numeric1 [, numeric2]) | Truncates *numeric1* to optionally *numeric2* (if not specified 0) places right to the decimal point

### Character string operators and functions

| Operator syntax            | Description
|:-------------------------- |:-----------
| string &#124;&#124; string | Concatenates two character strings
| CHAR_LENGTH(string)        | Returns the number of characters in a character string
| CHARACTER_LENGTH(string)   | As CHAR_LENGTH(*string*)
| UPPER(string)              | Returns a character string converted to upper case
| LOWER(string)              | Returns a character string converted to lower case
| POSITION(string1 IN string2) | Returns the position of the first occurrence of *string1* in *string2*
| POSITION(string1 IN string2 FROM integer) | Returns the position of the first occurrence of *string1* in *string2* starting at a given point (not standard SQL)
| TRIM( { BOTH &#124; LEADING &#124; TRAILING } string1 FROM string2) | Removes the longest string containing only the characters in *string1* from the start/end/both ends of *string1*
| OVERLAY(string1 PLACING string2 FROM integer [ FOR integer2 ]) | Replaces a substring of *string1* with *string2*
| SUBSTRING(string FROM integer)  | Returns a substring of a character string starting at a given point
| SUBSTRING(string FROM integer FOR integer) | Returns a substring of a character string starting at a given point with a given length
| INITCAP(string)            | Returns *string* with the first letter of each word converter to upper case and the rest to lower case. Words are sequences of alphanumeric characters separated by non-alphanumeric characters.

Not implemented:

* SUBSTRING(string FROM regexp FOR regexp)

### Binary string operators and functions

| Operator syntax | Description
|:--------------- |:-----------
| binary &#124;&#124; binary | Concatenates two binary strings
| POSITION(binary1 IN binary2) | Returns the position of the first occurrence of *binary1* in *binary2*
| POSITION(binary1 IN binary2 FROM integer) | Returns the position of the first occurrence of *binary1* in *binary2* starting at a given point (not standard SQL)
| OVERLAY(binary1 PLACING binary2 FROM integer [ FOR integer2 ]) | Replaces a substring of *binary1* with *binary2*
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

### Conditional functions and operators

| Operator syntax | Description
|:--------------- |:-----------
| CASE value<br/>WHEN value1 [, value11 ]* THEN result1<br/>[ WHEN valueN [, valueN1 ]* THEN resultN ]*<br/>[ ELSE resultZ ]<br/> END | Simple case
| CASE<br/>WHEN condition1 THEN result1<br/>[ WHEN conditionN THEN resultN ]*<br/>[ ELSE resultZ ]<br/>END | Searched case
| NULLIF(value, value) | Returns NULL if the values are the same.<br/><br/>For example, <code>NULLIF(5, 5)</code> returns NULL; <code>NULLIF(5, 0)</code> returns 5.
| COALESCE(value, value [, value ]*) | Provides a value if the first value is null.<br/><br/>For example, <code>COALESCE(NULL, 5)</code> returns 5.

### Type conversion

| Operator syntax | Description
|:--------------- | :----------
| CAST(value AS type) | Converts a value to a given type.

### Value constructors

| Operator syntax | Description
|:--------------- |:-----------
| ROW (value [, value ]*)  | Creates a row from a list of values.
| (value [, value ]* )     | Creates a row from a list of values.
| map '[' key ']'     | Returns the element of a map with a particular key.
| array '[' index ']' | Returns the element at a particular location in an array.
| ARRAY '[' value [, value ]* ']' | Creates an array from a list of values.
| MAP '[' key, value [, key, value ]* ']' | Creates a map from a list of key-value pairs.

### Collection functions

| Operator syntax | Description
|:--------------- |:-----------
| ELEMENT(value)  | Returns the sole element of a array or multiset; null if the collection is empty; throws if it has more than one element.
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
    <td>period1 CONTAINS dateTime</td>
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
      (dateTime, dateTime)
  |   (dateTime, interval)
  |   PERIOD (dateTime, dateTime)
  |   PERIOD (dateTime, interval)
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
| {fn ROUND(numeric1, numeric2)}    | Rounds *numeric1* to *numeric2* places right to the decimal point
| {fn SIGN(numeric)}                | Returns the signum of *numeric*
| {fn SIN(numeric)}                 | Returns the sine of *numeric*
| {fn SQRT(numeric)}                | Returns the square root of *numeric*
| {fn TAN(numeric)}                 | Returns the tangent of *numeric*
| {fn TRUNCATE(numeric1, numeric2)} | Truncates *numeric1* to *numeric2* places right to the decimal point

#### String

| Operator syntax | Description
|:--------------- |:-----------
| {fn CONCAT(character, character)} | Returns the concatenation of character strings
| {fn INSERT(string1, start, length, string2)} | Inserts *string2* into a slot in *string1*
| {fn LCASE(string)}            | Returns a string in which all alphabetic characters in *string* have been converted to lower case
| {fn LENGTH(string)} | Returns the number of characters in a string
| {fn LOCATE(string1, string2 [, integer])} | Returns the position in *string2* of the first occurrence of *string1*. Searches from the beginning of *string2*, unless *integer* is specified.
| {fn LTRIM(string)} | Returns *string* with leading space characters removed
| {fn RTRIM(string)} | Returns *string* with trailing space characters removed
| {fn SUBSTRING(string, offset, length)} | Returns a character string that consists of *length* characters from *string* starting at the *offset* position
| {fn UCASE(string)} | Returns a string in which all alphabetic characters in *string* have been converted to upper case
| {fn REPLACE(string, search, replacement)} | Returns a string in which all the occurrences of *search* in *string* are replaced with *replacement*; if *replacement* is the empty string, the occurrences of *search* are removed

Not implemented:

* {fn ASCII(string)} - Convert a single-character string to the corresponding ASCII code, an integer between 0 and 255
* {fn CHAR(string)}
* {fn DIFFERENCE(string, string)}
* {fn LEFT(string, integer)}
* {fn REPEAT(string, integer)}
* {fn RIGHT(string, integer)}
* {fn SOUNDEX(string)}
* {fn SPACE(integer)}

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
| {fn TIMESTAMPADD(timeUnit, count, timestamp)} | Adds an interval of *count* *timeUnit*s to a timestamp
| {fn TIMESTAMPDIFF(timeUnit, timestamp1, timestamp2)} | Subtracts *timestamp1* from *timestamp2* and returns the result in *timeUnit*s

Not implemented:

* {fn DAYNAME(date)}
* {fn MONTHNAME(date)}

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
        agg( [ ALL | DISTINCT ] value [, value ]*)
        [ WITHIN GROUP (ORDER BY orderItem [, orderItem ]*) ]
        [ FILTER (WHERE condition) ]
    |   agg(*) [ FILTER (WHERE condition) ]
{% endhighlight %}

If `FILTER` is present, the aggregate function only considers rows for which
*condition* evaluates to TRUE.

If `DISTINCT` is present, duplicate argument values are eliminated before being
passed to the aggregate function.

If `WITHIN GROUP` is present, the aggregate function sorts the input rows
according to the `ORDER BY` clause inside `WITHIN GROUP` before aggregating
values. `WITHIN GROUP` is only allowed for hypothetical set functions (`RANK`,
`DENSE_RANK`, `PERCENT_RANK` and `CUME_DIST`), inverse distribution functions
(`PERCENTILE_CONT` and `PERCENTILE_DISC`) and collection functions (`COLLECT`
and `LISTAGG`).

| Operator syntax                    | Description
|:---------------------------------- |:-----------
| COLLECT( [ ALL &#124; DISTINCT ] value)       | Returns a multiset of the values
| COUNT( [ ALL &#124; DISTINCT ] value [, value ]*) | Returns the number of input rows for which *value* is not null (wholly not null if *value* is composite)
| COUNT(*)                           | Returns the number of input rows
| FUSION( multiset )                 | Returns the multiset union of *multiset* across all input values
| APPROX_COUNT_DISTINCT(value [, value ]*)      | Returns the approximate number of distinct values of *value*; the database is allowed to use an approximation but is not required to
| AVG( [ ALL &#124; DISTINCT ] numeric)         | Returns the average (arithmetic mean) of *numeric* across all input values
| SUM( [ ALL &#124; DISTINCT ] numeric)         | Returns the sum of *numeric* across all input values
| MAX( [ ALL &#124; DISTINCT ] value)           | Returns the maximum value of *value* across all input values
| MIN( [ ALL &#124; DISTINCT ] value)           | Returns the minimum value of *value* across all input values
| ANY_VALUE( [ ALL &#124; DISTINCT ] value)     | Returns one of the values of *value* across all input values; this is NOT specified in the SQL standard
| STDDEV_POP( [ ALL &#124; DISTINCT ] numeric)  | Returns the population standard deviation of *numeric* across all input values
| STDDEV_SAMP( [ ALL &#124; DISTINCT ] numeric) | Returns the sample standard deviation of *numeric* across all input values
| VAR_POP( [ ALL &#124; DISTINCT ] value)       | Returns the population variance (square of the population standard deviation) of *numeric* across all input values
| VAR_SAMP( [ ALL &#124; DISTINCT ] numeric)    | Returns the sample variance (square of the sample standard deviation) of *numeric* across all input values
| COVAR_POP(numeric1, numeric2)      | Returns the population covariance of the pair (*numeric1*, *numeric2*) across all input values
| COVAR_SAMP(numeric1, numeric2)     | Returns the sample covariance of the pair (*numeric1*, *numeric2*) across all input values
| REGR_COUNT(numeric1, numeric2)     | Returns the number of rows where both dependent and independent expressions are not null
| REGR_SXX(numeric1, numeric2)       | Returns the sum of squares of the dependent expression in a linear regression model
| REGR_SYY(numeric1, numeric2)       | Returns the sum of squares of the independent expression in a linear regression model

Not implemented:

* LISTAGG(string)
* REGR_AVGX(numeric1, numeric2)
* REGR_AVGY(numeric1, numeric2)
* REGR_INTERCEPT(numeric1, numeric2)
* REGR_R2(numeric1, numeric2)
* REGR_SLOPE(numeric1, numeric2)
* REGR_SXY(numeric1, numeric2)

### Window functions

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

Not implemented:

* COUNT(DISTINCT value [, value ]*) OVER window
* APPROX_COUNT_DISTINCT(value [, value ]*) OVER window
* FIRST_VALUE(value) IGNORE NULLS OVER window
* LAST_VALUE(value) IGNORE NULLS OVER window
* PERCENT_RANK(value) OVER window
* CUME_DIST(value) OVER window
* NTH_VALUE(value, nth) [ FROM { FIRST | LAST } ] IGNORE NULLS OVER window

### Grouping functions

| Operator syntax      | Description
|:-------------------- |:-----------
| GROUPING(expression [, expression ]*) | Returns a bit vector of the given grouping expressions
| GROUP_ID()           | Returns an integer that uniquely identifies the combination of grouping keys
| GROUPING_ID(expression [, expression ]*) | Synonym for `GROUPING`

### Grouped window functions

Grouped window functions occur in the `GROUP BY` clause and define a key value
that represents a window containing several rows.

In some window functions, a row may belong to more than one window.
For example, if a query is grouped using
`HOP(t, INTERVAL '2' HOUR, INTERVAL '1' HOUR)`, a row with timestamp '10:15:00'
 will occur in both the 10:00 - 11:00 and 11:00 - 12:00 totals.

| Operator syntax      | Description
|:-------------------- |:-----------
| HOP(dateTime, slide, size [, time ]) | Indicates a hopping window for *dateTime*, covering rows within the interval of *size*, shifting every *slide*, and optionally aligned at *time*
| SESSION(dateTime, interval [, time ]) | Indicates a session window of *interval* for *dateTime*, optionally aligned at *time*
| TUMBLE(dateTime, interval [, time ]) | Indicates a tumbling window of *interval* for *dateTime*, optionally aligned at *time*

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
[version 1.2.1](http://www.opengeospatial.org/standards/sfs);
"p" indicates that the function is a
[PostGIS](http://www.postgis.net/docs/reference.html) extension to OpenGIS.

#### Geometry conversion functions (2D)

| C | Operator syntax      | Description
|:- |:-------------------- |:-----------
| p | ST_AsText(geom) | Alias for `ST_AsWKT`
| o | ST_AsWKT(geom) | Converts *geom* → WKT
| o | ST_GeomFromText(wkt [, srid ]) | Returns a specified GEOMETRY value from WKT representation
| o | ST_LineFromText(wkt [, srid ]) | Converts WKT → LINESTRING
| o | ST_MLineFromText(wkt [, srid ]) | Converts WKT → MULTILINESTRING
| o | ST_MPointFromText(wkt [, srid ]) | Converts WKT → MULTIPOINT
| o | ST_MPolyFromText(wkt [, srid ]) Converts WKT → MULTIPOLYGON
| o | ST_PointFromText(wkt [, srid ]) | Converts WKT → POINT
| o | ST_PolyFromText(wkt [, srid ]) | Converts WKT → POLYGON

Not implemented:

* ST_AsBinary(geom) GEOMETRY → WKB
* ST_AsGML(geom) GEOMETRY → GML
* ST_Force2D(geom) 3D GEOMETRY → 2D GEOMETRY
* ST_GeomFromGML(gml [, srid ]) GML → GEOMETRY
* ST_GeomFromWKB(wkb [, srid ]) WKB → GEOMETRY
* ST_GoogleMapLink(geom [, layerType [, zoom ]]) GEOMETRY → Google map link
* ST_LineFromWKB(wkb [, srid ]) WKB → LINESTRING
* ST_OSMMapLink(geom [, marker ]) GEOMETRY → OSM map link
* ST_PointFromWKB(wkb [, srid ]) WKB → POINT
* ST_PolyFromWKB(wkb [, srid ]) WKB → POLYGON
* ST_ToMultiLine(geom) Converts the coordinates of *geom* (which may be a GEOMETRYCOLLECTION) into a MULTILINESTRING
* ST_ToMultiPoint(geom)) Converts the coordinates of *geom* (which may be a GEOMETRYCOLLECTION) into a MULTIPOINT
* ST_ToMultiSegments(geom) Converts *geom* (which may be a GEOMETRYCOLLECTION) into a set of distinct segments stored in a MULTILINESTRING

#### Geometry conversion functions (3D)

Not implemented:

* ST_Force3D(geom) 2D GEOMETRY → 3D GEOMETRY

#### Geometry creation functions (2D)

| C | Operator syntax      | Description
|:- |:-------------------- |:-----------
| o | ST_MakeLine(point1 [, point ]*) | Creates a line-string from the given POINTs (or MULTIPOINTs)
| p | ST_MakePoint(x, y [, z ]) | Alias for `ST_Point`
| o | ST_Point(x, y [, z ]) | Constructs a point from two or three coordinates

Not implemented:

* ST_BoundingCircle(geom) Returns the minimum bounding circle of *geom*
* ST_Expand(geom, distance) Expands *geom*'s envelope
* ST_Expand(geom, deltaX, deltaY) Expands *geom*'s envelope
* ST_MakeEllipse(point, width, height) Constructs an ellipse
* ST_MakeEnvelope(xMin, yMin, xMax, yMax  [, srid ]) Creates a rectangular POLYGON
* ST_MakeGrid(geom, deltaX, deltaY) Calculates a regular grid of POLYGONs based on *geom*
* ST_MakeGridPoints(geom, deltaX, deltaY) Calculates a regular grid of points based on *geom*
* ST_MakePolygon(lineString [, hole ]*) Creates a POLYGON from *lineString* with the given holes (which are required to be closed LINESTRINGs)
* ST_MinimumDiameter(geom) Returns the minimum diameter of *geom*
* ST_MinimumRectangle(geom) Returns the minimum rectangle enclosing *geom*
* ST_OctogonalEnvelope(geom) Returns the octogonal envelope of *geom*
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
| o | ST_Distance(geom1, geom2) | Returns the distance between *geom1* and *geom2*
| o | ST_GeometryType(geom) | Returns the type of *geom*
| o | ST_GeometryTypeCode(geom) | Returns the OGC SFS type code of *geom*
| o | ST_Envelope(geom [, srid ]) | Returns the envelope of *geom* (which may be a GEOMETRYCOLLECTION) as a GEOMETRY
| o | ST_X(geom) | Returns the x-value of the first coordinate of *geom*
| o | ST_Y(geom) | Returns the y-value of the first coordinate of *geom*

Not implemented:

* ST_Centroid(geom) Returns the centroid of *geom* (which may be a GEOMETRYCOLLECTION)
* ST_CompactnessRatio(polygon) Returns the square root of *polygon*'s area divided by the area of the circle with circumference equal to its perimeter
* ST_CoordDim(geom) Returns the dimension of the coordinates of *geom*
* ST_Dimension(geom) Returns the dimension of *geom*
* ST_EndPoint(lineString) Returns the last coordinate of *lineString*
* ST_Envelope(geom [, srid ]) Returns the envelope of *geom* (which may be a GEOMETRYCOLLECTION) as a GEOMETRY
* ST_Explode(query [, fieldName]) Explodes the GEOMETRYCOLLECTIONs in the *fieldName* column of a query into multiple geometries
* ST_Extent(geom) Returns the minimum bounding box of *geom* (which may be a GEOMETRYCOLLECTION)
* ST_ExteriorRing(polygon) Returns the exterior ring of *polygon* as a linear-ring
* ST_GeometryN(geomCollection, n) Returns the *n*th GEOMETRY of *geomCollection*
* ST_InteriorRingN(polygon, n) Returns the *n*th interior ring of *polygon*
* ST_IsClosed(geom) Returns whether *geom* is a closed LINESTRING or MULTILINESTRING
* ST_IsEmpty(geom) Returns whether *geom* is empty
* ST_IsRectangle(geom) Returns whether *geom* is a rectangle
* ST_IsRing(geom) Returns whether *geom* is a closed and simple line-string or MULTILINESTRING
* ST_IsSimple(geom) Returns whether *geom* is simple
* ST_IsValid(geom) Returns whether *geom* is valid
* ST_IsValidDetail(geom [, selfTouchValid ]) Returns a valid detail as an array of objects
* ST_IsValidReason(geom [, selfTouchValid ]) Returns text stating whether *geom* is valid, and if not valid, a reason why
* ST_NPoints(geom) Returns the number of points in *geom*
* ST_NumGeometries(geom) Returns the number of geometries in *geom* (1 if it is not a GEOMETRYCOLLECTION)
* ST_NumInteriorRing(geom) Alias for `ST_NumInteriorRings`
* ST_NumInteriorRings(geom) Returns the number of interior rings of *geom*
* ST_NumPoints(lineString) Returns the number of points in *lineString*
* ST_PointN(geom, n) Returns the *n*th point of a *lineString*
* ST_PointOnSurface(geom) Returns an interior or boundary point of *geom*
* ST_SRID(geom) Returns SRID value of *geom* or 0 if it does not have one
* ST_StartPoint(lineString) Returns the first coordinate of *lineString*
* ST_XMax(geom) Returns the maximum x-value of *geom*
* ST_XMin(geom) Returns the minimum x-value of *geom*
* ST_YMax(geom) Returns the maximum y-value of *geom*
* ST_YMin(geom) Returns the minimum y-value of *geom*

#### Geometry properties (3D)

| C | Operator syntax      | Description
|:- |:-------------------- |:-----------
| p | ST_Is3D(s) | Returns whether *geom* has at least one z-coordinate
| o | ST_Z(geom) | Returns the z-value of the first coordinate of *geom*

Not implemented:

* ST_ZMax(geom) Returns the maximum z-value of *geom*
* ST_ZMin(geom) Returns the minimum z-value of *geom*

### Geometry predicates

| C | Operator syntax      | Description
|:- |:-------------------- |:-----------
| o | ST_Contains(geom1, geom2) | Returns whether *geom1* contains *geom2*
| p | ST_ContainsProperly(geom1, geom2) | Returns whether *geom1* contains *geom2* but does not intersect its boundary
| o | ST_Crosses(geom1, geom2) | Returns whether *geom1* crosses *geom2*
| o | ST_Disjoint(geom1, geom2) | Returns whether *geom1* and *geom2* are disjoint
| p | ST_DWithin(geom1, geom2, distance) | Returns whether *geom1* and *geom* are within *distance* of one another
| o | ST_EnvelopesIntersect(geom1, geom2) | Returns whether the envelope of *geom1* intersects the envelope of *geom2*
| o | ST_Equals(geom1, geom2) | Returns whether *geom1* equals *geom2*
| o | ST_Intersects(geom1, geom2) | Returns whether *geom1* intersects *geom2*
| o | ST_Overlaps(geom1, geom2) | Returns whether *geom1* overlaps *geom2*
| o | ST_Touches(geom1, geom2) | Returns whether *geom1* touches *geom2*
| o | ST_Within(geom1, geom2) | Returns whether *geom1* is within *geom2*

Not implemented:

* ST_Covers(geom1, geom2) Returns whether no point in *geom2* is outside *geom1*
* ST_OrderingEquals(geom1, geom2) Returns whether *geom1* equals *geom2* and their coordinates and component Geometries are listed in the same order
* ST_Relate(geom1, geom2) Returns the DE-9IM intersection matrix of *geom1* and *geom2*
* ST_Relate(geom1, geom2, iMatrix) Returns whether *geom1* and *geom2* are related by the given intersection matrix *iMatrix*

#### Geometry operators (2D)

The following functions combine 2D geometries.

| C | Operator syntax      | Description
|:- |:-------------------- |:-----------
| o | ST_Buffer(geom, distance [, quadSegs \| style ]) | Computes a buffer around *geom*
| o | ST_Union(geom1, geom2) | Computes the union of *geom1* and *geom2*
| o | ST_Union(geomCollection) | Computes the union of the geometries in *geomCollection*

See also: the `ST_Union` aggregate function.

Not implemented:

* ST_ConvexHull(geom) Computes the smallest convex polygon that contains all the points in *geom*
* ST_Difference(geom1, geom2) Computes the difference between two geometries
* ST_Intersection(geom1, geom2) Computes the intersection of two geometries
* ST_SymDifference(geom1, geom2) Computes the symmetric difference between two geometries

#### Affine transformation functions (3D and 2D)

Not implemented:

* ST_Rotate(geom, angle [, origin \| x, y]) Rotates a *geom* counter-clockwise by *angle* (in radians) about *origin* (or the point (*x*, *y*))
* ST_Scale(geom, xFactor, yFactor [, zFactor ]) Scales *geom* by multiplying the ordinates by the indicated scale factors
* ST_Translate(geom, x, y, [, z]) Translates *geom*

#### Geometry editing functions (2D)

The following functions modify 2D geometries.

Not implemented:

* ST_AddPoint(geom, point [, tolerance ]) Adds *point* to *geom* with a given *tolerance* (default 0)
* ST_CollectionExtract(geom, dimension) Filters *geom*, returning a multi-geometry of those members with a given *dimension* (1 = point, 2 = line-string, 3 = polygon)
* ST_Densify(geom, tolerance) Inserts extra vertices every *tolerance* along the line segments of *geom*
* ST_FlipCoordinates(geom) Flips the X and Y coordinates of *geom*
* ST_Holes(geom) Returns the holes in *geom* (which may be a GEOMETRYCOLLECTION)
* ST_Normalize(geom) Converts *geom* to normal form
* ST_RemoveDuplicatedCoordinates(geom) Removes duplicated coordinates from *geom*
* ST_RemoveHoles(geom) Removes a *geom*'s holes
* ST_RemovePoints(geom, poly) Removes all coordinates of *geom* located within *poly*; null if all coordinates are removed
* ST_RemoveRepeatedPoints(geom, tolerance) Removes from *geom* all repeated points (or points within *tolerance* of another point)
* ST_Reverse(geom) Reverses the vertex order of *geom*

#### Geometry editing functions (3D)

The following functions modify 3D geometries.

Not implemented:

* ST_AddZ(geom, zToAdd) Adds *zToAdd* to the z-coordinate of *geom*
* ST_Interpolate3DLine(geom) Returns *geom* with a interpolation of z values, or null if it is not a line-string or MULTILINESTRING
* ST_MultiplyZ(geom, zFactor) Returns *geom* with its z-values multiplied by *zFactor*
* ST_Reverse3DLine(geom [, sortOrder ]) Potentially reverses *geom* according to the z-values of its first and last coordinates
* ST_UpdateZ(geom, newZ [, updateCondition ]) Updates the z-values of *geom*
* ST_ZUpdateLineExtremities(geom, startZ, endZ [, interpolate ]) Updates the start and end z-values of *geom*

#### Geometry measurement functions (2D)

Not implemented:

* ST_Area(geom) Returns the area of *geom* (which may be a GEOMETRYCOLLECTION)
* ST_ClosestCoordinate(geom, point) Returns the coordinate(s) of *geom* closest to *point*
* ST_ClosestPoint(geom1, geom2) Returns the point of *geom1* closest to *geom2*
* ST_FurthestCoordinate(geom, point) Returns the coordinate(s) of *geom* that are furthest from *point*
* ST_Length(lineString) Returns the length of *lineString*
* ST_LocateAlong(geom, segmentLengthFraction, offsetDistance) Returns a MULTIPOINT containing points along the line segments of *geom* at *segmentLengthFraction* and *offsetDistance*
* ST_LongestLine(geom1, geom2) Returns the 2-dimensional longest line-string between the points of *geom1* and *geom2*
* ST_MaxDistance(geom1, geom2) Computes the maximum distance between *geom1* and *geom2*
* ST_Perimeter(polygon) Returns the length of the perimeter of *polygon* (which may be a MULTIPOLYGON)
* ST_ProjectPoint(point, lineString) Projects *point* onto a *lineString* (which may be a MULTILINESTRING)

#### Geometry measurement functions (3D)

Not implemented:

* ST_3DArea(geom) Return a polygon's 3D area
* ST_3DLength(geom) Returns the 3D length of a line-string
* ST_3DPerimeter(geom) Returns the 3D perimeter of a polygon or MULTIPOLYGON
* ST_SunPosition(point [, timestamp ]) Computes the sun position at *point* and *timestamp* (now by default)

#### Geometry processing functions (2D)

The following functions process geometries.

Not implemented:

* ST_LineIntersector(geom1, geom2) Splits *geom1* (a line-string) with *geom2*
* ST_LineMerge(geom) Merges a collection of linear components to form a line-string of maximal length
* ST_MakeValid(geom [, preserveGeomDim [, preserveDuplicateCoord [, preserveCoordDim]]]) Makes *geom* valid
* ST_Polygonize(geom) Creates a MULTIPOLYGON from edges of *geom*
* ST_PrecisionReducer(geom, n) Reduces *geom*'s precision to *n* decimal places
* ST_RingSideBuffer(geom, distance, bufferCount [, endCapStyle [, doDifference]]) Computes a ring buffer on one side
* ST_SideBuffer(geom, distance [, bufferStyle ]) Compute a single buffer on one side
* ST_Simplify(geom, distance) Simplifies *geom* using the [Douglas-Peuker algorithm](https://en.wikipedia.org/wiki/Ramer%E2%80%93Douglas%E2%80%93Peucker_algorithm) with a *distance* tolerance
* ST_SimplifyPreserveTopology(geom) Simplifies *geom*, preserving its topology
* ST_Snap(geom1, geom2, tolerance) Snaps *geom1* and *geom2* together
* ST_Split(geom1, geom2 [, tolerance]) Splits *geom1* by *geom2* using *tolerance* (default 1E-6) to determine where the point splits the line

#### Geometry projection functions

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
* ST_TriangleContouring(query \[, z1, z2, z3 ]\[, varArgs]*) Splits triangles into smaller triangles according to classes
* ST_TriangleDirection(geom) Computes the direction of steepest ascent of a triangle and returns it as a line-string
* ST_TriangleSlope(geom) Computes the slope of a triangle as a percentage
* ST_Voronoi(geom [, outDimension [, envelopePolygon ]]) Creates a Voronoi diagram

#### Triangulation functions

Not implemented:

* ST_ConstrainedDelaunay(geom [, flag [, quality ]]) Computes a constrained Delaunay triangulation based on *geom*
* ST_Delaunay(geom [, flag [, quality ]]) Computes a Delaunay triangulation based on points
* ST_Tessellate(polygon) Tessellates *polygon* (may be MULTIPOLYGON) with adaptive triangles

#### Geometry aggregate functions

Not implemented:

* ST_Accum(geom) Accumulates *geom* into a GEOMETRYCOLLECTION (or MULTIPOINT, MULTILINESTRING or MULTIPOLYGON if possible)
* ST_Collect(geom) Alias for `ST_Accum`
* ST_Union(geom) Computes the union of geometries

### JSON Functions

#### Query Functions

| Operator syntax        | Description
|:---------------------- |:-----------
| JSON_EXISTS(value, path [ { TRUE &#124; FALSE &#124; UNKNOWN &#124; ERROR ) ON ERROR } ) | Test whether a JSON **value** satisfies a search criterion described using JSON path expression **path**
| JSON_VALUE(value, path [ RETURNING type ] [ { ERROR &#124; NULL &#124; DEFAULT expr } ON EMPTY ] [ { ERROR &#124; NULL &#124; DEFAULT expr } ON ERROR ] ) | Extract an SQL scalar from a JSON **value** using JSON path expression **path**
| JSON_QUERY(value, path [ { WITHOUT [ ARRAY ] &#124; WITH [ CONDITIONAL &#124; UNCONDITIONAL ] [ ARRAY ] } WRAPPER ] [ { ERROR &#124; NULL &#124; EMPTY ARRAY &#124; EMPTY OBJECT } ON EMPTY ] [ { ERROR &#124; NULL &#124; EMPTY ARRAY &#124; EMPTY OBJECT } ON ERROR ] ) | Extract an JSON object or an JSON array from a JSON **value** using JSON path expression **path**

Note:

* The common structure `value, path` is JSON API common syntax. **value** is a character string type json input, and **path** is a JSON path expression (in character string type too), mode flag **strict** or **lax** should be specified in the beginning of **path**.
* **ON ERROR** clause, and **ON EMPTY** clause define the fallback behavior of the function when an error is thrown or a null value is about to be returned.
* **ARRAY WRAPPER** clause defines how to represent JSON array result in JSON_QUERY function. Following is a comparision to demonstrate the difference among different wrapper behaviors.

Example Data:

```JSON
{ "a": "[1,2]", "b": [1,2], "c": "hi"}
```

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
| JSON_OBJECT( { [ KEY ] name VALUE value [ FORMAT JSON ] &#124; name : value [ FORMAT JSON ] } * [ { NULL &#124; ABSENT } ON NULL ] ) | Construct json object using a series of key (**name**) value (**value**) pairs
| JSON_OBJECTAGG( { [ KEY ] name VALUE value [ FORMAT JSON ] &#124; name : value [ FORMAT JSON ] } [ { NULL &#124; ABSENT } ON NULL ] ) | Aggregate function to construct json object using a key (**name**) value (**value**) pair
| JSON_ARRAY( { value [ FORMAT JSON ] } * [ { NULL &#124; ABSENT } ON NULL ] ) | Construct json array using a series of values (**value**)
| JSON_ARRAYAGG( value [ FORMAT JSON ] [ { NULL &#124; ABSENT } ON NULL ] ) | Aggregate function to construct json array using a value (**value**)

Note:

* The flag **FORMAT JSON** indicates the value is formatted as JSON character string. When **FORMAT JSON** is used, value should be de-parse from JSON character string to SQL structured value.
* **ON NULL** clause defines how the JSON output represents null value. The default null behavior of **JSON_OBJECT** and **JSON_OBJECTAGG** is *NULL ON NULL*, and for **JSON_ARRAY** and **JSON_ARRAYAGG** it is *ABSENT ON NULL*.

#### Comparison Operators

| Operator syntax                                   | Description
|:------------------------------------------------- |:-----------
| value IS JSON [ VALUE ]                           | Whether *value* is a json value, *value* is in character string type
| value IS NOT JSON [ VALUE ]                       | Whether *value* is not a json value, *value* is in character string type
| value IS JSON SCALAR                              | Whether *value* is a json scalar value, *value* is in character string type
| value IS NOT JSON SCALAR                          | Whether *value* is not a json scalar value, *value* is in character string type
| value IS JSON OBJECT                              | Whether *value* is a json object, *value* is in character string type
| value IS NOT JSON OBJECT                          | Whether *value* is not a json object, *value* is in character string type
| value IS JSON ARRAY                               | Whether *value* is a json array, *value* is in character string type
| value IS NOT JSON ARRAY                           | Whether *value* is not a json array, *value* is in character string type

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
{% endhighlight sql %}

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
      [ AFTER MATCH
            ( SKIP TO NEXT ROW
            | SKIP PAST LAST ROW
            | SKIP TO FIRST variable
            | SKIP TO LAST variable
            | SKIP TO variable )
      ]
      PATTERN '(' pattern ')'
      [ WITHIN intervalLiteral ]
      [ SUBSET subsetItem [, subsetItem ]* ]
      DEFINE variable AS condition [, variable AS condition ]*
      ')'

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
`parserFactory=org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl#FACTORY`
to the JDBC connect string (see connect string property
[parserFactory]({{ site.apiRoot }}/org/apache/calcite/config/CalciteConnectionProperty.html#PARSER_FACTORY)).

{% highlight sql %}
ddlStatement:
      createSchemaStatement
  |   createForeignSchemaStatement
  |   createTableStatement
  |   createViewStatement
  |   createMaterializedViewStatement
  |   createTypeStatement
  |   dropSchemaStatement
  |   dropForeignSchemaStatement
  |   dropTableStatement
  |   dropViewStatement
  |   dropMaterializedViewStatement
  |   dropTypeStatement

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
{% endhighlight %}

In *createTableStatement*, if you specify *AS query*, you may omit the list of
*tableElement*s, or you can omit the data type of any *tableElement*, in which
case it just renames the underlying column.

In *columnGenerator*, if you do not specify `VIRTUAL` or `STORED` for a
generated column, `VIRTUAL` is the default.
