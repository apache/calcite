# Optiq SQL language reference

## SQL constructs

```SQL
query:
  {
      select
  |   query UNION [ ALL ] query
  |   query EXCEPT query
  |   query INTERSECT query
  }
  [ ORDER BY orderItem [, orderItem ]* ]
  [ LIMIT { count | ALL } ]
  [ OFFSET start { ROW | ROWS } ]
  [ FETCH { FIRST | NEXT } [ count ] { ROW | ROWS } ]

orderItem:
  expression [ ASC | DESC ] [ NULLS FIRST | NULLS LAST ]

select:
  SELECT [ ALL | DISTINCT ]
      { * | projectItem [, projectItem ]* }
  FROM tableExpression
  [ WHERE booleanExpression ]
  [ GROUP BY { () | expression [, expression]* } ]
  [ HAVING booleanExpression ]
  [ WINDOW windowName AS windowSpec [, windowName AS windowSpec ]* ]

projectItem:
      expression [ [ AS ] columnAlias ]
  |   tableAlias . *

tableExpression:
      tableReference [, tableReference ]*
  |   tableExpression [ NATURAL ] [ LEFT | RIGHT | FULL ] JOIN tableExpression [ joinCondition ]

joinCondition:
      ON booleanExpression
  |   USING ( column [, column ]* )

tableReference:
  tablePrimary [ [ AS ] alias [ ( columnAlias [, columnAlias ]* ) ] ]

tablePrimary:
      [ TABLE ] [ [ catalogName . ] schemaName . ] tableName
  |   ( query )
  |   VALUES expression [, expression ]*
  |   UNNEST ( expression )
  |   ( TABLE expression )

windowRef:
      windowName
  |   windowSpec

windowSpec:
  [ windowName ]
  (
      [ ORDER BY orderItem [, orderItem ]* ]
      [ PARTITION BY expression [, expression ]* ]
      {
          RANGE numericOrIntervalExpression { PRECEDING | FOLLOWING }
      |
          ROWS numericExpression { PRECEDING | FOLLOWING }
      }
  )
```

In *orderItem*, if *expression* is a positive integer *n*, it denotes
the *n*th item in the SELECT clause.

A scalar sub-query is a sub-query used as an expression. It can occur
in most places where an expression can occur (such as the SELECT
clause, WHERE clause, or as an argument to an aggregate
function). If the sub-query returns no rows, the value is NULL; if it
returns more than one row, it is an error.

A sub-query can occur in the FROM clause of a query and also in IN
and EXISTS expressions.  A sub-query that occurs in IN and
EXISTS expressions may be correlated; that is, refer to tables in
the FROM clause of an enclosing query.

## Identifiers

Identifiers are the names of tables, columns and other metadata
elements used in a SQL query.

Unquoted identifiers, such as emp, must start with a letter and can
only contain letters, digits, and underscores. They are implicitly
converted to upper case.

Quoted identifiers, such as "Employee Name", start and end with
double quotes.  They may contain virtually any character, including
spaces and other punctuation.  If you wish to include a double quote
in an identifier, use another double quote to escape it, like this:
"An employee called ""Fred""."

In Optiq, matching identifiers to the name of the referenced object is
case-sensitive.  But remember that unquoted identifiers are implicitly
converted to upper case before matching, and if the object it refers
to was created using an unquoted identifier for its name, then its
name will have been converted to upper case also.

## Data types

### Scalar types

| Data type   | Description               | Range and examples   |
| ----------- | ------------------------- | ---------------------|
| BOOLEAN   | Logical values            | Values: TRUE, FALSE, UNKNOWN
| TINYINT   | 1 byte signed integer     | Range is -255 to 256
| SMALLINT  | 2 byte signed integer     | Range is -32768 to 32767
| INTEGER, INT | 4 byte signed integer  | Range is -2147483648 to 2147483647
| BIGINT    | 8 byte signed integer     | Range is -9223372036854775808 to 9223372036854775807
| DECIMAL(p, s) | Fixed point           | Example: 123.45 is a DECIMAL(5, 2) value.
| NUMERIC   | Fixed point               |
| REAL, FLOAT | 4 byte floating point   | 6 decimal digits precision
| DOUBLE    | 8 byte floating point     | 15 decimal digits precision
| CHAR(n), CHARACTER(n) | Fixed-width character string | 'Hello', '' (empty string), _latin1'Hello', n'Hello', _UTF16'Hello', 'Hello' 'there' (literal split into multiple parts)
| VARCHAR(n), CHARACTER VARYING(n) | Variable-length character string | As CHAR(n)
| BINARY(n) | Fixed-width binary string | x'45F0AB', x'' (empty binary string), x'AB' 'CD' (multi-part binary string literal)
| VARBINARY(n), BINARY VARYING(n) | Variable-length binary string | As BINARY(n)
| DATE      | Date                      | Example: DATE '1969-07-20'
| TIME      | Time of day               | Example: TIME '20:17:40'
| TIMESTAMP [ WITHOUT TIME ZONE ] | Date and time | Example: TIMESTAMP '1969-07-20 20:17:40'
| TIMESTAMP WITH TIME ZONE | Date and time with time zone | Example: TIMESTAMP '1969-07-20 20:17:40 America/Los Angeles'
| INTERVAL timeUnit [ TO timeUnit ] | Date time interval | Examples: INTERVAL '1:5' YEAR TO MONTH, INTERVAL '45' DAY
| Anchored interval | Date time interval  | Example: (DATE '1969-07-20', DATE '1972-08-29')

Where:
```SQL
timeUnit:
  YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
```

Note:
* DATE, TIME and TIMESTAMP have no time zone. There is not even an implicit
  time zone, such as UTC (as in Java) or the local time zone. It is left to
  the user or application to supply a time zone.

### Non-scalar types

| Type     | Description
| -------- | -----------------------------------------------------------
| ANY      | A value of an unknown type
| ROW      | Row with 1 or more columns
| MAP      | Collection of keys mapped to values
| MULTISET | Unordered collection that may contain duplicates
| ARRAY    | Ordered, contiguous collection that may contain duplicates
| CURSOR   | Cursor over the result of executing a query

## Operators and functions

### Comparison operators

| Operator syntax | Description
| --------------- | -----------
| value = value
| value <> value
| value > value
| value >= value
| value < value
| value <= value
| value IS NULL
| value IS NOT NULL
| value IS DISTINCT FROM value
| value IS NOT DISTINCT FROM value
| value BETWEEN value AND value
| value NOT BETWEEN value AND value
| string LIKE string
| string NOT LIKE string
| string SIMILAR TO string
| string NOT SIMILAR TO string
| string SIMILAR TO string ESCAPE string
| string NOT SIMILAR TO string ESCAPE string
| value IN (value [, value]* )
| value NOT IN (value [, value]* )
| value IN (sub-query)
| value NOT IN (sub-query)
| EXISTS (sub-query)

### Logical operators

| Operator syntax | Description
| --------------- | -----------
| boolean OR boolean
| boolean AND boolean
| NOT boolean
| boolean IS FALSE
| boolean IS NOT FALSE
| boolean IS TRUE
| boolean IS NOT TRUE
| boolean IS UNKNOWN
| boolean IS NOT UNKNOWN

### Arithmetic operators and functions

| Operator syntax | Description
| --------------- | -----------
| + numeric
| - numeric
| numeric + numeric
| numeric - numeric
| numeric * numeric
| numeric / numeric
| POWER(numeric, numeric)
| ABS(numeric)
| MOD(numeric, numeric)
| SQRT(numeric)
| LN(numeric)
| LOG10(numeric)
| EXP(numeric)
| CEIL(numeric)
| FLOOR(numeric)

### Character string operators and functions

| Operator syntax | Description
| --------------- | -----------
| string &#124;&#124; string | Concatenates two character strings.
| CHAR_LENGTH(string)        | Returns the number of characters in a character string.
| CHARACTER_LENGTH(string)   | As CHAR_LENGTH(string)
| UPPER(string)              | Returns a character string converted to upper-case.
| LOWER(string)              | Returns a character string converted to lower-case.
| POSITION(string IN string)
| TRIM( { BOTH ;&#124; LEADING ;&#124; TRAILING } string FROM string)
| OVERLAY(string PLACING string FROM string)
| OVERLAY(string PLACING string FROM integer)
| SUBSTRING(string FROM integer)  | Returns a substring of a character string starting at a given point.
| SUBSTRING(string FROM integer FOR integer) | Returns a substring of a character string starting at a given point with a given length.
| INITCAP(string)

Not implemented:
* SUBSTRING(string FROM regexp FOR regexp)

### Binary string operators and functions

| Operator syntax | Description
| --------------- | -----------
| binary &#124;&#124; binary | Concatenates two binary strings.
| POSITION(binary IN binary)
| SUBSTRING(binary FROM integer FOR integer) | Returns a substring of a binary string starting at a given point with a given length.

### Date/time functions

| Operator syntax | Description
| --------------- | -----------
| LOCALTIME
| LOCALTIME(n)
| LOCALDATE
| LOCALTIMESTAMP
| LOCALTIMESTAMP(n)
| CURRENT_TIME
| CURRENT_DATE
| CURRENT_TIMESTAMP
| EXTRACT(timeUnit FROM datetime)

Not implemented:
* EXTRACT(timeUnit FROM interval)
* CEIL(interval)
* FLOOR(interval)
* datetime - datetime timeUnit [ TO timeUnit ]
* interval OVERLAPS interval
* + interval
* - interval
* interval + interval
* interval - interval
* interval / interval
* datetime + interval
* datetime - interval

### System functions

| Operator syntax | Description
| --------------- | -----------
| USER
| CURRENT_USER
| SESSION_USER
| SYSTEM_USER
| CURRENT_PATH
| CURRENT_ROLE

### Conditional functions and operators

| Operator syntax | Description
| --------------- | -----------
| CASE value<br/>WHEN value1 [, value11 ]* THEN result1<br/>[ WHEN valueN [, valueN1 ]* THEN resultN ]*<br/>[ ELSE resultZ ]<br/> END | Simple case
| CASE<br/>WHEN condition1 THEN result1<br/>[ WHEN conditionN THEN resultN ]*<br/>[ ELSE resultZ ]<br/>END | Searched case
| NULLIF(value, value) | Returns NULL if the values are the same. For example, <code>NULLIF(5, 5)</code> returns NULL; <code>NULLIF(5, 0)</code> returns 5.
| COALESCE(value, value [, value]* ) | Provides a value if the first value is null. For example, <code>COALESCE(NULL, 5)</code> returns 5.

### Type conversion

| Operator syntax | Description
| --------------- | -----------
| CAST(value AS type) | Converts a value to a given type.

### Value constructors

| Operator syntax | Description
| --------------- | -----------
| ROW (value [, value]* ) | Creates a row from a list of values.
| (value [, value]* )     | Creates a row from a list of values.
| map [ key ]     | Returns the element of a map with a particular key.
| array [ index ] | Returns the element at a particular location in an array.
| ARRAY [ value [, value ]* ] | Creates an array from a list of values.
| MAP [ key, value [, key, value ]* ] | Creates a map from a list of key-value pairs.

### Collection functions

| Operator syntax | Description
| --------------- | -----------
| ELEMENT(value)  | Returns the sole element of a array or multiset; null if the collection is empty; throws if it has more than one element.
| CARDINALITY(value) | Returns the number of elements in an array or multiset.

See also: UNNEST relational operator converts a collection to a relation.

### JDBC function escape

#### Numeric

| Operator syntax | Description
| --------------- | -----------
| {fn LOG10(numeric)}
| {fn POWER(numeric, numeric)}

Not implemented:
* {fn ABS(numeric)}
* {fn ACOS(numeric)}
* {fn ASIN(numeric)}
* {fn ATAN(numeric)}
* {fn ATAN2(numeric, numeric)}
* {fn CEILING(numeric)}
* {fn COS(numeric)}
* {fn COT(numeric)}
* {fn DEGREES(numeric)}
* {fn EXP(numeric)}
* {fn FLOOR(numeric)}
* {fn LOG(numeric)}
* {fn LOG10(numeric)}
* {fn MOD(numeric, numeric)}
* {fn PI()}
* {fn RADIANS(numeric)}
* {fn RAND(numeric)}
* {fn ROUND(numeric, numeric)}
* {fn SIGN(numeric)}
* {fn SIN(numeric)}
* {fn SQRT(numeric)}
* {fn TAN(numeric)}
* {fn TRUNCATE(numeric, numeric)}

#### String

| Operator syntax | Description
| --------------- | -----------
| {fn LOCATE(string, string)}
| {fn INSERT(string, integer, integer, string)}
| {fn LCASE(string)}

Not implemented:
* {fn ASCII(string)}
* {fn CHAR(string)}
* {fn CONCAT(string, string)}
* {fn DIFFERENCE(string, string)}
* {fn LEFT(string, integer)}
* {fn LENGTH(string)}
* {fn LOCATE(string, string, integer)}
* {fn LTRIM(string)}
* {fn REPEAT(string, integer)}
* {fn REPLACE(string, string, string)}
* {fn RIGHT(string, integer)}
* {fn RTRIM(string)}
* {fn SOUNDEX(string)}
* {fn SPACE(integer)}
* {fn SUBSTRING(string, integer, integer)}
* {fn UCASE(string)}

#### Date/time

Not implemented:
* {fn CURDATE()}
* {fn CURTIME()}
* {fn DAYNAME(date)}
* {fn DAYOFMONTH(date)}
* {fn DAYOFWEEK(date)}
* {fn DAYOFYEAR(date)}
* {fn HOUR(time)}
* {fn MINUTE(time)}
* {fn MONTH(date)}
* {fn MONTHNAME(date)}
* {fn NOW()}
* {fn QUARTER(date)}
* {fn SECOND(time)}
* {fn TIMESTAMPADD(interval, count, timestamp)}
* {fn TIMESTAMPDIFF(interval, timestamp, timestamp)}
* {fn WEEK(date)}
* {fn YEAR(date)}

#### System

Not implemented:
* {fn DATABASE()}
* {fn IFNULL(value, value)}
* {fn USER(value, value)}
* {fn CONVERT(value, type)}

### Aggregate functions

| Operator syntax | Description
| --------------- | -----------
| COUNT( [ DISTINCT ] value)
| COUNT(*)
| AVG( [ DISTINCT ] numeric)
| SUM( [ DISTINCT ] numeric)
| MAX( [ DISTINCT ] value)
| MIN( [ DISTINCT ] value)
| STDDEV_POP( [ DISTINCT ] value)
| STDDEV_SAMP( [ DISTINCT ] value)
| VAR( [ DISTINCT ] value)
| VAR_POP( [ DISTINCT ] value)

### Window functions

| Operator syntax | Description
| --------------- | -----------
| COUNT(value) OVER window
| COUNT(*) OVER window
| AVG(numeric) OVER window
| SUM(numeric) OVER window
| MAX(value) OVER window
| MIN(value) OVER window
| RANK() OVER window
| DENSE_RANK() OVER window
| ROW_NUMBER() OVER window
| FIRST_VALUE(value) OVER window
| LAST_VALUE(value) OVER window
| LEAD(value, offset, default) OVER window
| LAG(value, offset, default) OVER window
| NTILE(value) OVER window

Not implemented:
* COUNT(DISTINCT value) OVER window
* FIRST_VALUE(value) IGNORE NULLS OVER window
* LAST_VALUE(value) IGNORE NULLS OVER window
* PERCENT_RANK(value) OVER window
* CUME_DIST(value) OVER window
