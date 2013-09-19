# Optiq SQL language reference

## SQL constructs

query:
```SQL
{
  select
| query UNION [ ALL ] query
| query EXCEPT query
| query INTERSECT query
}
[ ORDER BY orderItem [, orderItem ]* ]
[ LIMIT { count | ALL } ]
[ OFFSET start { ROW | ROWS } ]
[ FETCH { FIRST | NEXT } [ count ] { ROW | ROWS } ]

```

select:
```SQL
SELECT [ ALL | DISTINCT ] { * | projectItem [, projectItem ]* }
FROM tableExpression
[ WHERE booleanExpression ]
[ GROUP BY { () | expression [, expression]* } ]
[ HAVING booleanExpression ]
```

projectItem:
```SQL
  expression [ [ AS ] columnAlias ]
| tableAlias . *
```

orderItem:
```SQL
expression [ ASC | DESC ] [ NULLS FIRST | NULLS LAST ] 
```

If expression is a positive integer n, it denotes the nth item in the SELECT clause.

tableExpression:
```SQL
  tableReference [, tableReference ]*
| tableExpression [ NATURAL ] [ LEFT | RIGHT | FULL ] JOIN tableExpression [ joinCondition ]

```

joinCondition:
```SQL
  ON booleanExpression
| USING ( column [, column ]* )
```

tableReference:
```SQL
tablePrimary [ [ AS ] alias [ ( columnAlias [, columnAlias ]* ) ] ]
```

tablePrimary:
```SQL
  [ TABLE ] [ [ catalogName . ] schemaName . ] tableName
| ( query )
| VALUES expression [, expression ]*
| ( TABLE expression )
```

A scalar sub-query is a sub-query used as an expression. It can occur
in most places where an expression can occur (such as the SELECT
clause, WHERE clause, or as an argument to an aggregate function). If
the sub-query returns no rows, the value is NULL; if it returns more
than one row, it is an error.

A sub-query can occur in the FROM clause of a query and also in IN and
EXISTS expressions.  A query-query that occurs in IN and EXISTS
expressions may be correlated; that is, refer to tables in the FROM
clause of an enclosing query.

## Data types

### Scalar types

| Data type | Description               | Range                |
| --------- | ------------------------- | ---------------------|
| BOOLEAN   | Logical values            | TRUE, FALSE, UNKNOWN
| TINYINT   | 1 byte signed integer     | -255 to 256
| SMALLINT  | 2 byte signed integer     | -32768 to 32767
| INTEGER, INT | 4 byte signed integer  | -2147483648 to 2147483647
| BIGINT    | 8 byte signed integer     | -9223372036854775808 to 9223372036854775807
| DECIMAL   | Fixed point               |
| NUMERIC   | Fixed point               |
| REAL, FLOAT | 4 byte floating point     | 6 decimal digits precision 
| DOUBLE    | 8 byte floating point     | 15 decimal digits precision
| CHAR(n), CHARACTER(n) | Fixed-width character string | 'Hello', '' (empty string), _latin1'Hello', n'Hello', _UTF16'Hello', 'Hello' 'there' (literal split into multiple parts)
| VARCHAR(n), CHARACTER VARYING(n) | Variable-length character string | As CHAR(n)
| BINARY(n) | Fixed-width binary string | x'45F0AB', x'' (empty binary string), x'AB' 'CD' (multi-part binary string literal)
| VARBINARY(n), BINARY VARYING(n) | Variable-length binary string | As BINARY(n)
| DATE      | Date                      | DATE '1969-07-20'
| TIME      | Time of day               | TIME '20:17:40'
| TIMESTAMP | Date and time             | TIMESTAMP '1969-07-20 20:17:40'
| INTERVAL period [ TO period ] | Date time interval | INTERVAL '1:5' YEAR TO MONTH, INTERVAL '45' DAY
| Anchored interval | Date time interval | (DATE '1969-07-20', DATE '1972-08-29')

* DATE, TIME and TIMESTAMP have no time zone. There is not even an implicit
  time zone, such as UTC (as in Java) or the local time zone. It is left to
  the user or application to supply a time zone.

### Non-scalar types

| Type       | Description
| ---------- | -----------------------------------------------------------
| `ANY`      | A value of an unknown type
| `ROW`      | Row with 1 or more columns
| `MAP`      | Collection of keys mapped to values
| `MULTISET` | Unordered collection that may contain duplicates
| `ARRAY`    | Ordered, contiguous collection that may contain duplicates
| `CURSOR`   | Cursor over the result of executing a query

## Operators and functions

### Comparison operators

* value = value
* value <> value
* value > value
* value >= value
* value < value
* value <= value
* value IS NULL
* value IS NOT NULL
* value IS DISTINCT FROM value
* value IS NOT DISTINCT FROM value
* value BETWEEN value AND value
* value NOT BETWEEN value AND value
* string LIKE string
* string NOT LIKE string
* string SIMILAR TO string
* string NOT SIMILAR TO string
* string SIMILAR TO string ESCAPE string
* string NOT SIMILAR TO string ESCAPE string
* value IN (value [, value]* )
* value NOT IN (value [, value]* )
* value IN (sub-query)
* value NOT IN (sub-query)
* EXISTS (sub-query)

### Logical operators

* boolean OR boolean
* boolean AND boolean
* NOT boolean
* boolean IS FALSE
* boolean IS NOT FALSE
* boolean IS TRUE
* boolean IS NOT TRUE
* boolean IS UNKNOWN
* boolean IS NOT UNKNOWN

### Arithmetic operators and functions

* + numeric
* - numeric
* numeric + numeric
* numeric - numeric
* numeric * numeric
* numeric / numeric
* POWER(numeric, numeric)
* ABS(numeric)
* MOD(numeric, numeric)
* SQRT(numeric)
* LN(numeric)
* LOG10(numeric)
* EXP(numeric)
* CEIL(numeric)
* FLOOR(numeric)

### String operators and functions

* string || string
* CHAR_LENGTH(string)
* CHARACTER_LENGTH(string)
* UPPER(string)
* LOWER(string)
* POSITION(string IN string)
* TRIM( { BOTH | LEADING | TRAILING } string FROM string)
* OVERLAY(string PLACING string FROM string)
* OVERLAY(string PLACING string FROM integer)
* SUBSTRING(string FROM integer)
* SUBSTRING(string FROM integer FOR integer)
* INITCAP(string)

### Binary string functions

* binary || binary
* POSITION(binary IN binary)
* SUBSTRING(binary FROM integer FOR integer)

### Date/time functions

* LOCALTIME
* LOCALTIME(n)
* LOCALDATE
* LOCALTIMESTAMP
* LOCALTIMESTAMP(n)
* CURRENT_TIME
* CURRENT_DATE
* CURRENT_TIMESTAMP
* EXTRACT(period FROM datetime)

Not implemented:
* EXTRACT(period FROM interval)
* CEIL(interval)
* FLOOR(interval)
* datetime - datetime period [ TO period ]
* interval OVERLAPS interval
* + interval
* - interval
* interval + interval
* interval - interval
* interval / interval

### System functions

* USER
* CURRENT_USER
* SESSION_USER
* SYSTEM_USER
* CURRENT_PATH
* CURRENT_ROLE

### Conditional functions and operators

* CASE value WHEN value1 THEN result1 [ WHEN valueN THEN resultN ]* [ ELSE resultZ ] END
* CASE WHEN condition1 THEN result1 [ WHEN conditionN THEN resultN ]* [ ELSE resultZ ] END
* NULLIF(value, value)
* COALESCE(value, value)

### Type conversion

* CAST(value AS type)

### Value constructors

* ROW (value [, value]* )
* (value [, value]* )
* map [ key ]
* array [ index ]
* ARRAY [ value [, value ]* ]
* MAP [ key, value [, key, value ]* ]

### JDBC function escape

#### Numeric

* {fn LOG10(numeric)}
* {fn POWER(numeric, numeric)}

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

* {fn LOCATE(string, string)}
* {fn INSERT(string, integer, integer, string)}
* {fn LCASE(string)}

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

* COUNT( [ DISTINCT ] value)
* COUNT(*)
* AVG( [ DISTINCT ] numeric)
* SUM( [ DISTINCT ] numeric)
* MAX( [ DISTINCT ] value)
* MIN( [ DISTINCT ] value)
* STDDEV_POP( [ DISTINCT ] value)
* STDDEV_SAMP( [ DISTINCT ] value)
* VAR( [ DISTINCT ] value)
* VAR_POP( [ DISTINCT ] value)

### Window functions

* COUNT(value) OVER window
* COUNT(*) OVER window
* AVG(numeric) OVER window
* SUM(numeric) OVER window
* MAX(value) OVER window
* MIN(value) OVER window
* RANK() OVER window

Not implemented:
* FIRST_VALUE() OVER window
* LAST_VALUE() OVER window
* DENSE_RANK(value) OVER window
* PERCENT_RANK(value) OVER window
* CUME_DIST(value) OVER window
* ROW_NUMBER() OVER window
