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
| VARCHAR(n), CHARACTER VARYING(n) | Variable-length character string | As CHAR 
| CHAR(n), CHARACTER(n) | Fixed-width character string | 'Hello', '' (empty string)
| VARBINARY(n), BINARY VARYING(n) | Variable-length binary string | As BINARY
| BINARY(n) | Fixed-width binary string | x'45F0AB', x'' (empty string)
| DATE      | Date                      | DATE '1969-07-20'
| TIME      | Time of day               | TIME '20:17:40'
| TIMESTAMP | Date and time             | TIMESTAMP '1969-07-20 20:17:40'

## Operators

To be written

## Functions

To be written

