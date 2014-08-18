# Optiq JSON model reference

## Elements

### Root

```json
{
  version: '1.0',
  defaultSchema: 'mongo',
  schemas: [ Schema... ]
}
```

`version` (required string) must have value `1.0`.

`defaultSchema` (optional string). If specified, it is
the name (case-sensitive) of a schema defined in this model, and will
become the default schema for connections to Optiq that use this model.

`schemas` (optional list of <a href="#schema">Schema</a> elements).

### Schema

Occurs within `root.schemas`.

```json
{
  name: 'foodmart',
  path: ['lib'],
  cache: true,
  materializations: [ Materialization... ]
}
```

`name` (required string) is the name of the schema.

`type` (optional string, default `map`) indicates sub-type. Values are:
* `map` for <a href="#map-schema">Map Schema</a>
* `custom` for <a href="#custom-schema">Custom Schema</a>
* `jdbc` for <a href="#jdbc-schema">JDBC Schema</a>

`path` (optional list) is the SQL path that is used to
resolve functions used in this schema. If specified it must be a list,
and each element of the list must be either a string or a list of
strings. For example,

```json
  path: [ ['usr', 'lib'], 'lib' ]
```

declares a path with two elements: the schema '/usr/lib' and the
schema '/lib'. Most schemas are at the top level, so you can use a
string.

`materializations` (optional list of
<a href="#materialization">Materialization</a>) defines the tables
in this schema that are materializations of queries.

`cache` (optional boolean, default true) tells Optiq whether to
cache metadata (tables, functions and sub-schemas) generated
by this schema.

* If `false`, Optiq will go back to the schema each time it needs
  metadata, for example, each time it needs a list of tables in order to
  validate a query against the schema.

* If `true`, Optiq will cache the metadata the first time it reads
  it. This can lead to better performance, especially if name-matching is
  case-insensitive.

However, it also leads to the problem of cache staleness.
A particular schema implementation can override the
`Schema.contentsHaveChangedSince` method to tell Optiq
when it should consider its cache to be out of date.

Tables, functions and sub-schemas explicitly created in a schema are
not affected by this caching mechanism. They always appear in the schema
immediately, and are never flushed.

### Map Schema

Like base class <a href="#schema">Schema</a>, occurs within `root.schemas`.

```json
{
  name: 'foodmart',
  type: 'map',
  tables: [ Table... ],
  functions: [ Function... ]
}
```

`name`, `type`, `path`, `cache`, `materializations` inherited from <a href="#schema">Schema</a>.

`tables` (optional list of <a href="#table">Table</a> elements)
defines the tables in this schema.

`functions` (optional list of <a href="#function">Function</a> elements)
defines the functions in this schema.

### Custom Schema

Like base class <a href="#schema">Schema</a>, occurs within `root.schemas`.

```json
{
  name: 'mongo',
  type: 'custom',
  factory: 'net.hydromatic.optiq.impl.mongodb.MongoSchemaFactory',
  operand: {
    host: 'localhost',
    database: 'test'
  }
}
```

`name`, `type`, `path`, `cache`, `materializations` inherited from <a href="#schema">Schema</a>.

`factory` (required string) is the name of the factory class for this
schema. Must implement interface `net.hydromatic.optiq.SchemaFactory`
and have a public default constructor.

`operand` (optional map) contains attributes to be passed to the
factory.

### JDBC Schema

Like base class <a href="#schema">Schema</a>, occurs within `root.schemas`.

```json
{
  name: 'foodmart',
  type: 'jdbc',
  jdbcDriver: TODO,
  jdbcUrl: TODO,
  jdbcUser: TODO,
  jdbcPassword: TODO,
  jdbcCatalog: TODO,
  jdbcSchema: TODO
}
```

`name`, `type`, `path`, `cache`, `materializations` inherited from <a href="#schema">Schema</a>.

`jdbcDriver` (optional string) is TODO.

`jdbcUrl` (optional string) is TODO.

`jdbcUser` (optional string) is TODO.

`jdbcPassword` (optional string) is TODO.

`jdbcCatalog` (optional string) is TODO.

`jdbcSchema` (optional string) is TODO.

### Materialization

Occurs within `root.schemas.materializations`.

```json
TODO
```

`view` (optional string) TODO

`table` (optional string) TODO

`sql` (optional string) TODO

### Table

Occurs within `root.schemas.tables`.

```json
{
  name: 'sales_fact',
  columns: [ Column... ]
}
```

`name` (required string) is the name of this table. Must be unique within the schema.

`type` (optional string, default `custom`) indicates sub-type. Values are:
* `custom` for <a href="#custom-table">Custom Table</a>
* `view` for <a href="#view">View</a>

`columns` (optional list of <a href="#column">Column</a> elements)

### View

Like base class <a href="#table">Table</a>, occurs within `root.schemas.tables`.

```json
{
  name: 'female_emps',
  type: 'view',
  sql: "select * from emps where gender = 'F'"
}
```

`name`, `type`, `columns` inherited from <a href="#table">Table</a>.

`sql` (required string) is the SQL definition of the view.

`path` (optional list) is the SQL path to resolve the query. If not
specified, defaults to the current schema.

### Custom Table

Like base class <a href="#table">Table</a>, occurs within `root.schemas.tables`.

```json
{
  name: 'female_emps',
  type: 'custom',
  factory: 'TODO',
  operand: {
    todo: 'TODO'
  }
}
```

`name`, `type`, `columns` inherited from <a href="#table">Table</a>.

`factory` (required string) is the name of the factory class for this
table. Must implement interface `net.hydromatic.optiq.TableFactory`
and have a public default constructor.

`operand` (optional map) contains attributes to be passed to the
factory.

### Column

Occurs within `root.schemas.tables.columns`.

```json
TODO
```

`name` (required string) is the name of this column.

### Function

Occurs within `root.schemas.functions`.

```json
TODO
```

`name` (required string) is the name of this function.

`className` (required string) is the name of the class that implements this function.

`path` (optional list of string) is the path for resolving this function.
