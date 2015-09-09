---
layout: docs
title: Avatica JSON Reference
sidebar_title: JSON Reference
permalink: /docs/avatica_json_reference.html
requests:
  - { name: "CatalogsRequest" }
  - { name: "CloseConnectionRequest" }
  - { name: "CloseStatementRequest" }
  - { name: "ColumnsRequest" }
  - { name: "ConnectionSyncRequest" }
  - { name: "CreateStatementRequest" }
  - { name: "DatabasePropertyRequest" }
  - { name: "FetchRequest" }
  - { name: "PrepareAndExecuteRequest" }
  - { name: "PrepareRequest" }
  - { name: "SchemasRequest" }
  - { name: "TableTypesRequest" }
  - { name: "TablesRequest" }
  - { name: "TypeInfoRequest" }
miscellaneous:
  - { name: "ConnectionProperties" }
  - { name: "TypedValue" }
  - { name: "Signature" }
  - { name: "Frame" }
  - { name: "StatementHandle" }
  - { name: "DatabaseProperty" }
  - { name: "ColumnMetaData" }
  - { name: "AvaticaParameter" }
  - { name: "AvaticaType" }
  - { name: "Rep" }
  - { name: "CursorFactory" }
  - { name: "Style" }
responses:
  - { name: "ResultSetResponse" }
  - { name: "ExecuteResponse" }
  - { name: "PrepareResponse" }
  - { name: "FetchResponse" }
  - { name: "CreateStatementResponse" }
  - { name: "CloseStatementResponse" }
  - { name: "CloseConnectionResponse" }
  - { name: "ConnectionSyncResponse" }
  - { name: "DatabasePropertyResponse" }
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

As Avatica uses JSON to serialize messages sent over an HTTP transport,
the RPC layer is agnostic of the language used by a client. While the Avatica
server is written in Java, this enables clients to interact with the server
using any language instead of being limited to Java.

A specification of the JSON request and response objects are documented
below. Programmatic bindings for these JSON objects are only available
in Java, so non-Java clients presently must re-implement language
specific objects on their own. Efforts to use [Protocol Buffers](https://developers.google.com/protocol-buffers/)
instead are underway that will provide native objects in many languages.

## Index

### Requests
<ul>
  {% for item in page.requests %}<li><a href="#{{ item.name | downcase }}">{{ item.name }}</a></li>{% endfor %}
</ul>

### Responses
<ul>
  {% for item in page.responses %}<li><a href="#{{ item.name | downcase }}">{{ item.name }}</a></li>{% endfor %}
</ul>

### Miscellaneous
<ul>
  {% for item in page.miscellaneous %}<li><a href="#{{ item.name | downcase }}">{{ item.name }}</a></li>{% endfor %}
</ul>


## Requests

The collection of all JSON objects accepted as requests to Avatica. All Requests include a `request` attribute
which uniquely identifies the concrete Request from all other Requests.

### CatalogsRequest

{% highlight json %}
{
  "request": "getCatalogs",
}
{% endhighlight %}

There are no extra attributes on this Request.

### CloseConnectionRequest

{% highlight json %}
{
  "request": "closeConnection",
  "connectionId": "000000-0000-0000-00000000"
}
{% endhighlight %}

`connectionId` (required string) The identifier of the connection to close.

### CloseStatementRequest

{% highlight json %}
{
  "request": "closeStatement",
  "connectionId": "000000-0000-0000-00000000",
  "statementId": 12345
}
{% endhighlight %}

`connectionId` (required string) The identifier of the connection to which the statement belongs.

`statementId` (required integer) The identifier of the statement to close.

### ColumnsRequest

{% highlight json %}
{
  "request": "getColumns",
  "catalog": "catalog",
  "schemaPattern": "schema_pattern.*",
  "tableNamePattern": "table_pattern.*",
  "columnNamePattern": "column_pattern.*"
}
{% endhighlight %}

`catalog` (optional string) The name of a catalog to limit returned columns.

`schemaPattern` (optional string) A Java Pattern against schemas to limit returned columns.

`tableNamePattern` (optional string) A Java Pattern against table names to limit returned columns.

`columnNamePattern` (optional string) A Java Pattern against column names to limit returned columns.

### ConnectionSyncRequest

{% highlight json %}
{
  "request": "connectionSync",
  "connectionId": "000000-0000-0000-00000000",
  "connProps": ConnectionProperties
}
{% endhighlight %}

`connectionId` (required string) The identifier of the connection to synchronize.

`connProps` (optional nested object) A <a href="#connectionproperties">ConnectionProperties</a> object to synchronize between the client and server.

### CreateStatementRequest

{% highlight json %}
{
  "request": "createStatement",
  "connectionId": "000000-0000-0000-00000000"
}
{% endhighlight %}

`connectionId` (required string) The identifier of the connection to use in creating a statement.

### DatabasePropertyRequest

{% highlight json %}
{
  "request": "databaseProperties",
}
{% endhighlight %}

There are no extra attributes on this Request.

### FetchRequest

{% highlight json %}
{
  "request": "fetch",
  "connectionId": "000000-0000-0000-00000000",
  "statementId": 12345,
  "offset": 0,
  "fetchMaxRowCount": 100,
  "parameterValues": [TypedValue, TypedValue, ...]
}
{% endhighlight %}

`connectionId` (required string) The identifier of the connection to use.

`statementId` (required integer) The identifier of the statement created using the above connection.

`offset` (required integer) The positional offset into a result set to fetch.

`fetchMatchRowCount` (required integer) The maximum number of rows to return in the response to this request.

`parameterValues` (optional array of nested objects) The types of the object to set on the prepared statement in use.

### PrepareAndExecuteRequest

{% highlight json %}
{
  "request": "prepareAndExecute",
  "connectionId": "000000-0000-0000-00000000",
  "statementId": 12345,
  "sql": "SELECT * FROM table",
  "maxRowCount": 100,
}
{% endhighlight %}

`connectionId` (required string) The identifier for the connection to use.

`statementId` (required integer) The identifier for the statement created by the above connection to use.

`sql` (required string) A SQL statement

`maxRowCount` (required long) The maximum number of rows returned in the response.

### PrepareRequest

{% highlight json %}
{
  "request": "prepare",
  "connectionId": "000000-0000-0000-00000000",
  "sql": "SELECT * FROM table",
  "maxRowCount": 100,
}
{% endhighlight %}

`connectionId` (required string) The identifier for the connection to use.

`sql` (required string) A SQL statement

`maxRowCount` (required long) The maximum number of rows returned in the response.

### SchemasRequest

{% highlight json %}
{
  "request": "getSchemas",
  "catalog": "name",
  "schemaPattern": "pattern.*"
}
{% endhighlight %}

`catalog` (required string) The name of the catalog to fetch the schema from.

`schemaPattern` (required string) A Java pattern of schemas to fetch.

### TableTypesRequest

{% highlight json %}
{
  "request": "getTableTypes",
}
{% endhighlight %}

There are no extra attributes on this Request.

### TablesRequest

{% highlight json %}
{
  "request": "getTables",
  "catalog": "catalog_name",
  "schemaPattern": "schema_pattern.*",
  "tableNamePattern": "table_name_pattern.*",
  "typeList": [ "TABLE", "VIEW", ... ]
}
{% endhighlight %}

`catalog` (optional string) The name of a catalog to restrict fetched tables.

`schemaPattern` (optional string) A Java Pattern representing schemas to include in fetched tables.

`tableNamePattern` (optional string) A Java Pattern representing table names to include in fetched tables.

`typeList` (optional array of string) A list of table types used to restrict fetched tables.

### TypeInfoRequest

{% highlight json %}
{
  "request": "getTypeInfo",
}
{% endhighlight %}

There are no extra attributes on this Request.

## Responses

The collection of all JSON objects returned as responses from Avatica. All Responses include a `response` attribute
which uniquely identifies the concrete Response from all other Responses.

### ResultSetResponse

{% highlight json %}
{
  "response": "resultSet",
  "connectionId": "000000-0000-0000-00000000",
  "statementId": 12345,
  "ownStatement": true,
  "signature": Signature,
  "firstFrame": Frame,
  "updateCount": 10
}
{% endhighlight %}

`connectionId` The identifier for the connection used to generate this response.

`statementId` The identifier for the statement used to generate this response.

`ownStatement` Whether the result set has its own dedicated statement. If true, the server must automatically close the
statement when the result set is closed. This is used for JDBC metadata result sets, for instance.

`signature` A non-optional nested object <a href="#signature">Signature</a>

`firstFrame` A optional nested object <a href="#frame">Frame</a>

`updateCount` A number which is always `-1` for normal result sets. Any other value denotes a "dummy" result set
that only contains this count and no additional data.

### ExecuteResponse

{% highlight json %}
{
  "response": "Service$ExecuteResponse",
  "resultSets": [ ResultSetResponse, ResultSetResponse, ... ]
}
{% endhighlight %}

`resultSets` An array of <a href="#resultsetresponse">ResultSetResponse</a>s.

### PrepareResponse

{% highlight json %}
{
  "response": "prepare",
  "statement": StatementHandle
}
{% endhighlight %}

`statement` A <a href="#statementhandle">StatementHandle</a> object.

### FetchResponse

{% highlight json %}
{
  "response": "fetch",
  "frame": Frame
}
{% endhighlight %}

`frame` A <a href="#frame">Frame</a> containing the results of the fetch.

### CreateStatementResponse

{% highlight json %}
{
  "response": "createStatement",
  "connectionId": "000000-0000-0000-00000000",
  "statementId": 12345
}
{% endhighlight %}

`connectionId` The identifier for the connection used to create the statement.

`statementId` The identifier for the created statement.

### CloseStatementResponse

{% highlight json %}
{
  "response": "closeStatement",
}
{% endhighlight %}

This response has no attributes.

### CloseConnectionResponse

{% highlight json %}
{
  "response": "closeConnection",
}
{% endhighlight %}

There are no extra attributes on this Response.

### ConnectionSyncResponse

{% highlight json %}
{
  "response": "connectionSync",
  "connProps": ConnectionProperties
}
{% endhighlight %}

`connProps` The <a href="#connectionproperties">ConnectionProperties</a> that were synchronized.

### DatabasePropertyResponse

{% highlight json %}
{
  "response": "databaseProperties",
  "map": { DatabaseProperty: Object, DatabaseProperty: Object, ... }
}
{% endhighlight %}

`map` A map of <a href="#databaseproperty">DatabaseProperty</a> to value of that property. The value may be some
primitive type or an array of primitive types.

## Miscellaneous

### ConnectionProperties

{% highlight json %}
{
  "connProps": "connPropsImpl",
  "autoCommit": true,
  "readOnly": true,
  "transactionIsolation": 0,
  "catalog": "catalog",
  "schema": "schema"
}
{% endhighlight %}

`autoCommit` (optional boolean) A boolean denoting if autoCommit is enabled for transactions.

`readOnly` (optional boolean) A boolean denoting if a JDBC connection is read-only.

`transactionIsolation` (optional integer) An integer which denotes the level of transactions isolation per the JDBC
specification. This value is analogous to the values define in `java.sql.Connection`.

* 0 = Transactions are not supported
* 1 = Dirty reads, non-repeatable reads and phantom reads may occur.
* 2 = Dirty reads are prevented, but non-repeatable reads and phantom reads may occur.
* 4 = Dirty reads and non-repeatable reads are prevented, but phantom reads may occur.
* 8 = Dirty reads, non-repeatable reads, and phantom reads are all prevented.

### TypedValue

{% highlight json %}
{
  "type": "type_name",
  "value": object
}
{% endhighlight %}

`type` A name referring to the type of the object stored in `value`.

`value` A JSON representation of a JDBC type.

### Signature

{% highlight json %}
{
  "columns": [ ColumnMetaData, ColumnMetaData, ... ],
  "sql": "SELECT * FROM table",
  "parameters": [ AvaticaParameter, AvaticaParameter, ... ],
  "cursorFactory": CursorFactory
}
{% endhighlight %}

`columns` An array of <a href="#columnmetadata">ColumnMetaData</a> objects denoting the schema of the result set.

`sql` The SQL executed.

`parameters` An array of <a href="#avaticaparameter">AvaticaParameter</a> objects denoting type-specific details.

`cursorFactory` An <a href="#cursorfactory">CursorFactory</a> object representing the Java representation of the frame.

### Frame

{% highlight json %}
{
  "offset": 100,
  "done": true,
  "rows": [ [ val1, val2, ... ], ... ]
}
{% endhighlight %}

`offset` The starting position of these `rows` in the encompassing result set.

`done` A boolean denoting whether more results exist for this result set.

`rows` An array of arrays corresponding to the rows and columns for the result set.

### StatementHandle

{% highlight json %}
{
  "connectionId": "000000-0000-0000-00000000",
  "id": 12345,
  "signature": Signature
}
{% endhighlight %}

`connectionId` The identifier of the connection to which this statement belongs.

`id` The identifier of the statement.

`signature` A <a href="#signature">Signature</a> object for the statement.

### DatabaseProperty

One of:

* "GET_STRING_FUNCTIONS"
* "GET_NUMERIC_FUNCTIONS"
* "GET_SYSTEM_FUNCTIONS"
* "GET_TIME_DATE_FUNCTIONS"
* "GET_S_Q_L_KEYWORDS"
* "GET_DEFAULT_TRANSACTION_ISOLATION"

### ColumnMetaData

{% highlight json %}
{
  "ordinal": 0,
  "autoIncrement": true,
  "caseSensitive": true,
  "searchable": false,
  "currency": false,
  "nullable": 0,
  "signed": true,
  "displaySize": 20,
  "label": "Description",
  "columnName": "col1",
  "schemaName": "schema",
  "precision": 10,
  "scale": 2,
  "tableName": "table",
  "catalogName": "catalog",
  "type": AvaticaType,
  "readOnly": false,
  "writable": true,
  "definitelyWritable": true,
  "columnClassName": "java.lang.String"
}
{% endhighlight %}

`ordinal` A positional offset number.

`autoIncrement` A boolean denoting whether the column is automatically incremented.

`caseSensitive` A boolean denoting whether the column is case sensitive.

`searchable` A boolean denoting whether this column supports all WHERE search clauses.

`currency` A boolean denoting whether this column represents currency.

`nullable` A number denoting whether this column supports null values.

* 0 = No null values are allowed
* 1 = Null values are allowed
* 2 = It is unknown if null values are allowed

`signed` A boolean denoting whether the column is a signed numeric.

`displaySize` The character width of the column.

`label` A description for this column.

`columnName` The name of the column.

`schemaName` The schema to which this column belongs.

`precision` The maximum numeric precision supported by this column.

`scale` The maximum numeric scale supported by this column.

`tableName` The name of the table to which this column belongs.

`catalogName` The name of the catalog to which this column belongs.

`type` A nested <a href="#avaticatype">AvaticaType</a> representing the type of the column.

`readOnly` A boolean denoting whether the column is read-only.

`writable` A boolean denoting whether the column is possible to be updated.

`definitelyWritable` A boolean denoting whether the column definitely can be updated.

`columnClassName` The name of the Java class backing the column's type.

### AvaticaParameter

{% highlight json %}
{
  "signed": true,
  "precision": 10,
  "scale": 2,
  "parameterType": 8,
  "typeName": "integer",
  "className": "java.lang.Integer",
  "name": "number"
}
{% endhighlight %}

`signed` A boolean denoting whether the column is a signed numeric.

`precision` The maximum numeric precision supported by this column.

`scale` The maximum numeric scale supported by this column.

`parameterType` An integer corresponding to the JDBC Types class denoting the column's type.

`typeName` The JDBC type name for this column.

`className` The Java class backing the JDBC type for this column.

`name` The name of the column.

### AvaticaType

{% highlight json %}
{
  "type": "scalar",
  "id": "identifier",
  "name": "column",
  "rep": Rep,
  "columns": [ ColumnMetaData, ColumnMetaData, ... ],
  "component": AvaticaType
}
{% endhighlight %}

`type` One of: `scalar`, `array`, `struct`.

`id` A numeric value corresponding to the type of the object per the JDBC Types class.

`name` The readable name of the JDBC type.

`rep` A nested <a href="#rep">Rep</a> object used by Avatica to hold additional type information.

`columns` For `STRUCT` types, a list of the columns contained in that `STRUCT`.

`component` For `ARRAY` types, the type of the elements contained in that `ARRAY`.

### Rep

One of:

* "PRIMITIVE_BOOLEAN"
* "PRIMITIVE_BYTE"
* "PRIMITIVE_CHAR"
* "PRIMITIVE_SHORT"
* "PRIMITIVE_INT"
* "PRIMITIVE_LONG"
* "PRIMITIVE_FLOAT"
* "PRIMITIVE_DOUBLE"
* "BOOLEAN"
* "BYTE"
* "CHARACTER"
* "SHORT"
* "INTEGER"
* "LONG"
* "FLOAT"
* "DOUBLE"
* "JAVA_SQL_TIME"
* "JAVA_SQL_TIMESTAMP"
* "JAVA_SQL_DATE"
* "JAVA_UTIL_DATE"
* "BYTE_STRING"
* "STRING"
* "NUMBER"
* "OBJECT"

### CursorFactory

{% highlight json %}
{
  "style": Style,
  "clazz": "java.lang.String",
  "fieldNames": [ "column1", "column2", ... ]
}
{% endhighlight %}

`style` A string denoting the <a href="#style">Style</a> of the contained objects.

### Style

One of:

* "OBJECT"
* "RECORD"
* "RECORD_PROJECTION"
* "ARRAY"
* "LIST"
* "MAP"
