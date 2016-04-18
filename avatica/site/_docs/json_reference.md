---
layout: docs
title: JSON Reference
sidebar_title: JSON Reference
permalink: /docs/json_reference.html
requests:
  - { name: "CatalogsRequest" }
  - { name: "CloseConnectionRequest" }
  - { name: "CloseStatementRequest" }
  - { name: "ColumnsRequest" }
  - { name: "CommitRequest" }
  - { name: "ConnectionSyncRequest" }
  - { name: "CreateStatementRequest" }
  - { name: "DatabasePropertyRequest" }
  - { name: "ExecuteRequest" }
  - { name: "ExecuteBatchRequest" }
  - { name: "FetchRequest" }
  - { name: "OpenConnectionRequest" }
  - { name: "PrepareAndExecuteBatchRequest" }
  - { name: "PrepareAndExecuteRequest" }
  - { name: "PrepareRequest" }
  - { name: "RollbackRequest" }
  - { name: "SchemasRequest" }
  - { name: "SyncResultsRequest" }
  - { name: "TableTypesRequest" }
  - { name: "TablesRequest" }
  - { name: "TypeInfoRequest" }
miscellaneous:
  - { name: "AvaticaParameter" }
  - { name: "AvaticaSeverity" }
  - { name: "AvaticaType" }
  - { name: "ColumnMetaData" }
  - { name: "ConnectionProperties" }
  - { name: "CursorFactory" }
  - { name: "DatabaseProperty" }
  - { name: "Frame" }
  - { name: "QueryState" }
  - { name: "Rep" }
  - { name: "RpcMetadata" }
  - { name: "Signature" }
  - { name: "StateType" }
  - { name: "StatementHandle" }
  - { name: "StatementType" }
  - { name: "Style" }
  - { name: "TypedValue" }
responses:
  - { name: "CloseConnectionResponse" }
  - { name: "CloseStatementResponse" }
  - { name: "CommitResponse" }
  - { name: "ConnectionSyncResponse" }
  - { name: "CreateStatementResponse" }
  - { name: "DatabasePropertyResponse" }
  - { name: "ErrorResponse" }
  - { name: "ExecuteBatchResponse" }
  - { name: "ExecuteResponse" }
  - { name: "FetchResponse" }
  - { name: "OpenConnectionResponse" }
  - { name: "PrepareResponse" }
  - { name: "ResultSetResponse" }
  - { name: "RollbackResponse" }
  - { name: "SyncResultsResponse" }
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
in Java. For support outside of Java, see the Protocol Buffer
[bindings]({{ site.baseurl }}/docs/avatica_protobuf_reference.html)

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

This request is used to fetch the available catalog names in the database.

{% highlight json %}
{
  "request": "getCatalogs",
  "connectionId": "000000-0000-0000-00000000"
}
{% endhighlight %}

`connectionId` (required string) The identifier of the connection to use.

### CloseConnectionRequest

This request is used to close the Connection object in the Avatica server identified by the given IDs.

{% highlight json %}
{
  "request": "closeConnection",
  "connectionId": "000000-0000-0000-00000000"
}
{% endhighlight %}

`connectionId` (required string) The identifier of the connection to close.

### CloseStatementRequest

This request is used to close the Statement object in the Avatica server identified by the given IDs.

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

This request is used to fetch columns in the database given some optional filtering criteria.

{% highlight json %}
{
  "request": "getColumns",
  "connectionId": "000000-0000-0000-00000000",
  "catalog": "catalog",
  "schemaPattern": "schema_pattern.*",
  "tableNamePattern": "table_pattern.*",
  "columnNamePattern": "column_pattern.*"
}
{% endhighlight %}

`connectionId` (required string) The identifier of the connection on which to fetch the columns.

`catalog` (optional string) The name of a catalog to limit returned columns.

`schemaPattern` (optional string) A Java Pattern against schemas to limit returned columns.

`tableNamePattern` (optional string) A Java Pattern against table names to limit returned columns.

`columnNamePattern` (optional string) A Java Pattern against column names to limit returned columns.

### CommitRequest

This request is used to issue a `commit` on the Connection in the Avatica server identified by the given ID.

{% highlight json %}
{
  "request": "commit",
  "connectionId": "000000-0000-0000-00000000"
}
{% endhighlight %}

`connectionId` (required string) The identifier of the connection on which to invoke commit.

### ConnectionSyncRequest

This request is used to ensure that the client and server have a consistent view of the database properties.

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

This request is used to create a new Statement in the Avatica server.

{% highlight json %}
{
  "request": "createStatement",
  "connectionId": "000000-0000-0000-00000000"
}
{% endhighlight %}

`connectionId` (required string) The identifier of the connection to use in creating a statement.

### DatabasePropertyRequest

This request is used to fetch all <a href="#databaseproperty">database properties</a>.

{% highlight json %}
{
  "request": "databaseProperties",
}
{% endhighlight %}

`connectionId` (required string) The identifier of the connection to use when fetching the database properties.

### ExecuteBatchRequest

This request is used to execute a batch of updates on a PreparedStatement.

{% highlight json %}
{
  "request": "executeBatch",
  "connectionId": "000000-0000-0000-00000000",
  "statementId": 12345,
  "parameterValues": [ [ TypedValue, TypedValue, ... ], [ TypedValue, TypedValue, ...], ... ]
}
{% endhighlight %}

`connectionId` (required string) The identifier of the connection to use when fetching the database properties.

`statementId` (required integer) The identifier of the statement created using the above connection.

`parameterValues` (required array of array) An array of arrays of <a href="#typedvalue">TypedValue</a>'s. Each element
  in the array is an update to a row, while the outer array represents the entire "batch" of updates.

### ExecuteRequest

This request is used to execute a PreparedStatement, optionally with values to bind to the parameters in the Statement.

{% highlight json %}
{
  "request": "execute",
  "statementHandle": StatementHandle,
  "parameterValues": [TypedValue, TypedValue, ... ],
  "maxRowCount": 100
}
{% endhighlight %}

`statementHandle` (required object) A <a href="#statementhandle">StatementHandle</a> object.

`parameterValues` (optional array of nested objects) The <a href="#typedvalue">TypedValue</a> for each parameter on the prepared statement.

`maxRowCount` (required long) The maximum number of rows returned in the response.

### FetchRequest

This request is used to fetch a batch of rows from a Statement previously created.

{% highlight json %}
{
  "request": "fetch",
  "connectionId": "000000-0000-0000-00000000",
  "statementId": 12345,
  "offset": 0,
  "fetchMaxRowCount": 100
}
{% endhighlight %}

`connectionId` (required string) The identifier of the connection to use.

`statementId` (required integer) The identifier of the statement created using the above connection.

`offset` (required integer) The positional offset into a result set to fetch.

`fetchMatchRowCount` (required integer) The maximum number of rows to return in the response to this request.

### OpenConnectionRequest

This request is used to open a new Connection in the Avatica server.

{% highlight json %}
{
  "request": "openConnection",
  "connectionId": "000000-0000-0000-00000000",
  "info": {"key":"value", ...}
}
{% endhighlight %}

`connectionId` (required string) The identifier of the connection to open in the server.

`info` (optional string-to-string map) A Map containing properties to include when creating the Connection.

### PrepareAndExecuteBatchRequest

This request is used as short-hand to create a Statement and execute an batch of SQL commands in that Statement.

{% highlight json %}
{
  "request": "prepareAndExecuteBatch",
  "connectionId": "000000-0000-0000-00000000",
  "statementId": 12345,
  "sqlCommands": [ "SQL Command", "SQL Command", ... ]
}
{% endhighlight %}

`connectionId` (required string) The identifier for the connection to use.

`statementId` (required integer) The identifier for the statement created by the above connection to use.

`sqlCommands` (required array of strings) An array of SQL commands

### PrepareAndExecuteRequest

This request is used as a short-hand for create a Statement and fetching the first batch of results in a single call without any parameter substitution.

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

This request is used to create create a new Statement with the given query in the Avatica server.

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

### SyncResultsRequest

This request is used to reset a ResultSet's iterator to a specific offset in the Avatica server.

{% highlight json %}
{
  "request": "syncResults",
  "connectionId": "000000-0000-0000-00000000",
  "statementId": 12345,
  "state": QueryState,
  "offset": 200
}
{% endhighlight %}

`connectionId` (required string) The identifier for the connection to use.

`statementId` (required integer) The identifier for the statement to use.

`state` (required object) The <a href="#querystate">QueryState</a> object.

`offset` (required long) The offset into the ResultSet to seek to.

### RollbackRequest

This request is used to issue a `rollback` on the Connection in the Avatica server identified by the given ID.

{% highlight json %}
{
  "request": "rollback",
  "connectionId": "000000-0000-0000-00000000"
}
{% endhighlight %}

`connectionId` (required string) The identifier for the connection on which to invoke rollback.

### SchemasRequest

This request is used to fetch the schemas matching the provided criteria in the database.

{% highlight json %}
{
  "request": "getSchemas",
  "connectionId": "000000-0000-0000-00000000",
  "catalog": "name",
  "schemaPattern": "pattern.*"
}
{% endhighlight %}

`connection_id` The identifier for the connection to fetch schemas from.

`catalog` (required string) The name of the catalog to fetch the schema from.

`schemaPattern` (required string) A Java pattern of schemas to fetch.

### TableTypesRequest

This request is used to fetch the table types available in this database.

{% highlight json %}
{
  "request": "getTableTypes",
  "connectionId": "000000-0000-0000-00000000"
}
{% endhighlight %}

`connectionId` The identifier of the connection to fetch the table types from.

### TablesRequest

This request is used to fetch the tables available in this database filtered by the provided criteria.

{% highlight json %}
{
  "request": "getTables",
  "connectionId": "000000-0000-0000-00000000",
  "catalog": "catalog_name",
  "schemaPattern": "schema_pattern.*",
  "tableNamePattern": "table_name_pattern.*",
  "typeList": [ "TABLE", "VIEW", ... ]
}
{% endhighlight %}

`catalog` (optional string) The name of a catalog to restrict fetched tables.

`connectionId` The identifier of the connection to fetch the tables from.

`schemaPattern` (optional string) A Java Pattern representing schemas to include in fetched tables.

`tableNamePattern` (optional string) A Java Pattern representing table names to include in fetched tables.

`typeList` (optional array of string) A list of table types used to restrict fetched tables.

### TypeInfoRequest

This request is used to fetch the types available in this database.

{% highlight json %}
{
  "request": "getTypeInfo",
  "connectionId": "000000-0000-0000-00000000"
}
{% endhighlight %}

`connectionId` The identifier of the connection to fetch the types from.

## Responses

The collection of all JSON objects returned as responses from Avatica. All Responses include a `response` attribute
which uniquely identifies the concrete Response from all other Responses.

### CloseConnectionResponse

A response to the <a href="#closeconnectionrequest">CloseConnectionRequest</a>.

{% highlight json %}
{
  "response": "closeConnection",
  "rpcMetadata": RpcMetadata
}
{% endhighlight %}

`rpcMetadata` <a href="#rpcmetadata">Server metadata</a> about this call.

### CloseStatementResponse

A response to the <a href="#closestatementrequest">CloseStatementRequest</a>.

{% highlight json %}
{
  "response": "closeStatement",
  "rpcMetadata": RpcMetadata
}
{% endhighlight %}

`rpcMetadata` <a href="#rpcmetadata">Server metadata</a> about this call.

### CommitResponse

A response to the <a href="#commitrequest">CommitRequest</a>.

{% highlight json %}
{
  "response": "commit"
}
{% endhighlight %}

There are no extra attributes on this Response.

### ConnectionSyncResponse

A response to the <a href="#connectionsyncrequest">ConnectionSyncRequest</a>. Properties included in the
response are those of the Connection in the Avatica server.

{% highlight json %}
{
  "response": "connectionSync",
  "connProps": ConnectionProperties,
  "rpcMetadata": RpcMetadata
}
{% endhighlight %}

`connProps` The <a href="#connectionproperties">ConnectionProperties</a> that were synchronized.

`rpcMetadata` <a href="#rpcmetadata">Server metadata</a> about this call.

### CreateStatementResponse

A response to the <a href="#createstatementrequest">CreateStatementRequest</a>. The ID of the statement
that was created is included in the response. Clients will use this `statementId` in subsequent calls.

{% highlight json %}
{
  "response": "createStatement",
  "connectionId": "000000-0000-0000-00000000",
  "statementId": 12345,
  "rpcMetadata": RpcMetadata
}
{% endhighlight %}

`connectionId` The identifier for the connection used to create the statement.

`statementId` The identifier for the created statement.

`rpcMetadata` <a href="#rpcmetadata">Server metadata</a> about this call.

### DatabasePropertyResponse

A response to the <a href="#databasepropertyrequest">DatabasePropertyRequest</a>. See <a hred="#databaseproperty">DatabaseProperty</a>
for information on the available property keys.

{% highlight json %}
{
  "response": "databaseProperties",
  "map": { DatabaseProperty: Object, DatabaseProperty: Object, ... },
  "rpcMetadata": RpcMetadata
}
{% endhighlight %}

`map` A map of <a href="#databaseproperty">DatabaseProperty</a> to value of that property. The value may be some
primitive type or an array of primitive types.

`rpcMetadata` <a href="#rpcmetadata">Server metadata</a> about this call.

### ErrorResponse

A response when an error was caught executing a request. Any request may return this response.

{% highlight json %}
{
  "response": "error",
  "exceptions": [ "stacktrace", "stacktrace", ... ],
  "errorMessage": "The error message",
  "errorCode": 42,
  "sqlState": "ABC12",
  "severity": AvaticaSeverity,
  "rpcMetadata": RpcMetadata
}
{% endhighlight %}

`exceptions` A list of stringified Java StackTraces.

`errorMessage` A human-readable error message.

`errorCode` A numeric code for this error.

`sqlState` A five character alphanumeric code for this error.

`severity` An <a href="#avaticaseverity">AvaticaSeverity</a> object which denotes how critical the error is.

`rpcMetadata` <a href="#rpcmetadata">Server metadata</a> about this call.

### ExecuteBatchResponse

A response to <a href="#executebatchrequest">ExecuteBatchRequest</a> and <a href="#prepareandexecutebatchrequest">PrepareAndExecuteRequest</a>
which encapsulates the update counts for a batch of updates.

{% highlight json %}
{
  "response": "executeBatch",
  "connectionId": "000000-0000-0000-00000000",
  "statementId": 12345,
  "updateCounts": [ 1, 1, 0, 1, ... ],
  "missingStatement": false,
  "rpcMetadata": RpcMetadata
}
{% endhighlight %}

`connectionId` The identifier for the connection used to create the statement.

`statementId` The identifier for the created statement.

`updateCounts` An array of integers corresponding to each update contained in the batch that was executed.

`missingStatement` True if the operation failed because the Statement is not cached in the server, false otherwise.

`rpcMetadata` <a href="#rpcmetadata">Server metadata</a> about this call.


### ExecuteResponse

A response to the <a href="#executerequest">ExecuteRequest</a> which contains the results for a metadata query.

{% highlight json %}
{
  "response": "executeResults",
  "resultSets": [ ResultSetResponse, ResultSetResponse, ... ],
  "missingStatement": false,
  "rpcMetadata": RpcMetadata
}
{% endhighlight %}

`resultSets` An array of <a href="#resultsetresponse">ResultSetResponse</a>s.

`missingStatement` A boolean which denotes if the request failed due to a missing Statement.

`rpcMetadata` <a href="#rpcmetadata">Server metadata</a> about this call.

### FetchResponse

A response to the <a href="#fetchrequest">FetchRequest</a> which contains the request for the query.

{% highlight json %}
{
  "response": "fetch",
  "frame": Frame,
  "missingStatement": false,
  "missingResults": false,
  "rpcMetadata": RpcMetadata
}
{% endhighlight %}

`frame` A <a href="#frame">Frame</a> containing the results of the fetch.

`missingStatement` A boolean which denotes if the request failed due to a missing Statement.

`missingResults` A boolean which denotes if the request failed due to a missing ResultSet.

`rpcMetadata` <a href="#rpcmetadata">Server metadata</a> about this call.

### OpenConnectionResponse

A response to the <a href="#openconnectionrequest">OpenConnectionRequest</a>. The ID for the connection that
the client should use in subsequent calls was provided by the client in the request.

{% highlight json %}
{
  "response": "openConnection",
  "rpcMetadata": RpcMetadata
}
{% endhighlight %}

`rpcMetadata` <a href="#rpcmetadata">Server metadata</a> about this call.

### PrepareResponse

A response to the <a href="#preparerequest">PrepareRequest</a>. This response includes a <a href="#statementhandle">StatementHandle</a>
which clients must use to fetch the results from the Statement.

{% highlight json %}
{
  "response": "prepare",
  "statement": StatementHandle,
  "rpcMetadata": RpcMetadata
}
{% endhighlight %}

`statement` A <a href="#statementhandle">StatementHandle</a> object.

`rpcMetadata` <a href="#rpcmetadata">Server metadata</a> about this call.

### ResultSetResponse

A response which contains the results and type details from a query.

{% highlight json %}
{
  "response": "resultSet",
  "connectionId": "000000-0000-0000-00000000",
  "statementId": 12345,
  "ownStatement": true,
  "signature": Signature,
  "firstFrame": Frame,
  "updateCount": 10,
  "rpcMetadata": RpcMetadata
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

`rpcMetadata` <a href="#rpcmetadata">Server metadata</a> about this call.

### RollbackResponse

A response to the <a href="#rollbackrequest">RollBackRequest</a>.

{% highlight json %}
{
  "response": "rollback"
}
{% endhighlight %}

There are no extra attributes on this Response.

### SyncResultsResponse

A response to the <a href="#syncresultsrequest">SyncResultsRequest</a>. When `moreResults` is true, a <a href="#fetchrequest">FetchRequest</a>
should be issued to get the next batch of records. When `missingStatement` is true, the statement must be re-created using <a href="#preparerequest">PrepareRequest</a>
or the appropriate Request for a DDL request (e.g. <a href="#catalogsrequest">CatalogsRequest</a> or <a href="#schemasrequest">SchemasRequest</a>).

{% highlight json %}
{
  "response": "syncResults",
  "moreResults": true,
  "missingStatement": false,
  "rpcMetadata": RpcMetadata
}
{% endhighlight %}

`moreResults` A boolean which denotes if results exist for the ResultSet being "synced" per the request.

`missingStatement` A boolean which denotes if the statement for the ResultSet still exists.

`rpcMetadata` <a href="#rpcmetadata">Server metadata</a> about this call.

## Miscellaneous

### AvaticaParameter

This object describes the "simple", or scalar, JDBC type representation of a column in a result. This does not include
complex types such as arrays.

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

### AvaticaSeverity

This enumeration describes the various levels of concern for an error in the Avatica server.

One of:

* `UNKNOWN`
* `FATAL`
* `ERROR`
* `WARNING`

### AvaticaType

This object describes a simple or complex type for a column. Complex types will contain
additional information in the `component` or `columns` attribute which describe the nested
types of the complex parent type.

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

### ColumnMetaData

This object represents the JDBC ResultSetMetaData for a column.

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

### ConnectionProperties

This object represents the properties for a given JDBC Connection.

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
specification. This value is analogous to the values defined in `java.sql.Connection`.

* 0 = Transactions are not supported
* 1 = Dirty reads, non-repeatable reads and phantom reads may occur.
* 2 = Dirty reads are prevented, but non-repeatable reads and phantom reads may occur.
* 4 = Dirty reads and non-repeatable reads are prevented, but phantom reads may occur.
* 8 = Dirty reads, non-repeatable reads, and phantom reads are all prevented.

`catalog` (optional string) The name of the catalog to include when fetching connection properties.

`schema` (optional string) The name of the schema to include when fetching connection properties.

### CursorFactory

This object represents the information required to cast untyped objects into the necessary type for some results.

{% highlight json %}
{
  "style": Style,
  "clazz": "java.lang.String",
  "fieldNames": [ "column1", "column2", ... ]
}
{% endhighlight %}

`style` A string denoting the <a href="#style">Style</a> of the contained objects.

### DatabaseProperty

This object represents the exposed database properties for a Connection through the Avatica server.

One of:

* `GET_STRING_FUNCTIONS`
* `GET_NUMERIC_FUNCTIONS`
* `GET_SYSTEM_FUNCTIONS`
* `GET_TIME_DATE_FUNCTIONS`
* `GET_S_Q_L_KEYWORDS`
* `GET_DEFAULT_TRANSACTION_ISOLATION`

### Frame

This object represents a batch of results, tracking the offset into the results and whether more results still exist
to be fetched in the Avatica server.

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

### QueryState

This object represents the way a ResultSet was created in the Avatica server. A ResultSet could be created by a user-provided
SQL or by a DatabaseMetaData operation with arguments on that operation.

{% highlight json %}
{
  "type": StateType,
  "sql": "SELECT * FROM table",
  "metaDataOperation": MetaDataOperation,
  "operationArgs": ["arg0", "arg1", ... ]
}
{% endhighlight %}

`type` A <a href="#statetype">StateType</a> object denoting what type of operation backs the ResultSet for this query.

`sql` The SQL statement which created the ResultSet for this query. Required if the `type` is `SQL`.

`metaDataOperation` The DML operation which created the ResultSet for this query. Required if the `type` is `METADATA`.

`operationArgs` The arguments to the invoked DML operation. Required if the `type` is `METADATA`.

### Rep

This enumeration represents the concrete Java type for some value.

One of:

* `PRIMITIVE_BOOLEAN`
* `PRIMITIVE_BYTE`
* `PRIMITIVE_CHAR`
* `PRIMITIVE_SHORT`
* `PRIMITIVE_INT`
* `PRIMITIVE_LONG`
* `PRIMITIVE_FLOAT`
* `PRIMITIVE_DOUBLE`
* `BOOLEAN`
* `BYTE`
* `CHARACTER`
* `SHORT`
* `INTEGER`
* `LONG`
* `FLOAT`
* `DOUBLE`
* `JAVA_SQL_TIME`
* `JAVA_SQL_TIMESTAMP`
* `JAVA_SQL_DATE`
* `JAVA_UTIL_DATE`
* `BYTE_STRING`
* `STRING`
* `NUMBER`
* `OBJECT`

### RpcMetadata

This object contains assorted per-call/contextual metadata returned by the Avatica server.

{% highlight json %}
{
  "serverAddress": "localhost:8765"
}
{% endhighlight %}

`serverAddress` The `host:port` of the server which created this object.

### Signature

This object represents the result of preparing a Statement in the Avatica server.

{% highlight json %}
{
  "columns": [ ColumnMetaData, ColumnMetaData, ... ],
  "sql": "SELECT * FROM table",
  "parameters": [ AvaticaParameter, AvaticaParameter, ... ],
  "cursorFactory": CursorFactory,
  "statementType": StatementType
}
{% endhighlight %}

`columns` An array of <a href="#columnmetadata">ColumnMetaData</a> objects denoting the schema of the result set.

`sql` The SQL executed.

`parameters` An array of <a href="#avaticaparameter">AvaticaParameter</a> objects denoting type-specific details.

`cursorFactory` An <a href="#cursorfactory">CursorFactory</a> object representing the Java representation of the frame.

`statementType` An <a href="#statementtype">StatementType</a> object representing the type of Statement.

### StateType

This enumeration denotes whether user-provided SQL or a DatabaseMetaData operation was used to create some ResultSet.

One of:

* `SQL`
* `METADATA`

### StatementHandle

This object encapsulates all of the information of a Statement created in the Avatica server.

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

### StatementType

This enumeration represents what kind the Statement is.

One of:

* `SELECT`
* `INSERT`
* `UPDATE`
* `DELETE`
* `UPSERT`
* `MERGE`
* `OTHER_DML`
* `CREATE`
* `DROP`
* `ALTER`
* `OTHER_DDL`
* `CALL`

### Style

This enumeration represents the generic "class" of type for a value.

One of:

* `OBJECT`
* `RECORD`
* `RECORD_PROJECTION`
* `ARRAY`
* `LIST`
* `MAP`

### TypedValue

This object encapsulates the type and value for a column in a row.

{% highlight json %}
{
  "type": "type_name",
  "value": object
}
{% endhighlight %}

`type` A name referring to the type of the object stored in `value`.

`value` A JSON representation of a JDBC type.

The following chart documents how each <a href="#rep">Rep</a> value is serialized
into a JSON value. Consult the [JSON documentation](http://json-spec.readthedocs.org/en/latest/reference.html)
for more information on valid attributes in JSON.

| <a href="#rep">Rep</a> Value | Serialized | Description |
| PRIMITIVE_BOOLEAN | boolean ||
| BOOLEAN | boolean ||
| PRIMITIVE_BYTE | number | The numeric value of the `byte`. |
| BYTE | number ||
| PRIMITIVE_CHAR | string ||
| CHARACTER | string ||
| PRIMITIVE_SHORT | number ||
| SHORT | number ||
| PRIMITIVE_INT | number ||
| INTEGER | number ||
| PRIMITIVE_LONG | number ||
| LONG | number ||
| PRIMITIVE_FLOAT | number ||
| FLOAT | number ||
| PRIMITIVE_DOUBLE | number ||
| DOUBLE | number ||
| BIG_INTEGER | number | Implicitly handled by Jackson. |
| BIG_DECIMAL | number | Implicitly handled by Jackson. |
| JAVA_SQL_TIME | number | As an integer, milliseconds since midnight. |
| JAVA_SQL_DATE | number | As an integer, the number of days since the epoch. |
| JAVA_SQL_TIMESTAMP | number | As a long, milliseconds since the epoch. |
| JAVA_UTIL_DATE | number | As a long, milliseconds since the epoch. |
| BYTE_STRING | string | A Base64-encoded string. |
| STRING | string | |
| NUMBER | number | A general number, unknown what concrete type. |
| OBJECT | null | Implicitly converted by Jackson. |
| NULL | null | Implicitly converted by Jackson. |
| ARRAY | N/A | Implicitly handled by Jackson. |
| STRUCT | N/A | Implicitly handled by Jackson. |
| MULTISET | N/A | Implicitly handled by Jackson. |
