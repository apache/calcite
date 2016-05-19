---
layout: docs
title: Protocol Buffers Reference
sidebar_title: Protobuf Reference
permalink: /docs/protobuf_reference.html
requests:
  - { name: "CatalogsRequest" }
  - { name: "CloseConnectionRequest" }
  - { name: "CloseStatementRequest" }
  - { name: "ColumnsRequest" }
  - { name: "CommitRequest" }
  - { name: "ConnectionSyncRequest" }
  - { name: "CreateStatementRequest" }
  - { name: "DatabasePropertyRequest" }
  - { name: "ExecuteBatchRequest" }
  - { name: "ExecuteRequest" }
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
  - { name: "ColumnValue" }
  - { name: "ConnectionProperties" }
  - { name: "CursorFactory" }
  - { name: "DatabaseProperty" }
  - { name: "Frame" }
  - { name: "QueryState" }
  - { name: "Rep" }
  - { name: "Row" }
  - { name: "RpcMetadata" }
  - { name: "Signature" }
  - { name: "StateType" }
  - { name: "StatementHandle" }
  - { name: "StatementType" }
  - { name: "Style" }
  - { name: "TypedValue" }
  - { name: "UpdateBatch" }
  - { name: "WireMessage" }
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

Avatica also supports [Protocol Buffers](https://developers.google.com/protocol-buffers/)
as a message format since version 1.5.0. The Protocol Buffer, or protobuf for
short, implementation is extremely similar to the JSON implementation. Some
differences include protobuf's expanded type support (such as native byte arrays)
and inability to differentiate between the default value for a field and the
absence of a value for a field.

Other notable structural differences to JSON include the addition of a
`WireMessage` message which is used to identify the type of the wrapped message
returned by the server (synonymous with `request` or `response` attribute on the
JSON messages) and a change to `TypedValue` containing an `Object` value to
a collection of optional strongly-typed values (as protobuf does not natively
support an `Object` type that is unwrapped at runtime).

Unless otherwise specified with use of the `required` modifier, all fields in
all protocol buffer messages are `optional` by default.

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

The collection of all protobuf objects accepted as requests to Avatica. All request
objects should be wrapped in a `WireMessage` before being sent to Avatica.

### CatalogsRequest

This request is used to fetch the available catalog names in the database.

{% highlight protobuf %}
message CatalogsRequest {
  string connection_id = 1;
}
{% endhighlight %}

`connection_id` The identifier for the connection to use.

### CloseConnectionRequest

This request is used to close the Connection object in the Avatica server identified by the given IDs.

{% highlight protobuf %}
message CloseConnectionRequest {
  string connection_id = 1;
}
{% endhighlight %}

`connection_id` The identifier of the connection to close.

### CloseStatementRequest

This request is used to close the Statement object in the Avatica server identified by the given IDs.

{% highlight protobuf %}
message CloseStatementRequest {
  string connection_id = 1;
  uint32 statement_id = 2;
}
{% endhighlight %}

`connection_id` The identifier of the connection to which the statement belongs.

`statement_id` The identifier of the statement to close.

### ColumnsRequest

This request is used to fetch columns in the database given some optional filtering criteria.

{% highlight protobuf %}
message ColumnsRequest {
  string catalog = 1;
  string schema_pattern = 2;
  string table_name_pattern = 3;
  string column_name_pattern = 4;
  string connection_id = 5;
}
{% endhighlight %}

`catalog` The name of a catalog to limit returned columns.

`schema_pattern` A Java Pattern against schemas to limit returned columns.

`table_name_pattern` A Java Pattern against table names to limit returned columns.

`column_name_pattern` A Java Pattern against column names to limit returned columns.

`connection_id` The identifier of the connection which to use to fetch the columns.

### CommitRequest

This request is used to issue a `commit` on the Connection in the Avatica server identified by the given ID.

{% highlight protobuf %}
message CommitRequest {
  string connection_id = 1;
}
{% endhighlight %}

`connection_id` The identifier of the connection on which to invoke commit.

### ConnectionSyncRequest

This request is used to ensure that the client and server have a consistent view of the database properties.

{% highlight protobuf %}
message ConnectionSyncRequest {
  string connection_id = 1;
  ConnectionProperties conn_props = 2;
}
{% endhighlight %}

`connection_id` The identifier of the connection to synchronize.

`conn_props` A <a href="#connectionproperties">ConnectionProperties</a> object to synchronize between the client and server.

### CreateStatementRequest

This request is used to create a new Statement in the Avatica server.

{% highlight protobuf %}
message CreateStatementRequest {
  string connection_id = 1;
}
{% endhighlight %}

`connection_id` The identifier of the connection to use in creating a statement.

### DatabasePropertyRequest

This request is used to fetch all <a href="#databaseproperty">database properties</a>.

{% highlight protobuf %}
message DatabasePropertyRequest {
  string connection_id = 1;
}
{% endhighlight %}

`connection_id` The identifier of the connection to use when fetching the database properties.

### ExecuteBatchRequest

This request is used to execute a batch of updates against a PreparedStatement.

{% highlight protobuf %}
message ExecuteBatchRequest {
  string connection_id = 1;
  uint32 statement_id = 2;
  repeated UpdateBatch updates = 3;
}
{% endhighlight %}

`connection_id` A string which refers to a connection.

`statement_id` An integer which refers to a statement.

`updates` A list of <a href="#updatebatch">UpdateBatch</a>'s; the batch of updates.

### ExecuteRequest

This request is used to execute a PreparedStatement, optionally with values to bind to the parameters in the Statement.

{% highlight protobuf %}
message ExecuteRequest {
  StatementHandle statementHandle = 1;
  repeated TypedValue parameter_values = 2;
  uint64 first_frame_max_size = 3;
  bool has_parameter_values = 4;
}
{% endhighlight %}

`statementHandle` A <a href="#statementhandle">StatementHandle</a> object.

`parameter_values` The <a href="#typedvalue">TypedValue</a> for each parameter on the prepared statement.

`first_frame_max_size` The maximum number of rows returned in the response.

`has_parameter_values` A boolean which denotes if the user set a value for the `parameter_values` field.

### FetchRequest

This request is used to fetch a batch of rows from a Statement previously created.

{% highlight protobuf %}
message FetchRequest {
  string connection_id = 1;
  uint32 statement_id = 2;
  uint64 offset = 3;
  uint32 fetch_max_row_count = 4; // Deprecated!
  int32 frame_max_size = 5;
}
{% endhighlight %}

`connection_id` The identifier of the connection to use.

`statement_id` The identifier of the statement created using the above connection.

`offset` The positional offset into a result set to fetch.

`fetch_match_row_count` The maximum number of rows to return in the response to this request. Negative means no limit. *Deprecated*, use `frame_max_size`.

`frame_max_size` The maximum number of rows to return in the response. Negative means no limit.

### OpenConnectionRequest

This request is used to open a new Connection in the Avatica server.

{% highlight protobuf %}
message OpenConnectionRequest {
  string connection_id = 1;
  map<string, string> info = 2;
}
{% endhighlight %}

`connection_id` The identifier of the connection to open in the server.

`info` A Map containing properties to include when creating the Connection.

### PrepareAndExecuteBatchRequest

This request is used as short-hand to create a Statement and execute a batch of updates against that Statement.

{% highlight protobuf %}
message PrepareAndExecuteBatchRequest {
  string connection_id = 1;
  uint32 statement_id = 2;
  repeated string sql_commands = 3;
}
{% endhighlight %}

`connection_id` The identifier for the connection to use.

`statement_id` The identifier for the statement created by the above connection to use.

`sql_commands` A list of SQL commands to execute; a batch.

### PrepareAndExecuteRequest

This request is used as a short-hand for create a Statement and fetching the first batch of results in a single call without any parameter substitution.

{% highlight protobuf %}
message PrepareAndExecuteRequest {
  string connection_id = 1;
  uint32 statement_id = 4;
  string sql = 2;
  uint64 max_row_count = 3; // Deprecated!
  int64 max_rows_total = 5;
  int32 max_rows_in_first_frame = 6;
}
{% endhighlight %}

`connection_id` The identifier for the connection to use.

`statement_id` The identifier for the statement created by the above connection to use.

`sql` A SQL statement

`max_row_count` The maximum number of rows returned in the response. *Deprecated*, use `max_rows_total`.

`max_rows_total` The maximum number of rows which this query should return (over all `Frame`s).

`first_frame_max_size` The maximum number of rows which should be included in the first `Frame` in the `ExecuteResponse`.

### PrepareRequest

This request is used to create create a new Statement with the given query in the Avatica server.

{% highlight protobuf %}
message PrepareRequest {
  string connection_id = 1;
  string sql = 2;
  uint64 max_row_count = 3; // Deprecated!
  int64 max_rows_total = 4;
}
{% endhighlight %}

`connection_id` The identifier for the connection to use.

`sql` A SQL statement

`max_row_count` The maximum number of rows returned in the response. *Deprecated*, use `max_rows_total` instead.

`max_rows_total` The maximum number of rows returned for the query in total.

### SyncResultsRequest

This request is used to reset a ResultSet's iterator to a specific offset in the Avatica server.

{% highlight protobuf %}
message SyncResultsRequest {
  string connection_id = 1;
  uint32 statement_id = 2;
  QueryState state = 3;
  uint64 offset = 4;
}
{% endhighlight %}

`connection_id` The identifier for the connection to use.

`statement_id` The identifier for the statement to use.

`state` The <a href="#querystate">QueryState</a> object.

`offset` The offset into the ResultSet to seek to.

### RollbackRequest

This request is used to issue a `rollback` on the Connection in the Avatica server identified by the given ID.

{% highlight protobuf %}
message RollbackRequest {
  string connection_id = 1;
}
{% endhighlight %}

`connection_id` The identifier for the connection on which to invoke rollback.

### SchemasRequest

This request is used to fetch the schemas matching the provided criteria in the database.

{% highlight protobuf %}
message SchemasRequest {
  string catalog = 1;
  string schema_pattern = 2;
  string connection_id = 3;
}
{% endhighlight %}

`catalog` The name of the catalog to fetch the schema from.

`schema_pattern` A Java pattern of schemas to fetch.

`connection_id` The identifier for the connection to fetch schemas from.

### TableTypesRequest

This request is used to fetch the table types available in this database.

{% highlight protobuf %}
message TableTypesRequest {
  string connection_id = 1;
}
{% endhighlight %}

`connection_id` The identifier of the connection to fetch the table types from.

### TablesRequest

This request is used to fetch the tables available in this database filtered by the provided criteria.

{% highlight protobuf %}
message TablesRequest {
  string catalog = 1;
  string schema_pattern = 2;
  string table_name_pattern = 3;
  repeated string type_list = 4;
  bool has_type_list = 6;
  string connection_id = 7;
}
{% endhighlight %}

`catalog` The name of a catalog to restrict fetched tables.

`schema_pattern` A Java Pattern representing schemas to include in fetched tables.

`table_name_pattern` A Java Pattern representing table names to include in fetched tables.

`type_list` A list of table types used to restrict fetched tables.

`has_type_list` A boolean which denotes if the field `type_list` was provided.

`connection_id` The identifier of the connection to fetch the tables from.

### TypeInfoRequest

This request is used to fetch the types available in this database.

{% highlight protobuf %}
message TypeInfoRequest {
  string connection_id = 1;
}
{% endhighlight %}

`connection_id` The identifier of the connection to fetch the types from.

## Responses

The collection of all protobuf objects accepted as requests to Avatica. All response
objects will be wrapped in a `WireMessage` before being returned from Avatica.

### CloseConnectionResponse

A response to the <a href="#closeconnectionrequest">CloseConnectionRequest</a>.

{% highlight protobuf %}
message CloseConnectionResponse {
  RpcMetadata metadata = 1;
}
{% endhighlight %}

`metadata` <a href="#rpcmetadata">Server metadata</a> about this call.

### CloseStatementResponse

A response to the <a href="#closestatementrequest">CloseStatementRequest</a>.

{% highlight protobuf %}
message CloseStatementResponse {
  RpcMetadata metadata = 1;
}
{% endhighlight %}

`metadata` <a href="#rpcmetadata">Server metadata</a> about this call.

### CommitResponse

A response to the <a href="#commitrequest">CommitRequest</a>.

{% highlight protobuf %}
message CommitResponse {

}
{% endhighlight %}

There are no attributes on this Response.

### ConnectionSyncResponse

A response to the <a href="#connectionsyncrequest">ConnectionSyncRequest</a>. Properties included in the
response are those of the Connection in the Avatica server.

{% highlight protobuf %}
message ConnectionSyncResponse {
  ConnectionProperties conn_props = 1;
  RpcMetadata metadata = 2;
}
{% endhighlight %}

`conn_props` The <a href="#connectionproperties">ConnectionProperties</a> that were synchronized.

`metadata` <a href="#rpcmetadata">Server metadata</a> about this call.

### CreateStatementResponse

A response to the <a href="#createstatementrequest">CreateStatementRequest</a>. The ID of the statement
that was created is included in the response. Clients will use this `statement_id` in subsequent calls.

{% highlight protobuf %}
message CreateStatementResponse {
  string connection_id = 1;
  uint32 statement_id = 2;
  RpcMetadata metadata = 3;
}
{% endhighlight %}

`connection_id` The identifier for the connection used to create the statement.

`statement_id` The identifier for the created statement.

`metadata` <a href="#rpcmetadata">Server metadata</a> about this call.

### DatabasePropertyResponse

A response to the <a href="#databasepropertyrequest">DatabasePropertyRequest</a>. See <a hred="#databaseproperty">DatabaseProperty</a>
for information on the available property keys.

{% highlight protobuf %}
message DatabasePropertyResponse {
  repeated DatabasePropertyElement props = 1;
  RpcMetadata metadata = 2;
}
{% endhighlight %}

`props` A collection of <a href="#databaseproperty">DatabaseProperty</a>'s.

`metadata` <a href="#rpcmetadata">Server metadata</a> about this call.

### ErrorResponse

A response when an error was caught executing a request. Any request may return this response.

{% highlight protobuf %}
message ErrorResponse {
  repeated string exceptions = 1;
  bool has_exceptions = 7;
  string error_message = 2;
  Severity severity = 3;
  uint32 error_code = 4;
  string sql_state = 5;
  RpcMetadata metadata = 6;
}
{% endhighlight %}

`exceptions` A list of stringified Java StackTraces.

`has_exceptions` A boolean which denotes the presence of `exceptions`.

`error_message` A human-readable error message.

`error_code` A numeric code for this error.

`sql_state` A five character alphanumeric code for this error.

`severity` An <a href="#avaticaseverity">AvaticaSeverity</a> object which denotes how critical the error is.

`metadata` <a href="#rpcmetadata">Server metadata</a> about this call.

### ExecuteBatchResponse

A response to the <a href="#executebatchrequest">ExecuteBatchRequest</a> and <a href="#prepareandexecutebatchrequest">PrepareAndExecuteBatchRequest</a>.

{% highlight protobuf %}
message ExecuteBatchResponse {
  string connection_id = 1;
  uint32 statement_id = 2;
  repeated uint32 update_counts = 3;
  bool missing_statement = 4;
  RpcMetadata metadata = 5;
}
{% endhighlight %}

`connection_id` The ID referring to the connection that was used.

`statment_id` The ID referring to the statement that was used.

`update_counts` An array of integer values corresponding to the update count for each update in the batch.

`missing_statement` A boolean which denotes if the request failed due to a missing statement.

`metadata` <a href="#rpcmetadata">Server metadata</a> about this call.

### ExecuteResponse

A response to the <a href="#executerequest">ExecuteRequest</a> which contains the results for a metadata query.

{% highlight protobuf %}
message ExecuteResponse {
  repeated ResultSetResponse results = 1;
  bool missing_statement = 2;
  RpcMetadata metadata = 3;
}
{% endhighlight %}

`results` An array of <a href="#resultsetresponse">ResultSetResponse</a>s.

`missing_statement` A boolean which denotes if the request failed due to a missing statement.

`metadata` <a href="#rpcmetadata">Server metadata</a> about this call.

### FetchResponse

A response to the <a href="#fetchrequest">FetchRequest</a> which contains the request for the query.

{% highlight protobuf %}
message FetchResponse {
  Frame frame = 1;
  bool missing_statement = 2;
  bool missing_results = 3;
  RpcMetadata metadata = 4;
}
{% endhighlight %}

`frame` A <a href="#frame">Frame</a> containing the results of the fetch.

`missing_statement` A boolean which denotes if the request failed due to a missing Statement.

`missing_results` A boolean which denotes if the request failed due to a missing ResultSet.

`metadata` <a href="#rpcmetadata">Server metadata</a> about this call.

### OpenConnectionResponse

A response to the <a href="#openconnectionrequest">OpenConnectionRequest</a>. The ID for the connection that
the client should use in subsequent calls was provided by the client in the request.

{% highlight protobuf %}
message OpenConnectionResponse {
  RpcMetadata metadata = 1;
}

{% endhighlight %}

`metadata` <a href="#rpcmetadata">Server metadata</a> about this call.

### PrepareResponse

A response to the <a href="#preparerequest">PrepareRequest</a>. This response includes a <a href="#statementhandle">StatementHandle</a>
which clients must use to fetch the results from the Statement.

{% highlight protobuf %}
message PrepareResponse {
  StatementHandle statement = 1;
  RpcMetadata metadata = 2;
}
{% endhighlight %}

`statement` A <a href="#statementhandle">StatementHandle</a> object.

`metadata` <a href="#rpcmetadata">Server metadata</a> about this call.

### ResultSetResponse

A response which contains the results and type details from a query.

{% highlight protobuf %}
message ResultSetResponse {
  string connection_id = 1;
  uint32 statement_id = 2;
  bool own_statement = 3;
  Signature signature = 4;
  Frame first_frame = 5;
  uint64 update_count = 6;
  RpcMetadata metadata = 7;
}
{% endhighlight %}

`connection_id` The identifier for the connection used to generate this response.

`statement_id` The identifier for the statement used to generate this response.

`own_statement` Whether the result set has its own dedicated statement. If true, the server must automatically close the
statement when the result set is closed. This is used for JDBC metadata result sets, for instance.

`signature` A non-optional nested object <a href="#signature">Signature</a>

`first_frame` A optional nested object <a href="#frame">Frame</a>

`update_count` A number which is always `-1` for normal result sets. Any other value denotes a "dummy" result set
that only contains this count and no additional data.

`metadata` <a href="#rpcmetadata">Server metadata</a> about this call.

### RollbackResponse

A response to the <a href="#rollbackrequest">RollBackRequest</a>.

{% highlight protobuf %}
message RollbackResponse {

}
{% endhighlight %}

There are no attributes on this Response.

### SyncResultsResponse

A response to the <a href="#syncresultsrequest">SyncResultsRequest</a>. When `moreResults` is true, a <a href="#fetchrequest">FetchRequest</a>
should be issued to get the next batch of records. When `missingStatement` is true, the statement must be re-created using <a href="#preparerequest">PrepareRequest</a>
or the appropriate Request for a DDL request (e.g. <a href="#catalogsrequest">CatalogsRequest</a> or <a href="#schemasrequest">SchemasRequest</a>).

{% highlight protobuf %}
message SyncResultsResponse {
  bool missing_statement = 1;
  bool more_results = 2;
  RpcMetadata metadata = 3;
}
{% endhighlight %}

`more_results` A boolean which denotes if results exist for the ResultSet being "synced" per the request.

`missing_statement` A boolean which denotes if the statement for the ResultSet still exists.

`metadata` <a href="#rpcmetadata">Server metadata</a> about this call.

## Miscellaneous

### AvaticaParameter

This object describes the "simple", or scalar, JDBC type representation of a column in a result. This does not include
complex types such as arrays.

{% highlight protobuf %}
message AvaticaParameter {
  bool signed = 1;
  uint32 precision = 2;
  uint32 scale = 3;
  uint32 parameter_type = 4;
  string class_name = 5;
  string class_name = 6;
  string name = 7;
}
{% endhighlight %}

`signed` A boolean denoting whether the column is a signed numeric.

`precision` The maximum numeric precision supported by this column.

`scale` The maximum numeric scale supported by this column.

`parameter_type` An integer corresponding to the JDBC Types class denoting the column's type.

`type_name` The JDBC type name for this column.

`class_name` The Java class backing the JDBC type for this column.

`name` The name of the column.

### AvaticaSeverity

This enumeration describes the various levels of concern for an error in the Avatica server.

{% highlight protobuf %}
enum Severity {
  UNKNOWN_SEVERITY = 0;
  FATAL_SEVERITY = 1;
  ERROR_SEVERITY = 2;
  WARNING_SEVERITY = 3;
}
{% endhighlight %}

### AvaticaType

This object describes a simple or complex type for a column. Complex types will contain
additional information in the `component` or `columns` attribute which describe the nested
types of the complex parent type.

{% highlight protobuf %}
message AvaticaType {
  uint32 id = 1;
  string name = 2;
  Rep rep = 3;
  repeated ColumnMetaData columns = 4;
  AvaticaType component = 5;
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

{% highlight protobuf %}
message ColumnMetaData {
  uint32 ordinal = 1;
  bool auto_increment = 2;
  bool case_sensitive = 3;
  bool searchable = 4;
  bool currency = 5;
  uint32 nullable = 6;
  bool signed = 7;
  uint32 display_size = 8;
  string label = 9;
  string column_name = 10;
  string schema_name = 11;
  uint32 precision = 12;
  uint32 scale = 13;
  string table_name = 14;
  string catalog_name = 15;
  bool read_only = 16;
  bool writable = 17;
  bool definitely_writable = 18;
  string column_class_name = 19;
  AvaticaType type = 20;
}
{% endhighlight %}

`ordinal` A positional offset number.

`auto_increment` A boolean denoting whether the column is automatically incremented.

`case_sensitive` A boolean denoting whether the column is case sensitive.

`searchable` A boolean denoting whether this column supports all WHERE search clauses.

`currency` A boolean denoting whether this column represents currency.

`nullable` A number denoting whether this column supports null values.

* 0 = No null values are allowed
* 1 = Null values are allowed
* 2 = It is unknown if null values are allowed

`signed` A boolean denoting whether the column is a signed numeric.

`display_size` The character width of the column.

`label` A description for this column.

`column_name` The name of the column.

`schema_name` The schema to which this column belongs.

`precision` The maximum numeric precision supported by this column.

`scale` The maximum numeric scale supported by this column.

`table_name` The name of the table to which this column belongs.

`catalog_name` The name of the catalog to which this column belongs.

`type` A nested <a href="#avaticatype">AvaticaType</a> representing the type of the column.

`read_only` A boolean denoting whether the column is read-only.

`writable` A boolean denoting whether the column is possible to be updated.

`definitely_writable` A boolean denoting whether the column definitely can be updated.

`column_class_name` The name of the Java class backing the column's type.

### ConnectionProperties

This object represents the properties for a given JDBC Connection.

{% highlight protobuf %}
message ConnectionProperties {
  bool is_dirty = 1;
  bool auto_commit = 2;
  bool has_auto_commit = 7;
  bool read_only = 3;
  bool has_read_only = 8;
  uint32 transaction_isolation = 4;
  string catalog = 5;
  string schema = 6;
}
{% endhighlight %}

`is_dirty` A boolean denoting if the properties have been altered.

`auto_commit` A boolean denoting if autoCommit is enabled for transactions.

`has_auto_commit` A boolean denoting if `auto_commit` was set.

`read_only` A boolean denoting if a JDBC connection is read-only.

`has_read_only` A boolean denoting if `read_only` was set.

`transaction_isolation` An integer which denotes the level of transactions isolation per the JDBC
specification. This value is analogous to the values defined in `java.sql.Connection`.

* 0 = Transactions are not supported
* 1 = Dirty reads, non-repeatable reads and phantom reads may occur.
* 2 = Dirty reads are prevented, but non-repeatable reads and phantom reads may occur.
* 4 = Dirty reads and non-repeatable reads are prevented, but phantom reads may occur.
* 8 = Dirty reads, non-repeatable reads, and phantom reads are all prevented.

`catalog` The name of a catalog to use when fetching connection properties.

`schema` The name of the schema to use when fetching connection properties.

### CursorFactory

This object represents the information required to cast untyped objects into the necessary type for some results.

{% highlight protobuf %}
message CursorFactory {
  enum Style {
    OBJECT = 0;
    RECORD = 1;
    RECORD_PROJECTION = 2;
    ARRAY = 3;
    LIST = 4;
    MAP = 5;
  }

  Style style = 1;
  string class_name = 2;
  repeated string field_names = 3;
}
{% endhighlight %}

`style` A string denoting the <a href="#style">Style</a> of the contained objects.

`class_name` The name of the for `RECORD` or `RECORD_PROJECTION`.

### DatabaseProperty

This object represents the exposed database properties for a Connection through the Avatica server.

{% highlight protobuf %}
message DatabaseProperty {
  string name = 1;
  repeated string functions = 2;
}
{% endhighlight %}

`name` The name of the database property.

`functions` A collection of values for the property.

### Frame

This object represents a batch of results, tracking the offset into the results and whether more results still exist
to be fetched in the Avatica server.

{% highlight protobuf %}
message Frame {
  uint64 offset = 1;
  bool done = 2;
  repeated Row rows = 3;
}
{% endhighlight %}

`offset` The starting position of these `rows` in the encompassing result set.

`done` A boolean denoting whether more results exist for this result set.

`rows` A collection of <a href="#row">Row</a>s.

### Row

This object represents a row in a relational database table.

{% highlight protobuf %}
message Row {
  repeated ColumnValue value = 1;
}
{% endhighlight %}

`value` A collection of <a href="#columnvalue">ColumnValue</a>s, the columns in the row.

### ColumnValue

{% highlight protobuf %}
message ColumnValue {
  repeated TypedValue value = 1; // Deprecated!
  repeated ColumnValue array_value = 2;
  boolean has_array_value = 3;
  TypedValue scalar_value = 4;
}
{% endhighlight %}

`value` The pre Calcite-1.6 means of serializing <a href="#typedvalue">TypedValue</a>s. Not used anymore.

`array_value` The value of this column if it is an array (not a scalar).

`has_array_value` Should be set to true if `array_value` is set.

`scalar_value` The value of this column if it is a scalar (not an array).

### QueryState

This object represents the way a ResultSet was created in the Avatica server. A ResultSet could be created by a user-provided
SQL or by a DatabaseMetaData operation with arguments on that operation.

{% highlight protobuf %}
message QueryState {
  StateType type = 1;
  string sql = 2;
  MetaDataOperation op = 3;
  repeated MetaDataOperationArgument args = 4;
  bool has_args = 5;
  bool has_sql = 6;
  bool has_op = 7;
}
{% endhighlight %}

`type` A <a href="#statetype">StateType</a> object denoting what type of operation backs the ResultSet for this query.

`sql` The SQL statement which created the ResultSet for this query. Required if the `type` is `SQL`.

`op` The DML operation which created the ResultSet for this query. Required if the `type` is `METADATA`.

`args` The arguments to the invoked DML operation. Required if the `type` is `METADATA`.

`has_args` A boolean which denotes if the field `args` is provided.

`has_sql` A boolean which denotes if the field `sql` is provided.

`has_op` A boolean which denotes if the field `op` is provided.

### Rep

This enumeration represents the concrete Java type for some value.

{% highlight protobuf %}
enum Rep {
  PRIMITIVE_BOOLEAN = 0;
  PRIMITIVE_BYTE = 1;
  PRIMITIVE_CHAR = 2;
  PRIMITIVE_SHORT = 3;
  PRIMITIVE_INT = 4;
  PRIMITIVE_LONG = 5;
  PRIMITIVE_FLOAT = 6;
  PRIMITIVE_DOUBLE = 7;
  BOOLEAN = 8;
  BYTE = 9;
  CHARACTER = 10;
  SHORT = 11;
  INTEGER = 12;
  LONG = 13;
  FLOAT = 14;
  DOUBLE = 15;
  BIG_INTEGER = 25;
  BIG_DECIMAL = 26;
  JAVA_SQL_TIME = 16;
  JAVA_SQL_TIMESTAMP = 17;
  JAVA_SQL_DATE = 18;
  JAVA_UTIL_DATE = 19;
  BYTE_STRING = 20;
  STRING = 21;
  NUMBER = 22;
  OBJECT = 23;
  NULL = 24;
  ARRAY = 27;
  STRUCT = 28;
  MULTISET = 29;
}
{% endhighlight %}

### RpcMetadata

This object contains assorted per-call/contextual metadata returned by the Avatica server.

{% highlight protobuf %}
message RpcMetadata {
  string server_address = 1;
}
{% endhighlight %}

`serverAddress` The `host:port` of the server which created this object.

### Signature

This object represents the result of preparing a Statement in the Avatica server.

{% highlight protobuf %}
message Signature {
  repeated ColumnMetaData columns = 1;
  string sql = 2;
  repeated AvaticaParameter parameters = 3;
  CursorFactory cursor_factory = 4;
  StatementType statementType = 5;
}
{% endhighlight %}

`columns` An array of <a href="#columnmetadata">ColumnMetaData</a> objects denoting the schema of the result set.

`sql` The SQL executed.

`parameters` An array of <a href="#avaticaparameter">AvaticaParameter</a> objects denoting type-specific details.

`cursor_factory` An <a href="#cursorfactory">CursorFactory</a> object representing the Java representation of the frame.

`statementType` The type of the statement.

### StateType

This enumeration denotes whether user-provided SQL or a DatabaseMetaData operation was used to create some ResultSet.

{% highlight protobuf %}
enum StateType {
  SQL = 0;
  METADATA = 1;
}
{% endhighlight %}

### StatementHandle

This object encapsulates all of the information of a Statement created in the Avatica server.

{% highlight protobuf %}
message StatementHandle {
  string connection_id = 1;
  uint32 id = 2;
  Signature signature = 3;
}
{% endhighlight %}

`connection_id` The identifier of the connection to which this statement belongs.

`id` The identifier of the statement.

`signature` A <a href="#signature">Signature</a> object for the statement.

### StatementType

This message represents what kind the Statement is.

{% highlight protobuf %}
enum StatementType {
  SELECT = 0;
  INSERT = 1;
  UPDATE = 2;
  DELETE = 3;
  UPSERT = 4;
  MERGE = 5;
  OTHER_DML = 6;
  CREATE = 7;
  DROP = 8;
  ALTER = 9;
  OTHER_DDL = 10;
  CALL = 11;
}
{% endhighlight %}

### Style

This enumeration represents the generic "class" of type for a value. Defined within <a href="#cursorfactory">CursorFactory</a>.

{% highlight protobuf %}
enum Style {
  OBJECT = 0;
  RECORD = 1;
  RECORD_PROJECTION = 2;
  ARRAY = 3;
  LIST = 4;
  MAP = 5;
}
{% endhighlight %}

### TypedValue

This object encapsulates the type and value for a column in a row.

{% highlight protobuf %}
message TypedValue {
  Rep type = 1;
  bool bool_value = 2;
  string string_value = 3;
  sint64 number_value = 4;
  bytes bytes_value = 5;
  double double_value = 6;
  bool null = 7;
}
{% endhighlight %}

`type` A name referring to which attribute is populated with the column's value.

`bool_value` A boolean value.

`string_value` A character/string value.

`number_value` A numeric value (non-`double`).

`bytes_value` A byte-array value.

`double_value` A `double` value.

`null` A boolean which denotes if the value was null.

The following chart documents how each <a href="#rep">Rep</a> value corresponds
to the attributes in this message:

| <a href="#rep">Rep</a> Value | <a href="#typedvalue">TypedValue</a> attribute | Description |
| PRIMITIVE_BOOLEAN | `bool_value` ||
| BOOLEAN | `bool_value` ||
| PRIMITIVE_BYTE | `number_value` | The numeric value of the `byte`. |
| BYTE | `number_value` ||
| PRIMITIVE_CHAR | `string_value` ||
| CHARACTER | `string_value` ||
| PRIMITIVE_SHORT | `number_value` ||
| SHORT | `number_value` ||
| PRIMITIVE_INT | `number_value` ||
| INTEGER | `number_value` ||
| PRIMITIVE_LONG | `number_value` ||
| LONG | `number_value` ||
| PRIMITIVE_FLOAT | `number_value` ||
| FLOAT | `number_value` | IEEE 754 floating-point "single format" bit layout. |
| PRIMITIVE_DOUBLE | `number_value` ||
| DOUBLE | `number_value` ||
| BIG_INTEGER | `bytes_value` | The two's-complement representation of the BigInteger. See `BigInteger#toByteArray().` |
| BIG_DECIMAL | `string_value` | A string-ified representation of the value. See `BigDecimal#toString()`. |
| JAVA_SQL_TIME | `number_value` | As an integer, milliseconds since midnight. |
| JAVA_SQL_DATE | `number_value` | As an integer, the number of days since the epoch. |
| JAVA_SQL_TIMESTAMP | `number_value` | As a long, milliseconds since the epoch. |
| JAVA_UTIL_DATE | `number_value` | As a long, milliseconds since the epoch. |
| BYTE_STRING | `bytes_value` ||
| STRING | `string_value` | This must be a UTF-8 string. |
| NUMBER | `number_value` | A general number, unknown what concrete type. |
| OBJECT | `null` | The only general Object we can serialize is "null". Non-null OBJECT's will throw an error. |
| NULL | `null` ||
| ARRAY | N/A | Unhandled. |
| STRUCT | N/A | Unhandled. |
| MULTISET | N/A | Unhandled. |

### UpdateBatch

This is a message which serves as a wrapper around a collection of <a href="#typedvalue">TypedValue</a>'s.

{% highlight protobuf %}
message UpdateBatch {
  repeated TypedValue parameter_values = 1;
}
{% endhighlight %}

`parameter_values` A collection of parameter values for one SQL command update.

### WireMessage

This message wraps all `Request`s and `Response`s.

{% highlight protobuf %}
message WireMessage {
  string name = 1;
  bytes wrapped_message = 2;
}
{% endhighlight %}

`name` The Java class name of the wrapped message.

`wrapped_message` A serialized representation of the wrapped message of the same type specified by `name`.
