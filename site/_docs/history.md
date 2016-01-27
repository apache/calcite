---
layout: docs
title: History
permalink: "/docs/history.html"
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

For a full list of releases, see
<a href="https://github.com/apache/calcite/releases">github</a>.
Downloads are available on the
[downloads page]({{ site.baseurl }}/downloads/).

## 1.7.0 / (Under Development)
{: #v1-7-0}

One notable change is that the use of JUL (java.util.logging) has been replaced
with [SLF4J](http://slf4j.org/). SLF4J provides an API which Calcite can use
independent of the logging implementation. This ultimately provides additional
flexibility to users, allowing them to configure Calcite's logging within their
own chosen logging framework. This work was done in [CALCITE-669](https://issues.apache.org/jira/browse/CALCITE-669).

For users experienced with configuring JUL in Calcite previously, there are some
differences as some the JUL logging levels do not exist in SLF4J: `FINE`,
`FINER`, and `FINEST`, specifically. To deal with this, `FINE` was mapped
to SLF4J's `DEBUG` level, while `FINER` and `FINEST` were mapped to SLF4J's `TRACE`.

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.6.0">1.6.0</a> / 2016-01-22
{: #v1-6-0}

As usual in this release, there are new SQL features, improvements to
planning rules and Avatica, and lots of bug fixes. We'll spotlight a
couple of features make it easier to handle complex queries.

[<a href="https://issues.apache.org/jira/browse/CALCITE-816">CALCITE-816</a>]
allows you to represent sub-queries (`EXISTS`, `IN` and scalar) as
`RexSubQuery`, a kind of expression in the relational algebra. Until
now, the sql-to-rel converter was burdened with expanding sub-queries,
and people creating relational algebra directly (or via RelBuilder)
could only create 'flat' relational expressions. Now we have planner
rules to expand and de-correlate sub-queries.

Metadata is the fuel that powers query planning. It includes
traditional query-planning statistics such as cost and row-count
estimates, but also information such as which columns form unique
keys, unique and what predicates are known to apply to a relational
expression's output rows. From the predicates we can deduce which
columns are constant, and following
[<a href="https://issues.apache.org/jira/browse/CALCITE-1023">CALCITE-1023</a>]
we can now remove constant columns from `GROUP BY` keys.

Metadata is often computed recursively, and it is hard to safely and
efficiently calculate metadata on a graph of `RelNode`s that is large,
frequently cyclic, and constantly changing.
[<a href="https://issues.apache.org/jira/browse/CALCITE-794">CALCITE-794</a>]
introduces a context to each metadata call. That context can detect
cyclic metadata calls and produce a safe answer to the metadata
request. It will also allow us to add finer-grained caching and
further tune the metadata layer.

New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-816">CALCITE-816</a>]
  Represent sub-query as a `RexNode`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-854">CALCITE-854</a>]
  Implement `UNNEST ... WITH ORDINALITY`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1003">CALCITE-1003</a>]
  Utility to convert `RelNode` to SQL (Amogh Margoor)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-1010">CALCITE-1010</a>]
    `FETCH/LIMIT` and `OFFSET` in RelToSqlConverter (Amogh Margoor)
  * Move code from `JdbcImplementor` and `JdbcRules` to new class
    `SqlImplementor`
  * Deduce dialect's null collation from `DatabaseMetaData`
  * Fix `RelToSqlConverterTest` on Windows
* Following
  [<a href="https://issues.apache.org/jira/browse/CALCITE-897">CALCITE-897</a>],
  empty string for `boolean` properties means true
* [<a href="https://issues.apache.org/jira/browse/CALCITE-992">CALCITE-992</a>]
  Validate and resolve sequence reference as a `Table` object
* [<a href="https://issues.apache.org/jira/browse/CALCITE-968">CALCITE-968</a>]
  Stream-to-relation and stream-to-stream joins (Milinda Pathirage)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1041">CALCITE-1041</a>]
  User-defined function that returns `DATE` or `TIMESTAMP` value
* [<a href="https://issues.apache.org/jira/browse/CALCITE-986">CALCITE-986</a>]
  User-defined function with `DATE` or `TIMESTAMP` parameters
* [<a href="https://issues.apache.org/jira/browse/CALCITE-958">CALCITE-958</a>]
  Overloaded Table Functions with named arguments (Julien Le Dem)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-970">CALCITE-970</a>]
  If `NULLS FIRST`/`NULLS LAST` not specified, sort `NULL` values high

Avatica features and bug fixes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1040">CALCITE-1040</a>]
  Differentiate better between arrays and scalars in protobuf
* [<a href="https://issues.apache.org/jira/browse/CALCITE-934">CALCITE-934</a>]
  Use an OS-assigned ephemeral port for `CalciteRemoteDriverTest`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-767">CALCITE-767</a>]
  Create Avatica RPC endpoints for commit and rollback commands
* [<a href="https://issues.apache.org/jira/browse/CALCITE-983">CALCITE-983</a>]
  Handle nulls in `ErrorResponse`'s protobuf representation better
* [<a href="https://issues.apache.org/jira/browse/CALCITE-989">CALCITE-989</a>]
  Add server's address in each response
* Fix some bugs found by static analysis
* Make all `equals` and `hashCode` methods uniform
* [<a href="https://issues.apache.org/jira/browse/CALCITE-962">CALCITE-962</a>]
  Propagate the cause, not just the cause's message, from `JdbcMeta`

Planner rules

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1057">CALCITE-1057</a>]
  Add `RelMetadataProvider` parameter to standard planner `Program`s
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1055">CALCITE-1055</a>]
  `SubQueryRemoveRule` should create `Correlate`, not `Join`, for correlated
  sub-queries
* [<a href="https://issues.apache.org/jira/browse/CALCITE-978">CALCITE-978</a>]
  Enable customizing constant folding rule behavior when a `Filter` simplifies
  to false (Jason Altekruse)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-977">CALCITE-977</a>]
  Make the constant expression `Executor` configurable in `FrameworkConfig`
  (Jason Altekruse)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1058">CALCITE-1058</a>]
  Add method `RelBuilder.empty`, and rewrite LIMIT 0 and WHERE FALSE to it
* [<a href="https://issues.apache.org/jira/browse/CALCITE-996">CALCITE-996</a>]
  Simplify predicate when we create a `Filter` operator
* Simplify `RexProgram`, in particular `(NOT CASE ... END) IS TRUE`, which
  occurs in when `NOT IN` is expanded
* Fix variant of
  [<a href="https://issues.apache.org/jira/browse/CALCITE-923">CALCITE-923</a>]
  that occurs in `RelOptRulesTest.testPushFilterPastProject`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1023">CALCITE-1023</a>]
  and
  [<a href="https://issues.apache.org/jira/browse/CALCITE-1038">CALCITE-1038</a>]
  Planner rule that removes `Aggregate` keys that are constant
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1018">CALCITE-1018</a>]
  `SortJoinTransposeRule` not firing due to `getMaxRowCount(RelSubset)` returning
  null
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1019">CALCITE-1019</a>]
  `RelMdUtil.checkInputForCollationAndLimit()` was wrong with `alreadySorted`
  check
* Not safe to use '=' for predicates on constant expressions that might be null
* [<a href="https://issues.apache.org/jira/browse/CALCITE-993">CALCITE-993</a>]
  Pull up all constant expressions, not just literals, as predicates
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1005">CALCITE-1005</a>]
  Handle null in `getMaxRowCount` for `Aggregate` (Mike Hinchey)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-995">CALCITE-995</a>]
  Sort transpose rules might fall in an infinite loop
* [<a href="https://issues.apache.org/jira/browse/CALCITE-987">CALCITE-987</a>]
  Pushing `LIMIT 0` results in an infinite loop (Pengcheng Xiong)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-988">CALCITE-988</a>]
  `FilterToProjectUnifyRule.invert(MutableRel, MutableRel, MutableProject)`
  works incorrectly
* [<a href="https://issues.apache.org/jira/browse/CALCITE-969">CALCITE-969</a>]
  Composite `EnumerableSort` with `DESC` wrongly sorts `NULL` values low
* [<a href="https://issues.apache.org/jira/browse/CALCITE-959">CALCITE-959</a>]
  Add description to `SortProjectTransposeRule`'s constructor

Bug fixes, API changes and minor enhancements

* [<a href="https://issues.apache.org/jira/browse/CALCITE-1060">CALCITE-1060</a>]
  Fix test deadlock by initializing `DriverManager` before registering `AlternatingDriver`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1047">CALCITE-1047</a>]
  `ChunkList.clear` throws `AssertionError`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1053">CALCITE-1053</a>]
  CPU spin, `ReflectiveRelMetadataProvider.apply` waiting for `HashMap.get`
* Upgrade toolbox, to fix line length issue on Windows
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1051">CALCITE-1051</a>]
  Underflow exception due to scaling IN clause literals (Frankie Bollaert)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-975">CALCITE-975</a>]
  Allow Planner to return validated row type together with SqlNode
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1020">CALCITE-1020</a>]
  Add `MILLISECOND` in `TimeUnit` (Pengcheng Xiong)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-794">CALCITE-794</a>]
  Detect cycles when computing statistics
  (**This is a breaking change**.)
* Tune algorithm that deduces the return type of `AND` expression
* [<a href="https://issues.apache.org/jira/browse/CALCITE-842">CALCITE-842</a>]
  Decorrelator gets field offsets confused if fields have been trimmed
* Fix `NullPointerException` in `SqlJoin.toString()`
* Add `ImmutableBitSet.rebuild()`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-915">CALCITE-915</a>]
  Tests now unset `ThreadLocal` values on exit
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1036">CALCITE-1036</a>]
  `DiffRepository` should not insert new resources at the end of the repository
* [<a href="https://issues.apache.org/jira/browse/CALCITE-955">CALCITE-955</a>]
  `Litmus` (continuation-passing style for methods that check invariants)
* `RelBuilder.project` now does nothing if asked to project the identity with
  the same field names
* Deprecate some `Util` methods, and upgrade last Maven modules to JDK 1.7
* Document `RelOptPredicateList`
* Add `ImmutableNullableList.copyOf(Iterable)`
* Fix "endPosTable already set" error from `javac`
* Add benchmark of `Parser.create(sql).parseQuery()`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1042">CALCITE-1042</a>]
  Ensure that `FILTER` is `BOOLEAN NOT NULL`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1039">CALCITE-1039</a>]
  Assign a `SqlKind` value for each built-in aggregate function
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1030">CALCITE-1030</a>]
  JSON `ModelHandler` calling `SchemaPlus.setCacheEnabled()` causes
  `UnsupportedOperationException` when using `SimpleCalciteSchema`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1028">CALCITE-1028</a>]
  Move populate materializations after sql-to-rel conversion
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1034">CALCITE-1034</a>]
  Use a custom checker for code style rules that Checkstyle cannot express
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1032">CALCITE-1032</a>]
  Verify javadoc of private methods
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1015">CALCITE-1015</a>]
  `OFFSET 0` causes `AssertionError` (Zhen Wang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1024">CALCITE-1024</a>]
  In a planner test, if a rule should have no effect, state that explicitly
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1016">CALCITE-1016</a>]
  `GROUP BY *constant*` on empty relation should return 0 rows
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1022">CALCITE-1022</a>]
  Rename `.oq` Quidem files to `.iq`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-980">CALCITE-980</a>]
  Fix `AND` and `OR` implementation in `Enumerable` convention
* [<a href="https://issues.apache.org/jira/browse/CALCITE-459">CALCITE-459</a>]
  When parsing SQL, allow single line comment on last line (Zhen Wang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1009">CALCITE-1009</a>]
  `SelfPopulatingList` is not thread-safe
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1008">CALCITE-1008</a>]
  Replace `Closeable` with `AutoCloseable`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-1001">CALCITE-1001</a>]
  Upgrade to quidem-0.7
* [<a href="https://issues.apache.org/jira/browse/CALCITE-990">CALCITE-990</a>]
  In `VolcanoPlanner`, populate `RelOptRuleCall.nodeInputs` for operands of type
  "any"
* [<a href="https://issues.apache.org/jira/browse/CALCITE-966">CALCITE-966</a>]
  `VolcanoPlanner` now clears `ruleNames` in order to avoid rule name
  conflicting error
* Factor user-defined function tests from `JdbcTest` to `UdfTest`, and classes
  into `Smalls`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-974">CALCITE-974</a>]
  Exception while validating `DELETE` (Yuri Au Yong)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-964">CALCITE-964</a>]
  Rename `timezone` connection property to `timeZone`

Web site and documentation

* Avatica
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-1033">CALCITE-1033</a>]
    Introduce Avatica protobuf documentation
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-1029">CALCITE-1029</a>]
     Add "purpose" descriptions to Avatica JSON docs
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-984">CALCITE-984</a>]
    Massive cleanup of Avatica JSON docs
* [<a href="https://issues.apache.org/jira/browse/CALCITE-861">CALCITE-861</a>]
  Be explicit that `mvn test` needs to be invoked
* [<a href="https://issues.apache.org/jira/browse/CALCITE-997">CALCITE-997</a>]
  Document keywords
* [<a href="https://issues.apache.org/jira/browse/CALCITE-979">CALCITE-979</a>]
  Broken links in web site
* [<a href="https://issues.apache.org/jira/browse/CALCITE-961">CALCITE-961</a>]
  Web site: Add downloads and Apache navigation links
* [<a href="https://issues.apache.org/jira/browse/CALCITE-960">CALCITE-960</a>]
  Download links for pgp, md5, `KEYS` files, and direct from mirrors
* Remove embedded date-stamps from javadoc; add javadoc for test classes
* [<a href="https://issues.apache.org/jira/browse/CALCITE-965">CALCITE-965</a>]
  Link to downloads page from each release news item

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.5.0">1.5.0</a> / 2015-11-06
{: #v1-5-0}

Our first release as a top-level Apache project!

Avatica has undergone major improvements,
including a new RPC layer that uses
[protocol buffers](https://developers.google.com/protocol-buffers/),
support for DML statements, better support for bind variables and
unique identifiers for connections and statements.

There are lots of improvements to planner rules, and the logic
that replaces relational expressions with equivalent materializations.

We continue to find more uses for
[RelBuilder]({{ site.baseurl }}/docs/algebra.html).
We now recommend that you use `RelBuilder` whenever you create
relational expressions within a planner rule; the rule can then be
re-used to create different sub-classes of relational expression, and
the builder will perform simple optimizations automatically.

Using `RelBuilder` we built Piglet,
a subset of the classic Hadoop language
[Pig](https://pig.apache.org/).
Pig is particularly interesting because it makes heavy use of nested
multi-sets.  You can follow this example to implement your own query
language, and immediately taking advantage of Calcite's back-ends and
optimizer rules. It's all just algebra, after all!

New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-911">CALCITE-911</a>]
  Add a variant of `CalciteSchema` that does not cache sub-objects
* [<a href="https://issues.apache.org/jira/browse/CALCITE-845">CALCITE-845</a>]
  Derive `SUM`’s return type by a customizable policy (Maryann Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-916">CALCITE-916</a>]
  Support table function that implements `ScannableTable`
  * Example table function that generates mazes and their solutions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-941">CALCITE-941</a>]
  Named, optional and `DEFAULT` arguments to function calls;
  support named arguments when calling table functions and table macros
* [<a href="https://issues.apache.org/jira/browse/CALCITE-910">CALCITE-910</a>]
  Improve handling of `ARRAY`, `MULTISET`, `STRUCT` types
* [<a href="https://issues.apache.org/jira/browse/CALCITE-879">CALCITE-879</a>]
  `COLLECT` aggregate function
* [<a href="https://issues.apache.org/jira/browse/CALCITE-546">CALCITE-546</a>]
  Allow table, column and field called '*'
* [<a href="https://issues.apache.org/jira/browse/CALCITE-893">CALCITE-893</a>]
  Theta join in JDBC adapter
* Linq4j: Implement `EnumerableDefaults` methods (MiNG)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-823">CALCITE-823</a>]
  Add `ALTER ... RESET` statement (Sudheesh Katkam)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-881">CALCITE-881</a>]
  Allow schema.table.column references in `GROUP BY`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-852">CALCITE-852</a>]
  DDL statements
* [<a href="https://issues.apache.org/jira/browse/CALCITE-851">CALCITE-851</a>]
  Add original SQL string as a field in the parser
* [<a href="https://issues.apache.org/jira/browse/CALCITE-819">CALCITE-819</a>]
  Add `RelRoot`, a contract for the result of a relational expression

Avatica features and bug fixes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-951">CALCITE-951</a>]
  Print the server-side stack in the local exception (Josh Elser)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-936">CALCITE-936</a>]
  Make HttpServer configurable (Navis Ryu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-903">CALCITE-903</a>]
  Enable Avatica client to recover from missing server-side state (Josh Elser)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-921">CALCITE-921</a>]
  Fix incorrectness when calling `getString()` on binary data (Josh Elser)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-913">CALCITE-913</a>]
  Construct proper `ColumnMetaData` for arrays (Josh Elser)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-871">CALCITE-871</a>]
  In `JdbcMeta`, register each statement using an id from a generator (Bruno
  Dumon)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-645">CALCITE-645</a>]
  Implement `AvaticaSqlException` to pass server-side exception information to
  clients (Josh Elser)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-912">CALCITE-912</a>]
  Add Avatica `OpenConnectionRequest` (Bruno Dumon)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-919">CALCITE-919</a>]
  Avoid `setScale` on `BigDecimal` when scale is 0 (Josh Elser)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-927">CALCITE-927</a>]
  Call finagle for all calls that return ResultSetResponses (Josh Elser)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-705">CALCITE-705</a>]
  DML in Avatica, and split `Execute` out from `Fetch` request (Yeong Wei)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-914">CALCITE-914</a>]
  Add `JsonSubType` for `ExecuteResponse`, and fix JSON docs (Josh Elser)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-905">CALCITE-905</a>]
  `getTables` returns empty result in `JdbcMeta` (Jan Van Besien)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-906">CALCITE-906</a>]
  Avatica `JdbcMeta` statement IDs are not unique
* [<a href="https://issues.apache.org/jira/browse/CALCITE-866">CALCITE-866</a>]
  Break out Avatica documentation and add JSON reference (Josh Elser)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-843">CALCITE-843</a>]
  `AvaticaConnection.getAutoCommit` throws `NullPointerException`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-840">CALCITE-840</a>]
  Protocol buffer serialization over HTTP for Avatica Server (Josh Elser)

Materializations

* [<a href="https://issues.apache.org/jira/browse/CALCITE-952">CALCITE-952</a>]
  Organize applicable materializations in reversed topological order (Maryann
  Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-890">CALCITE-890</a>]
  Register all combinations of materialization substitutions (Maryann Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-891">CALCITE-891</a>]
  When substituting materializations, match `TableScan` without `Project`
  (Maryann Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-890">CALCITE-890</a>]
  Register all combinations of materialization substitutions (Maryann Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-925">CALCITE-925</a>]
  Match materialized views when predicates contain strings and ranges (Amogh
  Margoor)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-793">CALCITE-793</a>]
  Planner requires unnecessary collation when using materialized view (Maryann
  Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-825">CALCITE-825</a>]
  Allow user to specify sort order of an `ArrayTable`

Planner rules

* [<a href="https://issues.apache.org/jira/browse/CALCITE-953">CALCITE-953</a>]
  Improve `RelMdPredicates` to deal with `RexLiteral` (Pengcheng Xiong)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-939">CALCITE-939</a>]
  Variant of `SortUnionTransposeRule` for order-preserving `Union`
  (Maryann Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-931">CALCITE-931</a>]
  Wrong collation trait in `SortJoinTransposeRule` for right joins
  (Maryann Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-938">CALCITE-938</a>]
  More accurate rowCount for `Aggregate` applied to already unique keys
  (Maryann Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-935">CALCITE-935</a>]
  Improve how `ReduceExpressionsRule` handles duplicate constraints (Pengcheng
  Xiong)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-922">CALCITE-922</a>]
  Extract value of an `INTERVAL` literal (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-889">CALCITE-889</a>]
  Implement `SortUnionTransposeRule` (Pengcheng Xiong)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-909">CALCITE-909</a>]
  Make `ReduceExpressionsRule` extensible
* [<a href="https://issues.apache.org/jira/browse/CALCITE-856">CALCITE-856</a>]
  Make more rules extensible
* [<a href="https://issues.apache.org/jira/browse/CALCITE-902">CALCITE-902</a>]
  Match nullability when reducing expressions in a `Project`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-895">CALCITE-895</a>]
  Simplify "(`CASE` ... `END`) = constant" inside `AND` or `OR` (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-828">CALCITE-828</a>]
  Use RelBuilder in rules rather than type-specific RelNode factories
* [<a href="https://issues.apache.org/jira/browse/CALCITE-892">CALCITE-892</a>]
  Implement `SortJoinTransposeRule`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-876">CALCITE-876</a>]
  After pushing `LogicalProject` past `LogicalWindow`, adjust references to
  constants properly (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-844">CALCITE-844</a>]
  Push `Project` through `Window` (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-841">CALCITE-841</a>]
  Redundant windows when window function arguments are expressions (Hsuan-Yi
  Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-846">CALCITE-846</a>]
  Push `Aggregate` with `Filter` through `Union(all)`

RelBuilder and Piglet

* [<a href="https://issues.apache.org/jira/browse/CALCITE-933">CALCITE-933</a>]
  `RelBuilder.scan()` now gives a nice exception if the table does not exist
  (Andy Grove)
* Fix Piglet `DUMP` applied to multisets and structs
* Multisets and `COLLECT` in Piglet
* [<a href="https://issues.apache.org/jira/browse/CALCITE-785">CALCITE-785</a>]
  Add "Piglet", a subset of Pig Latin on top of Calcite algebra
* [<a href="https://issues.apache.org/jira/browse/CALCITE-869">CALCITE-869</a>]
  Add `VALUES` command to Piglet
* [<a href="https://issues.apache.org/jira/browse/CALCITE-868">CALCITE-868</a>]
  Add API to execute queries expressed as `RelNode`
* In RelBuilder, build expressions by table alias

Bug fixes, API changes and minor enhancements

* [<a href="https://issues.apache.org/jira/browse/CALCITE-948">CALCITE-948</a>]
  Indicator columns not preserved by `RelFieldTrimmer`
* Fix Windows issues (line endings and checkstyle suppressions)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-937">CALCITE-937</a>]
  User-defined function within view
* [<a href="https://issues.apache.org/jira/browse/CALCITE-926">CALCITE-926</a>]
  Rules fail to match because of missing link to parent equivalence set
  (Maryann Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-908">CALCITE-908</a>]
  Bump protobuf to 3.0.0-beta-1, fix deprecations and update docs (Josh Elser)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-932">CALCITE-932</a>]
  Fix muddled columns when `RelFieldTrimmer` is applied to `Aggregate`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-930">CALCITE-930</a>]
  Now Calcite is a top-level project, remove references to "incubating"
* [<a href="https://issues.apache.org/jira/browse/CALCITE-929">CALCITE-929</a>]
  Calls to `AbstractRelNode` may result in NPE
* [<a href="https://issues.apache.org/jira/browse/CALCITE-923">CALCITE-923</a>]
  Type mismatch when converting `LEFT JOIN` to `INNER`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-666">CALCITE-666</a>]
  Anti-semi-joins against JDBC adapter give wrong results (Yeong Wei)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-918">CALCITE-918</a>]
  `createProject` in `RelOptUtil` should uniquify field names
* [<a href="https://issues.apache.org/jira/browse/CALCITE-792">CALCITE-792</a>]
  Obsolete `RelNode.isKey` and `isDistinct` methods
* Allow FlatLists of different length to be compared
* [<a href="https://issues.apache.org/jira/browse/CALCITE-898">CALCITE-898</a>]
  Type of 'Java<Long> * `INTEGER`' should be `BIGINT`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-894">CALCITE-894</a>]
  Do not generate redundant column alias for the left relation when
  translating `IN` subquery (Maryann Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-897">CALCITE-897</a>]
  Enable debugging using "-Dcalcite.debug"
* [<a href="https://issues.apache.org/jira/browse/CALCITE-885">CALCITE-885</a>]
  Add Oracle test environment
* [<a href="https://issues.apache.org/jira/browse/CALCITE-888">CALCITE-888</a>]
  Overlay window loses `PARTITION BY` list (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-886">CALCITE-886</a>]
  System functions in `GROUP BY` clause
* [<a href="https://issues.apache.org/jira/browse/CALCITE-860">CALCITE-860</a>]
  Correct LICENSE file for generated web site
* [<a href="https://issues.apache.org/jira/browse/CALCITE-882">CALCITE-882</a>]
  Allow web site to be deployed not as the root directory of the web server
  (Josh Elser)
* Upgrade parent POM to apache-17
* [<a href="https://issues.apache.org/jira/browse/CALCITE-687">CALCITE-687</a>]
  Synchronize HSQLDB at a coarse level using a Lock (Josh Elser)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-870">CALCITE-870</a>]
  Remove copyright content from archers.json
* Replace `Stack` with `ArrayDeque`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-874">CALCITE-874</a>]
  `ReflectiveRelMetadataProvider` is not thread-safe
* Add `LogicalWindow.create()`
* Add `ImmutableBitSet.get(int, int)`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-865">CALCITE-865</a>]
  Unknown table type causes `NullPointerException` in `JdbcSchema`
  * Add table types used by Oracle and DB2
* [<a href="https://issues.apache.org/jira/browse/CALCITE-862">CALCITE-862</a>]
  `JdbcSchema` gives `NullPointerException` on non-standard column type (Marc
  Prud'hommeaux)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-847">CALCITE-847</a>]
  `AVG` window function in `GROUP BY` gives `AssertionError` (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-827">CALCITE-827</a>]
  Calcite incorrectly permutes columns of `OVER` query (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-809">CALCITE-809</a>]
  `TableScan` does not support large/infinite scans (Jesse Yates)
* Lazily create exception only when it needs to be thrown (Marc Prud'hommeaux)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-812">CALCITE-812</a>]
  Make JSON reader and writer use properly quoted key names (Marc
  Prud'hommeaux)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-820">CALCITE-820</a>]
  Validate that window functions have `OVER` clause (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-824">CALCITE-824</a>]
  Type inference when converting `IN` clause to semijoin (Josh Wills)


## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.4.0-incubating">1.4.0-incubating</a> / 2015-09-02
{: #v1-4-0}

In addition to a large number of bug fixes and minor enhancements,
this release includes improvements to lattices and materialized views,
and adds a builder API so that you can easily create relational
algebra expressions.

New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-748">CALCITE-748</a>]
      Add `RelBuilder`, builder for expressions in relational algebra
* [<a href="https://issues.apache.org/jira/browse/CALCITE-758">CALCITE-758</a>]
      Use more than one lattice in the same query (Rajat Venkatesh)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-761">CALCITE-761</a>]
      Pre-populated materializations (Maryann Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-786">CALCITE-786</a>]
      Detect if materialized view can be used to rewrite a query in
  non-trivial cases (Amogh Margoor)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-732">CALCITE-732</a>]
      Implement multiple distinct-`COUNT` using `GROUPING SETS`
* Add various `BitSet` and `ImmutableBitSet` utilities

Website updates

* [<a href="https://issues.apache.org/jira/browse/CALCITE-810">CALCITE-810</a>]
      Add committers' organizations to the web site
* Add news item (XLDB best lighting talk), and some talks
* Fix javadoc links
* Add license notice for web site
* Wrap file header in HTML comments
* How to release
* Move disclaimer out of every page's footer and into home page and downloads
  page
* For web site files, add license headers where possible, apache-rat
  exclusions otherwise
* Calcite DOAP
* [<a href="https://issues.apache.org/jira/browse/CALCITE-355">CALCITE-355</a>]
      Web site

Bug fixes, API changes and minor enhancements

* [<a href="https://issues.apache.org/jira/browse/CALCITE-741">CALCITE-741</a>]
      Ensure that the source release's `DEPENDENCIES` file includes all module
  dependencies
* [<a href="https://issues.apache.org/jira/browse/CALCITE-743">CALCITE-743</a>]
      Ensure only a single source assembly is executed
* [<a href="https://issues.apache.org/jira/browse/CALCITE-850">CALCITE-850</a>]
      Remove push down expressions from `FilterJoinRule` and create a new rule
  for it
* [<a href="https://issues.apache.org/jira/browse/CALCITE-834">CALCITE-834</a>]
      `StackOverflowError` getting predicates from the metadata provider
* [<a href="https://issues.apache.org/jira/browse/CALCITE-833">CALCITE-833</a>]
      `RelOptUtil.splitJoinCondition` incorrectly splits a join condition
  (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-822">CALCITE-822</a>]
      Add a unit test case to test collation of `LogicalAggregate`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-822">CALCITE-822</a>]
      Revert incorrect `LogicalAggregate` collation inferring logic made in
  [<a href="https://issues.apache.org/jira/browse/CALCITE-783">CALCITE-783</a>]
  (Milinda Pathirage)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-826">CALCITE-826</a>]
      Use `ProjectFactory` in `AggregateJoinTranposeRule` and `FilterJoinRule`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-821">CALCITE-821</a>]
      `Frameworks` gives NPE when `FrameworkConfig` has no default schema
* [<a href="https://issues.apache.org/jira/browse/CALCITE-811">CALCITE-811</a>]
      Extend `JoinProjectTransposeRule` with option to support outer joins
* [<a href="https://issues.apache.org/jira/browse/CALCITE-805">CALCITE-805</a>]
      Add support for using an alternative grammar specification for left and
  right curly braces. Additionally, add support for including addition token
  manager declarations
* [<a href="https://issues.apache.org/jira/browse/CALCITE-803">CALCITE-803</a>]
      Add `MYSQL_ANSI` Lexing policy
* [<a href="https://issues.apache.org/jira/browse/CALCITE-717">CALCITE-717</a>]
      Compare BINARY and VARBINARY on unsigned byte values (Low Chin Wei)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-814">CALCITE-814</a>]
      `RexBuilder` reverses precision and scale of `DECIMAL` literal
* [<a href="https://issues.apache.org/jira/browse/CALCITE-813">CALCITE-813</a>]
      Upgrade `updateCount`, `maxRows` from int to long
* [<a href="https://issues.apache.org/jira/browse/CALCITE-714">CALCITE-714</a>]
      When de-correlating, push join condition into subquery
* [<a href="https://issues.apache.org/jira/browse/CALCITE-751">CALCITE-751</a>]
      Push aggregate with aggregate functions through join
* Add `RelBuilder.avg`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-806">CALCITE-806</a>]
      `ROW_NUMBER` should emit distinct values
* Document JSON model, making javadoc consistent with the model reference
* [<a href="https://issues.apache.org/jira/browse/CALCITE-808">CALCITE-808</a>]
      Optimize `ProjectMergeRule`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-791">CALCITE-791</a>]
      Optimize `RelOptUtil.pushFilterPastProject`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-783">CALCITE-783</a>]
      Infer collation of `Project` using monotonicity (Milinda Pathirage)
* Change the argument types of `SqlOperator.getMonotonicity` to allow it to be
  used for `RexNode` as well as `SqlNode`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-800">CALCITE-800</a>]
      Window function defined within another window function should be invalid
  (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-787">CALCITE-787</a>]
      Star table wrongly assigned to materialized view (Amogh Margoor)
* Remove duplicate resources from XML test reference files
* [<a href="https://issues.apache.org/jira/browse/CALCITE-795">CALCITE-795</a>]
      Loss of precision when sending a decimal number via the remote JSON
  service (Lukáš Lalinský)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-774">CALCITE-774</a>]
      When `GROUP BY` is present, ensure that window function operands only
  refer to grouping keys (Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-799">CALCITE-799</a>]
      Incorrect result for `HAVING count(*) > 1`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-801">CALCITE-801</a>]
      `NullPointerException` using `USING` on table alias with column aliases
* [<a href="https://issues.apache.org/jira/browse/CALCITE-390">CALCITE-390</a>]
      Infer predicates for semi-join
* [<a href="https://issues.apache.org/jira/browse/CALCITE-789">CALCITE-789</a>]
      `MetaImpl.MetaCatalog` should expose `TABLE_CAT` instead of
      `TABLE_CATALOG`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-752">CALCITE-752</a>]
      Add back sqlline as a dependency to csv example
* [<a href="https://issues.apache.org/jira/browse/CALCITE-780">CALCITE-780</a>]
      HTTP error 413 when sending a long string to the Avatica server
* In `RelBuilder`, calling `sort` then `limit` has same effect as calling
  `sortLimit`
* Add `Ord.reverse`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-788">CALCITE-788</a>]
      Allow `EnumerableJoin` to be sub-classed (Li Yang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-280">CALCITE-280</a>]
      `BigDecimal` underflow (Li Yang)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-763">CALCITE-763</a>]
      Missing translation from `Sort` to `MutableSort` (Maryann Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-770">CALCITE-770</a>]
      Ignore window aggregates and ranking functions when finding aggregate
  functions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-765">CALCITE-765</a>]
      Set `Content-Type` from the RPC server to `application/json` (Lukáš Lalinský)
* Fix Windows line-endings in `RelBuilderTest`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-727">CALCITE-727</a>]
      Constant folding involving `CASE` and `NULL`
* Related to
  [<a href="https://issues.apache.org/jira/browse/CALCITE-758">CALCITE-758</a>],
  speed up matching by not considering tiles separately from other
  materialized views
* Test case and workaround for
  [<a href="https://issues.apache.org/jira/browse/CALCITE-760">CALCITE-760</a>]
      `Aggregate` recommender blows up if row count estimate is too high
* [<a href="https://issues.apache.org/jira/browse/CALCITE-753">CALCITE-753</a>]
      `Aggregate` operators may derive row types with duplicate column names
* [<a href="https://issues.apache.org/jira/browse/CALCITE-457">CALCITE-457</a>]
      Push condition of non-ansi join into join operator
* Change jsonRequest encoding to UTF-8 (Guitao Ding)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-757">CALCITE-757</a>]
      Fix expansion of view of another view (Venki Korukanti)
* Fix coverity warnings
* Remove deprecated `SqlTypeName` methods
* [<a href="https://issues.apache.org/jira/browse/CALCITE-754">CALCITE-754</a>]
      Validator error when resolving `OVER` clause of `JOIN` query
* [<a href="https://issues.apache.org/jira/browse/CALCITE-429">CALCITE-429</a>]
      Cardinality provider for use by lattice algorithm
* [<a href="https://issues.apache.org/jira/browse/CALCITE-740">CALCITE-740</a>]
      Redundant `WHERE` clause causes wrong result in MongoDB adapter
* [<a href="https://issues.apache.org/jira/browse/CALCITE-665">CALCITE-665</a>]
      `ClassCastException` in MongoDB adapter
* Separate `TableFactory` from suggested table name, so one `TableFactory` can be
  used for several tables
* [<a href="https://issues.apache.org/jira/browse/CALCITE-749">CALCITE-749</a>]
      Add `MaterializationService.TableFactory` (Rajat Venkatesh)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-718">CALCITE-718</a>]
      Enable fetch to work for `Statement.execute()` for Avatica (Xavier Leong)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-712">CALCITE-712</a>]
      Obey `setMaxRows` for statement execute (Xavier Leong)
* Add `LoggingLocalJsonService`, to make it easier to test that JDBC requests
  cause the right RPCs
* [<a href="https://issues.apache.org/jira/browse/CALCITE-708">CALCITE-708</a>]
      Implement `DatabaseMetaData.getTypeInfo` (Xavier Leong)
* Enable Travis CI on new-master branch and bug-fix branches named
  "NNN-description"
* Clean up
* Upgrade tpcds
* Make `JdbcTest.testVersion` more permissive, so that `version.major` and
  `version.minor` can be set just before a release, rather than just after as at
  present

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.3.0-incubating">1.3.0-incubating</a> / 2015-05-30
{: #v1-3-0}

Mainly bug-fixes, but this release adds support for
<a href="https://issues.apache.org/jira/browse/CALCITE-505">modifiable views</a>
and
<a href="https://issues.apache.org/jira/browse/CALCITE-704">filtered aggregate functions</a>
and various improvements to Avatica.

New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-505">CALCITE-505</a>]
  Support modifiable view
* [<a href="https://issues.apache.org/jira/browse/CALCITE-704">CALCITE-704</a>]
  `FILTER` clause for aggregate functions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-522">CALCITE-522</a>]
  In remote JDBC driver, transmit static database properties as a map
* [<a href="https://issues.apache.org/jira/browse/CALCITE-661">CALCITE-661</a>]
  Remote fetch in Calcite JDBC driver
* Support Date, Time, Timestamp parameters

API changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-722">CALCITE-722</a>]
  Rename markdown files to lower-case
* [<a href="https://issues.apache.org/jira/browse/CALCITE-697">CALCITE-697</a>]
  Obsolete class `RelOptQuery`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-693">CALCITE-693</a>]
  Allow clients to control creation of `RelOptCluster`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-691">CALCITE-691</a>]
  Allow projects to supply alternate SQL parser
* [<a href="https://issues.apache.org/jira/browse/CALCITE-675">CALCITE-675</a>]
  Enable `AggregateProjectMergeRule` in standard rule set
* [<a href="https://issues.apache.org/jira/browse/CALCITE-679">CALCITE-679</a>]
  Factory method for `SemiJoin`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-674">CALCITE-674</a>]
  Add a `SWAP_OUTER` static instance to `JoinCommuteRule` (Maryann Xue)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-735">CALCITE-735</a>]
  `Primitive.DOUBLE.min` should be large and negative

Bug-fixes and internal changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-688">CALCITE-688</a>]
  `splitCondition` does not behave correctly when one side of the condition
  references columns from different inputs
* [<a href="https://issues.apache.org/jira/browse/CALCITE-259">CALCITE-259</a>]
  Using sub-queries in `CASE` statement against JDBC tables generates invalid
  Oracle SQL (Yeong Wei)
* In sample code in README.md, rename optiq to calcite (Ethan)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-720">CALCITE-720</a>]
  `VolcanoPlanner.ambitious` comment doc is inconsistent (Santiago M. Mola)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-729">CALCITE-729</a>]
  `IndexOutOfBoundsException` in `ROLLUP` query on JDBC data source
* [<a href="https://issues.apache.org/jira/browse/CALCITE-733">CALCITE-733</a>]
  Multiple distinct-`COUNT` query gives wrong results
* [<a href="https://issues.apache.org/jira/browse/CALCITE-730">CALCITE-730</a>]
  `ClassCastException` in table from `CloneSchema`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-728">CALCITE-728</a>]
  Test suite hangs on Windows
* [<a href="https://issues.apache.org/jira/browse/CALCITE-723">CALCITE-723</a>]
  Document lattices
* [<a href="https://issues.apache.org/jira/browse/CALCITE-515">CALCITE-515</a>]
  Add Apache headers to markdown files
* Upgrade quidem
* [<a href="https://issues.apache.org/jira/browse/CALCITE-716">CALCITE-716</a>]
  Scalar sub-query and aggregate function in `SELECT` or `HAVING` clause gives
  `AssertionError`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-694">CALCITE-694</a>]
  Scan `HAVING` clause for sub-queries and `IN`-lists (Hsuan-Yi Chu)
* Upgrade hydromatic-resource-maven-plugin
* [<a href="https://issues.apache.org/jira/browse/CALCITE-710">CALCITE-710</a>]
  Identical conditions in the `WHERE` clause cause `AssertionError` (Sean
  Hsuan-Yi Chu)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-695">CALCITE-695</a>]
  Do not add `SINGLE_VALUE` aggregate function to a sub-query that will never
  return more than one row (Hsuan-Yi Chu)
* Add tests for scalar sub-queries, including test cases for
  [<a href="https://issues.apache.org/jira/browse/CALCITE-709">CALCITE-709</a>]
  Errors with `LIMIT` inside scalar sub-query
* [<a href="https://issues.apache.org/jira/browse/CALCITE-702">CALCITE-702</a>]
  Add validator test for monotonic expressions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-699">CALCITE-699</a>]
  In Avatica, synchronize access to Calendar
* [<a href="https://issues.apache.org/jira/browse/CALCITE-700">CALCITE-700</a>]
  Pass time zone into tests
* [<a href="https://issues.apache.org/jira/browse/CALCITE-698">CALCITE-698</a>]
  For `GROUP BY ()`, `areColumnsUnique()` should return true for any key
* Disable tests that fail under JDK 1.7 due to
  [<a href="https://issues.apache.org/jira/browse/CALCITE-687">CALCITE-687</a>]
* Add "getting started" to HOWTO
* [<a href="https://issues.apache.org/jira/browse/CALCITE-692">CALCITE-692</a>]
  Add back sqlline as a dependency
* [<a href="https://issues.apache.org/jira/browse/CALCITE-677">CALCITE-677</a>]
  `RemoteDriverTest.testTypeHandling` fails east of Greenwich
* Disable test for
  [<a href="https://issues.apache.org/jira/browse/CALCITE-687">CALCITE-687</a>]
  Make `RemoteDriverTest.testStatementLifecycle` thread-safe
* [<a href="https://issues.apache.org/jira/browse/CALCITE-686">CALCITE-686</a>]
  `SqlNode.unparse` produces invalid SQL
* [<a href="https://issues.apache.org/jira/browse/CALCITE-507">CALCITE-507</a>]
  Update HOWTO.md with running integration tests
* Add H2 integration test
* Add PostgreSQL integration test
* [<a href="https://issues.apache.org/jira/browse/CALCITE-590">CALCITE-590</a>]
  Update MongoDB test suite to calcite-test-dataset
* Add `CalciteAssert.assertArrayEqual` for more user-friendly asserts
* [<a href="https://issues.apache.org/jira/browse/CALCITE-585">CALCITE-585</a>]
  Avatica JDBC methods should throw `SQLFeatureNotSupportedException` (Ng Jiunn
  Jye)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-671">CALCITE-671</a>]
  `ByteString` does not deserialize properly as a `FetchRequest` parameter value
* [<a href="https://issues.apache.org/jira/browse/CALCITE-676">CALCITE-676</a>]
  `AssertionError` in `GROUPING SETS` query
* [<a href="https://issues.apache.org/jira/browse/CALCITE-678">CALCITE-678</a>]
  `SemiJoinRule` mixes up fields when `Aggregate.groupSet` is not field #0

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.2.0-incubating">1.2.0-incubating</a> / 2015-04-07
{: #v1-2-0}

A short release, less than a month after 1.1.

There have been many changes to Avatica, hugely improving its coverage of the
JDBC API and overall robustness. A new provider, `JdbcMeta`, allows
you to remote an existing JDBC driver.

[<a href="https://issues.apache.org/jira/browse/CALCITE-606">CALCITE-606</a>]
improves how the planner propagates traits such as collation and
distribution among relational expressions.

[<a href="https://issues.apache.org/jira/browse/CALCITE-613">CALCITE-613</a>]
and [<a href="https://issues.apache.org/jira/browse/CALCITE-307">CALCITE-307</a>]
improve implicit and explicit conversions in SQL.

New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-366">CALCITE-366</a>]
  Support Aggregate push down in bushy joins (Jesus Camacho Rodriguez)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-613">CALCITE-613</a>]
  Implicitly convert character values in comparisons
* [<a href="https://issues.apache.org/jira/browse/CALCITE-307">CALCITE-307</a>]
  Implement `CAST` between date-time types
* [<a href="https://issues.apache.org/jira/browse/CALCITE-634">CALCITE-634</a>]
  Allow `ORDER BY` aggregate function in `SELECT DISTINCT`, provided that it
  occurs in `SELECT` clause (Sean Hsuan-Yi Chu)
* In linq4j, implement `firstOrDefault`, `single`, and `singleOrDefault` methods
  (Daniel Cooper)
* JDBC adapter
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-631">CALCITE-631</a>]
    Push theta joins down to JDBC adapter (Ng Jiunn Jye)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-657">CALCITE-657</a>]
    `NullPointerException` when executing `JdbcAggregate.implement`
    method (Yuri Au Yong)
* Metadata
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-659">CALCITE-659</a>]
    Missing types in `averageTypeValueSize` method in `RelMdSize`
    (Jesus Camacho Rodriguez)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-650">CALCITE-650</a>]
    Add metadata for average size of a tuple in `SemiJoin` (Jesus
    Camacho Rodriguez)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-649">CALCITE-649</a>]
    Extend `splitCondition` method in `RelOptUtil` to handle multiple
    joins on the same key (Jesus Camacho Rodriguez)

Avatica features and bug fixes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-670">CALCITE-670</a>]
  `AvaticaPreparedStatement` should support `execute()` and
  `executeUpdate()` (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-641">CALCITE-641</a>]
  Implement logging throughout Avatica server (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-646">CALCITE-646</a>]
  `AvaticaStatement.execute` method broken over remote JDBC (Yeong Wei
  and Julian Hyde)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-660">CALCITE-660</a>]
  Improve Avatica date support
* [<a href="https://issues.apache.org/jira/browse/CALCITE-655">CALCITE-655</a>]
  Implement `ConnectionSync` RPC (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-654">CALCITE-654</a>]
  Tighten up `AvaticaStatement.execute` semantics (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-658">CALCITE-658</a>]
  Cleanup dependency usage (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-652">CALCITE-652</a>]
  Move server pieces of `avatica` into `avatica-server` (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-651">CALCITE-651</a>]
  In `JdbcMeta`, convert property definitions to an enum (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-640">CALCITE-640</a>]
  Avatica server should expire stale connections/statements (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-639">CALCITE-639</a>]
  Open up permissions on avatica server components (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-637">CALCITE-637</a>]
  Implement Avatica `CloseConnection` RPC (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-636">CALCITE-636</a>]
  Connection isolation for Avatica clients (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-626">CALCITE-626</a>]
  Implement `CloseStatement` RPC (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-630">CALCITE-630</a>]
  Flesh out `AvaticaParameter.setObject` (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-627">CALCITE-627</a>]
  Add Avatica support for `getTableTypes`, `getColumns` (Xavier FH Leong)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-618">CALCITE-618</a>]
  Add Avatica support for `getTables` (Julian Hyde and Nick Dimiduk)

API changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-617">CALCITE-617</a>]
  Check at initialization time in `CachingInvocationHandler` that MD provider
  is not null (Jesus Camacho Rodriguez)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-638">CALCITE-638</a>]
  SQL standard `REAL` is 4 bytes, `FLOAT` is 8 bytes

Bug-fixes and internal changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-672">CALCITE-672</a>]
  SQL `ANY` type should be nullable (Jinfeng Ni)
* Disable tests, pending
  [<a href="https://issues.apache.org/jira/browse/CALCITE-673">CALCITE-673</a>]
  Timeout executing joins against MySQL
* Fix traits in MongoDB adapter, and `NullPointerException` in `JdbcTest`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-662">CALCITE-662</a>]
  Query validation fails when an `ORDER BY` clause is used with `WITH CLAUSE`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-606">CALCITE-606</a>]
  Fix trait propagation and add test case
* Remove checkstyle Eclipse properties from git tracking
* [<a href="https://issues.apache.org/jira/browse/CALCITE-644">CALCITE-644</a>]
  Increase check style line limit to 100 chars (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-648">CALCITE-648</a>]
  Update `ProjectMergeRule` description for new naming convention (Jinfeng Ni)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-625">CALCITE-625</a>]
  `README.md` linking to the wrong page of `optiq-csv` (hongbin ma)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-632">CALCITE-632</a>]
  Sort order returned by `SUPERCLASS_COMPARATOR` in
  `ReflectiveRelMetadataProvider` is inconsistent (Jesus Camacho
  Rodriguez)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-335">CALCITE-335</a>]
  Remove uses of linq4j `Functions.adapt`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-592">CALCITE-592</a>]
  Upgrade to Guava 14.0.1
* [<a href="https://issues.apache.org/jira/browse/CALCITE-596">CALCITE-596</a>]
  JDBC adapter incorrectly reads null values as 0 (Ng Jiunn Jye)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-633">CALCITE-633</a>]
  `WITH ... ORDER BY` cannot find table
* [<a href="https://issues.apache.org/jira/browse/CALCITE-614">CALCITE-614</a>]
  `IN` clause in `CASE` in `GROUP BY` gives `AssertionError`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-619">CALCITE-619</a>]
  Slim down dependencies in parent POM

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.1.0-incubating">1.1.0-incubating</a> / 2015-03-13
{: #v1-1-0}

This Calcite release makes it possible to exploit physical properties
of relational expressions to produce more efficient plans, introducing
collation and distribution as traits, `Exchange` relational operator,
and several new forms of metadata.

We add experimental support for streaming SQL.

This release drops support for JDK 1.6; Calcite now requires 1.7 or
later.

We have introduced static `create` methods for many sub-classes of
`RelNode`. We strongly suggest that you use these rather than
calling constructors directly.

New features

* SQL
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-602">CALCITE-602</a>]
    Streaming queries (experimental)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-588">CALCITE-588</a>]
    Allow `TableMacro` to consume maps and collections
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-583">CALCITE-583</a>]
    Operator `||` mishandles `ANY` type (Sean Hsuan-Yi Chu)
* Planner rule improvements
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-445">CALCITE-445</a>]
    Pull up filters rejected by a `ProjectableFilterableTable`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-600">CALCITE-600</a>]
    Use `SetOpFactory` in rules containing `Union` operator (Jesus
    Camacho Rodriguez)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-603">CALCITE-603</a>]
    Metadata providers for size, memory, parallelism
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-607">CALCITE-607</a>]
      Change visibility of constructor in metadata providers for size,
      memory, parallelism (Jesus Camacho Rodriguez)
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-608">CALCITE-608</a>]
      Exception is thrown when `RelMdDistribution` for `Project`
      operator is called (Jesus Camacho Rodriguez)
* Collation and distribution as traits
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-88">CALCITE-88</a>]
    Add collation as a trait and a kind of `RelNode` metadata
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-569">CALCITE-569</a>]
    `ArrayIndexOutOfBoundsException` when deducing collation (Aman Sinha)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-581">CALCITE-581</a>]
    Add `LogicalSort` relational expression, and make `Sort` abstract
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-526">CALCITE-526</a>]
    Add `EnumerableMergeJoin`, which exploits sorted inputs
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-71">CALCITE-71</a>]
    Provide a way to declare that tables are sorted
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-576">CALCITE-576</a>]
    Make `RelCollation` trait and `AbstractRelNode.getCollationList` consistent
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-254">CALCITE-254</a>]
    Propagate `RelCollation` on aliased columns in `JoinRule`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-569">CALCITE-569</a>]
    `ArrayIndexOutOfBoundsException` when deducing collation
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-594">CALCITE-594</a>]
    Add `RelDistribution` trait and `Exchange` relational expression

API changes

* Many sub-classes of `RelNode` now have a static `create` method
  which automatically sets up traits such as collation and
  distribution. The constructors are not marked deprecated, but we
  strongly suggest that you use the `create` method if it exists.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-591">CALCITE-591</a>]
  Drop support for Java 1.6 (and JDBC 4.0)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-587">CALCITE-587</a>]
  Upgrade `jetty-server` to 9.2.7.v20150116 and port avatica-server `HttpServer`
  (Trevor Hartman)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-577">CALCITE-577</a>]
  Revert temporary API changes introduced in
  [<a href="https://issues.apache.org/jira/browse/CALCITE-575">CALCITE-575</a>]
* Add means to create `Context` instances by wrapping objects and by chaining
  contexts
* [<a href="https://issues.apache.org/jira/browse/CALCITE-599">CALCITE-599</a>]
  `EquiJoin` in wrong package (Jesus Camacho Rodriguez)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-573">CALCITE-573</a>]
  Use user-given names in `RelOptUtil.createProject` and `createRename`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-572">CALCITE-572</a>]
  Remove `Project.flags` (methods are deprecated, to be removed before 2.0)

Bug-fixes and internal changes

* Remove the `LICENSE` file of calcite-example-csv (the former
  optiq-csv) and move its history into main history
* [<a href="https://issues.apache.org/jira/browse/CALCITE-615">CALCITE-615</a>]
  AvaticaParameter should be Jackson serializable (Nick Dimiduk)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-612">CALCITE-612</a>]
  Update AvaticaStatement to handle cancelled queries (Parth Chandra)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-605">CALCITE-605</a>]
  Reduce dependency on third-party maven repositories
* [<a href="https://issues.apache.org/jira/browse/CALCITE-611">CALCITE-611</a>]
  Method `setAggChildKeys` should take into account indicator columns of
  `Aggregate` operator (Jesus Camacho Rodriguez)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-566">CALCITE-566</a>]
  `ReduceExpressionsRule` requires planner to have an `Executor`
* Refactor `TableScanNode.create` method
* [<a href="https://issues.apache.org/jira/browse/CALCITE-593">CALCITE-593</a>]
  Validator in `Frameworks` should expand identifiers (Jinfeng Ni)
* Australian time-zones changed in `tzdata2014f`, Java 1.8.0_31
* [<a href="https://issues.apache.org/jira/browse/CALCITE-580">CALCITE-580</a>]
  Average aggregation on an `Integer` column throws `ClassCastException`
* In Travis, ask Surefire to print results to screen
* [<a href="https://issues.apache.org/jira/browse/CALCITE-586">CALCITE-586</a>]
  Prevent JSON serialization of `Signature.internalParameters`

## <a href="https://github.com/apache/calcite/releases/tag/calcite-1.0.0-incubating">1.0.0-incubating</a> / 2015-01-31
{: #v1-0-0}

Calcite's first major release.

Since the previous release we have re-organized the into the `org.apache.calcite`
namespace. To make migration of your code easier, we have described the
<a href="https://issues.apache.org/jira/secure/attachment/12681620/mapping.txt">mapping from old to new class names</a>
as an attachment to
[<a href="https://issues.apache.org/jira/browse/CALCITE-296">CALCITE-296</a>].

The release adds SQL support for `GROUPING SETS`, `EXTEND`, `UPSERT` and sequences;
a remote JDBC driver;
improvements to the planner engine and built-in planner rules;
improvements to the algorithms that implement the relational algebra,
including an interpreter that can evaluate queries without compilation;
and fixes about 30 bugs.

New features

* SQL
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-494">CALCITE-494</a>]
    Support `NEXT`/`CURRENT VALUE FOR` syntax for using sequences
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-492">CALCITE-492</a>]
    Support `UPSERT` statement in parser
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-493">CALCITE-493</a>]
    Add `EXTEND` clause, for defining columns and their types at query/DML time
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-497">CALCITE-497</a>]
    Support optional qualifier for column name references
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-356">CALCITE-356</a>]
    Allow column references of the form `schema.table.column`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-462">CALCITE-462</a>]
    Allow table functions in `LATERAL` expression
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-282">CALCITE-282</a>]
    Add `{fn QUARTER(date)}` function (Benoy Antony)
  * Grouping sets
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-370">CALCITE-370</a>]
      Support `GROUPING SETS`, `CUBE`, `ROLLUP` in SQL and algebra
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-512">CALCITE-512</a>]
      Add `GROUP_ID`,`GROUPING_ID`, `GROUPING` functions
* Planner rule improvements
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-92">CALCITE-92</a>]
    Optimize away `Project` that merely renames fields
  * Detect and merge duplicate predicates `AND(x, y, x)` to `AND(x, y)` in more
    circumstances
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-557">CALCITE-557</a>]
    Speed up planning by never creating `AbstractConverter`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-545">CALCITE-545</a>]
    When a projected expression can only have one value, replace with that
    constant
  * Grouping sets
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-542">CALCITE-542</a>]
      Support for `Aggregate` with grouping sets in `RelMdColumnOrigins` (Jesus
      Camacho Rodriguez)
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-533">CALCITE-533</a>]
      Support for grouping sets in `FilterAggregateTransposeRule` (Jesus Camacho
      Rodriguez)
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-532">CALCITE-532</a>]
      Support for grouping sets in `AggregateFilterTransposeRule` (Jesus Camacho
      Rodriguez)
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-513">CALCITE-513</a>]
      Support for grouping sets in `AggregateProjectMergeRule` (Jesus Camacho
      Rodriguez)
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-510">CALCITE-510</a>]
      Support for grouping sets in `AggregateExpandDistinctAggregatesRule` (Jesus
      Camacho Rodriguez)
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-502">CALCITE-502</a>]
      Support for grouping sets in `AggregateUnionTransposeRule` (Jesus Camacho
      Rodriguez)
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-503">CALCITE-503</a>]
      Tests to check rules on `Aggregate` operator without grouping sets (Jesus
      Camacho Rodriguez)
* Algorithms
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-451">CALCITE-451</a>]
    Implement theta join, inner and outer, in enumerable convention
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-489">CALCITE-489</a>]
    Update `Correlate` mechanics and implement `EnumerableCorrelate` (aka nested
    loops join)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-544">CALCITE-544</a>]
    Implement `Union` in interpreter
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-562">CALCITE-562</a>]
    Implement inner `Join` in interpreter and improve handling of scalar expressions
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-543">CALCITE-543</a>]
    Implement `Aggregate` (including `GROUPING SETS`) in interpreter (Jacques
    Nadeau)
  * In progress towards
    [<a href="https://issues.apache.org/jira/browse/CALCITE-558">CALCITE-558</a>]
    add `BINDABLE` convention (but `ENUMERABLE` is still the default), and add
    `ArrayBindable` and `Scalar` interfaces
* Remote driver
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-93">CALCITE-93</a>]
    Calcite RPC server
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-94">CALCITE-94</a>]
    Remote JDBC driver
  * Make `JsonHandler` and `JsonService` thread-safe

API changes

* The great code re-org
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-296">CALCITE-296</a>]
    Re-organize package structure
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-419">CALCITE-419</a>]
    Naming convention for planner rules
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-306">CALCITE-306</a>]
    Standardize code style for "import package.*;"
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-474">CALCITE-474</a>]
    Clean up rule naming in order to support enabling/disabling rules
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-460">CALCITE-460</a>]
    Add `ImmutableBitSet` and replace uses of `BitSet`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-479">CALCITE-479</a>]
    Migrate `RelNode.getChildExps` to `RelNode.accept(RexShuttle)`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-527">CALCITE-527</a>]
    Drop `rowType` field and constructor/copy argument of `Calc`
* Add linq4j and example-csv modules
  * Remove unused packages in linq4j, and fix checkstyle issues in linq4j and csv
  * Add calcite-linq4j and calcite-example-csv as POM sub-modules
  * Import 'optiq-csv' project as 'example/csv/', and add Apache headers
  * Import 'linq4j' project, and add Apache headers
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-478">CALCITE-478</a>]
    Move CSV tutorial (Siva Narayanan)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-464">CALCITE-464</a>]
  Make parser accept configurable max length for SQL identifier
* [<a href="https://issues.apache.org/jira/browse/CALCITE-465">CALCITE-465</a>]
  Remove `OneRow` and `Empty` relational expressions; `Values` will suffice

Bug-fixes and internal changes

* Build improvements
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-541">CALCITE-541</a>]
    Update maven-source-plugin to 2.4 to get speedup in jdk 1.8
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-537">CALCITE-537</a>]
    Skip overwrite of `NOTICE`, `DEPENDENCIES`, and `LICENSE` files
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-538">CALCITE-538</a>]
    Generate `Parser.jj` only at first build
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-539">CALCITE-539</a>]
    Avoid rewrite of `org-apache-calcite-jdbc.properties`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-540">CALCITE-540</a>]
    Create git.properties file only at first build. This saves time in
    development at a cost of stale `git.properties`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-536">CALCITE-536</a>]
    Add `@PackageMarker` to `package-info.java` so maven-compiler skips
    compilation when the sources are unchanged
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-535">CALCITE-535</a>]
    Support skip overwrite in hydromatic-resource
* [<a href="https://issues.apache.org/jira/browse/CALCITE-582">CALCITE-582</a>]
  `EnumerableTableScan` broken when table has single column
* [<a href="https://issues.apache.org/jira/browse/CALCITE-575">CALCITE-575</a>]
  Variant of `ProjectRemoveRule` that considers a project trivial only if its
  field names are identical (John Pullokkaran)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-571">CALCITE-571</a>]
  `ReduceExpressionsRule` tries to reduce `SemiJoin` condition to non-equi
  condition
* [<a href="https://issues.apache.org/jira/browse/CALCITE-568">CALCITE-568</a>]
  Upgrade to a version of `pentaho-aggdesigner` that does not pull in
  `servlet-api`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-567">CALCITE-567</a>]
  Make `quidem` dependency have scope "test"
* [<a href="https://issues.apache.org/jira/browse/CALCITE-570">CALCITE-570</a>]
  `ReduceExpressionsRule` throws "duplicate key" exception
* [<a href="https://issues.apache.org/jira/browse/CALCITE-561">CALCITE-561</a>]
  Upgrade parent POM
* [<a href="https://issues.apache.org/jira/browse/CALCITE-458">CALCITE-458</a>]
  ArrayIndexOutOfBoundsException when using just a single column in interpreter
* Fix spurious extra row from `FULL JOIN`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-554">CALCITE-554</a>]
  Outer join over NULL keys generates wrong result
* [<a href="https://issues.apache.org/jira/browse/CALCITE-489">CALCITE-489</a>]
  Teach `CalciteAssert` to respect multiple settings
* [<a href="https://issues.apache.org/jira/browse/CALCITE-516">CALCITE-516</a>]
  `GROUP BY` on a `CASE` expression containing `IN` predicate fails (Aman Sinha)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-552">CALCITE-552</a>]
  Upgrade tpcds (which depends on an old version of guava)
* Copy identifier when fully-qualifying, so column aliases have the right case
* [<a href="https://issues.apache.org/jira/browse/CALCITE-548">CALCITE-548</a>]
  Extend `induce` method to return `CUBE` and `ROLLUP` (Jesus Camacho Rodriguez)
  * Simplify `Group.induce` by assuming that group sets are sorted
* Test case for
  [<a  href="https://issues.apache.org/jira/browse/CALCITE-212">CALCITE-212</a>]
  Join condition with `OR`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-550">CALCITE-550</a>]
  Case-insensitive matching of sub-query columns fails
  * Add more unit tests (Jinfeng Ni)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-448">CALCITE-448</a>]
  `FilterIntoJoinRule` creates filters containing invalid `RexInputRef`
* When registering a `RelNode`, be tolerant if it is equivalent to a `RelNode`
  with different traits
* [<a href="https://issues.apache.org/jira/browse/CALCITE-547">CALCITE-547</a>]
  Set nullability while inferring return type of `item(any,...)` operator
* In Travis CI, enable containers, and cache `.m2` directory
* [<a href="https://issues.apache.org/jira/browse/CALCITE-534">CALCITE-534</a>]
  Missing implementation of `ResultSetMetaData.getColumnClassName` (Knut
  Forkalsrud)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-506">CALCITE-506</a>]
  Update `EnumerableRelImplementor.stash` so it is suitable for all kinds of
  classes
* Merge join algorithm for `Enumerable`s
* Efficient `Enumerable` over random-access list
* Add a test that calls all functions with arguments of all types that they
  claim to accept
* [<a href="https://issues.apache.org/jira/browse/CALCITE-511">CALCITE-511</a>]
  `copy` method in `LogicalAggregate` not copying the indicator value properly
* Add a model that has lattices and works against HSQLDB
* [<a href="https://issues.apache.org/jira/browse/CALCITE-509">CALCITE-509</a>]
  `RelMdColumnUniqueness` uses `ImmutableBitSet.Builder` twice, gets
  `NullPointerException`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-488">CALCITE-488</a>]
  `Enumerable<Holder>` does not work if where `Holder` is a custom class
  with a single field; Calcite tries to treat it as `SCALAR` due to premature
  `JavaRowFormat.optimize`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-352">CALCITE-352</a>]
  Throw exception if `ResultSet.next()` is called after `close()`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-403">CALCITE-403</a>]
  `Enumerable` gives `NullPointerException` with `NOT` on nullable expression
* [<a href="https://issues.apache.org/jira/browse/CALCITE-469">CALCITE-469</a>]
  Update example/csv README.md instructions
* Document `WITH`, `LATERAL`, `GROUPING SETS`, `CUBE`, `ROLLUP`;
  add descriptions for all built-in functions and operators
* [<a href="https://issues.apache.org/jira/browse/CALCITE-470">CALCITE-470</a>]
  Print warning when column type hint is not understood;
  Update `EMPS.deptno` column Integer &rarr; int
* Fix `Linq4j.product`; the cartesian product of 0 attributes is one row of 0
  attributes
* Update link optiq-mat-plugin &rarr; mat-calcite-plugin
* [<a href="https://issues.apache.org/jira/browse/CALCITE-467">CALCITE-467</a>]
  Incorrect namespace in `package-info.java`
* Add headers, to appease the RAT
* [<a href="https://issues.apache.org/jira/browse/CALCITE-446">CALCITE-446</a>]
  CSV adapter should read from directory relative to the model file
* Add examples of scannable and filterable tables, matching
  [<a href="https://issues.apache.org/jira/browse/CALCITE-436">CALCITE-436</a>]
  Simpler SPI to query Table
* Fix `JdbcTest.testVersion` now that version is 1.0
* Update release HOWTO

## <a href="https://github.com/apache/calcite/releases/tag/calcite-0.9.2-incubating">0.9.2-incubating</a> / 2014-11-05
{: #v0-9-2}

A fairly minor release, and last release before we rename all of the
packages and lots of classes, in what we expect to call 1.0. If you
have an existing application, it's worth upgrading to this first,
before you move on to 1.0.

New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-436">CALCITE-436</a>]
  Simpler SPI to query `Table`

API changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-447">CALCITE-447</a>]
  Change semi-join rules to make use of factories
* [<a href="https://issues.apache.org/jira/browse/CALCITE-442">CALCITE-442</a>
  Add `RelOptRuleOperand` constructor that takes a predicate

Bug-fixes and internal changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-397">CALCITE-397</a>]
  `SELECT DISTINCT *` on reflective schema gives `ClassCastException` at runtime
* Various lattice improvements.
* sqlline: Looking for class-path in inconsistent locations.
* Re-order test suite, so that fast tests are run first.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-444">CALCITE-444</a>]
  Filters wrongly pushed into full outer join
* Make it more convenient to unit test `RelMetadataQuery`, and add some more
  tests for
  [<a href="https://issues.apache.org/jira/browse/CALCITE-443">CALCITE-443</a>]
* [<a href="https://issues.apache.org/jira/browse/CALCITE-443">CALCITE-443</a>]
  `getPredicates` from a Union is not correct
* Update references to web sites, git repositories, jira, mailing lists,
  travis CI now that [INFRA-8413] is fixed
* [<a href="https://issues.apache.org/jira/browse/CALCITE-434">CALCITE-435</a>]
  `FilterAggregateTransposeRule` loses conditions that cannot be pushed
* [<a href="https://issues.apache.org/jira/browse/CALCITE-435">CALCITE-435</a>]
  `LoptOptimizeJoinRule` incorrectly re-orders outer joins
* [<a href="https://issues.apache.org/jira/browse/CALCITE-439">CALCITE-439</a>]
  `SqlValidatorUtil.uniquify()` may not terminate under some conditions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-438">CALCITE-438</a>]
  Push predicates through `SemiJoinRel`
* Add test case for `LIKE ... ESCAPE`.
* HOWTO: Modify release instructions.
* Update `DiffRepository` documentation.
* Add tests for windowed aggregates without `ORDER BY`. (Works already.)

## <a href="https://github.com/apache/calcite/releases/tag/calcite-0.9.1-incubating">0.9.1-incubating</a> / 2014-10-02
{: #v0-9-1}

This is the first release as Calcite. (The project was previously called Optiq.)

New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-430">CALCITE-430</a>]
  Rename project from Optiq to Calcite
* [<a href="https://issues.apache.org/jira/browse/CALCITE-426">CALCITE-426</a>]
  Pool JDBC data sources, to make it easier to pool connections
* [<a href="https://issues.apache.org/jira/browse/CALCITE-416">CALCITE-416</a>]
  Execute logical `RelNode`s using an interpreter
* [<a href="https://issues.apache.org/jira/browse/CALCITE-376">CALCITE-376</a>]
  Move `SqlRun` into its own artifact,
  <a href="https://github.com/julianhyde/quidem">Quidem</a>.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-269">CALCITE-269</a>]
  MongoDB result sets larger than 16MB
* [<a href="https://issues.apache.org/jira/browse/CALCITE-373">CALCITE-373</a>]
  `NULL` values in `NOT IN` sub-queries
* SQL functions:
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-422">CALCITE-422</a>]
    Add `REGR_SXX` and `REGR_SYY` regression functions
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-421">CALCITE-421</a>]
    Add `COVAR_POP` and `COVAR_SAMP` aggregate functions
* Planner rules:
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-425">CALCITE-425</a>]
    Add `FilterAggregateTransposeRule`, that pushes a filter through an
    aggregate
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-399">CALCITE-399</a>]
    Factorize common `AND` factors out of `OR` predicates
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-404">CALCITE-404</a>]
    `MergeProjectRule` should not construct `RexProgram`s for simple mappings
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-394">CALCITE-394</a>]
    Add `RexUtil.toCnf()`, to convert expressions to conjunctive normal form
    (CNF)
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-389">CALCITE-389</a>]
    `MergeFilterRule` should flatten `AND` condition
* Lattices:
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-428">CALCITE-428</a>]
    Use optimization algorithm to suggest which tiles of a lattice to
    materialize
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-410">CALCITE-410</a>]
    Allow lattice tiles to satisfy a query by rolling up
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-406">CALCITE-406</a>]
    Add tile and measure elements to lattice model element
  * Now, a lattice can materialize an aggregate-join and use it in a subsequent
    query.
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-402">CALCITE-402</a>]
    Lattice should create materializations on demand
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-344">CALCITE-344</a>]
    Lattice data structure
* Field trimmer:
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-408">CALCITE-408</a>]
    Make `FieldTrimmer` work with `RelNode` base classes
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-388">CALCITE-388</a>]
    Handle semi-joins in field trimmer
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-395">CALCITE-395</a>]
    Make `FieldTrimmer.trimFields(SetOp)` generate `ProjectRel` instead of
    `CalcRel`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-393">CALCITE-393</a>]
    If no fields are projected from a table, field trimmer should project a
    dummy expression

API changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-413">CALCITE-413</a>]
  Add `RelDataTypeSystem` plugin, allowing different max precision of a
  `DECIMAL`
* In `Planner`, query de-correlation no longer requires state in a
  `SqlToRelConverter`.
* Factories:
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-392">CALCITE-392</a>]
    `RelFieldTrimmer` should use factory to create new rel nodes
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-382">CALCITE-382</a>]
    Refactoring rules to use factories
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-398">CALCITE-398</a>]
    Move `CalcRel.createProject` methods to `RelOptUtil`
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-396">CALCITE-396</a>]
    Change return type of `JoinFactory.createJoin()`; add `SemiJoinFactory`

Bug-fixes and internal changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-386">CALCITE-386</a>]
  Fix NOTICE
* Add tests inspired by Derby bugs.
* Add recent presentation to README.md.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-427">CALCITE-427</a>]
  Off-by-one issues in `RemoveDistinctAggregateRule`,
  `AggregateFilterTransposeRule`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-414">CALCITE-414</a>]
  Bad class name in `sqlline` shell script
* Bad package name in `package-info.java` was causing errors in Eclipse.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-412">CALCITE-412</a>]
  `RelFieldTrimmer`: when trimming `SortRel`, the collation and trait set don't
  match
* Add test case for
  [<a href="https://issues.apache.org/jira/browse/CALCITE-411">CALCITE-411</a>]
  Duplicate column aliases
* [<a href="https://issues.apache.org/jira/browse/CALCITE-407">CALCITE-407</a>]
  `RemoveTrivialProjectRule` drops child node's traits
* [<a href="https://issues.apache.org/jira/browse/CALCITE-409">CALCITE-409</a>]
  `PushFilterPastProjectRule` should not push filters past windowed aggregates
* Fix tests on Windows.
* Don't load `FoodMartQuerySet` unless we have to. It's big.
* Enable connection pooling in test suite.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-384">CALCITE-384</a>]
  Add `apache-` prefix to tarball and directory within tarball
* Freeze hive fmpp > freemarker plugin dependency.
* Upgrade Janino
* Removed hardcoded foodmart schema information
* [<a href="https://issues.apache.org/jira/browse/CALCITE-387">CALCITE-387</a>]
  CompileException when cast TRUE to nullable boolean
* Temporary fix for
  [<a href="https://issues.apache.org/jira/browse/CALCITE-390">CALCITE-390</a>]
  Transitive inference (`RelMdPredicates`) doesn't handle semi-join
* [<a href="https://issues.apache.org/jira/browse/CALCITE-385">CALCITE-385</a>]
  Change comment style for Java headers
* Disable test that is inconistent between JDK 1.7 and 1.8.
* Fix `git-commit-id-plugin` error when running in Travis-CI.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-381">CALCITE-381</a>]
  Remove plugin versions from the `<plugins>` tag in root pom
* [<a href="https://issues.apache.org/jira/browse/CALCITE-383">CALCITE-383</a>]
  Each jar should have a `git.properties` file describing its exact version
* Fix `mvn site` on JDK 1.8 and enable in Travis-CI.
* Status icon based on master branch, not whichever branch happened to build
  most recently.
* HOWTO:
  * Document how to build from git, and how to get jars from maven repo.
  * Optiq web site
  * Template emails for Apache votes
  * Update JIRA cases following release
  * Instructions for making and verifying a release

## <a href="https://github.com/apache/calcite/releases/tag/optiq-0.9.0-incubating">0.9.0-incubating</a> / 2014-08-19
{: #v0-9-0}

This is the first release under the Apache incubator process.

New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-371">CALCITE-371</a>]
  Implement `JOIN` whose `ON` clause contains mixed equi and theta
* [<a href="https://issues.apache.org/jira/browse/CALCITE-369">CALCITE-369</a>]
  Add `EnumerableSemiJoinRel`, implementation of semi-join in enumerable
  convention
* Add class `Strong`, for detecting null-rejecting predicates.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-368">CALCITE-368</a>]
  Add SemiJoinRule, planner rule to convert project-join-aggregate into semi-join
* [<a href="https://issues.apache.org/jira/browse/CALCITE-367">CALCITE-367</a>]
  `PushFilterPastJoinRule` should strengthen join type
* Add `EquiJoinRel`, base class for joins known to be equi-joins.
* Implement `CAST(<string> AS <datetime>)` and
  `<datetime> + <interval>`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-360">CALCITE-360</a>]
  Introduce a rule to infer predicates from equi-join conditions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-349">CALCITE-349</a>]
  Add heuristic join-optimizer that can generate bushy joins
* [<a href="https://issues.apache.org/jira/browse/CALCITE-346">CALCITE-346</a>]
  Add commutative join rule
* [<a href="https://issues.apache.org/jira/browse/CALCITE-347">CALCITE-347</a>]
  In `SqlRun`, add `!plan` command
* [<a href="https://issues.apache.org/jira/browse/CALCITE-314">CALCITE-314</a>]
  Allow simple UDFs based on methods
* [<a href="https://issues.apache.org/jira/browse/CALCITE-327">CALCITE-327</a>]
  Rules should use base class to find rule match & use factory for object
  creation
* [<a href="https://issues.apache.org/jira/browse/CALCITE-316">CALCITE-316</a>]
  In `SqlRun`, match output regardless of order if `ORDER BY` not present
* [<a href="https://issues.apache.org/jira/browse/CALCITE-300">CALCITE-300</a>]
  Support multiple parameters in `COUNT(DISTINCT x, y, ...)`

API changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-343">CALCITE-343</a>]
  RelDecorrelator should build its own mappings, not inherit from SqlToRelConverter
* Remove deprecated methods.
* Convert `Hook` to use Guava `Function` (was linq4j `Function1`).
* Add fluent method `withHook`, to more easily add hooks in tests.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-321">CALCITE-321</a>]
  Add support for overriding implementation of `CompoundIdentifier` in
  `SqlParser`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-322">CALCITE-322</a>]
  Add support for `SqlExplain`, `SqlOrderBy` and `SqlWith` to support
  `SqlShuttle` use.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-323">CALCITE-323</a>]
  Override `SqlUnresolvedFunction.inferReturnType()` to return `ANY` type
  so framework implementors can support late bound function implementations.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-324">CALCITE-324</a>]
  Add `ViewExpander` for `Planner` in `Frameworks`. Expose additional
  properties of `ViewTable` to allow subclassing.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-247">CALCITE-247</a>]
  Add `Context` and `FrameworkConfig`

Bug-fixes and internal changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-380">CALCITE-380</a>]
  Downgrade to Guava 11.0.2
* Move several .md files into new 'doc' directory, to keep the root directory simple.
* Add DISCLAIMER
* Update history and HOWTO
* [<a href="https://issues.apache.org/jira/browse/CALCITE-377">CALCITE-377</a>]
  UnregisteredDriver should catch, log and re-throw NoClassDefFoundError
* Inherit maven-release-plugin from Apache POM.
* Test case for
  [<a href="https://issues.apache.org/jira/browse/CALCITE-373">CALCITE-373</a>]
  NOT IN and NULL values
* [<a href="https://issues.apache.org/jira/browse/CALCITE-372">CALCITE-372</a>]
  Change `LoptOptimizeJoinRule` &amp; `PushFilterPast`* rules to use factory
* Upgrade `maven-checkstyle-plugin`.
* Add class `Holder`, a mutable slot that can contain one object.
* Remove the 2-minute wait at the top of the hour for tests of
  `CURRENT_TIME`, etc.
* Tune `ImmutableIntList`'s iterators.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-364">CALCITE-364</a>]
  Validator rejects valid `WITH ... ORDER BY` query
* [<a href="https://issues.apache.org/jira/browse/CALCITE-363">CALCITE-363</a>]
  Use `dependencyManagement` and `pluginManagement` in POM files
* Add `FilterFactory`.
* Add `README` file, incubation disclaimers, and how-to build and running tests.
* Add `KEYS` and start how-to for making snapshots and releases.
* Capital case component names; inherit license info from Apache parent POM.
* Only run `apache-rat` and `git-commit-id-plugin` in "release" maven profile.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-348">CALCITE-348</a>]
  Add Apache RAT as maven plugin
* Change license headers from "Julian Hyde" to "ASF"; add headers where missing.
* Fix build breakage on JDK 1.6 due to missing method `BitSet.previousClearBit`.
* Refactor test infrastructure to allow testing against heuristic bushy-join
  optimizer.
* Add methods and tests for BitSets, and re-organize tests.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-354">CALCITE-354</a>]
  Change maven groupId to "org.apache.optiq"
* Specify return type when calling `RexBuilder.makeCall`, if possible.
* Eliminate duplicate conditions in `RexProgramBuilder.addCondition`, not
  `RexBuilder.makeCall` as previously.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-345">CALCITE-345</a>]
  `AssertionError` in `RexToLixTranslator` comparing to date literal
* Restore `PushFilterPastJoinRule` to `RelDecorrelator`; interim pending
  [<a href="https://issues.apache.org/jira/browse/CALCITE-343">CALCITE-343</a>]
  fix.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-340">CALCITE-340</a>]
  Fix bug in `SqlToRelConverter` when push expressions in join conditions into
  `ProjectRel`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-313">CALCITE-313</a>]
  Query decorrelation fails
* While unifying a `RelNode` tree with a materialized view expression,
  switch representation to `MutableRel`s.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-305">CALCITE-305</a>]
  Unit test failure on release candidates
* [<a href="https://issues.apache.org/jira/browse/CALCITE-325">CALCITE-325</a>]
  Use Java list instead of Guava list to avoid null checks in case of
  `SqlTypeExplicitPrecedenceList`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-326">CALCITE-326</a>]
  Fix `RelOptUtil` `ANY` type check.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-303">CALCITE-303</a>]
  Migrate issue URLs
* [<a href="https://issues.apache.org/jira/browse/CALCITE-331">CALCITE-331</a>]
  Precision/scale compatibility checks should always succeed for `ANY` type
* In `SqlRun`, allow `!plan` after `!ok` for same SQL statement.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-318">CALCITE-318</a>]
  Add unit test for `SqlRun`
* Fix a bug where composite `SELECT DISTINCT` would return duplicate rows.

## <a href="https://github.com/apache/calcite/releases/tag/optiq-parent-0.8">0.8</a> / 2014-06-27
{: #v0-8}

New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-310">CALCITE-310</a>]
   Implement LEAD, LAG and NTILE windowed aggregates
* Reduce `COUNT(not-null-expression)` to `COUNT()`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-292">CALCITE-292</a>]
   Improve windowed aggregate return types
* [<a href="https://issues.apache.org/jira/browse/CALCITE-302">CALCITE-302</a>]
   Use heuristic rule to plan queries with large numbers of joins
* [<a href="https://issues.apache.org/jira/browse/CALCITE-283">CALCITE-283</a>]
  Add TPC-DS data generator
* [<a href="https://issues.apache.org/jira/browse/CALCITE-294">CALCITE-294</a>]
  Implement DENSE_RANK windowed aggregate function
* SqlRun utility
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-290">CALCITE-290</a>]
    Add `SqlRun`, an idempotent utility for running SQL test scripts
  * Add "!skip" command to SqlRun.
  * Add MySQL formatting mode to SqlRun.

API changes

* Re-organize planner initialization,
  to make it easier to use heuristic join order.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-301">CALCITE-301</a>]
  Add `Program` interface, a planner phase more general than current `RuleSet`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-263">CALCITE-263</a>]
  Add operand type that will cause a rule to fire when a new subset is created
* Clean up and document SqlKind.
  * Add `IS_NOT_TRUE` and `IS_NOT_FALSE` `SqlKind` enums.
  * Add `SqlKind.IS_NOT_NULL` enum value, and use where possible,
    including for `IS_NOT_UNKNOWN` operator.

Bug-fixes and internal changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-312">CALCITE-312</a>]
  Trim non-required fields before `WindowRel`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-311">CALCITE-311</a>]
  Wrong results when filtering the results of windowed aggregation
* More tests for `WITH ... ORDER BY`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-309">CALCITE-309</a>]
  `WITH ... ORDER BY` query gives `AssertionError`
* Enable `MultiJoinRel` and some other planner rule tests.
* Add `ImmutableNullableList` and `UnmodifiableArrayList`,
  and remove calls to `Arrays.asList`.
* Add method `IntPair.zip`.
* Reimplement regular and windowed aggregates
* Switch from github to Apache JIRA for issues tracking.
  * In release history, update issue URLs from github to Apache JIRA
* The Apache mailing list is now the official mailing list. Add presentations.
* Add test for overloaded UDF.
* Add tests for `NOT IN` where sub-query returns NULL values.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-288">CALCITE-288</a>]
  Add tests for windowed aggregation based on Postgres reference queries
* [<a href="https://issues.apache.org/jira/browse/CALCITE-286">CALCITE-286</a>]
  Error casting MongoDB date
* [<a href="https://issues.apache.org/jira/browse/CALCITE-284">CALCITE-284</a>]
  Window functions range defaults to `CURRENT ROW`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-285">CALCITE-285</a>]
  Window functions throw exception without `ORDER BY`
* Test case for
  [<a href=“https://issues.apache.org/jira/browse/CALCITE-285”>CALCITE-285</a>].
* [<a href="https://issues.apache.org/jira/browse/CALCITE-281">CALCITE-281</a>]
  `EXTRACT` function's SQL return type is `BIGINT` but implemented as Java `int`

## <a href="https://github.com/apache/calcite/releases/tag/optiq-parent-0.7">0.7</a> / 2014-05-13
{: #v0-7}

New features

* Implement table functions.
* Arrays and multi-sets:
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-267">CALCITE-267</a>]
    Improve support for ARRAY data type
  * Better type information for JDBC Array; nested array now possible.
  * Implement `JOIN LATERAL` and `JOIN UNNEST`.
  * Implement the `UNNEST` relational operator, and various improvements
    to `ARRAY` and `MULTISET` data types.
  * Represent `ARRAY` columns as Java lists.
  * Implement `CARDINALITY(ARRAY)` SQL operator.
* Implement scalar sub-query in `SELECT` clause.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-273">CALCITE-273</a>]
  Support column alias in WITH queries (common table expressions)
* Windowed aggregates:
  * Aggregate over constants, e.g. `SUM(1) OVER (ROWS 10 PRECEDING)`;
  * `UNBOUNDED PRECEDING` window range;
  * Windowed aggregates computed over primitive scalars.
* Fix return type inference for aggregate calls. If the `GROUP BY` clause is
  empty, `SUM` may return null.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-37">CALCITE-37</a>]
  Document JSON model file format (as <a href="{{ site.sourceRoot }}/site/_docs/model.md">model.md</a>).
* [<a href="https://issues.apache.org/jira/browse/CALCITE-238">CALCITE-238</a>]
  Add adapter that generates TPC-H data
* Improve exception message in `AvaticaConnection`; add
  `ExceptionMessageTest`.
* Implement micro-benchmarks via
  <a href="http://openjdk.java.net/projects/code-tools/jmh/">JMH</a>.

API changes

* Provide an option to create root schema without the "metadata" schema.
* Schema SPI:
  * [<a href="https://issues.apache.org/jira/browse/CALCITE-175">CALCITE-175</a>]
    Modify Schema SPI to allow caching
  * Get sub-schemas defined by a Schema SPI, and cache their `OptiqSchema`
    wrappers. (Tobi Vollebregt and Julian Hyde)
* SqlAdvisor callable from client via JDBC.

Bug-fixes and internal changes

* Add Apache incubator proposal.
* Rename RELEASE.md to HISTORY.md.
* Upgrade maven-release-plugin.
* Upgrade to linq4j-0.3.
* Code generation improvements:
 * Move code-generation optimizer to linq4j;
 * Improve translation of strict functions;
 * Mark most methods in `SqlFunctions` as `@Deterministic`;
 * Support `static final` constants generated by linq4j.
 * Avoid excessive box and unbox of primitives when using `Object[]` storage.
 * In JDBC result set, avoid row computation on each accessor call.
* Test composite join conditions in various flavors of outer join.
* Use `fromTrait` of the just previously converted `RelNode` instead
  of the original `RelNode`.
* Disable a MongoDB test, pending
  [<a href="https://issues.apache.org/jira/browse/CALCITE-270">CALCITE-270</a>].
* Hush warnings from `SplunkAdapterTest` if Splunk is not available.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-252">CALCITE-252</a>]
  Scalar sub-query that returns 0 rows should become NULL value
* `SplunkAdapterTest` now uses the same Foodmart database as `JdbcTest`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-242">CALCITE-242</a>]
  SplunkAdapterTest fails
* Remove some obsolete classes.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-205">CALCITE-205</a>]
  Suspicious map.get in VolcanoPlanner.reregister

## <a href="https://github.com/apache/calcite/releases/tag/optiq-parent-0.6">0.6</a> / 2014-04-11
{: #v0-6}

New features

* [<a href="https://issues.apache.org/jira/browse/CALCITE-214">CALCITE-214</a>]
  Modify Frameworks to allow Schema to be re-used
  Obsoletes `name` field of `ReflectiveSchema`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-237">CALCITE-237</a>]
  Allow user-defined aggregate functions (UDAs) to be defined in a model
* [<a href="https://issues.apache.org/jira/browse/CALCITE-227">CALCITE-227</a>]
  Extend `EXTRACT` function to support `DATE`, `TIME` and `TIMESTAMP` values
* [<a href="https://issues.apache.org/jira/browse/CALCITE-222">CALCITE-222</a>]
  User-defined table macros
* [<a href="https://issues.apache.org/jira/browse/CALCITE-179">CALCITE-179</a>]
  Optiq on Windows
  * Add `sqlline.bat` and fix issues running `sqlline` under Cygwin.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-195">CALCITE-195</a>]
  Push aggregation into MongoDB adapter
* [<a href="https://issues.apache.org/jira/browse/CALCITE-193">CALCITE-193</a>]
  Implement OFFSET and LIMIT in MongoDB adapter
* [<a href="https://issues.apache.org/jira/browse/CALCITE-164">CALCITE-164</a>]
  Improve query performance of optiq over MongoDB
* Add Phoenix (HBase) SQL dialect (Bruno Dumon)

API changes

* Obsolete `RexImpTable.AggregateImplementor` and rename `AggImplementor2`.
  (**This is a breaking change**.)
* Convert `CombinedParser.jj` into freemarker template to allow
  custom parser implementations. (Venki Korukanti)
* Extend `Planner` to pass a custom `ConvertletTable` and custom SQL parser.
* In `Frameworks`, add a way to specify list of `TraitDef`s that will be used
  by planner. (Jinfeng Ni)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-198">CALCITE-198</a>]
  Use `RexExecutor` to evaluate projections and filters
* [<a href="https://issues.apache.org/jira/browse/CALCITE-219">CALCITE-219</a>]
  Parse `ALTER scope SET option = value` statement
* [<a href="https://issues.apache.org/jira/browse/CALCITE-215">CALCITE-215</a>]
  A Schema should not have to remember its name and parent
  (**This is a breaking change**.)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-180">CALCITE-180</a>]
  Common base class for TableFunction, ScalarFunction
  (**This is a breaking change**.)
* Add methods for dealing with symbols; deprecate
  `SqlLiteral.booleanValue(SqlNode)`, `SqlLiteral.symbolValue(SqlNode)`.
* Add `RelOptPlanner.clear()`; now it is safe to call `transform` twice.
  (Jinfeng Ni)
* Remove APIs deprecated for 0.5.
* Move around some operator classes and singletons.

Bug-fixes and internal changes

* Upgrade to linq4j-0.2.
* `FETCH` and `LIMIT` are ignored during SQL-to-RelNode translation.
  (Venki Korukanti)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-245">CALCITE-245</a>]
  Off-by-one translation of ON clause of JOIN
* [<a href="https://issues.apache.org/jira/browse/CALCITE-191">CALCITE-191</a>]
  Rotate time/date/timestamp vals to local timezone
* [<a href="https://issues.apache.org/jira/browse/CALCITE-244">CALCITE-244</a>]
  `RelOptTableImpl.create` always expects `QueryableTable` type in
  `OptiqSchema.TableEntry`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-225">CALCITE-225</a>]
  Optiq doesn't correctly decorrelate queries
* Clean up package-info.  Remove duplicates in test packages so they
  don't conflict with those in non-test packages.
* Add `Pair.adjacents(Iterable)`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-199">CALCITE-199</a>]
  Various `ANY` type conditions aren't correctly being considered
  (Jacques Nadeau)
* Add files to `.gitignore` that shouldn't be checked in when using
  Eclipse. (Jacques Nadeau)
* Add class `ControlFlowException`, and make it base class of
  existing control-flow exception classes.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-232">CALCITE-232</a>]
  Sum and avg of empty set should be null as per SQL specification
* Add `SqlUnresolvedFunction`, to improve how return type of
  user-defined functions is resolved. (Vladimir Sitnikov)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-228">CALCITE-228</a>]
  Error while compiling generated Java code when using UDF in expression
* [<a href="https://issues.apache.org/jira/browse/CALCITE-226">CALCITE-226</a>]
  User-defined functions should work without explicit schema prefix
* [<a href="https://issues.apache.org/jira/browse/CALCITE-229">CALCITE-229</a>]
  Join between different JDBC schemas not implementable
* [<a href="https://issues.apache.org/jira/browse/CALCITE-230">CALCITE-230</a>]
  RemoveSortRule derives trait set from sort, should derive it from sort's child
* Test view and sub-query with `ORDER BY` and `LIMIT`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-223">CALCITE-223</a>]
  Add `NOTICE` and `LICENSE` files in all generated JAR files
* [<a href="https://issues.apache.org/jira/browse/CALCITE-209">CALCITE-209</a>]
  Consistent strategy for line-endings in tests
  Convert uses of `NL` in tests to Linux newline "\n".
  This makes string constants simpler.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-218">CALCITE-218</a>]
  Functions case sensitive when using `Lex.MYSQL`
* Add tests that a query with aggregate expressions in the `SELECT`
  clause is considered an aggregate query, even if there is no `GROUP BY`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-216">CALCITE-216</a>]
  Inconsistent use of provided operator table causes inability to
  add aggregate functions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-200">CALCITE-200</a>]
  Javadoc generation fails under JDK 1.8
* Add class `XmlOutput` (based on `org.eigenbase.xom.XMLOutput`) and remove
  dependency on eigenbase-xom.
* Performance: Don't create stack-trace for exceptions used for control-flow.
  (Vladimir Sitnikov)
* Performance: Tune `RexProgramBuilder` by using `Pair` rather than `String` as
  expression key. (Vladimir Sitnikov)
* Fix NPE using TRIM function with JDBC. (Bruno Dumon)
* Add dependency on
  <a href="https://github.com/julianhyde/hydromatic-resource">hydromatic-resource-maven-plugin</a>
  and obsolete our copy of the resource framework.
* Fix race condition in `SpaceList`.
* In planner, use `RelTrait.subsumes` rather than `equals` in an assert.
  (Jinfeng Ni)

## <a href="https://github.com/apache/calcite/releases/tag/optiq-parent-0.5">0.5</a> / 2014-03-14
{: #v0-5}

New features

* Allow `quoting`, `quotedCasing`, `unquotedCasing`, and `caseSensitive`
  properties to be specified explicitly (Vladimir Sitnikov)
* Recognize more kinds of materializations, including filter-on-project (where
  project contains expressions) and some kinds of aggregation.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-128">CALCITE-128</a>]
  Support `WITH` queries (common table expressions)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-53">CALCITE-53</a>]
  Allow `WHEN` clause in simple `CASE` expression to have multiple values
* [<a href="https://issues.apache.org/jira/browse/CALCITE-156">CALCITE-156</a>]
  Optiq should recognize 'SYSTEM TABLE', 'JOIN', 'INDEX' as table types
* Support querying ARRAY columns from JDBC source. (Gabriel Reid)

API changes

* Add
  `ProjectRelBase.copy(RelTraitSet, RelNode, List<RexNode>, RelDataType)`
  and make `ProjectRelBase.copy(RelTraitSet, RelNode)` final.
  (**This is a breaking change** for sub-classes of `ProjectRelBase`.)
* Change `RexBuilder.makeRangeReference` parameter type.
* `RexBuilder.makeInputRef` replaces `RelOptUtil.createInputRef`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-160">CALCITE-160</a>]
  Allow comments in schema definitions
* [<a href="https://issues.apache.org/jira/browse/CALCITE-147">CALCITE-147</a>]
  Create a new kind of `SqlCall` that keeps operands in fields, not an operands
  array
  * Very widely used parse tree nodes with complex operands, including
    `SqlSelect`, `SqlJoin`, `SqlInsert`, and a new node type `SqlOrderBy`, are
    now sub-classes of `SqlCall` but not `SqlBasicCall`.
  * (**This is a breaking change** to code that assumes that, say,
    `SqlSelect` has an `operands` field.)
* Convert all enum constants to upper-case.
  (**This is a breaking change**.)

Bug-fixes and internal changes

* Generate optiq-core-VERSION-tests.jar not parent-VERSION-tests.jar.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-176">CALCITE-176</a>]
  ORDER BY expression doesn't work with SELECT \*
* Fix VARCHAR casts sent to hsqldb source (Bruno Dumon)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-143">CALCITE-143</a>]
  Remove dependency on eigenbase-resgen
* [<a href="https://issues.apache.org/jira/browse/CALCITE-173">CALCITE-173</a>]
  Case-insensitive table names are not supported for `Casing.UNCHANGED`
* `DATE.getLimit` now returns `Calendar` in GMT time zone (Vladimir Sitnikov)
* Set `en_US` locale in tests that match against error numbers, dates
  (Vladimir Sitnikov)
* Use 1 test thread per CPU to avoid thread starvation on dual core CPUs
  (Vladimir Sitnikov)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-174">CALCITE-174</a>]
  Move hsqldb to test scope
* Add unit tests for `RexExecutorImpl`.
* Correct JSON model examples in Javadoc comments. (Karel Vervaeke)
* Move test reference logs from `src/test/java` to `src/test/resources`
  (reduces the number of 'untracked files' reported by git)
* Tune `Util.SpaceList`, fix race condition, and move into new utility class
  `Spaces`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-163">CALCITE-163</a>]
  Equi-join warning
* [<a href="https://issues.apache.org/jira/browse/CALCITE-157">CALCITE-157</a>]
  Handle `SQLFeatureNotSupported` when calling `setQueryTimeout`
  (Karel Vervaeke)
* Fix Optiq on Windows. (All tests and checkstyle checks pass.)
* In checkstyle, support Windows-style file separator, otherwise build fails in
  Windows due to suppressions not used. (Vladimir Sitnikov)
* Enable MongoDB tests when `-Dcalcite.test.mongodb=true`.
* Cleanup cache exception-handling and an assert.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-153">CALCITE-153</a>]
  Error using MongoDB adapter: Failed to set setXIncludeAware(true)
* Disable spark engine unless Spark libraries are on the class path and
  `spark=true` is specified in the connect string.
* Fix path to `mongo-zips-model.json` in HOWTO. (Mariano Luna)
* Fix bug deriving the type of a join-key.
* Fix the value of `ONE_MINUS_EPSILON`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-158">CALCITE-158</a>]
  Optiq fails when call `Planner.transform()` multiple times, each with
  different ruleset
* [<a href="https://issues.apache.org/jira/browse/CALCITE-148">CALCITE-148</a>]
 Less verbose description of collation. Also, optimize `RelTraitSet` creation
 and amortize `RelTraitSet.toString()`.
* Add generics to SQL parser.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-145">CALCITE-145</a>]
  Unexpected upper-casing of keywords when using java lexer
* Remove duplicate `maven-source-plugin`.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-141">CALCITE-141</a>]
  Downgrade to guava-11.0.2. (This is necessary for Hadoop compatibility.
  Later versions of Guava can also be used.)
* Upgrade to spark-0.9.0. (Because this version of spark is available from
  maven-central, we can make optiq-spark part of the regular build, and remove
  the spark profile.)

## <a href="https://github.com/apache/calcite/releases/tag/optiq-parent-0.4.18">0.4.18</a> / 2014-02-14
{: #v0-4-18}

API and functionality changes

* Configurable lexical policy
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-33">CALCITE-33</a>]
      SQL parser should allow different identifier quoting
    * [<a href="https://issues.apache.org/jira/browse/CALCITE-34">CALCITE-34</a>]
      Policy for case-sensitivity of identifiers should be configurable
    * New connect-string parameter "lex", with allowable values
      "ORACLE", "MYSQL", "SQL_SERVER", "JAVA" sets policy to be like those
      databases, in terms of quote string, whether quoted and unquoted
      identifiers are converted to upper/lower case, and whether
      identifiers are matched case-sensitively. "JAVA" is case-sensitive,
      even for unquoted identifiers. It should be possible
      for each connection to have its own settings for these. Objects
      shared between sessions (views, materialized views) might
      require more work.
    * Added various internals to make it easy for developers to do the
      right thing. When you need to look up a schema, table or
      column/field name, you should use a catalog reader, and it will
      apply the right case-sensitivity policy.
    * Enable optiq consumer to utilize different lexical settings in
      Frameworks/Planner. (Jacques Nadeau)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-135">CALCITE-115</a>]
  Add a PARSE_TREE hook point with SqlNode parameter
* Change planner rules to use `ProjectFactory` for creating
  projects. (John Pullokkaran)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-131">CALCITE-131</a>]
  Add interfaces for metadata (statistics)
  (**This is a breaking change**.)
* Update Avatica to allow `Cursor` & `Accessor` implementations to throw
  `SQLException`. (Jacques Nadeau)
* Separate cost model (`RelOptCostFactory`) from planner. Allow
  `VolcanoPlanner` to be sub-classed with different cost factory.
    * Remove references to VolcanoCost from RelSubset, so clients can
      use a different `RelOptCost`. (Harish Butani)
    * Make `VolcanoCost` immutable.
* Break `SqlTypeStrategies` into `OperandTypes`, `ReturnTypes` and
  `InferTypes`, and rename its static members to upper-case, per
  checkstyle. (**This is a breaking change**.)
* Add a mechanism for defining configuration parameters and have them
  appear in the responses to `AvaticaDatabaseMetaData` methods.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-113">CALCITE-113</a>]
  User-defined scalar functions
* Add rules to short-cut a query if `LIMIT 0` is present. Also remove
  sort, aggregation, join if their inputs are known to be empty, and
  propagate the fact that the relational expressions are known to be
  empty up the tree. (We already do this for union, filter, project.)
* `RexNode` and its sub-classes are now immutable.

Bug-fixes and internal changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-61">CALCITE-16</a>]
  Upgrade to janino-2.7
* Upgrade to guava-15.0 (guava-14.0.1 still allowed), sqlline-1.1.7,
  maven-surefire-plugin-2.16, linq4j-0.1.13.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-136">CALCITE-136</a>]
  Support Hive dialect
* [<a href="https://issues.apache.org/jira/browse/CALCITE-138">CALCITE-138</a>]
  SqlDataTypeSpec.clone handles collection types wrong
* [<a href="https://issues.apache.org/jira/browse/CALCITE-137">CALCITE-137</a>]
  If a subset is created that is subsumed by an existing subset, its
  'best' is not assigned
    * If best rel in a Volcano subset doesn't have metadata, see if
      other rels have metadata.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-127">CALCITE-127</a>]
  EnumerableCalcRel can't support 3+ AND conditions (Harish Butani)
* Fix push-down of datetime literals to JDBC data sources.
* Add `Util.startsWith(List, List)` and `Util.hashCode(double)`.
* Add maven-checkstyle-plugin, enable in "verify" phase, and fix exceptions.
* Fix `SqlValidator` to rely on `RelDataType` to do field name matching.  Fix
  `RelDataTypeImpl` to correctly use the case sensitive flag rather than
  ignoring it.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-119">CALCITE-119</a>]
  Comparing Java type long with SQL type INTEGER gives wrong answer
* Enable multi-threaded testing, and fix race conditions.
    * Two of the race conditions involved involving trait caches. The
      other was indeterminacy in type system when precision was not
      specified but had a default; now we canonize TIME to TIME(0), for
      instance.
* Convert files to `us-ascii`.
* Work around
  [<a href="http://jira.codehaus.org/browse/JANINO-169">JANINO-169</a>].
* Refactor SQL validator testing infrastructure so SQL parser is
  configurable.
* Add `optiq-mat-plugin` to README.
* Fix the check for duplicate subsets in a rule match.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-112">CALCITE-112</a>]
  Java boolean column should be treated as SQL boolean
* Fix escaped unicode characters above 0x8000. Add tests for unicode
  strings.

## <a href="https://github.com/apache/calcite/releases/tag/optiq-parent-0.4.17">0.4.17</a> / 2014-01-13
{: #v0-4-17}

API changes

* [<a href="https://issues.apache.org/jira/browse/CALCITE-106">CALCITE-106</a>]
  Make `Schema` and `Table` SPIs simpler to implement, and make them
  re-usable across connections
  (**This is a breaking change**.)
* Make it easier to define sub-classes of rule operands. The new class
  `RelOptRuleOperandChildren` contains the children of an operand and
  the policy for dealing with them. Existing rules now use the new
  methods to construct operands: `operand()`, `leaf()`, `any()`, `none()`,
  `unordered()`. The previous methods are now deprecated and will be
  removed before 0.4.18. (**This is a breaking change**.)
* [<a href="https://issues.apache.org/jira/browse/CALCITE-101">CALCITE-101</a>]
  Enable phased access to the Optiq engine
* List-handling methods in `Util`: add methods `skipLast`, `last`, `skip`;
  remove `subList`, `butLast`.
* Convert `SqlIdentifier.names` from `String[]` to `ImmutableList<String>`.
* Rename `OptiqAssert.assertThat()` to `that()`, to avoid clash with junit's
  `Assert.assertThat()`.
* Usability improvements for `RelDataTypeFactory.FieldInfoBuilder`. It
  now has a type-factory, so you can just call `build()`.
* Rework `HepProgramBuilder` into a fluent API.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-105">CALCITE-105</a>]
  Externalize RelNode to and from JSON

Tuning

* If `EnumerableAggregateRel` has no aggregate functions, generate a
   call to `Enumerable.distinct()`, thereby saving the effort of
   building trivial accumulators.
* Default rule set now does not introduce `CalcRel` until a later phase
  of planning. This reduces the number of trivial projects and calcs
  created, merged, and elimated.
* Reduce the amount of time spent creating record types that
  already exist.
* More efficient implementation of `Util.isDistinct` for small lists.
* When an internal record has 0 fields, rather than generating a
  synthetic class and lots of instances that are all the same, use the
  new `Unit` class, which is a singleton.
* To take advantage of asymmetric hash join added recently in linq4j,
  tweak cost of `EnumerableJoinRel` so that join is cheaper if the
  larger input is on the left, and more expensive if it is a cartesian
  product.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-70">CALCITE-70</a>]
  Joins seem to be very expensive in memory
* Make planning process more efficient by not sorting the list of
  matched rules each cycle. It is sorted if tracing is enabled;
  otherwise we scan to find the most important element. For this list,
  replace `LinkedList` with `ChunkList`, which has an O(1) remove and add,
  a fast O(n) get, and fast scan.

Other

* [<a href="https://issues.apache.org/jira/browse/CALCITE-87">CALCITE-87</a>]
  Constant folding
  * Rules for constant-expression reduction, and to simplify/eliminate
    `VALUES` operator.
* Graph algorithms: Implement breadth-first iterator and cycle-detector.
* Fix bug in planner which occurred when two `RelNode`s have identical
  digest but different row-type.
* Fix link to optiq-csv tutorial.
* Fix bugs in `RemoveTrivialProjectRule.strip`, `JdbcProjectRel.implement`
  and `SortRel.computeSelfCost`.
* Reformat code, and remove `@author` tags.
* Upgrade to eigenbase-xom-1.3.4, eigenbase-properties-1.1.4,
  eigenbase-resgen-1.3.6.
* Upgrade to linq4j-0.1.12.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-97">CALCITE-97</a>]
  Correlated EXISTS
* Fix a bug in `VolcanoCost`.
* Add class `FoodMartQuerySet`, that contains the 6,700 foodmart queries.
* Fix factory class names in `UnregisteredDriver`
* [<a href="https://issues.apache.org/jira/browse/CALCITE-96">CALCITE-96</a>]
  LIMIT against a table in a clone schema causes UnsupportedOperationException
* Disable spark module by default.
* Allow `CloneSchema` to be specified in terms of url, driver, user,
  password; not just dataSource.
* Wrap internal error in `SQLException`.

## <a href="https://github.com/apache/calcite/releases/tag/optiq-parent-0.4.16">0.4.16</a> / 2013-11-24
{: #v0-4-16}

* [<a href="https://issues.apache.org/jira/browse/CALCITE-69">CALCITE-69</a>]
  Can't join on string columns and other problems with expressions in the join
  condition
* [<a href="https://issues.apache.org/jira/browse/CALCITE-74">CALCITE-74</a>]
  JOIN ... USING fails in 3-way join with UnsupportedOperationException.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-65">CALCITE-65</a>]
  Fix issues in the JDBC driver, and in particular to DatabaseMetaData methods,
  to make Squirrel-SQL run better.
* Fix JDBC column, table, schema names for when the table is not in a schema of
  depth 1.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-85">CALCITE-85</a>]
  Adding a table to the root schema causes breakage in OptiqPrepareImpl
* [<a href="https://issues.apache.org/jira/browse/CALCITE-84">CALCITE-84</a>]
  Extract Optiq's JDBC driver as a new JDBC driver framework, Avatica.
  Other projects can use this to implement a JDBC driver by implementing
  just a few methods. If you wish to use Optiq's JDBC driver, you will
  now need to include optiq-avatica.jar in addition to optiq-core.jar.
  Avatica does not depend on anything besides the standard Java library.
* Support for parameters in PreparedStatement.
* First steps in recognizing complex materializations. Internally we introduce a
  concept called a "star table", virtual table composed of real tables joined
  together via many-to-one relationships. The queries that define
  materializations and end-user queries are canonized in terms of star tables.
  Matching (not done yet) will then be a matter of looking for sort, groupBy,
  project. It is not yet possible to define a star in an Optiq model file.
* Add section to <a href="{{ site.sourceRoot }}/site/_docs/howto.md">HOWTO</a>
  on implementing adapters.
* Fix data type conversions when creating a clone table in memory.
* Fix how strings are escaped in JsonBuilder.
* Test suite now depends on an embedded hsqldb database, so you can run
  <code>mvn test</code> right after pulling from git. You can instead use a
  MySQL database if you specify '-Dcalcite.test.db=mysql', but you need to
  manually populate it.
* Fix a planner issue which occurs when the left and right children of join are
  the same relational expression, caused by a self-join query.
* [<a href="https://issues.apache.org/jira/browse/CALCITE-76">CALCITE-76</a>]
  Precedence of the item operator, <code>map[index]</code>; remove the space
  before '[' when converting parse tree to string.
* Allow <code>CAST(expression AS ANY)</code>, and fix an issue with the ANY type
  and NULL values.
* Handle null timestamps and dates coming out of JDBC adapter.
* Add <code>jdbcDriver</code> attribute to JDBC schema in model, for drivers
  that do not auto-register.
* Allow join rules to match any subclass of JoinRelBase.
* Push projects, filters and sorts down to MongoDB. (Fixes
  [<a href="https://issues.apache.org/jira/browse/CALCITE-57">CALCITE-57</a>],
  [<a href="https://issues.apache.org/jira/browse/CALCITE-60">CALCITE-60</a>] and
  [<a href="https://issues.apache.org/jira/browse/CALCITE-72">CALCITE-72</a>].)
* Add instructions for loading FoodMart data set into MongoDB, and how to enable
  tracing.
* Now runs on JDK 1.8 (still runs on JDK 1.6 and JDK 1.7).
* Upgrade to junit-4.11 (avoiding the dodgy junit-4.1.12).
* Upgrade to linq4j-0.1.11.

## <a href="https://github.com/apache/calcite/releases/tag/optiq-parent-0.4.15">0.4.15</a> / 2013-10-14
{: #v0-4-15}

* Lots of good stuff that this margin is too small to contain. See
  <a href="{{ site.sourceRoot }}/site/_docs/reference.md">SQL language reference</a> and
  <a href="{{ site.sourceRoot }}/site/_docs/model.md">JSON model reference</a>.

# Optiq-csv release history

Optiq-csv-0.3 was the last independent release of optiq-csv. From
calcite-0.9.2 onwards, the code was included as the
calcite-example-csv module.

* Upgrade to calcite-0.9.1
* Support gzip-compressed CSV and JSON files (recognized by '.gz' suffix)
* Cleanup, and fix minor timezone issue in a test
* Support for date types (date, time, timestamp) (Martijn van den Broek)
* Upgrade to optiq-0.8, optiq-avatica-0.8, linq4j-0.4
* Add support for JSON files (recognized by '.json' suffix)
* Upgrade maven-release-plugin to version 2.4.2
* Upgrade to optiq-0.6, linq4j-0.2
* Add NOTICE and LICENSE files in generated JAR file

## <a href="https://github.com/julianhyde/optiq-csv/releases/tag/optiq-csv-0.3">0.3</a> / 2014-03-21
{: #csv-v0-3}

* Upgrade to optiq-0.5
* Add workaround to
  [<a href="https://github.com/jline/jline2/issues/62">JLINE2-62</a>]
  to `sqlline.bat` (windows) and `sqlline` (windows using cygwin)
* Fix classpath construction: `sqlline.bat` copies dependencies to
  `target/dependencies`; `sqlline` constructs `target/classpath.txt`
* Build, checkstyle and tests now succeed on windows (both native and cygwin)
* Models can now contain comments
* [<a href="https://github.com/julianhyde/optiq-csv/issues/2">OPTIQ-CSV-2</a>]
  Update tutorial to reflect changes to Optiq's JDBC adapter

## <a href="https://github.com/julianhyde/optiq-csv/releases/tag/optiq-csv-0.2">0.2</a> / 2014-02-18
{: #csv-v0-2}

* Add test case for
  [<a href="https://issues.apache.org/jira/browse/CALCITE-112">CALCITE-112</a>]
* Add `sqlline.bat`, Windows SQL shell (based on fix for
  [<a href="https://issues.apache.org/jira/browse/DRILL-338">DRILL-338</a>])
* Upgrade to optiq-0.4.18, sqlline-1.1.7
* Return a single object for single-col enumerator (Gabriel Reid)
* Enable maven-checkstyle-plugin; fix checkstyle exceptions

## <a href="https://github.com/julianhyde/optiq-csv/releases/tag/optiq-csv-0.1">0.1</a> / 2014-01-13
{: #csv-v0-1}

* Add release notes and history
* Enable maven-release-plugin
* Upgrade to optiq-0.4.17, linq4j-0.1.12, sqlline-1.1.6
* Upgrade tutorial for new Schema and Table SPIs
* Fixes for optiq SPI changes in
  [<a href="https://issues.apache.org/jira/browse/CALCITE-106">CALCITE-106</a>]
* Enable oraclejdk8 in Travis CI
* Fix bug where non-existent directory would give NPE; instead print warning
* Add an example of a planner rule
* Add `CsvTableFactory`, an example of a custom table
* Add a view to tutorial
* Split into scenario with a "simple" schema that generates tables
  (`CsvTable`) that just execute and a "smart" schema that generates
  tables (`CsvSmartTable`) that undergo optimization
* Make `CsvEnumerator` a top-level class
* Implement the algorithms to sniff names and types from the first
  row, and to return an enumerator of all rows
* Read column types from header of CSV file

# Linq4j release history

Linq4j-0.4 was the last independent release of linq4j. From
calcite-0.9.2 onwards, the code was included as calcite-linq4j, and
features added to linq4j in a particular calcite release are described
with the other changes in that release.

## <a href="https://github.com/julianhyde/linq4j/releases/tag/linq4j-0.4">0.4</a> / 2014-05-28
{: #linq4j-v0-4}

* Fix <a href="https://github.com/julianhyde/linq4j/issues/27">#27</a>,
  "Incorrectly inlines non-final variable".
* Maven build process now deploys web site.
* Implement `Enumerable` methods: `any`, `all`,
  `contains` with `EqualityComparer`, `first`, `first` with predicate.

## <a href="https://github.com/julianhyde/linq4j/releases/tag/linq4j-0.3">0.3</a> / 2014-04-21
{: #linq4j-v0-3}

* Move optimizer visitor from optiq to linq4j; add
  `ExpressionType.modifiesLvalue` to avoid invalid inlining.
* Fix <a href="https://github.com/julianhyde/linq4j/issues/17">#17</a>,
  "Assign constant expressions to 'static final' members";
  add `@Deterministic` annotation to help deduce which expressions are
  constant.
* Multi-pass optimization: some of the variables might be avoided and
  inlined after the first pass.
* Various other peephole optimizations: `Boolean.valueOf(const)`,
  'not' expressions (`!const`, `!!a`, `!(a==b)`, `!(a!=b)`, `!(a>b)`,
  etc.),
  '?' expressions coming from `CASE` (`a ? booleanConstant : b` and `a
  ? b : booleanConstant`).
* Implement left, right and full outer join.
* Clean build on cygwin/Windows.

## <a href="https://github.com/julianhyde/linq4j/releases/tag/linq4j-0.2">0.2</a> / 2014-04-11
{: #linq4j-v0-2}

* Fix <a href="https://github.com/julianhyde/linq4j/issues/8">#8</a>,
  "Javadoc generation fails under JDK 1.8".
* Fix <a href="https://github.com/julianhyde/linq4j/issues/15">#15</a>,
  "`Expressions.ifThenElse` does not work".
* Use `HashMap` for searching of declarations to reuse; consider both
  `optimizing` and `optimize` flags when reusing.
* Implement `equals` and `hashCode` for expressions. Hash codes for
  complex expressions are cached into a field of the expression.
* Add example, `com.example.Linq4jExample`.
* Fix optimizing away parameter declarations in assignment target.
* Support Windows path names in checkstyle-suppresions.
* Support `Statement.toString` via `ExpressionWriter`.
* Use `AtomicInteger` for naming of `ParameterExpression`s to avoid
  conflicts in multithreaded usage
* Cleanup: use `Functions.adapt` rather than `new AbstractList`
* Add `NOTICE` and `LICENSE` files in generated JAR file.
* Optimize `select()` if selector is identity.
* Enable checkstyle.

## <a href="https://github.com/julianhyde/linq4j/releases/tag/linq4j-0.1.13">0.1.13</a> / 2014-01-20
{: #linq4j-v0-1-13}

* Remove spurious "null" generated when converting expression to string.
* Allow a field declaration to not have an initializer.
* Add `Primitive.defaultValue`.
* Enable `oraclejdk8` in <a href="https://travis-ci.org/julianhyde/linq4j">Travis CI</a>.

## <a href="https://github.com/julianhyde/linq4j/releases/tag/linq4j-0.1.12">0.1.12</a> / 2013-12-07
{: #linq4j-v0-1-12}

* Add release notes.
* Fix implementation of `Enumerable.asEnumerable` in
  `DefaultQueryable` (inherited by most classes that implement
  `Queryable`).

## <a href="https://github.com/julianhyde/linq4j/releases/tag/linq4j-0.1.11">0.1.11</a> / 2013-11-06
{: #linq4j-v0-1-11}

* Initial commit
