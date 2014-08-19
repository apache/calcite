# Apache Optiq release history

For a full list of releases, see
<a href="https://github.com/apache/incubator-optiq/releases">github</a>.

## <a href="https://github.com/apache/incubator-optiq/releases/tag/optiq-0.9.0-incubating">0.9.0-incubating</a> / 2014-08-19

This is the first release under the Apache incubator process.

New features
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-371">OPTIQ-371</a>]
  Implement `JOIN` whose `ON` clause contains mixed equi and theta
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-369">OPTIQ-369</a>]
  Add `EnumerableSemiJoinRel`, implementation of semi-join in enumerable
  convention
* Add class `Strong`, for detecting null-rejecting predicates.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-368">OPTIQ-368</a>]
  Add SemiJoinRule, planner rule to convert project-join-aggregate into semi-join
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-367">OPTIQ-367</a>]
  `PushFilterPastJoinRule` should strengthen join type
* Add `EquiJoinRel`, base class for joins known to be equi-joins.
* Implement `CAST(&lt;string&gt; AS &lt;datetime&gt;)` and
  `&lt;datetime&gt; + &lt;interval&gt;`.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-360">OPTIQ-360</a>]
  Introduce a rule to infer predicates from equi-join conditions
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-349">OPTIQ-349</a>]
  Add heuristic join-optimizer that can generate bushy joins
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-346">OPTIQ-346</a>]
  Add commutative join rule
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-347">OPTIQ-347</a>]
  In `SqlRun`, add `!plan` command
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-314">OPTIQ-314</a>]
  Allow simple UDFs based on methods
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-327">OPTIQ-327</a>]
  Rules should use base class to find rule match & use factory for object
  creation
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-316">OPTIQ-316</a>]
  In `SqlRun`, match output regardless of order if `ORDER BY` not present
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-300">OPTIQ-300</a>]
  Support multiple parameters in `COUNT(DISTINCT x, y, ...)`

API changes
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-343">OPTIQ-343</a>]
  RelDecorrelator should build its own mappings, not inherit from SqlToRelConverter
* Remove deprecated methods.
* Convert `Hook` to use Guava `Function` (was linq4j `Function1`).
* Add fluent method `withHook`, to more easily add hooks in tests.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-321">OPTIQ-321</a>]
  Add support for overriding implementation of `CompoundIdentifier` in
  `SqlParser`.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-322">OPTIQ-322</a>]
  Add support for `SqlExplain`, `SqlOrderBy` and `SqlWith` to support
  `SqlShuttle` use.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-323">OPTIQ-323</a>]
  Override `SqlUnresolvedFunction.inferReturnType()` to return `ANY` type
  so framework implementors can support late bound function implementations.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-324">OPTIQ-324</a>]
  Add `ViewExpander` for `Planner` in `Frameworks`. Expose additional
  properties of `ViewTable` to allow subclassing.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-247">OPTIQ-247</a>]
  Add `Context` and `FrameworkConfig`

Bug-fixes and internal changes
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-380">OPTIQ-380</a>]
  Downgrade to Guava 11.0.2
* Move several .md files into new 'doc' directory, to keep the root directory simple.
* Add DISCLAIMER
* Update history and HOWTO
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-377">OPTIQ-377</a>]
  UnregisteredDriver should catch, log and re-throw NoClassDefFoundError
* Inherit maven-release-plugin from Apache POM.
* Test case for
  [<a href="https://issues.apache.org/jira/browse/OPTIQ-373">OPTIQ-373</a>]
  NOT IN and NULL values
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-372">OPTIQ-372</a>]
  Change `LoptOptimizeJoinRule` &amp; `PushFilterPast`* rules to use factory
* Upgrade `maven-checkstyle-plugin`.
* Add class `Holder`, a mutable slot that can contain one object.
* Remove the 2-minute wait at the top of the hour for tests of
  `CURRENT_TIME`, etc.
* Tune `ImmutableIntList`'s iterators.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-364">OPTIQ-364</a>]
  Validator rejects valid `WITH ... ORDER BY` query
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-363">OPTIQ-363</a>]
  Use `dependencyManagement` and `pluginManagement` in POM files
* Add `FilterFactory`.
* Add `README` file, incubation disclaimers, and how-to build and running tests.
* Add `KEYS` and start how-to for making snapshots and releases.
* Capital case component names; inherit license info from Apache parent POM.
* Only run `apache-rat` and `git-commit-id-plugin` in "release" maven profile.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-348">OPTIQ-348</a>]
  Add Apache RAT as maven plugin
* Change license headers from "Julian Hyde" to "ASF"; add headers where missing.
* Fix build breakage on JDK 1.6 due to missing method `BitSet.previousClearBit`.
* Refactor test infrastructure to allow testing against heuristic bushy-join
  optimizer.
* Add methods and tests for BitSets, and re-organize tests.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-354">OPTIQ-354</a>]
  Change maven groupId to "org.apache.optiq"
* Specify return type when calling `RexBuilder.makeCall`, if possible.
* Eliminate duplicate conditions in `RexProgramBuilder.addCondition`, not
  `RexBuilder.makeCall` as previously.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-345">OPTIQ-345</a>]
  `AssertionError` in `RexToLixTranslator` comparing to date literal
* Restore `PushFilterPastJoinRule` to `RelDecorrelator`; interim pending
  [<a href="https://issues.apache.org/jira/browse/OPTIQ-343">OPTIQ-343</a>]
  fix.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-340">OPTIQ-340</a>]
  Fix bug in `SqlToRelConverter` when push expressions in join condtions into
  `ProjectRel`.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-313">OPTIQ-313</a>]
  Query decorrelation fails
* While unifying a `RelNode` tree with a materialized view expression,
  switch representation to `MutableRel`s.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-305">OPTIQ-305</a>]
  Unit test failure on release candidates
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-325">OPTIQ-325</a>]
  Use Java list instead of Guava list to avoid null checks in case of
  `SqlTypeExplicitPrecedenceList`.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-326">OPTIQ-326</a>]
  Fix `RelOptUtil` `ANY` type check.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-303">OPTIQ-303</a>]
  Migrate issue URLs
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-331">OPTIQ-331</a>]
  Precision/scale compatibility checks should always succeed for `ANY` type
* In `SqlRun`, allow `!plan` after `!ok` for same SQL statement.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-318">OPTIQ-318</a>]
  Add unit test for `SqlRun`
* Fix a bug where composite `SELECT DISTINCT` would return duplicate rows.

## <a href="https://github.com/julianhyde/optiq/releases/tag/optiq-parent-0.8">0.8</a> / 2014-06-27

New features
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-310">OPTIQ-310</a>]
   Implement LEAD, LAG and NTILE windowed aggregates
* Reduce `COUNT(not-null-expression)` to `COUNT()`
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-292">OPTIQ-292</a>]
   Improve windowed aggregate return types
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-302">OPTIQ-302</a>]
   Use heuristic rule to plan queries with large numbers of joins
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-283">OPTIQ-283</a>]
  Add TPC-DS data generator
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-294">OPTIQ-294</a>]
  Implement DENSE_RANK windowed aggregate function
* SqlRun utility
  * [<a href="https://issues.apache.org/jira/browse/OPTIQ-290">OPTIQ-290</a>]
    Add `SqlRun`, an idempotent utility for running SQL test scripts
  * Add "!skip" command to SqlRun.
  * Add MySQL formatting mode to SqlRun.

API changes
* Re-organize planner initialization,
  to make it easier to use heuristic join order.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-301">OPTIQ-301</a>]
  Add `Program` interface, a planner phase more general than current `RuleSet`
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-263">OPTIQ-263</a>]
  Add operand type that will cause a rule to fire when a new subset is created
* Clean up and document SqlKind.
  * Add `IS_NOT_TRUE` and `IS_NOT_FALSE` `SqlKind` enums.
  * Add `SqlKind.IS_NOT_NULL` enum value, and use where possible,
    including for `IS_NOT_UNKNOWN` operator.

Bug-fixes and internal changes
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-312">OPTIQ-312</a>]
  Trim non-required fields before `WindowRel`
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-311">OPTIQ-311</a>]
  Wrong results when filtering the results of windowed aggregation
* More tests for `WITH ... ORDER BY`
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-309">OPTIQ-309</a>]
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
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-288">OPTIQ-288</a>]
  Add tests for windowed aggregation based on Postgres reference queries
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-286">OPTIQ-286</a>]
  Error casting MongoDB date
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-284">OPTIQ-284</a>]
  Window functions range defaults to `CURRENT ROW`
* [<a href=https://issues.apache.org/jira/browse/OPTIQ-285">OPTIQ-285</a>]
  Window functions throw exception without `ORDER BY`
* Test case for
  [<a href=““https://issues.apache.org/jira/browse/OPTIQ-285”>OPTIQ-285</a>].
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-281">OPTIQ-281</a>]
  `EXTRACT` function's SQL return type is `BIGINT` but implemented as Java `int`

## <a href="https://github.com/julianhyde/optiq/releases/tag/optiq-parent-0.7">0.7</a> / 2014-05-13

New features
* Implement table functions.
* Arrays and multi-sets:
  * [<a href="https://issues.apache.org/jira/browse/OPTIQ-267">OPTIQ-267</a>]
    Improve support for ARRAY data type
  * Better type information for JDBC Array; nested array now possible.
  * Implement `JOIN LATERAL` and `JOIN UNNEST`.
  * Implement the `UNNEST` relational operator, and various improvements
    to `ARRAY` and `MULTISET` data types.
  * Represent `ARRAY` columns as Java lists.
  * Implement `CARDINALITY(ARRAY)` SQL operator.
* Implement scalar sub-query in `SELECT` clause.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-273">OPTIQ-273</a>]
  Support column alias in WITH queries (common table expressions)
* Windowed aggregates:
  * Aggregate over constants, e.g. `SUM(1) OVER (ROWS 10 PRECEDING)`;
  * `UNBOUNDED PRECEDING` window range;
  * Windowed aggregates computed over primitive scalars.
* Fix return type inference for aggregate calls. If the `GROUP BY` clause is
  empty, `SUM` may return null.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-37">OPTIQ-37</a>]
  Document JSON model file format (as <a href="MODEL.md">MODEL.md</a>).
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-238">OPTIQ-238</a>]
  Add adapter that generates TPC-H data
* Improve exception message in `AvaticaConnection`; add
  `ExceptionMessageTest`.
* Implement micro-benchmarks via
  <a href="http://openjdk.java.net/projects/code-tools/jmh/">JMH</a>.

API changes
* Provide an option to create root schema without the "metadata" schema.
* Schema SPI:
  * [<a href="https://issues.apache.org/jira/browse/OPTIQ-175">OPTIQ-175</a>]
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
  [<a href="https://issues.apache.org/jira/browse/OPTIQ-270">OPTIQ-270</a>.
* Hush warnings from `SplunkAdapterTest` if Splunk is not available.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-252">OPTIQ-252</a>]
  Scalar sub-query that returns 0 rows should become NULL value
* `SplunkAdapterTest` now uses the same Foodmart database as `JdbcTest`.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-242">OPTIQ-242</a>]
  SplunkAdapterTest fails
* Remove some obsolete classes.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-205">OPTIQ-205</a>]
  Suspicious map.get in VolcanoPlanner.reregister

## <a href="https://github.com/julianhyde/optiq/releases/tag/optiq-parent-0.6">0.6</a> / 2014-04-11

New features
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-214">OPTIQ-214</a>]
  Modify Frameworks to allow Schema to be re-used
  Obsoletes `name` field of `ReflectiveSchema`.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-237">OPTIQ-237</a>]
  Allow user-defined aggregate functions (UDAs) to be defined in a model
* Fix <a href="https://github.com/julianhyde/optiq/pull/227">OPTIQ-227</a>,
  "Extend `EXTRACT` function to support `DATE`, `TIME` and `TIMESTAMP` values".
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-222">OPTIQ-222</a>]
  User-defined table macros
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-179">OPTIQ-179</a>]
  Optiq on Windows
  * Add `sqlline.bat` and fix issues running `sqlline` under Cygwin.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-195">OPTIQ-195</a>]
  Push aggregation into MongoDB adapter
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-193">OPTIQ-193</a>]
  Implement OFFSET and LIMIT in MongoDB adapter
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-164">OPTIQ-164</a>]
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
* Fix <a href="https://github.com/julianhyde/optiq/pull/198">OPTIQ-198</a>,
  "Use `RexExecutor` to evaluate projections and filters".
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-219">OPTIQ-219</a>]
  Parse `ALTER scope SET option = value` statement
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-215">OPTIQ-215</a>]
  A Schema should not have to remember its name and parent
  (**This is a breaking change**.)
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-180">OPTIQ-180</a>]
  Common base class for TableFunction, ScalarFunction
  (**This is a breaking change**.)
* Add methods for dealing with symbols; deprecate
  `SqlLiteral.booleanValue(SqlNode)`, `SqlLiteral.symbolValue(SqlNode)`.
* Add `RelOptPlanner.clear()`; now it is safe to call `transform` twice.
  (Jinfeng Ni)
* Remove APIs deprecated for 0.5.
* Move around some operator classes and singletons.

Bug fixes and internal changes
* Upgrade to linq4j-0.2.
* `FETCH` and `LIMIT` are ignored during SQL-to-RelNode translation.
  (Venki Korukanti)
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-245">OPTIQ-245</a>]
  Off-by-one translation of ON clause of JOIN
* Fix <a href="https://github.com/julianhyde/optiq/pull/191">OPTIQ-191</a>,
  "Rotate time/date/timestamp vals to local timezone".
* Fix <a href="https://github.com/julianhyde/optiq/pull/244">OPTIQ-244</a>,
  "`RelOptTableImpl.create` always expects `QueryableTable` type in
  `OptiqSchema.TableEntry`".
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-225">OPTIQ-225</a>]
  Optiq doesn't correctly decorrelate queries
* Clean up package-info.  Remove duplicates in test packages so they
  don't conflict with those in non-test packages.
* Add `Pair.adjacents(Iterable)`.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-199">OPTIQ-199</a>]
  Various `ANY` type conditions aren't correctly being considered
  (Jacques Nadeau)
* Add files to `.gitignore` that shouldn't be checked in when using
  Eclipse. (Jacques Nadeau)
* Add class `ControlFlowException`, and make it base class of
  existing control-flow exception classes.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-232">OPTIQ-232</a>]
  Sum and avg of empty set should be null as per SQL specification
* Add `SqlUnresolvedFunction`, to improve how return type of
  user-defined functions is resolved. (Vladimir Sitnikov)
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-228">OPTIQ-228</a>]
  Error while compiling generated Java code when using UDF in expression
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-226">OPTIQ-226</a>]
  User-defined functions should work without explicit schema prefix
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-229">OPTIQ-229</a>]
  Join between different JDBC schemas not implementable
* Fix <a href="https://github.com/julianhyde/optiq/pull/230">OPTIQ-230</a>,
  "RemoveSortRule derives trait set from sort, should derive it from sort's child".
* Test view and sub-query with `ORDER BY` and `LIMIT`.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-223">OPTIQ-223</a>]
  Add `NOTICE` and `LICENSE` files in all generated JAR files
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-209">OPTIQ-209</a>]
  Consistent strategy for line-endings in tests
  Convert uses of `NL` in tests to Linux newline "\n".
  This makes string constants simpler.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-218">OPTIQ-218</a>]
  Functions case sensitive when using `Lex.MYSQL`
* Add tests that a query with aggregate expressions in the `SELECT`
  clause is considered an aggregate query, even if there is no `GROUP BY`.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-216">OPTIQ-216</a>]
  Inconsistent use of provided operator table causes inability to
  add aggregate functions
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-200">OPTIQ-200</a>]
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
* In planner, use `RelTrait.subsumes` rather than `equals` in an assert. (Jinfeng Ni)

## <a href="https://github.com/julianhyde/optiq/releases/tag/optiq-parent-0.5">0.5</a> / 2014-03-14

New features
* Allow `quoting`, `quotedCasing`, `unquotedCasing`, and `caseSensitive`
  properties to be specified explicitly (Vladimir Sitnikov)
* Recognize more kinds of materializations, including filter-on-project (where
  project contains expressions) and some kinds of aggregation.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-128">OPTIQ-128</a>]
  Support `WITH` queries (common table expressions)
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-53">OPTIQ-53</a>]
  Allow `WHEN` clause in simple `CASE` expression to have multiple values
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-156">OPTIQ-156</a>]
  Optiq should recognize 'SYSTEM TABLE', 'JOIN', 'INDEX' as table types
* Support querying ARRAY columns from JDBC source. (Gabriel Reid)

API changes
* Add `ProjectRelBase.copy(RelTraitSet, RelNode, List&lt;RexNode&gt;,
  RelDataType)` and make `ProjectRelBase.copy(RelTraitSet, RelNode)` final.
  (**This is a breaking change** for sub-classes of `ProjectRelBase`.)
* Change `RexBuilder.makeRangeReference` parameter type.
* `RexBuilder.makeInputRef` replaces `RelOptUtil.createInputRef`.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-160">OPTIQ-160</a>]
  Allow comments in schema definitions
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-147">OPTIQ-147</a>]
  Create a new kind of `SqlCall` that keeps operands in fields, not an operands
  array
  * Very widely used parse tree nodes with complex operands, including
    `SqlSelect`, `SqlJoin`, `SqlInsert`, and a new node type `SqlOrderBy`, are now
    sub-classes of `SqlCall` but not `SqlBasicCall`.
  * (**This is a breaking change** to code that assumes that, say,
    `SqlSelect` has an `operands` field.)
* Convert all enum constants to upper-case.
  (**This is a breaking change**.)

Bug-fixes and internal changes
* Generate optiq-core-VERSION-tests.jar not parent-VERSION-tests.jar.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-176">OPTIQ-176</a>]
  ORDER BY expression doesn't work with SELECT \*
* Fix VARCHAR casts sent to hsqldb source (Bruno Dumon)
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-143">OPTIQ-143</a>]
  Remove dependency on eigenbase-resgen
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-173">OPTIQ-173</a>]
  Case-insensitive table names are not supported for `Casing.UNCHANGED`
* `DATE.getLimit` now returns `Calendar` in GMT time zone (Vladimir Sitnikov)
* Set `en_US` locale in tests that match against error numbers, dates
  (Vladimir Sitnikov)
* Use 1 test thread per CPU to avoid thread starvation on dual core CPUs
  (Vladimir Sitnikov)
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-174">OPTIQ-174</a>]
  Move hsqldb to test scope
* Add unit tests for `RexExecutorImpl`.
* Correct JSON model examples in Javadoc comments. (Karel Vervaeke)
* Move test reference logs from `src/test/java` to `src/test/resources`
  (reduces the number of 'untracked files' reported by git)
* Tune `Util.SpaceList`, fix race condition, and move into new utility class
  `Spaces`.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-163">OPTIQ-163</a>]
  Equi-join warning
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-157">OPTIQ-157</a>]
  Handle `SQLFeatureNotSupported` when calling `setQueryTimeout`
  (Karel Vervaeke)
* Fix Optiq on Windows. (All tests and checkstyle checks pass.)
* In checkstyle, support Windows-style file separator, otherwise build fails in
  Windows due to suppressions not used. (Vladimir Sitnikov)
* Enable MongoDB tests when `-Doptiq.test.mongodb=true`.
* Cleanup cache exception-handling and an assert.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-153">OPTIQ-153</a>]
  Error using MongoDB adapter: Failed to set setXIncludeAware(true)
* Disable spark engine unless Spark libraries are on the class path and
  `spark=true` is specified in the connect string.
* Fix path to `mongo-zips-model.json` in HOWTO. (Mariano Luna)
* Fix bug deriving the type of a join-key.
* Fix the value of `ONE_MINUS_EPSILON`.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-158">OPTIQ-158</a>]
  Optiq fails when call `Planner.transform()` multiple times, each with
  different ruleset
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-148">OPTIQ-148</a>]
 Less verbose description of collation. Also, optimize `RelTraitSet` creation
 and amortize `RelTraitSet.toString()`.
* Add generics to SQL parser.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-145">OPTIQ-145</a>]
  Unexpected upper-casing of keywords when using java lexer
* Remove duplicate `maven-source-plugin`.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-141">OPTIQ-141</a>]
  Downgrade to guava-11.0.2. (This is necessary for Hadoop compatibility.
  Later versions of Guava can also be used.)
* Upgrade to spark-0.9.0. (Because this version of spark is available from
  maven-central, we can make optiq-spark part of the regular build, and remove
  the spark profile.)

## <a href="https://github.com/julianhyde/optiq/releases/tag/optiq-parent-0.4.18">0.4.18</a> / 2014-02-14

API and functionality changes
* Configurable lexical policy
    * [<a href="https://issues.apache.org/jira/browse/OPTIQ-33">OPTIQ-33</a>]
      SQL parser should allow different identifier quoting
    * [<a href="https://issues.apache.org/jira/browse/OPTIQ-34">OPTIQ-34</a>]
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
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-135">OPTIQ-115</a>]
  Add a PARSE_TREE hook point with SqlNode parameter
* Change planner rules to use `ProjectFactory` for creating
  projects. (John Pullokkaran)
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-131">OPTIQ-131</a>]
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
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-113">OPTIQ-113</a>]
  User-defined scalar functions
* Add rules to short-cut a query if `LIMIT 0` is present. Also remove
  sort, aggregation, join if their inputs are known to be empty, and
  propagate the fact that the relational expressions are known to be
  empty up the tree. (We already do this for union, filter, project.)
* `RexNode` and its sub-classes are now immutable.

Bug fixes and internal changes
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-61">OPTIQ-16</a>]
  Upgrade to janino-2.7
* Upgrade to guava-15.0 (guava-14.0.1 still allowed), sqlline-1.1.7,
  maven-surefire-plugin-2.16, linq4j-0.1.13.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-136">OPTIQ-136</a>]
  Support Hive dialect
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-138">OPTIQ-138</a>]
  SqlDataTypeSpec.clone handles collection types wrong
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-137">OPTIQ-137</a>]
  If a subset is created that is subsumed by an existing subset, its
  'best' is not assigned
    * If best rel in a Volcano subset doesn't have metadata, see if
      other rels have metadata.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-127">OPTIQ-127</a>]
  EnumerableCalcRel can't support 3+ AND conditions (Harish Butani)
* Fix push-down of datetime literals to JDBC data sources.
* Add `Util.startsWith(List, List)` and `Util.hashCode(double)`.
* Add maven-checkstyle-plugin, enable in "verify" phase, and fix exceptions.
* Fix `SqlValidator` to rely on `RelDataType` to do field name matching.  Fix
  `RelDataTypeImpl` to correctly use the case sensitive flag rather than
  ignoring it.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-119">OPTIQ-119</a>]
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
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-112">OPTIQ-112</a>]
  Java boolean column should be treated as SQL boolean
* Fix escaped unicode characters above 0x8000. Add tests for unicode
  strings.

## <a href="https://github.com/julianhyde/optiq/releases/tag/optiq-parent-0.4.17">0.4.17</a> / 2014-01-13

API changes
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-106">OPTIQ-106</a>]
  Make `Schema` and `Table` SPIs simpler to implement, and make them
  re-usable across connections
  (**This is a breaking change**.)
* Make it easier to define sub-classes of rule operands. The new class
  `RelOptRuleOperandChildren` contains the children of an operand and
  the policy for dealing with them. Existing rules now use the new
  methods to construct operands: `operand()`, `leaf()`, `any()`, `none()`,
  `unordered()`. The previous methods are now deprecated and will be
  removed before 0.4.18. (**This is a breaking change**.)
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-101">OPTIQ-101</a>]
  Enable phased access to the Optiq engine
* List-handling methods in `Util`: add methods `skipLast`, `last`, `skip`; remove `subList`, `butLast`.
* Convert `SqlIdentifier.names` from `String[]` to `ImmutableList<String>`.
* Rename `OptiqAssert.assertThat()` to `that()`, to avoid clash with junit's `Assert.assertThat()`.
* Usability improvements for `RelDataTypeFactory.FieldInfoBuilder`. It
  now has a type-factory, so you can just call `build()`.
* Rework `HepProgramBuilder` into a fluent API.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-105">OPTIQ-105</a>]
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
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-70">OPTIQ-70</a>]
  Joins seem to be very expensive in memory
* Make planning process more efficient by not sorting the list of
  matched rules each cycle. It is sorted if tracing is enabled;
  otherwise we scan to find the most important element. For this list,
  replace `LinkedList` with `ChunkList`, which has an O(1) remove and add,
  a fast O(n) get, and fast scan.

Other
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-87">OPTIQ-87</a>]
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
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-97">OPTIQ-97</a>]
  Correlated EXISTS
* Fix a bug in `VolcanoCost`.
* Add class `FoodMartQuerySet`, that contains the 6,700 foodmart queries.
* Fix factory class names in `UnregisteredDriver`
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-96">OPTIQ-96</a>]
  LIMIT against a table in a clone schema causes UnsupportedOperationException
* Disable spark module by default.
* Allow `CloneSchema` to be specified in terms of url, driver, user,
  password; not just dataSource.
* Wrap internal error in `SQLException`.

## <a href="https://github.com/julianhyde/optiq/releases/tag/optiq-parent-0.4.16">0.4.16</a> / 2013-11-24

* [<a href="https://issues.apache.org/jira/browse/OPTIQ-69">OPTIQ-69</a>]
  Can't join on string columns and other problems with expressions in the join condition
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-74">OPTIQ-74</a>]
  JOIN ... USING fails in 3-way join with UnsupportedOperationException.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-65">OPTIQ-65</a>]
  Fix issues in the JDBC driver, and in particular to DatabaseMetaData methods, to make Squirrel-SQL run better.
* Fix JDBC column, table, schema names for when the table is not in a schema of depth 1.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-85">OPTIQ-85</a>]
  Adding a table to the root schema causes breakage in OptiqPrepareImpl
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-84">OPTIQ-84</a>]
  Extract Optiq's JDBC driver as a new JDBC driver framework, Avatica.
  Other projects can use this to implement a JDBC driver by implementing
  just a few methods. If you wish to use Optiq's JDBC driver, you will
  now need to include optiq-avatica.jar in addition to optiq-core.jar.
  Avatica does not depend on anything besides the standard Java library.
* Support for parameters in PreparedStatement.
* First steps in recognizing complex materializations. Internally we introduce a concept called a "star table", virtual table composed of real tables joined together via many-to-one relationships. The queries that define materializations and end-user queries are canonized in terms of star tables. Matching (not done yet) will then be a matter of looking for sort, groupBy, project. It is not yet possible to define a star in an Optiq model file.
* Add section to <a href="HOWTO.md">HOWTO</a> on implementing adapters.
* Fix data type conversions when creating a clone table in memory.
* Fix how strings are escaped in JsonBuilder.
* Test suite now depends on an embedded hsqldb database, so you can run <code>mvn test</code> right after pulling from git. You can instead use a MySQL database if you specify '-Doptiq.test.db=mysql', but you need to manually populate it.
* Fix a planner issue which occurs when the left and right children of join are the same relational expression, caused by a self-join query.
* [<a href="https://issues.apache.org/jira/browse/OPTIQ-76">OPTIQ-76</a>]
  Precedence of the item operator, <code>map[index]</code>; remove the space before '[' when converting parse tree to string.
* Allow <code>CAST(expression AS ANY)</code>, and fix an issue with the ANY type and NULL values.
* Handle null timestamps and dates coming out of JDBC adapter.
* Add <code>jdbcDriver</code> attribute to JDBC schema in model, for drivers that do not auto-register.
* Allow join rules to match any subclass of JoinRelBase.
* Push projects, filters and sorts down to MongoDB.
  (Fixes [<a href="https://issues.apache.org/jira/browse/OPTIQ-57">OPTIQ-57</a>],
  <a href="https://github.com/julianhyde/optiq/pull/60">pull #60</a> and
  [<a href="https://issues.apache.org/jira/browse/OPTIQ-72">OPTIQ-72</a>.)
* Add instructions for loading FoodMart data set into MongoDB, and how to enable tracing.
* Now runs on JDK 1.8 (still runs on JDK 1.6 and JDK 1.7).
* Upgrade to junit-4.11 (avoiding the dodgy junit-4.1.12).
* Upgrade to linq4j-0.1.11.

## <a href="https://github.com/julianhyde/optiq/releases/tag/optiq-parent-0.4.15">0.4.15</a> / 2013-10-14

* Lots of good stuff that this margin is too small to contain. See
  <a href="REFERENCE.md">SQL language reference</a> and
  <a href="MODEL.md">JSON model reference</a>.
