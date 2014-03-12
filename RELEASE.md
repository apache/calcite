# Optiq release history

For a full list of releases, see <a href="https://github.com/julianhyde/optiq/releases">github</a>.

## <a href="https://github.com/julianhyde/optiq/releases/tag/optiq-parent-0.5">0.5</a> / 2014-03-14

New features
* Allow `quoting`, `quotedCasing`, `unquotedCasing`, and `caseSensitive`
  properties to be specified explicitly (Vladimir Sitnikov)
* Recognize more kinds of materializations, including filter-on-project (where
  project contains expressions) and some kinds of aggregation.
* Fix <a href="https://github.com/julianhyde/optiq/issues/128">#128</a>,
  "Support `WITH` queries (common table expressions)".
* Fix <a href="https://github.com/julianhyde/optiq/issues/53">#53</a>,
  "Allow `WHEN` clause in simple `CASE` expression to have multiple values".
* Fix <a href="https://github.com/julianhyde/optiq/issues/156">#156</a>,
  "Optiq should recognize 'SYSTEM TABLE', 'JOIN', 'INDEX' as table types".

API changes
* Add `ProjectRelBase.copy(RelTraitSet, RelNode, List&lt;RexNode&gt;,
  RelDataType)` and make `ProjectRelBase.copy(RelTraitSet, RelNode)`
  final.
  (**This is a breaking change** for sub-classes of `ProjectRelBase`.)
* Change `RexBuilder.makeRangeReference` parameter type.
* `RexBuilder.makeInputRef` replaces `RelOptUtil.createInputRef`.
* Fix <a href="https://github.com/julianhyde/optiq/issues/160">#160</a>,
 "Allow comments in schema definitions".
* Fix <a href="https://github.com/julianhyde/optiq/issues/147">#147</a>,
  "Create a new kind of `SqlCall` that keeps operands in fields, not an operands
  array". Very widely used parse tree nodes with complex operands, including
  `SqlSelect`, `SqlJoin`, `SqlInsert`, and a new node type `SqlOrderBy`, are now
  sub-classes of `SqlCall` but not `SqlBasicCall`.
  (**This is a breaking change** to code that assumes that, say,
 `SqlSelect` has an `operands` field.)
* Convert all enum constants to upper-case.
  (**This is a breaking change**.)

Bug-fixes and internal changes
* Fix <a href="https://github.com/julianhyde/optiq/issues/143">#143</a>,
  "Remove dependency on eigenbase-resgen".
* Fix <a href="https://github.com/julianhyde/optiq/issues/173">#173</a>,
 "Case-insensitive table names are not supported for `Casing.UNCHANGED`".
* `DATE.getLimit` now returns `Calendar` in GMT time zone (Vladimir Sitnikov)
* Set `en_US` locale in tests that match against error numbers, dates
  (Vladimir Sitnikov)
* Use 1 test thread per CPU to avoid thread starvation on dual core CPUs
  (Vladimir Sitnikov)
* Fix <a href="https://github.com/julianhyde/optiq/issues/174">#174</a>,
  "Move hsqldb to test scope".
* Add unit tests for `RexExecutorImpl`.
* Correct JSON model examples in Javadoc comments. (Karel Vervaeke)
* Move test reference logs from `src/test/java` to `src/test/resources`
  (reduces the number of 'untracked files' reported by git)
* Tune `Util.SpaceList`, fix race condition, and move into new utility class
  `Spaces`.
* Fix <a href="https://github.com/julianhyde/optiq/issues/163">#163</a>,
  "Equi-join warning".
* Fix <a href="https://github.com/julianhyde/optiq/issues/157">#157</a>,
  "Handle `SQLFeatureNotSupported` when calling `setQueryTimeout`".
  (Karel Vervaeke)
* Fix Optiq on Windows. (All tests and checkstyle checks pass.)
* In checkstyle, support Windows-style file separator, otherwise build fails in
  Windows due to suppressions not used. (Vladimir Sitnikov)
* Enable MongoDB tests when `-Doptiq.test.mongodb=true`.
* Cleanup cache exception-handling and an assert.
* Fix <a href="https://github.com/julianhyde/optiq/issues/153">#153</a>,
  "Error using MongoDB adapter: Failed to set setXIncludeAware(true)".
* Disable spark engine unless Spark libraries are on the class path and
  `spark=true` is specified in the connect string.
* Fix path to `mongo-zips-model.json` in HOWTO.
* Fix bug deriving the type of a join-key.
* Fix the value of `ONE_MINUS_EPSILON`.
* Fix <a href="https://github.com/julianhyde/optiq/issues/158">#158</a>,
  "Optiq fails when call `Planner.transform()` multiple times, each with
  different ruleset".
* Fix <a href="https://github.com/julianhyde/optiq/issues/148">#148</a>,
 "Less verbose description of collation". Also, optimize `RelTraitSet` creation
 and amortize `RelTraitSet.toString()`.
* Add generics to SQL parser.
* Fix <a href="https://github.com/julianhyde/optiq/issues/145">#145</a>,
  "Unexpected upper-casing of keywords when using java lexer".
* Remove duplicate `maven-source-plugin`.
* Fix <a href="https://github.com/julianhyde/optiq/issues/141">#141</a>,
  "Downgrade to guava-11.0.2". This is necessary for Hadoop compatibility.
  Later versions of Guava can also be used.
* Upgrade to spark-0.9.0. (Because this version of spark is available from
  maven-central, we can make optiq-spark part of the regular build, and remove
  the spark profile.)

## <a href="https://github.com/julianhyde/optiq/releases/tag/optiq-parent-0.4.18">0.4.18</a> / 2014-02-14

API and functionality changes
* Configurable lexical policy
    * Fix <a href="https://github.com/julianhyde/optiq/issues/33">#33</a>,
      "SQL parser should allow different identifier quoting".
    * Fix <a href="https://github.com/julianhyde/optiq/issues/34">#34</a>,
      "Policy for case-sensitivity of identifiers should be configurable".
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
* Fix <a href="https://github.com/julianhyde/optiq/issues/135">#115</a>,
  "Add a PARSE_TREE hook point with SqlNode parameter".
* Change planner rules to use `ProjectFactory` for creating
  projects. (John Pullokkaran)
* Fix <a href="https://github.com/julianhyde/optiq/issues/131">#131</a>,
  "Add interfaces for metadata (statistics)". (**This is a breaking
  change**.)
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
* Fix <a href="https://github.com/julianhyde/optiq/issues/113">#113</a>,
  "User-defined scalar functions".
* Add rules to short-cut a query if `LIMIT 0` is present. Also remove
  sort, aggregation, join if their inputs are known to be empty, and
  propagate the fact that the relational expressions are known to be
  empty up the tree. (We already do this for union, filter, project.)
* `RexNode` and its sub-classes are now immutable.

Bug fixes and internal changes
* Fix <a href="https://github.com/julianhyde/optiq/issues/61">#16</a>,
  "Upgrade to janino-2.7".
* Upgrade to guava-15.0 (guava-14.0.1 still allowed), sqlline-1.1.7,
  maven-surefire-plugin-2.16, linq4j-0.1.13.
* Fix <a href="https://github.com/julianhyde/optiq/issues/136">#136</a>,
  "Support Hive dialect".
* Fix <a href="https://github.com/julianhyde/optiq/issues/138">#138</a>,
  "SqlDataTypeSpec.clone handles collection types wrong".
* Fix <a href="https://github.com/julianhyde/optiq/issues/137">#137</a>,
  "If a subset is created that is subsumed by an existing subset, its
  "best" is not assigned".
    * If best rel in a Volcano subset doesn't have metadata, see if
      other rels have metadata.
* Fix <a href="https://github.com/julianhyde/optiq/issues/127">#127</a>,
 "EnumerableCalcRel can't support 3+ AND conditions". (Harish Butani)
* Fix push-down of datetime literals to JDBC data sources.
* Add `Util.startsWith(List, List)` and `Util.hashCode(double)`.
* Add maven-checkstyle-plugin, enable in "verify" phase, and fix exceptions.
* Fix `SqlValidator` to rely on `RelDataType` to do field name matching.  Fix
  `RelDataTypeImpl` to correctly use the case sensitive flag rather than
  ignoring it.
* Fix <a href="https://github.com/julianhyde/optiq/issues/119">#119</a>,
  "Comparing Java type long with SQL type INTEGER gives wrong answer".
* Enable multi-threaded testing, and fix race conditions.
    * Two of the race conditions involved involving trait caches. The
      other was indeterminacy in type system when precision was not
      specified but had a default; now we canonize TIME to TIME(0), for
      instance.
* Convert files to `us-ascii`.
* Work around
  <a href="http://jira.codehaus.org/browse/JANINO-169">JANINO-169</a>.
* Refactor SQL validator testing infrastructure so SQL parser is
  configurable.
* Add `optiq-mat-plugin` to README.
* Fix the check for duplicate subsets in a rule match.
* Fix <a href="https://github.com/julianhyde/optiq/issues/112">#112</a>,
  "Java boolean column should be treated as SQL boolean".
* Fix escaped unicode characters above 0x8000. Add tests for unicode
  strings.

## <a href="https://github.com/julianhyde/optiq/releases/tag/optiq-parent-0.4.17">0.4.17</a> / 2014-01-13

API changes

* Fix <a href="https://github.com/julianhyde/optiq/issues/106">#106</a>,
  "Make `Schema` and `Table` SPIs simpler to implement, and make them
  re-usable across connections". (**This is a breaking change**.)
* Make it easier to define sub-classes of rule operands. The new class
  `RelOptRuleOperandChildren` contains the children of an operand and
  the policy for dealing with them. Existing rules now use the new
  methods to construct operands: `operand()`, `leaf()`, `any()`, `none()`,
  `unordered()`. The previous methods are now deprecated and will be
  removed before 0.4.18. (**This is a breaking change**.)
* Fix <a href="https://github.com/julianhyde/optiq/issues/101">#101</a>,
  "Enable phased access to the Optiq engine".
* List-handling methods in `Util`: add methods `skipLast`, `last`, `skip`; remove `subList`, `butLast`.
* Convert `SqlIdentifier.names` from `String[]` to `ImmutableList<String>`.
* Rename `OptiqAssert.assertThat()` to `that()`, to avoid clash with junit's `Assert.assertThat()`.
* Usability improvements for `RelDataTypeFactory.FieldInfoBuilder`. It
  now has a type-factory, so you can just call `build()`.
* Rework `HepProgramBuilder` into a fluent API.
* Fix <a href="https://github.com/julianhyde/optiq/issues/105">#105</a>, "Externalize RelNode to and from JSON".

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
* Fix <a href="https://github.com/julianhyde/optiq/issues/70">#70</a>,
  "Joins seem to be very expensive in memory".
* Make planning process more efficient by not sorting the list of
  matched rules each cycle. It is sorted if tracing is enabled;
  otherwise we scan to find the most important element. For this list,
  replace `LinkedList` with `ChunkList`, which has an O(1) remove and add,
  a fast O(n) get, and fast scan.

Other

* Fix <a href="https://github.com/julianhyde/optiq/issues/87">#87</a>,
  "Constant folding". Rules for constant-expression reduction, and to simplify/eliminate
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
* Fix <a href="https://github.com/julianhyde/optiq/issues/97">#97</a>,
  correlated EXISTS.
* Fix a bug in `VolcanoCost`.
* Add class `FoodMartQuerySet`, that contains the 6,700 foodmart queries.
* Fix factory class names in `UnregisteredDriver`
* Fix <a href="https://github.com/julianhyde/optiq/issues/96">#96</a>,
  "LIMIT against a table in a clone schema causes UnsupportedOperationException".
* Disable spark module by default.
* Allow `CloneSchema` to be specified in terms of url, driver, user,
  password; not just dataSource.
* Wrap internal error in `SQLException`.

## <a href="https://github.com/julianhyde/optiq/releases/tag/optiq-parent-0.4.16">0.4.16</a> / 2013-11-24

* Fix <a href="https://github.com/julianhyde/optiq/issues/69">#69</a>, "Can't join on string columns" and other problems with expressions in the join condition
* Fix <a href="https://github.com/julianhyde/optiq/issues/74">#74</a>, "JOIN ... USING fails in 3-way join with UnsupportedOperationException".
* Fix <a href="https://github.com/julianhyde/optiq/issues/65">#65</a> issues in the JDBC driver, and in particular to DatabaseMetaData methods, to make Squirrel-SQL run better. Fix JDBC column, table, schema names for when the table is not in a schema of depth 1.
* Fix <a href="https://github.com/julianhyde/optiq/issues/85">#85</a>, "Adding a table to the root schema causes breakage in OptiqPrepareImpl".
* Extract Optiq's JDBC driver as a new JDBC driver framework, Avatica (<a href="https://github.com/julianhyde/optiq/issues/84">#84</a>). Other projects can use this to implement a JDBC driver by implementing just a few methods. If you wish to use Optiq's JDBC driver, you will now need to include optiq-avatica.jar in addition to optiq-core.jar. Avatica does not depend on anything besides the standard Java library.
* Support for parameters in PreparedStatement.
* First steps in recognizing complex materializations. Internally we introduce a concept called a "star table", virtual table composed of real tables joined together via many-to-one relationships. The queries that define materializations and end-user queries are canonized in terms of star tables. Matching (not done yet) will then be a matter of looking for sort, groupBy, project. It is not yet possible to define a star in an Optiq model file.
* Add section to <a href="HOWTO.md">HOWTO</a> on implementing adapters.
* Fix data type conversions when creating a clone table in memory.
* Fix how strings are escaped in JsonBuilder.
* Test suite now depends on an embedded hsqldb database, so you can run <code>mvn test</code> right after pulling from git. You can instead use a MySQL database if you specify '-Doptiq.test.db=mysql', but you need to manually populate it.
* Fix a planner issue which occurs when the left and right children of join are the same relational expression, caused by a self-join query.
* Fix <a href="https://github.com/julianhyde/optiq/issues/76">#76</a> precedence of the item operator, <code>map[index]</code>; remove the space before '[' when converting parse tree to string.
* Allow <code>CAST(expression AS ANY)</code>, and fix an issue with the ANY type and NULL values.
* Handle null timestamps and dates coming out of JDBC adapter.
* Add <code>jdbcDriver</code> attribute to JDBC schema in model, for drivers that do not auto-register.
* Allow join rules to match any subclass of JoinRelBase.
* Push projects, filters and sorts down to MongoDB. (Fixes <a href="https://github.com/julianhyde/optiq/issues/57">#57</a>, <a href="https://github.com/julianhyde/optiq/pull/60">#60</a> and <a href="https://github.com/julianhyde/optiq/issues/72">#72</a>.)
* Add instructions for loading FoodMart data set into MongoDB, and how to enable tracing.
* Now runs on JDK 1.8 (still runs on JDK 1.6 and JDK 1.7).
* Upgrade to junit-4.11 (avoiding the dodgy junit-4.1.12).
* Upgrade to linq4j-0.1.11.

## <a href="https://github.com/julianhyde/optiq/releases/tag/optiq-parent-0.4.15">0.4.15</a> / 2013-10-14

* Lots of good stuff that this margin is too small to contain. See a <a href="REFERENCE.md">SQL language reference</a>.
