# Optiq release history

For a full list of releases, see <a href="https://github.com/julianhyde/optiq/releases">github</a>.

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

###Tuning
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
* Fix <a href="https://github.com/julianhyde/optiq/issues/70>#70</a>,
  "Joins seem to be very expensive in memory".
* Make planning process more efficient by not sorting the list of
  matched rules each cycle. It is sorted if tracing is enabled;
  otherwise we scan to find the most important element. For this list,
  replace `LinkedList` with `ChunkList`, which has an O(1) remove and add,
  a fast O(n) get, and fast scan.

###Other
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
* Fix <a href="https://github.com/julianhyde/optiq/issues/97>#97</a>,
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
