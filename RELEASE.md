# Optiq release history

For a full list of releases, see <a href="https://github.com/julianhyde/optiq/releases">github</a>.

## <a href="https://github.com/julianhyde/optiq/releases/tag/optiq-parent-0.4.16">0.4.16</a> (2013-11-24)

* Fix <a href="#69">https://github.com/julianhyde/optiq/issues/69</a>, "Can't join on string columns" and other problems with expressions in the join condition
* Fix <a href="#74">https://github.com/julianhyde/optiq/issues/74</a>, "JOIN ... USING fails in 3-way join with UnsupportedOperationException".
* Fix <a href="#65">https://github.com/julianhyde/optiq/issues/65</a> issues in the JDBC driver, and in particular to DatabaseMetaData methods, to make Squirrel-SQL run better. Fix JDBC column, table, schema names for when the table is not in a schema of depth 1.
* Fix <a href="#85">https://github.com/julianhyde/optiq/issues/85</a>, "Adding a table to the root schema causes breakage in OptiqPrepareImpl".
* Extract Optiq's JDBC driver as a new JDBC driver framework, Avatica (<a href="#84">https://github.com/julianhyde/optiq/issues/84</a>). Other projects can use this to implement a JDBC driver by implementing just a few methods. If you wish to use Optiq's JDBC driver, you will now need to include optiq-avatica.jar in addition to optiq-core.jar. Avatica does not depend on anything besides the standard Java library.
* Support for parameters in PreparedStatement.
* First steps in recognizing complex materializations. Internally we introduce a concept called a "star table", virtual table composed of real tables joined together via many-to-one relationships. The queries that define materializations and end-user queries are canonized in terms of star tables. Matching (not done yet) will then be a matter of looking for sort, groupBy, project. It is not yet possible to define a star in an Optiq model file.
* Add section to <a href="HOWTO.md">HOWTO</a> on implementing adapters.
* Fix data type conversions when creating a clone table in memory.
* Fix how strings are escaped in JsonBuilder.
* Test suite now depends on an embedded hsqldb database, so you can run <code>mvn test</code> right after pulling from git. You can instead use a MySQL database if you specify '-Doptiq.test.db=mysql', but you need to manually populate it.
* Fix a planner issue which occurs when the left and right children of join are the same relational expression, caused by a self-join query.
* Fix <a href="#76">https://github.com/julianhyde/optiq/issues/76</a> precedence of the item operator, <code>map[index]</code>; remove the space before '[' when converting parse tree to string.
* Allow <code>CAST(expression AS ANY)</code>, and fix an issue with the ANY type and NULL values.
* Handle null timestamps and dates coming out of JDBC adapter.
* Add <code>jdbcDriver</code> attribute to JDBC schema in model, for drivers that do not auto-register.
* Allow join rules to match any subclass of JoinRelBase.
* Push projects, filters and sorts down to MongoDB. (Fixes <a href="#57">https://github.com/julianhyde/optiq/issues/57</a> and <a href="#60">https://github.com/julianhyde/optiq/pull/60</a>.)
* Add instructions for loading FoodMart data set into MongoDB, and how to enable tracing.
* Now runs on JDK 1.8 (still runs on JDK 1.6 and JDK 1.7).
* Upgrade to junit-4.11 (avoiding the dodgy junit-4.1.12).
* Upgrade to linq4j-0.1.11.

## <a href="https://github.com/julianhyde/optiq/releases/tag/optiq-parent-0.4.15">0.4.15</a> (2013-10-14)

* Lots of good stuff that this margin is too small to contain. See a <a href="REFERENCE.md">SQL language reference</a>.
