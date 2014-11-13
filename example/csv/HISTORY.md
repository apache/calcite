# Optiq-csv release history

For a full list of releases, see
<a href="https://github.com/julianhyde/optiq-csv/releases">github</a>.

## HEAD

* Upgrade to calcite-0.9.1.
* Support gzip-compressed CSV and JSON files (recognized by '.gz' suffix).
* Cleanup, and fix minor timezone issue in a test.
* Support for date types (date, time, timestamp) (Martijn van den Broek)
* Upgrade to optiq-0.8, optiq-avatica-0.8, linq4j-0.4.
* Add support for JSON files (recognized by '.json' suffix).
* Upgrade maven-release-plugin to version 2.4.2.
* Upgrade to optiq-0.6, linq4j-0.2.
* Add NOTICE and LICENSE files in generated JAR file.

## <a href="https://github.com/julianhyde/optiq-csv/releases/tag/optiq-csv-0.3">0.3</a> / 2014-03-21

* Upgrade to optiq-0.5.
* Add workaround to
  <a href="https://github.com/jline/jline2/issues/62">jline2 #62</a>
  to `sqlline.bat` (windows) and `sqlline` (windows using cygwin).
* Fix classpath construction: `sqlline.bat` copies dependencies to `target/dependencies`; `sqlline` constructs `target/classpath.txt`.
* Build, checkstyle and tests now succeed on windows (both native and cygwin).
* Models can now contain comments.
* Fix <a href="https://github.com/julianhyde/optiq-csv/issues/2">#2</a>,
  "Update tutorial to reflect changes to Optiq's JDBC adapter".

## <a href="https://github.com/julianhyde/optiq-csv/releases/tag/optiq-csv-0.2">0.2</a> / 2014-02-18

* Add test case for <a href="https://github.com/julianhyde/optiq/issues/112">optiq-112</a>.
* Add `sqlline.bat`, Windows SQL shell. (Based on fix for <a href="https://issues.apache.org/jira/browse/DRILL-338">DRILL-338</a>.)
* Upgrade to optiq-0.4.18, sqlline-1.1.7.
* Return a single object for single-col enumerator (Gabriel Reid)
* Enable maven-checkstyle-plugin; fix checkstyle exceptions.

## <a href="https://github.com/julianhyde/optiq-csv/releases/tag/optiq-csv-0.1">0.1</a> / 2014-01-13

* Add release notes and history.
* Enable maven-release-plugin.
* Upgrade to optiq-0.4.17, linq4j-0.1.12, sqlline-1.1.6.
* Upgrade tutorial for new Schema and Table SPIs.
* Fixes for optiq SPI changes in <a href="https://github.com/julianhyde/optiq/issues/106">optiq-106</a>.
* Enable oraclejdk8 in Travis CI.
* Fix bug where non-existent directory would give NPE. Instead print warning.
* Add an example of a planner rule.
* Add CsvTableFactory, an example of a custom table.
* Add a view to tutorial.
* Split into scenario with a "simple" schema that generates tables (CsvTable) that just execute and a "smart" schema that generates tables (CsvSmartTable) that undergo optimization.
* Make CsvEnumerator a top-level class.
* Implement the algorithms to sniff names and types from the first row, and to return an enumerator of all rows.
* Read column types from header of CSV file.
