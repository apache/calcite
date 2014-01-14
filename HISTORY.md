# Optiq-csv release history

For a full list of releases, see <a href="https://github.com/julianhyde/optiq-csv/releases">github</a>.

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
