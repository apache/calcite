# Changelog

All notable changes to the Apache Calcite File Adapter will be documented in this file.

## [1.0.0] - Unreleased

### Added
- **PPTX File Support**: Added PowerPoint presentation table extraction
  - Automatically scans PPTX files for tables and converts them to JSON
  - Extracts tables from all slides with slide context
  - Detects table titles from text shapes above tables
  - Preserves slide titles and slide numbers in generated file names
  - Handles multiple tables per slide and complex table structures
  - Similar functionality to DOCX support but adapted for presentation format

### Changed
- **Production Logging**: Replaced all `System.out.println` and `System.err.println` statements with proper SLF4J logging
  - Added proper loggers to all major components (FileSchema, CsvTable, CsvEnumerator, StorageProviderSource, ParquetExecutionEngine, etc.)
  - Debug statements now use `LOGGER.debug()` for better production control
  - Error messages use appropriate log levels (`error`, `warn`)
  - Performance improvement: Parameterized logging avoids string concatenation when log level is disabled
  - Added comprehensive logging documentation to README.md

### Technical Details
- **Files Updated**:
  - FileSchema.java - 44 debug/error statements converted
  - CsvTable.java - 3 debug statements converted
  - StorageProviderSource.java - 1 debug statement converted
  - CsvEnumerator.java - 11 statements converted (using CalciteLogger)
  - ParquetScannableTable.java - 1 statement converted
  - ParquetExecutionEngine.java - 1 statement converted
  - ParquetEnumerableFactory.java - 2 statements converted
  - RefreshablePartitionedParquetTable.java - 2 statements converted
  - ParquetEnumerator.java - 9 statements converted
  - MaterializedViewTable.java - 3 statements converted
  - ExecutionEngineConfig.java - 6 statements converted
  - ConcurrentSpilloverManager.java - 3 statements converted

### Benefits
- **Production Ready**: Logging can now be configured via standard SLF4J configuration files
- **Better Debugging**: Log levels can be adjusted without code changes
- **Performance**: No string concatenation when logging is disabled
- **Consistency**: All components now follow standard Java logging practices

### Known Issues
- **SharePoint Integration**: MicrosoftGraphStorageProvider currently has hardcoded "Shared Documents" and "Documents" paths that may not work with non-English SharePoint sites or custom document library names. This needs to be made configurable.

