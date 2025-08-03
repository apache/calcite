/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

# Test Consolidation Notes

This document tracks the test consolidation efforts for the file adapter test suite.

## Completed Consolidations

### Excel Tests
**New Test:** `format/ExcelComprehensiveTest.java`

**Tests to Remove:**
- `SimpleExcelTest.java` - Basic functionality covered in ExcelComprehensiveTest
- `ExcelFileTest.java` - All test cases migrated to comprehensive test
- `MultiTableExcelTest.java` - Multi-sheet functionality included
- `ExcelNamingTest.java` - Sheet naming tests now parameterized
- `ExcelConversionTest.java` - Conversion tests integrated

**Keep:**
- `DocxTableTest.java` - Different format (Word documents)

## Pending Consolidations

### Spurious Table Tests
**Tests to Consolidate:**
- `SpuriousTableTest.java`
- `RealSpuriousTest.java`
- `ReproduceSpuriousTest.java`

**Action:** Create `feature/SpuriousTableComprehensiveTest.java` that covers all spurious table edge cases.

### Recursive Directory Tests
**Tests to Consolidate:**
- `RecursiveDirectoryTest.java`
- `RecursiveProofTest.java`
- `RecursiveDemoTest.java`
- `RecursiveGlobTest.java` (partially - keep glob-specific tests)

**Action:** Create `feature/RecursiveDirectoryComprehensiveTest.java` for all recursive directory scenarios.

## Test Organization Plan

### Package Structure
```
src/test/java/org/apache/calcite/adapter/file/
├── format/                    # Format-specific tests
│   ├── ArrowFileTest.java     # NEW
│   ├── CompressedFileTest.java # NEW
│   ├── CsvFormatTest.java     # Rename from CsvEnumeratorTest
│   ├── ExcelComprehensiveTest.java # NEW
│   ├── JsonFormatTest.java    # Rename from JsonFlattenTest
│   ├── ParquetFormatTest.java # Rename from ParquetFileTest
│   └── YamlFileTest.java      # NEW
├── storage/                   # Storage provider tests
│   ├── FtpStorageProviderTest.java
│   ├── S3StorageProviderTest.java
│   ├── SftpStorageProviderTest.java
│   └── SharePointStorageProviderTest.java
├── feature/                   # Feature-specific tests
│   ├── MaterializedViewTest.java
│   ├── PartitioningTest.java
│   ├── RedisIntegrationTest.java # NEW
│   ├── RefreshableTableTest.java
│   └── SpuriousTableComprehensiveTest.java # TODO
├── performance/               # Performance benchmarks
│   ├── ComprehensiveEnginePerformanceTest.java
│   ├── DirectParquetPerformanceTest.java
│   └── LargeScaleEnginePerformanceTest.java
├── integration/               # End-to-end integration tests
│   ├── FileAdapterTest.java   # Main integration test
│   ├── MixedFormatGlobTest.java # NEW
│   └── RemoteFileIntegrationTest.java
└── error/                     # Error handling tests
    └── ErrorHandlingTest.java # NEW

# Root level (to be moved)
- FileAdapterCapabilitiesTest.java → feature/
- GlobPatternTest.java → feature/
- HtmlToJsonConverterTest.java → format/
- MarkdownTableTest.java → format/
```

## Migration Guidelines

1. When consolidating tests:
   - Preserve all unique test cases
   - Use parameterized tests where applicable
   - Remove redundant setup/teardown code
   - Improve test naming for clarity

2. When moving tests:
   - Update package declarations
   - Fix imports
   - Update any references in other tests

3. Documentation:
   - Add class-level JavaDoc explaining test purpose
   - Document any special setup requirements
   - Note any tests that require external services
