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

/**
 * XBRL adapter for Apache Calcite that processes SEC EDGAR filings.
 *
 * <p>This adapter extends the file adapter to provide specialized support for
 * XBRL (eXtensible Business Reporting Language) documents, particularly those
 * from the SEC EDGAR database. It converts XBRL instance documents and taxonomies
 * into partitioned Parquet files for efficient analytical querying.
 *
 * <h2>Key Features</h2>
 * <ul>
 *   <li>Automatic XBRL to Parquet conversion</li>
 *   <li>CIK/filing-type/date partitioning for efficient queries</li>
 *   <li>Financial statement line item extraction with hierarchy</li>
 *   <li>Footnote linking to financial statement items</li>
 *   <li>Company metadata extraction</li>
 *   <li>Support for all major SEC filing types (10-K, 10-Q, 8-K, etc.)</li>
 * </ul>
 *
 * <h2>Architecture</h2>
 * <p>The adapter consists of:
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.sec.SecSchemaFactory} - Entry point that extends FileSchemaFactory</li>
 *   <li>{@link org.apache.calcite.adapter.sec.SecToParquetConverter} - Converts XBRL to Parquet</li>
 *   <li>{@link org.apache.calcite.adapter.sec.EdgarPartitionStrategy} - Implements CIK/type/date partitioning</li>
 * </ul>
 *
 * <h2>Configuration Example</h2>
 * <pre>{@code
 * {
 *   "version": "1.0",
 *   "defaultSchema": "EDGAR",
 *   "schemas": [{
 *     "name": "EDGAR",
 *     "type": "custom",
 *     "factory": "org.apache.calcite.adapter.sec.SecSchemaFactory",
 *     "operand": {
 *       "directory": "/data/edgar",
 *       "executionEngine": "duckdb",
 *       "enableSecProcessing": true,
 *       "secSourceDirectory": "/data/edgar/sec",
 *       "processSecOnInit": true,
 *       "partitionedTables": [
 *         {
 *           "name": "financial_line_items",
 *           "partitionColumns": ["cik", "filing_type", "filing_date"]
 *         }
 *       ]
 *     }
 *   }]
 * }
 * }</pre>
 *
 * <h2>Query Examples</h2>
 * <pre>{@code
 * -- Query Apple's revenue over time
 * SELECT filing_date, value as revenue
 * FROM financial_line_items
 * WHERE cik = '0000320193'
 *   AND concept = 'Revenue'
 *   AND filing_type = '10-K'
 * ORDER BY filing_date;
 *
 * -- Find companies with highest net income
 * SELECT c.company_name, f.value as net_income
 * FROM financial_line_items f
 * JOIN company_info c ON f.cik = c.cik
 * WHERE f.concept = 'NetIncome'
 *   AND f.filing_type = '10-K'
 *   AND f.filing_date >= '2023-01-01'
 * ORDER BY f.value DESC
 * LIMIT 10;
 * }</pre>
 */
package org.apache.calcite.adapter.sec;
