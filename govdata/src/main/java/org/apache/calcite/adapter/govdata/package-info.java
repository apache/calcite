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
 * Government Data adapter for Apache Calcite.
 *
 * <p>This adapter provides access to various U.S. government data sources
 * through SQL queries. It acts as an uber-adapter that routes to specialized
 * sub-adapters based on the data source requested.
 *
 * <h2>Supported Data Sources</h2>
 * <ul>
 *   <li><strong>SEC (Securities and Exchange Commission)</strong> - EDGAR filings,
 *       company financials, insider trading data</li>
 *   <li><strong>Census (U.S. Census Bureau)</strong> - Demographics, economic data
 *       <em>(coming soon)</em></li>
 *   <li><strong>IRS (Internal Revenue Service)</strong> - Tax statistics, exempt
 *       organizations <em>(coming soon)</em></li>
 *   <li><strong>Treasury (U.S. Treasury)</strong> - Economic indicators, debt data
 *       <em>(coming soon)</em></li>
 * </ul>
 *
 * <h2>Quick Start</h2>
 * <p>Connect using JDBC URL:
 * <pre>
 * jdbc:govdata:source=sec&ciks=AAPL,MSFT
 * </pre>
 *
 * <p>Or using a model file:
 * <pre>
 * {
 *   "version": "1.0",
 *   "defaultSchema": "GOV",
 *   "schemas": [{
 *     "name": "GOV", 
 *     "type": "custom",
 *     "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
 *     "operand": {
 *       "dataSource": "sec",
 *       "ciks": ["AAPL", "MSFT"]
 *     }
 *   }]
 * }
 * </pre>
 *
 * <h2>Architecture</h2>
 * <p>The government data adapter uses a modular architecture:
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.govdata.GovDataSchemaFactory} - Main factory
 *       that routes to specialized factories</li>
 *   <li>{@link org.apache.calcite.adapter.govdata.GovDataDriver} - JDBC driver with
 *       unified connection interface</li>
 *   <li>Sub-packages for each data source (e.g., 
 *       {@link org.apache.calcite.adapter.govdata.sec})</li>
 *   <li>Common utilities in {@link org.apache.calcite.adapter.govdata.common}</li>
 * </ul>
 */
package org.apache.calcite.adapter.govdata;