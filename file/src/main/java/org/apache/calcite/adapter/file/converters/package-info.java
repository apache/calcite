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
 * File format converters for transforming complex file types to JSON.
 *
 * <p>This package contains converters that transform various complex file formats
 * into JSON format, which can then be queried as relational tables through the
 * file adapter. These converters handle the extraction and transformation of
 * structured data from office documents, web content, and other formats.</p>
 *
 * <h2>Converter Components</h2>
 *
 * <h3>Excel Converters</h3>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.converters.ExcelToJsonConverter} - Basic Excel to JSON conversion</li>
 *   <li>{@link org.apache.calcite.adapter.file.converters.MultiTableExcelToJsonConverter} - Extracts multiple tables from Excel sheets</li>
 *   <li>{@link org.apache.calcite.adapter.file.converters.SafeExcelToJsonConverter} - Robust Excel conversion with error handling</li>
 * </ul>
 *
 * <h3>Document Scanners</h3>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.converters.HtmlTableScanner} - Extracts tables from HTML documents</li>
 *   <li>{@link org.apache.calcite.adapter.file.converters.DocxTableScanner} - Extracts tables from Word documents</li>
 *   <li>{@link org.apache.calcite.adapter.file.converters.PptxTableScanner} - Extracts tables from PowerPoint presentations</li>
 *   <li>{@link org.apache.calcite.adapter.file.converters.MarkdownTableScanner} - Extracts tables from Markdown files</li>
 * </ul>
 *
 * <h3>Web Content</h3>
 * <ul>
 *   <li>{@link org.apache.calcite.adapter.file.converters.HtmlToJsonConverter} - Converts HTML content to JSON format</li>
 * </ul>
 *
 * <h2>Conversion Process</h2>
 * <ol>
 *   <li>File is read and parsed using format-specific libraries (Apache POI, JSoup, etc.)</li>
 *   <li>Structured data (tables, lists) is identified and extracted</li>
 *   <li>Data is transformed into JSON format with appropriate schema</li>
 *   <li>JSON output can be queried through the file adapter's JSON table implementation</li>
 * </ol>
 *
 * <h2>Excel Conversion Features</h2>
 * <ul>
 *   <li>Support for both .xls and .xlsx formats</li>
 *   <li>Formula evaluation and cell type detection</li>
 *   <li>Date and time value handling</li>
 *   <li>Multi-sheet processing</li>
 *   <li>Header row detection and column naming</li>
 *   <li>Empty row and column filtering</li>
 * </ul>
 *
 * <h2>Document Table Extraction</h2>
 * <p>The table scanners can extract structured data from various document formats:</p>
 * <ul>
 *   <li><b>HTML</b> - Extracts &lt;table&gt; elements with proper row/column structure</li>
 *   <li><b>Word</b> - Extracts tables from .docx files using OOXML parsing</li>
 *   <li><b>PowerPoint</b> - Extracts tables from slides in .pptx presentations</li>
 *   <li><b>Markdown</b> - Parses pipe-delimited table syntax</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * // Convert Excel file to JSON
 * File excelFile = new File("data.xlsx");
 * File jsonFile = new File("data.json");
 * ExcelToJsonConverter.convert(excelFile, jsonFile);
 * 
 * // Extract multiple tables from Excel
 * MultiTableExcelToJsonConverter converter = new MultiTableExcelToJsonConverter();
 * Map<String, JsonNode> tables = converter.extractTables(excelFile);
 * 
 * // Extract tables from HTML
 * HtmlTableScanner scanner = new HtmlTableScanner();
 * List<Table> tables = scanner.scanDocument(htmlFile);
 * }</pre>
 *
 * <h2>Error Handling</h2>
 * <p>The converters provide different levels of error handling:</p>
 * <ul>
 *   <li>Basic converters may throw exceptions on invalid input</li>
 *   <li>Safe converters (e.g., SafeExcelToJsonConverter) handle errors gracefully</li>
 *   <li>Multi-table converters skip invalid tables and continue processing</li>
 * </ul>
 *
 * <h2>Performance Considerations</h2>
 * <ul>
 *   <li>Large Excel files are processed in streaming mode when possible</li>
 *   <li>Document scanners use DOM parsing for reliability</li>
 *   <li>JSON output is generated incrementally to manage memory</li>
 *   <li>Temporary files may be created for very large conversions</li>
 * </ul>
 *
 * <h2>Supported Formats Summary</h2>
 * <table>
 *   <tr>
 *     <th>Format</th>
 *     <th>Converter</th>
 *     <th>Output</th>
 *   </tr>
 *   <tr>
 *     <td>Excel (.xlsx, .xls)</td>
 *     <td>ExcelToJsonConverter</td>
 *     <td>JSON array of row objects</td>
 *   </tr>
 *   <tr>
 *     <td>HTML</td>
 *     <td>HtmlTableScanner</td>
 *     <td>JSON table structures</td>
 *   </tr>
 *   <tr>
 *     <td>Word (.docx)</td>
 *     <td>DocxTableScanner</td>
 *     <td>JSON table data</td>
 *   </tr>
 *   <tr>
 *     <td>PowerPoint (.pptx)</td>
 *     <td>PptxTableScanner</td>
 *     <td>JSON slide tables</td>
 *   </tr>
 *   <tr>
 *     <td>Markdown</td>
 *     <td>MarkdownTableScanner</td>
 *     <td>JSON table representation</td>
 *   </tr>
 * </table>
 */
package org.apache.calcite.adapter.file.converters;