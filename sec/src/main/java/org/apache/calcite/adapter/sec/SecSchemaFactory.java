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
package org.apache.calcite.adapter.sec;

import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Factory for SEC schemas that creates FileSchema instances configured
 * for SEC filing to Parquet conversion and partitioned table support.
 *
 * <p>Converts SEC filings (including XBRL instance documents) to partitioned Parquet files
 * with CIK/filing-type/date partitioning for efficient querying of SEC EDGAR data.
 */
public class SecSchemaFactory implements SchemaFactory {
  private static final Logger LOGGER = Logger.getLogger(SecSchemaFactory.class.getName());

  public static final SecSchemaFactory INSTANCE = new SecSchemaFactory();

  @Override public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    // Check for SEC-specific configuration
    Boolean enableSecProcessing = (Boolean) operand.get("enableSecProcessing");
    if (enableSecProcessing == null || !enableSecProcessing) {
      // If SEC processing not enabled, just create regular FileSchema
      return FileSchemaFactory.INSTANCE.create(parentSchema, name, operand);
    }

    // Check for EDGAR source configuration
    Map<String, Object> edgarSource = (Map<String, Object>) operand.get("edgarSource");
    if (edgarSource != null) {
      Boolean autoDownload = (Boolean) edgarSource.get("autoDownload");
      if (autoDownload != null && autoDownload) {
        downloadEdgarFilings(edgarSource, operand);
      }
    }

    // Get SEC source directory
    String secSourceDir = (String) operand.get("secSourceDirectory");
    if (secSourceDir == null) {
      secSourceDir = (String) operand.get("directory");
      if (secSourceDir != null) {
        secSourceDir = new File(secSourceDir, "sec").getAbsolutePath();
      }
    }

    // Process SEC files on initialization if configured
    Boolean processOnInit = (Boolean) operand.get("processSecOnInit");
    if (processOnInit == null && edgarSource != null) {
      processOnInit = true; // Default to true when EDGAR source is configured
    }
    if (processOnInit != null && processOnInit && secSourceDir != null) {
      processSecFiles(new File(secSourceDir), operand);
    }

    // Create FileSchema with the directory containing Parquet files
    return FileSchemaFactory.INSTANCE.create(parentSchema, name, operand);
  }

  private void downloadEdgarFilings(Map<String, Object> edgarSource, Map<String, Object> operand) {
    try {
      LOGGER.info("Downloading EDGAR filings based on declarative configuration");

      String targetDir = (String) operand.get("directory");
      if (targetDir == null) {
        targetDir = System.getProperty("user.dir");
      }
      File secDir = new File(targetDir, "sec");
      secDir.mkdirs();

      // Download from SEC EDGAR
      LOGGER.info("Downloading from SEC EDGAR");
      EdgarDownloader downloader = new EdgarDownloader(edgarSource, secDir);
      List<File> downloaded = downloader.downloadFilings();
      LOGGER.info("Downloaded " + downloaded.size() + " XBRL files from SEC EDGAR");

      // Update operand to point to SEC directory for processing
      operand.put("secSourceDirectory", secDir.getAbsolutePath());

    } catch (Exception e) {
      LOGGER.warning("Failed to download EDGAR filings: " + e.getMessage());
    }
  }

  private void processSecFiles(File secSourceDir, Map<String, Object> operand) {
    try {
      LOGGER.info("Processing SEC files from: " + secSourceDir);

      // Get target directory for Parquet files
      String targetDir = (String) operand.get("directory");
      if (targetDir == null) {
        targetDir = System.getProperty("user.dir");
      }
      File parquetTargetDir = new File(targetDir, "sec_parquet");

      // Create converter and process files
      SecToParquetConverter converter = new SecToParquetConverter(secSourceDir, parquetTargetDir);
      int processed = converter.processAllSecFiles();

      LOGGER.info("Processed " + processed + " SEC files to Parquet");
    } catch (Exception e) {
      LOGGER.warning("Failed to process SEC files: " + e.getMessage());
    }
  }
}
