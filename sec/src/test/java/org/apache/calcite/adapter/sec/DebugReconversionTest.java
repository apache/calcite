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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Debug test to verify SEC adapter regenerates missing Parquet files.
 */
@Tag("unit")
public class DebugReconversionTest {

  @Test void testReconversionLogic() throws Exception {
    // First verify the files we expect
    File xbrlFile = new File("/Volumes/T9/calcite-sec-cache/sec-data/0000320193/000032019323000106/aapl-20230930_htm.xml");
    System.out.println("\n=== DEBUG RECONVERSION TEST ===");
    System.out.println("XBRL file exists: " + xbrlFile.exists());
    System.out.println("XBRL file size: " + xbrlFile.length());

    File parquetDir = new File("/Volumes/T9/calcite-sec-cache/sec-parquet/cik=0000320193/filing_type=10K/year=2023");
    System.out.println("Parquet dir exists: " + parquetDir.exists());

    File factsFile = new File(parquetDir, "0000320193_2023-09-30_facts.parquet");
    System.out.println("Facts file exists: " + factsFile.exists());
    if (factsFile.exists()) {
      System.out.println("Facts file size: " + factsFile.length());
    }

    // Now directly test the conversion
    if (!factsFile.exists() && xbrlFile.exists()) {
      System.out.println("\nDirect conversion test:");
      XbrlToParquetConverter converter = new XbrlToParquetConverter();
      java.util.List<File> outputs =
          converter.convert(xbrlFile, new File("/Volumes/T9/calcite-sec-cache/sec-parquet"), null);
      System.out.println("Created " + outputs.size() + " files");
      for (File f : outputs) {
        System.out.println("  " + f.getName() + " (" + f.length() + " bytes)");
      }
    }

    // Now check if SEC adapter would detect this
    System.out.println("\n=== SIMULATING SEC ADAPTER CHECK ===");
    String cik = "0000320193";
    String form = "10-K";
    String filingDate = "2023-09-30";
    String year = String.valueOf(java.time.LocalDate.parse(filingDate).getYear());

    File secParquetDir = new File("/Volumes/T9/calcite-sec-cache/sec-parquet");
    File cikParquetDir = new File(secParquetDir, "cik=" + cik);
    File filingTypeDir = new File(cikParquetDir, "filing_type=" + form.replace("-", ""));
    File yearDir = new File(filingTypeDir, "year=" + year);
    File checkFile = new File(yearDir, String.format("%s_%s_facts.parquet", cik, filingDate));

    System.out.println("SEC adapter would check: " + checkFile.getAbsolutePath());
    System.out.println("File exists: " + checkFile.exists());
    if (checkFile.exists()) {
      System.out.println("File size: " + checkFile.length());
    }

    boolean needParquetReprocessing = !checkFile.exists() || checkFile.length() == 0;
    System.out.println("needParquetReprocessing: " + needParquetReprocessing);

    if (needParquetReprocessing && xbrlFile.exists()) {
      System.out.println("SEC adapter SHOULD schedule reconversion for: " + xbrlFile.getName());
    }
  }
}
