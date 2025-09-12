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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generates mock SEC data for testing.
 * Creates Parquet files in the SEC adapter's expected partition structure.
 */
public class MockSecDataGenerator {

  private static final String AVRO_SCHEMA = "{"
      + "\"type\": \"record\","
      + "\"name\": \"SecFiling\","
      + "\"fields\": ["
      + "{\"name\": \"cik\", \"type\": \"string\"},"
      + "{\"name\": \"company_name\", \"type\": \"string\"},"
      + "{\"name\": \"filing_type\", \"type\": \"string\"},"
      + "{\"name\": \"filing_date\", \"type\": \"string\"},"
      + "{\"name\": \"fiscal_year\", \"type\": \"int\"},"
      + "{\"name\": \"fiscal_period\", \"type\": \"string\"},"
      + "{\"name\": \"accession\", \"type\": \"string\"},"
      + "{\"name\": \"revenue\", \"type\": [\"null\", \"double\"], \"default\": null},"
      + "{\"name\": \"net_income\", \"type\": [\"null\", \"double\"], \"default\": null},"
      + "{\"name\": \"total_assets\", \"type\": [\"null\", \"double\"], \"default\": null},"
      + "{\"name\": \"total_liabilities\", \"type\": [\"null\", \"double\"], \"default\": null}"
      + "]"
      + "}";

  private final File baseDirectory;
  private final Random random = new Random(12345); // Fixed seed for reproducible tests

  public MockSecDataGenerator(File baseDirectory) {
    this.baseDirectory = baseDirectory;
  }

  /**
   * Generate mock SEC filing data.
   */
  public void generateMockData(List<String> ciks, List<String> filingTypes,
                               int startYear, int endYear) throws IOException {
    Schema schema = new Schema.Parser().parse(AVRO_SCHEMA);

    // Sample company names
    String[] companies = {
      "Apple Inc.", "Microsoft Corporation", "Google LLC",
      "Amazon.com Inc.", "Meta Platforms Inc.", "Tesla Inc.",
      "NVIDIA Corporation", "Berkshire Hathaway Inc.", "Johnson & Johnson",
      "JPMorgan Chase & Co."
    };

    for (int i = 0; i < ciks.size(); i++) {
      String cik = String.format("%010d", Long.parseLong(ciks.get(i).replaceAll("[^0-9]", "")));
      String companyName = companies[i % companies.length];

      for (String filingType : filingTypes) {
        for (int year = startYear; year <= endYear; year++) {
          // Generate quarterly filings for 10-Q, annual for 10-K
          int filingCount = filingType.equals("10-K") ? 1 : 4;

          for (int quarter = 1; quarter <= filingCount; quarter++) {
            // Create partition directory
            File partitionDir =
                new File(
                    baseDirectory, String.format("cik=%s/filing_type=%s/year=%d",
                    cik, filingType.replace("-", ""), year));
            partitionDir.mkdirs();

            // Create Parquet file
            String fileName =
                String.format("%s_%s_%d_Q%d.parquet", cik, filingType.replace("-", ""), year, quarter);
            File parquetFile = new File(partitionDir, fileName);

            // Write data
            List<GenericRecord> records =
                createMockRecords(schema, cik, companyName, filingType, year, quarter);
            writeParquetFile(parquetFile, schema, records);
          }
        }
      }
    }
  }

  private List<GenericRecord> createMockRecords(Schema schema, String cik,
                                                String companyName, String filingType,
                                                int year, int quarter) {
    List<GenericRecord> records = new ArrayList<>();

    GenericRecord record = new GenericData.Record(schema);
    record.put("cik", cik);
    record.put("company_name", companyName);
    record.put("filing_type", filingType);

    // Generate filing date
    LocalDate filingDate = LocalDate.of(year, quarter * 3, 15);
    record.put("filing_date", filingDate.format(DateTimeFormatter.ISO_DATE));

    record.put("fiscal_year", year);
    record.put("fiscal_period", "Q" + quarter);
    record.put(
        "accession", String.format("%s-%d-%06d",
        cik, year, random.nextInt(999999)));

    // Generate financial metrics (in millions)
    if (filingType.equals("10-K") || filingType.equals("10-Q")) {
      record.put("revenue", 50000.0 + random.nextDouble() * 100000.0);
      record.put("net_income", 5000.0 + random.nextDouble() * 20000.0);
      record.put("total_assets", 100000.0 + random.nextDouble() * 300000.0);
      record.put("total_liabilities", 50000.0 + random.nextDouble() * 150000.0);
    }

    records.add(record);
    return records;
  }

  private void writeParquetFile(File file, Schema schema, List<GenericRecord> records)
      throws IOException {
    Path path = new Path(file.getAbsolutePath());
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(
            new org.apache.parquet.io.OutputFile() {
              @Override public org.apache.parquet.io.PositionOutputStream create(long blockSizeHint) throws IOException {
                return new org.apache.parquet.io.PositionOutputStream() {
                  private final java.io.OutputStream out = new java.io.FileOutputStream(file);
                  private long pos = 0;

                  @Override public long getPos() throws IOException {
                    return pos;
                  }

                  @Override public void write(int b) throws IOException {
                    out.write(b);
                    pos++;
                  }

                  @Override public void write(byte[] b, int off, int len) throws IOException {
                    out.write(b, off, len);
                    pos += len;
                  }

                  @Override public void close() throws IOException {
                    out.close();
                  }
                };
              }

              @Override public org.apache.parquet.io.PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
                return create(blockSizeHint);
              }

              @Override public boolean supportsBlockSize() {
                return false;
              }

              @Override public long defaultBlockSize() {
                return 0;
              }
            })
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {

      for (GenericRecord record : records) {
        writer.write(record);
      }
    }
  }

}
