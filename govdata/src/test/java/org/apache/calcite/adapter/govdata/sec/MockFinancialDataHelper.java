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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper to generate mock financial_line_items data for testing.
 */
public class MockFinancialDataHelper {

  private static final String FINANCIAL_SCHEMA = "{"
      + "\"type\": \"record\","
      + "\"name\": \"FinancialLineItem\","
      + "\"fields\": ["
      + "{\"name\": \"cik\", \"type\": \"string\"},"
      + "{\"name\": \"company_name\", \"type\": \"string\"},"
      + "{\"name\": \"filing_type\", \"type\": \"string\"},"
      + "{\"name\": \"filing_date\", \"type\": \"string\"},"
      + "{\"name\": \"fiscal_year\", \"type\": \"int\"},"
      + "{\"name\": \"fiscal_period\", \"type\": \"string\"},"
      + "{\"name\": \"line_item\", \"type\": \"string\"},"
      + "{\"name\": \"value\", \"type\": \"double\"},"
      + "{\"name\": \"unit\", \"type\": \"string\"}"
      + "]"
      + "}";

  /**
   * Create a mock financial_line_items.parquet file for testing.
   */
  public static void createMockFinancialData(File directory) throws IOException {
    directory.mkdirs();

    Schema schema = new Schema.Parser().parse(FINANCIAL_SCHEMA);
    File parquetFile = new File(directory, "financial_line_items.parquet");

    List<GenericRecord> records = new ArrayList<>();

    // Add sample data
    String[] companies = {"Apple Inc.", "Microsoft Corporation"};
    String[] ciks = {"0000320193", "0000789019"};
    String[] lineItems = {"Revenue", "NetIncome", "TotalAssets", "TotalLiabilities"};
    double[] values = {100000000.0, 25000000.0, 500000000.0, 200000000.0};

    for (int i = 0; i < companies.length; i++) {
      for (int j = 0; j < lineItems.length; j++) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("cik", ciks[i]);
        record.put("company_name", companies[i]);
        record.put("filing_type", "10-K");
        record.put("filing_date", "2024-01-31");
        record.put("fiscal_year", 2024);
        record.put("fiscal_period", "FY");
        record.put("line_item", lineItems[j]);
        record.put("value", values[j] * (i + 1));
        record.put("unit", "USD");
        records.add(record);
      }
    }

    // Write Parquet file
    @SuppressWarnings("deprecation")
    ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(new Path(parquetFile.getPath()))
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build();

    try {
      for (GenericRecord record : records) {
        writer.write(record);
      }
    } finally {
      writer.close();
    }
  }

  /**
   * Create a mock dow30_constituents.parquet file for testing.
   */
  public static void createMockDow30Data(File directory) throws IOException {
    directory.mkdirs();

    String dow30Schema = "{"
        + "\"type\": \"record\","
        + "\"name\": \"Dow30Constituent\","
        + "\"fields\": ["
        + "{\"name\": \"ticker\", \"type\": \"string\"},"
        + "{\"name\": \"company_name\", \"type\": \"string\"},"
        + "{\"name\": \"cik\", \"type\": \"string\"}"
        + "]"
        + "}";

    Schema schema = new Schema.Parser().parse(dow30Schema);
    File parquetFile = new File(directory, "dow30_constituents.parquet");

    List<GenericRecord> records = new ArrayList<>();

    // Add DOW 30 companies (abbreviated list for testing)
    String[][] dow30 = {
      {"AAPL", "Apple Inc.", "0000320193"},
      {"MSFT", "Microsoft Corporation", "0000789019"},
      {"AMZN", "Amazon.com Inc.", "0001018724"},
      {"GOOGL", "Alphabet Inc.", "0001652044"},
      {"JPM", "JPMorgan Chase & Co.", "0000019617"}
    };

    for (String[] company : dow30) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("ticker", company[0]);
      record.put("company_name", company[1]);
      record.put("cik", company[2]);
      records.add(record);
    }

    // Write Parquet file
    @SuppressWarnings("deprecation")
    ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(new Path(parquetFile.getPath()))
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build();

    try {
      for (GenericRecord record : records) {
        writer.write(record);
      }
    } finally {
      writer.close();
    }
  }
}
