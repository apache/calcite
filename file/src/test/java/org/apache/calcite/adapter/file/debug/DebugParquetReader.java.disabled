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
package org.apache.calcite.adapter.file.debug;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import org.junit.jupiter.api.Test;

import java.io.File;

/**
 * Debug utility to read Parquet files directly and inspect stored values.
 */
public class DebugParquetReader {
    @Test public void inspectParquetFile() throws Exception {
        String parquetFile = "build/resources/test/bug/.parquet_cache/DATE.parquet";

        File file = new File(parquetFile);
        if (!file.exists()) {
            System.out.println("Parquet file not found: " + file.getAbsolutePath());
            return;
        }

        System.out.println("Reading Parquet file: " + file.getAbsolutePath());

        Path path = new Path(file.getAbsolutePath());
        Configuration conf = new Configuration();

        @SuppressWarnings("deprecation")
        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(path)
                .withConf(conf)
                .build();

        try {

            GenericRecord record;
            int rowCount = 0;
            while ((record = reader.read()) != null && rowCount < 10) {
                rowCount++;
                System.out.println("Row " + rowCount + ":");

                // Print all fields
                for (org.apache.avro.Schema.Field field : record.getSchema().getFields()) {
                    String fieldName = field.name();
                    Object value = record.get(fieldName);
                    System.out.println("  " + fieldName + " = " + value + " (" +
                        (value != null ? value.getClass().getSimpleName() : "null") + ")");

                    // If it's our timestamp field, convert to readable format
                    if ("JOINTIMES".equals(fieldName) && value instanceof Long) {
                        long millis = (Long) value;
                        java.sql.Timestamp ts = new java.sql.Timestamp(millis);
                        System.out.println("    -> As Timestamp: " + ts);
                        System.out.println("    -> Millis: " + millis);

                        // Check if this matches our expected values
                        if (millis == 839044862000L) {
                            System.out.println("    -> MATCH: This is the CORRECT local time value (what CSV parsing produced)!");
                        } else if (millis == 839073662000L) {
                            System.out.println("    -> MATCH: This is the WRONG offset value (8 hours later - what test sees)!");
                        } else {
                            System.out.println("    -> Different value than both expected values");
                            System.out.println("    -> Expected (correct): 839044862000");
                            System.out.println("    -> Expected (wrong):   839073662000");
                        }
                    }
                }
                System.out.println();
            }

            System.out.println("Total rows read: " + rowCount);
        } finally {
            reader.close();
        }
    }
}
