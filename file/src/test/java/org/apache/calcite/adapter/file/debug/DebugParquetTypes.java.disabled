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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.File;

public class DebugParquetTypes {
  public static void main(String[] args) throws Exception {
    File parquetFile = new File("/Users/kennethstott/ndc-calcite/calcite-rs-jni/calcite/file/build/resources/test/bug/.parquet_cache/DATE.parquet");

    if (!parquetFile.exists()) {
      System.out.println("Parquet file does not exist: " + parquetFile.getAbsolutePath());
      return;
    }

    Path hadoopPath = new Path(parquetFile.getAbsolutePath());
    Configuration conf = new Configuration();

    @SuppressWarnings("deprecation")
    ParquetMetadata metadata = ParquetFileReader.readFooter(conf, hadoopPath);
    MessageType messageType = metadata.getFileMetaData().getSchema();

    System.out.println("Parquet Schema:");
    System.out.println(messageType);
    System.out.println("\nField Details:");

    for (Type field : messageType.getFields()) {
      System.out.println("\nField: " + field.getName());
      System.out.println("  Primitive Type: " + field.asPrimitiveType().getPrimitiveTypeName());

      LogicalTypeAnnotation logicalType = field.getLogicalTypeAnnotation();
      if (logicalType != null) {
        System.out.println("  Logical Type: " + logicalType);
        System.out.println("  Logical Type Class: " + logicalType.getClass().getName());
      } else {
        System.out.println("  No logical type annotation");
      }
    }
  }
}
