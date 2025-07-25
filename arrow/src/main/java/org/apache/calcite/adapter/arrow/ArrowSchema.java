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
package org.apache.calcite.adapter.arrow;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.Util;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

class ArrowSchema extends AbstractSchema {
  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowSchema.class);
  private final Supplier<Map<String, Table>> tableMapSupplier;

  ArrowSchema(File baseDirectory) {
    LOGGER.info("ArrowSchema constructor called with baseDirectory: {}", baseDirectory);
    requireNonNull(baseDirectory, "baseDirectory");
    LOGGER.info("baseDirectory is not null");
    this.tableMapSupplier =
        Suppliers.memoize(() -> {
          LOGGER.info("Initializing tableMapSupplier");
          return deduceTableMap(baseDirectory);
        });
    LOGGER.info("ArrowSchema constructor completed");
  }

  private static String trim(String s, String suffix) {
    String trimmed = trimOrNull(s, suffix);
    return trimmed != null ? trimmed : s;
  }

  private static @Nullable String trimOrNull(String s, String suffix) {
    return s.endsWith(suffix)
        ? s.substring(0, s.length() - suffix.length())
        : null;
  }

  @Override protected Map<String, Table> getTableMap() {
    return tableMapSupplier.get();
  }

  private static Map<String, Table> deduceTableMap(File baseDirectory) {
    File[] files = baseDirectory.listFiles((dir, name) ->
        name.endsWith(".arrow") || name.endsWith(".parquet"));
    if (files == null) {
      LOGGER.info("directory " + baseDirectory + " not found");
      return ImmutableMap.of();
    }

    final Map<String, Table> tables = new HashMap<>();
    for (File file : files) {
      final String fileName = file.getName();
      final String tableName = trim(trim(fileName, ".arrow"), ".parquet").toUpperCase(Locale.ROOT);
      final Table table;

      if (fileName.endsWith(".arrow")) {
        table = createArrowTable(file);
      } else if (fileName.endsWith(".parquet")) {
        table = createParquetTable(file);
      } else {
        continue;
      }

      tables.put(tableName, table);
    }

    return ImmutableMap.copyOf(tables);
  }

  private static Table createArrowTable(File file) {
    try {
      final FileInputStream fileInputStream = new FileInputStream(file);
      final SeekableReadChannel seekableReadChannel =
          new SeekableReadChannel(fileInputStream.getChannel());
      final RootAllocator allocator = new RootAllocator();
      final ArrowFileReader arrowFileReader =
          new ArrowFileReader(seekableReadChannel, allocator);
      return new ArrowTable(null, arrowFileReader);
    } catch (FileNotFoundException e) {
      throw Util.toUnchecked(e);
    }
  }


  @SuppressWarnings("deprecation")
  private static Table createParquetTable(File file) {
    try {
      Path path = new Path(file.toString());
      InputFile inputFile = HadoopInputFile.fromPath(path, new Configuration());
      AvroParquetReader.Builder<GenericRecord> builder = AvroParquetReader.<GenericRecord>builder(inputFile);
      return new ParquetTable(file.getAbsolutePath(), builder, null);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return new ParquetTable(file.getAbsolutePath(), null, null); // Pass null for
    // RelProtoDataType
  }
}
