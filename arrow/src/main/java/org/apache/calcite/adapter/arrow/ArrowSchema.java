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
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.Util;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;

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

/**
 * Schema mapped onto a set of Arrow files.
 */
class ArrowSchema extends AbstractSchema {

  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowSchema.class);
  private final Supplier<Map<String, Table>> tableMapSupplier;

  /**
   * Creates an Arrow schema.
   *
   * @param baseDirectory Base directory to look for relative files
   */
  ArrowSchema(File baseDirectory) {
    requireNonNull(baseDirectory, "baseDirectory");
    this.tableMapSupplier =
        Suppliers.memoize(() -> deduceTableMap(baseDirectory));
  }

  /**
   * Looks for a suffix on a string and returns
   * either the string with the suffix removed
   * or the original string.
   */
  private static String trim(String s, String suffix) {
    String trimmed = trimOrNull(s, suffix);
    return trimmed != null ? trimmed : s;
  }

  /**
   * Looks for a suffix on a string and returns
   * either the string with the suffix removed
   * or null.
   */
  private static @Nullable String trimOrNull(String s, String suffix) {
    return s.endsWith(suffix)
        ? s.substring(0, s.length() - suffix.length())
        : null;
  }

  @Override protected Map<String, Table> getTableMap() {
    return tableMapSupplier.get();
  }

  private static Map<String, Table> deduceTableMap(File baseDirectory) {
    File[] files = baseDirectory.listFiles((dir, name) -> name.endsWith(".arrow"));
    if (files == null) {
      LOGGER.info("directory " + baseDirectory + " not found");
      return ImmutableMap.of();
    }

    final Map<String, Table> tables = new HashMap<>();
    for (File file : files) {
      final File arrowFile = new File(Sources.of(file).path());
      final FileInputStream fileInputStream;
      try {
        fileInputStream = new FileInputStream(arrowFile);
      } catch (FileNotFoundException e) {
        throw Util.toUnchecked(e);
      }
      final SeekableReadChannel seekableReadChannel =
          new SeekableReadChannel(fileInputStream.getChannel());
      final RootAllocator allocator = new RootAllocator();
      final ArrowFileReader arrowFileReader =
          new ArrowFileReader(seekableReadChannel, allocator);
      final String tableName =
          trim(file.getName(), ".arrow").toUpperCase(Locale.ROOT);
      final ArrowTable table =
          new ArrowTable(null, arrowFileReader);
      tables.put(tableName, table);
    }

    return ImmutableMap.copyOf(tables);
  }
}
