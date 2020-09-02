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

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import org.apache.calcite.util.Sources;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Schema mapped onto a set of Arrow files.
 */
class ArrowSchema extends AbstractSchema {
  private Map<String, Table> tables;
  private final File baseDirectory;

  /**
   * Creates an Arrow schema.
   *
   * @param baseDirectory Base directory to look for relative files, or null
   */
  ArrowSchema(File baseDirectory) {
    this.baseDirectory = baseDirectory;
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
  private static String trimOrNull(String s, String suffix) {
    return s.endsWith(suffix)
        ? s.substring(0, s.length() - suffix.length())
        : null;
  }

  @Override protected Map<String, Table> getTableMap() {

    if (tables == null) {
      tables = new HashMap<>();

      File[] files = baseDirectory.listFiles((dir, name) -> name.endsWith(".arrow"));
      if (files == null) {
        System.out.println("directory " + baseDirectory + " not found");
        files = new File[0];
      }
      for (File file : files) {

          File arrowFile = new File(Sources.of(file).path());
        FileInputStream fileInputStream = null;
        try {
          fileInputStream = new FileInputStream(arrowFile);
        } catch (FileNotFoundException e) {
          e.printStackTrace();
        }
        SeekableReadChannel seekableReadChannel = new SeekableReadChannel(fileInputStream.getChannel());
          RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
          ArrowFileReader arrowFileReader = new ArrowFileReader(seekableReadChannel, allocator);
        try {
          arrowFileReader.initialize();
        } catch (IOException e) {
          e.printStackTrace();
        }

        List<VectorSchemaRoot> list = null;
        try {
          list = arrowFileReader.getRecordBlocks().stream().map(block -> {
            try {
              return arrowFileReader.getVectorSchemaRoot();
            } catch (IOException e) {
              throw new IllegalStateException();
            }
          }).collect(Collectors.toList());
        } catch (IOException e) {
          e.printStackTrace();
        }
        VectorSchemaRoot[] vectorSchemaRoots = new VectorSchemaRoot[list.size()];
        list.toArray(vectorSchemaRoots);
        UInt4Vector intVector = new UInt4Vector("vectorName", allocator);
        tables.put(
            trim(file.getName(), ".arrow").toUpperCase(),
            new ArrowTable(vectorSchemaRoots, intVector, null));
      }
    }
    return tables;
  }
}
