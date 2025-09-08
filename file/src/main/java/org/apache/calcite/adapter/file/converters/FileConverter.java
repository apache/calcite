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
package org.apache.calcite.adapter.file.converters;

import org.apache.calcite.adapter.file.metadata.ConversionMetadata;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Interface for file format converters.
 * Implementations convert files from one format to another.
 */
public interface FileConverter {

  /**
   * Checks if this converter can handle the conversion.
   *
   * @param sourceFormat The source file format
   * @param targetFormat The target file format
   * @return true if this converter can handle the conversion
   */
  boolean canConvert(String sourceFormat, String targetFormat);

  /**
   * Converts a source file to the target format.
   *
   * @param sourceFile The source file to convert
   * @param targetDirectory The directory to write converted files to
   * @param metadata Optional metadata about the conversion
   * @return List of generated files
   * @throws IOException if conversion fails
   */
  List<File> convert(File sourceFile, File targetDirectory,
      ConversionMetadata metadata) throws IOException;

  /**
   * Gets the source format this converter reads.
   *
   * @return The source format name
   */
  String getSourceFormat();

  /**
   * Gets the target format this converter produces.
   *
   * @return The target format name
   */
  String getTargetFormat();
}
