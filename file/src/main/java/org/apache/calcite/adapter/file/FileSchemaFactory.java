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
package org.apache.calcite.adapter.file;

import org.apache.calcite.model.ModelHandler;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Factory that creates a {@link FileSchema}.
 *
 * <p>Allows a custom schema to be included in a model.json file.
 * See <a href="http://calcite.apache.org/docs/file_adapter.html">File adapter</a>.
 */
@SuppressWarnings("UnusedDeclaration")
public class FileSchemaFactory implements SchemaFactory {
  // public constructor, per factory contract
  public FileSchemaFactory() {
  }

  public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    @SuppressWarnings("unchecked") List<Map<String, Object>> tables =
        (List) operand.get("tables");
    final File baseDirectory =
        (File) operand.get(ModelHandler.ExtraOperand.BASE_DIRECTORY.camelName);
    File directoryFile = baseDirectory;
    final String directory = (String) operand.get("directory");
    if (baseDirectory != null && directory != null) {
      directoryFile = new File(directory);
      if (!directoryFile.isAbsolute()) {
        directoryFile = new File(baseDirectory, directory);
      }
    }
    return new FileSchema(parentSchema, name, directoryFile, tables);
  }
}

// End FileSchemaFactory.java
