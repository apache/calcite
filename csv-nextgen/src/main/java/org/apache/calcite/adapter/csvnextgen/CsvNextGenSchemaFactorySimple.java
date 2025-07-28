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
package org.apache.calcite.adapter.csvnextgen;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.io.File;
import java.util.Map;

/**
 * Simplified factory for creating CsvNextGen schemas.
 * Supports configuration of execution engine and other parameters.
 */
public class CsvNextGenSchemaFactorySimple implements SchemaFactory {

  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {

    // Extract configuration parameters
    String directory = (String) operand.get("directory");
    String engineType = (String) operand.getOrDefault("engine", "linq4j");
    Integer batchSize = (Integer) operand.getOrDefault("batchSize", 1024);
    Boolean header = (Boolean) operand.getOrDefault("header", true);

    if (directory == null) {
      throw new IllegalArgumentException("directory parameter is required");
    }

    File baseDirectory = new File(directory);
    if (!baseDirectory.exists() || !baseDirectory.isDirectory()) {
      throw new IllegalArgumentException("Directory does not exist: " + directory);
    }

    return new CsvNextGenSchemaSimple(baseDirectory, engineType,
        batchSize, header);
  }
}
