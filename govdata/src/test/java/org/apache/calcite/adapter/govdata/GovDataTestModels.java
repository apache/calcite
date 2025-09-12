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
package org.apache.calcite.adapter.govdata;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Utility class for loading test models from resources and customizing them.
 */
public final class GovDataTestModels {
  private GovDataTestModels() {
    // Utility class
  }

  /**
   * Loads a test model from resources and writes it to a temp file.
   *
   * @param modelName Name of the model (without .json extension)
   * @return Path to the temp file containing the model
   */
  public static String loadTestModel(String modelName) throws IOException {
    return loadTestModel(modelName, null);
  }

  /**
   * Loads a test model from resources, optionally customizes it, and writes to temp file.
   *
   * @param modelName Name of the model (without .json extension)
   * @param customizer Optional customizer to modify the model content
   * @return Path to the temp file containing the model
   */
  public static String loadTestModel(String modelName, ModelCustomizer customizer) throws IOException {
    String resourcePath = "/models/" + modelName + ".json";

    try (InputStream is = GovDataTestModels.class.getResourceAsStream(resourcePath)) {
      if (is == null) {
        throw new IOException("Model not found: " + resourcePath);
      }

      String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);

      if (customizer != null) {
        content = customizer.customize(content);
      }

      File tempFile = File.createTempFile("govdata-test-model", ".json");
      tempFile.deleteOnExit();

      try (FileWriter writer = new FileWriter(tempFile, StandardCharsets.UTF_8)) {
        writer.write(content);
      }

      return tempFile.getAbsolutePath();
    }
  }

  /**
   * Creates a test model with custom directory.
   */
  public static String createTestModel(String modelName, String directory) throws IOException {
    return loadTestModel(modelName, content -> content.replace("\"/tmp/sec-schema-test\"", "\"" + directory + "\""));
  }

  /**
   * Creates a test model with custom CIKs.
   */
  public static String createTestModelWithCiks(String modelName, String... ciks) throws IOException {
    StringBuilder cikArray = new StringBuilder("[");
    for (int i = 0; i < ciks.length; i++) {
      if (i > 0) cikArray.append(", ");
      cikArray.append("\"").append(ciks[i]).append("\"");
    }
    cikArray.append("]");

    return loadTestModel(modelName, content ->
        content.replaceAll("\"ciks\"\\s*:\\s*\\[[^\\]]*\\]", "\"ciks\": " + cikArray.toString()));
  }

  /**
   * Creates a test model with a custom year range.
   */
  public static String createTestModelWithYears(String modelName, int startYear, int endYear) throws IOException {
    return loadTestModel(modelName, content -> {
      String result = content;
      if (content.contains("startYear")) {
        result = result.replaceAll("\"startYear\"\\s*:\\s*\\d+", "\"startYear\": " + startYear);
      } else {
        result = result.replace("\"ciks\"", "\"startYear\": " + startYear + ",\n      \"ciks\"");
      }

      if (content.contains("endYear")) {
        result = result.replaceAll("\"endYear\"\\s*:\\s*\\d+", "\"endYear\": " + endYear);
      } else {
        result = result.replace("\"startYear\": " + startYear + ",", "\"startYear\": " + startYear + ",\n      \"endYear\": " + endYear + ",");
      }

      return result;
    });
  }

  /**
   * Functional interface for customizing model content.
   */
  @FunctionalInterface
  public interface ModelCustomizer {
    String customize(String content);
  }
}
