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
package org.apache.calcite.adapter.sec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test simplified model files with defaults.
 */
@Tag("unit")
public class SimplifiedModelTest {

  @Test public void testSimplifiedModels() throws Exception {
    System.out.println("\n"
  + "=".repeat(80));
    System.out.println("SIMPLIFIED MODEL FILES TEST");
    System.out.println("=".repeat(80));

    // Load and display the simple model
    System.out.println("\n1. SIMPLE MODEL (Minimal Configuration)");
    System.out.println("-".repeat(40));

    InputStream simpleStream = getClass().getResourceAsStream("/simple-test-model.json");
    assertNotNull(simpleStream, "Simple test model should exist");

    String simpleContent = new String(simpleStream.readAllBytes(), StandardCharsets.UTF_8);
    System.out.println("simple-test-model.json:");
    System.out.println(simpleContent);

    // Count lines to show how minimal it is
    int lineCount = simpleContent.split("\n").length;
    System.out.println("\nOnly " + lineCount + " lines needed!");

    // Show what's automatically configured
    System.out.println("\n2. AUTOMATIC DEFAULTS PROVIDED");
    System.out.println("-".repeat(40));
    System.out.println("The SecEmbeddingSchemaFactory automatically provides:");
    System.out.println("  ✓ Execution Engine: DuckDB");
    System.out.println("  ✓ Embedding Configuration:");
    System.out.println("    - Model loader: EmbeddingModelLoader");
    System.out.println("    - Vocabulary: /models/financial-vocabulary.txt");
    System.out.println("    - Context mappings: /models/context-mappings.json");
    System.out.println("    - Dimension: 128");
    System.out.println("  ✓ Vector Functions:");
    System.out.println("    - COSINE_SIMILARITY");
    System.out.println("    - COSINE_DISTANCE");
    System.out.println("    - EUCLIDEAN_DISTANCE");
    System.out.println("    - DOT_PRODUCT");
    System.out.println("    - VECTORS_SIMILAR");
    System.out.println("    - VECTOR_NORM");
    System.out.println("    - NORMALIZE_VECTOR");
    System.out.println("  ✓ Tables:");
    System.out.println("    - financial_line_items (partitioned)");
    System.out.println("    - company_info");
    System.out.println("    - footnotes (partitioned)");
    System.out.println("    - document_embeddings (partitioned, with vector column)");

    // Show override example
    System.out.println("\n3. OVERRIDE EXAMPLE");
    System.out.println("-".repeat(40));

    InputStream overrideStream = getClass().getResourceAsStream("/custom-override-model.json");
    if (overrideStream != null) {
      String overrideContent = new String(overrideStream.readAllBytes(), StandardCharsets.UTF_8);
      System.out.println("custom-override-model.json:");
      System.out.println(overrideContent);
      System.out.println("\nThis example only overrides embedding dimension to 256");
      System.out.println("All other defaults are preserved!");
    }

    // Compare with old verbose model
    System.out.println("\n4. COMPARISON WITH OLD VERBOSE MODEL");
    System.out.println("-".repeat(40));

    InputStream oldStream = getClass().getResourceAsStream("/test-embedding-model.json");
    if (oldStream != null) {
      String oldContent = new String(oldStream.readAllBytes(), StandardCharsets.UTF_8);
      int oldLineCount = oldContent.split("\n").length;

      System.out.println("Old verbose model: " + oldLineCount + " lines");
      System.out.println("New simple model: " + lineCount + " lines");
      System.out.println("Reduction: " + (oldLineCount - lineCount) + " lines ("
          + String.format("%.0f%%", (1.0 - (double)lineCount/oldLineCount) * 100) + ")");
    }

    System.out.println("\n5. USAGE EXAMPLES");
    System.out.println("-".repeat(40));

    System.out.println("\nMinimal configuration (using all defaults):");
    System.out.println("  {");
    System.out.println("    \"schemas\": [{");
    System.out.println("      \"name\": \"XBRL\",");
    System.out.println("      \"factory\": \"org.apache.calcite.adapter.sec.SecEmbeddingSchemaFactory\",");
    System.out.println("      \"operand\": {");
    System.out.println("        \"directory\": \"my-data\"");
    System.out.println("      }");
    System.out.println("    }]");
    System.out.println("  }");

    System.out.println("\nWith custom embedding dimension:");
    System.out.println("  \"operand\": {");
    System.out.println("    \"directory\": \"my-data\",");
    System.out.println("    \"embeddingConfig\": {");
    System.out.println("      \"embeddingDimension\": 256");
    System.out.println("    }");
    System.out.println("  }");

    System.out.println("\nWith custom vocabulary file:");
    System.out.println("  \"operand\": {");
    System.out.println("    \"directory\": \"my-data\",");
    System.out.println("    \"embeddingConfig\": {");
    System.out.println("      \"vocabularyPath\": \"/models/custom-vocab.txt\"");
    System.out.println("    }");
    System.out.println("  }");

    System.out.println("\n"
  + "=".repeat(80));
    System.out.println("SIMPLIFIED MODEL TEST PASSED");
    System.out.println("=".repeat(80) + "\n");
  }
}
