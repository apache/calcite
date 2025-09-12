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
package org.apache.calcite.adapter.govdata.sec;

import org.apache.calcite.adapter.file.similarity.SimilarityFunctions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test static model file loading for embeddings.
 */
@Tag("unit")
public class StaticModelTest {

  @Test public void testStaticModelLoading() {
    System.out.println("\n" + "=".repeat(80));
    System.out.println("STATIC MODEL FILE LOADING TEST");

    // Get the singleton instance
    EmbeddingModelLoader loader = EmbeddingModelLoader.getInstance();

    // Verify model is loaded
    System.out.println("\n1. MODEL INFORMATION");
    System.out.println("Embedding dimension: " + loader.getEmbeddingDim());
    System.out.println("Vocabulary size: " + loader.getVocabularySize());

    assertTrue(loader.getEmbeddingDim() > 0, "Embedding dimension should be positive");
    assertTrue(loader.getVocabularySize() > 0, "Vocabulary should not be empty");

    // Show some vocabulary terms
    System.out.println("\nSample vocabulary terms:");
    loader.getVocabularyTerms().stream()
        .limit(10)
        .forEach(term -> System.out.println("  - " + term));

    // Test embedding generation
    System.out.println("\n2. EMBEDDING GENERATION FROM STATIC MODEL");

    String[] testTexts = {
        "Revenue increased 15% to $394 billion",
        "Net income was $97 billion for the year",
        "Total debt stands at $109 billion"
    };

    String[] contexts = {"Revenue", "Profitability", "Debt"};

    for (int i = 0; i < testTexts.length; i++) {
      String embedding = loader.generateEmbedding(testTexts[i], contexts[i]);
      assertNotNull(embedding, "Embedding should not be null");

      String[] dims = embedding.split(",");
      assertEquals(loader.getEmbeddingDim(), dims.length,
          "Embedding should have correct dimensions");

      System.out.printf("[%s] %s\n", contexts[i], testTexts[i]);
      System.out.printf("  Embedding dims: %d\n", dims.length);

      // Show first few dimensions
      System.out.print("  First 5 dims: [");
      for (int j = 0; j < 5 && j < dims.length; j++) {
        System.out.printf("%s%s", dims[j], j < 4 ? ", " : "");
      }
      System.out.println("...]");
    }

    // Test similarity with embeddings from static model
    System.out.println("\n3. SIMILARITY CALCULATIONS");

    String revenue1 =
        loader.generateEmbedding("Revenue grew 12% to $380 billion", "Revenue");
    String revenue2 =
        loader.generateEmbedding("Sales increased to $394 billion", "Revenue");
    String debt =
        loader.generateEmbedding("Debt increased to $109 billion", "Debt");

    double revRevSim = SimilarityFunctions.cosineSimilarity(revenue1, revenue2);
    double revDebtSim = SimilarityFunctions.cosineSimilarity(revenue1, debt);

    System.out.printf("Revenue-Revenue similarity: %.4f\n", revRevSim);
    System.out.printf("Revenue-Debt similarity:    %.4f\n", revDebtSim);

    assertTrue(revRevSim > revDebtSim,
        "Similar revenue texts should have higher similarity than revenue-debt");

    // Test model consistency
    System.out.println("\n4. MODEL CONSISTENCY");

    String embed1 = loader.generateEmbedding("Test text", "Revenue");
    String embed2 = loader.generateEmbedding("Test text", "Revenue");

    assertEquals(embed1, embed2, "Same input should produce same embedding");
    System.out.println("✓ Deterministic embeddings verified");

    // Test singleton pattern
    EmbeddingModelLoader loader2 = EmbeddingModelLoader.getInstance();
    assertSame(loader, loader2, "Should return same instance");
    System.out.println("✓ Singleton pattern verified");

    System.out.println("\n" + "=".repeat(80));
    System.out.println("STATIC MODEL TEST PASSED");
  }

  @Test public void testModelFileUpdate() {
    System.out.println("\n" + "=".repeat(80));
    System.out.println("MODEL FILE UPDATE SIMULATION");

    EmbeddingModelLoader loader = EmbeddingModelLoader.getInstance();

    System.out.println("\nCurrent model configuration:");
    System.out.println("  Model files location: /models/");
    System.out.println("  Vocabulary file: financial-vocabulary.txt");
    System.out.println("  Context mappings: context-mappings.json");

    System.out.println("\nTo update the model:");
    System.out.println("  1. Edit /sec/src/main/resources/models/financial-vocabulary.txt");
    System.out.println("     - Add new terms with weights");
    System.out.println("     - Adjust existing term weights");
    System.out.println("  2. Edit /sec/src/main/resources/models/context-mappings.json");
    System.out.println("     - Add new contexts");
    System.out.println("     - Update sentiment words");
    System.out.println("  3. Rebuild and restart application");

    System.out.println("\nModel versioning:");
    System.out.println("  Current version: 1.0 (from context-mappings.json)");
    System.out.println("  No code changes needed for model updates!");

    System.out.println("\n" + "=".repeat(80) + "\n");
  }
}
