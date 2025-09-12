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

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test real embeddings with TF-IDF based model.
 */
@Tag("unit")
public class RealEmbeddingTest {

  @Test public void testRealEmbeddings() throws Exception {
    System.out.println("\n=== Testing Real Embeddings with TF-IDF Model ===\n");

    // Test different financial text samples
    String revenueText = "Revenue increased 15% to $394 billion in fiscal 2023, driven by strong iPhone sales "
        + "and growth in services revenue. Product revenue was $298B while services reached $85B. "
        + "The company expects continued revenue growth in 2024.";

    String profitText = "Net income declined to $97 billion due to increased operating expenses "
        + "and higher tax rates. Operating margin compressed to 30% from 33% in the prior year. "
        + "Earnings per share was $6.13 on a diluted basis.";

    String debtText = "Total debt increased to $109 billion with the issuance of new bonds. "
        + "Interest expense rose to $3.9 billion. The debt-to-equity ratio remains conservative "
        + "at 1.95. Credit facilities of $10 billion remain undrawn.";

    // Generate embeddings
    String revenueEmbed = SecEmbeddingModel.generateEmbedding(revenueText, "Revenue");
    String profitEmbed = SecEmbeddingModel.generateEmbedding(profitText, "Profitability");
    String debtEmbed = SecEmbeddingModel.generateEmbedding(debtText, "Debt");

    // Verify embeddings are generated
    assertNotNull(revenueEmbed);
    assertNotNull(profitEmbed);
    assertNotNull(debtEmbed);

    // Check embedding dimensions (should be 128)
    assertEquals(128, revenueEmbed.split(",").length);
    assertEquals(128, profitEmbed.split(",").length);
    assertEquals(128, debtEmbed.split(",").length);

    System.out.println("Generated embeddings with 128 dimensions");

    // Test similarity calculations
    double revenueSelfSim = SimilarityFunctions.cosineSimilarity(revenueEmbed, revenueEmbed);
    assertEquals(1.0, revenueSelfSim, 0.001, "Self-similarity should be 1.0");

    double revenueProfitSim = SimilarityFunctions.cosineSimilarity(revenueEmbed, profitEmbed);
    double revenueDebtSim = SimilarityFunctions.cosineSimilarity(revenueEmbed, debtEmbed);
    double profitDebtSim = SimilarityFunctions.cosineSimilarity(profitEmbed, debtEmbed);

    System.out.println("\nCosine Similarities:");
    System.out.println("Revenue-Revenue: " + String.format("%.4f", revenueSelfSim));
    System.out.println("Revenue-Profit:  " + String.format("%.4f", revenueProfitSim));
    System.out.println("Revenue-Debt:    " + String.format("%.4f", revenueDebtSim));
    System.out.println("Profit-Debt:     " + String.format("%.4f", profitDebtSim));

    // Test that similar contexts have higher similarity
    String revenueText2 = "Sales grew by 12% to reach $380 billion, with product sales of $285B "
        + "and service revenue of $82B. Revenue growth expected to continue.";

    String revenueEmbed2 = SecEmbeddingModel.generateEmbedding(revenueText2, "Revenue");
    double revenueRevenueSim = SimilarityFunctions.cosineSimilarity(revenueEmbed, revenueEmbed2);

    System.out.println("\nSimilar Revenue Texts:");
    System.out.println("Revenue1-Revenue2: " + String.format("%.4f", revenueRevenueSim));

    // Similar revenue texts should have higher similarity than revenue-debt
    assertTrue(revenueRevenueSim > revenueDebtSim,
        "Similar revenue texts should have higher similarity than revenue-debt");

    // Test distance functions
    double distance = SimilarityFunctions.cosineDistance(revenueEmbed, profitEmbed);
    assertEquals(1.0 - revenueProfitSim, distance, 0.001, "Distance should be 1 - similarity");

    System.out.println("\nCosine Distance (Revenue-Profit): " + String.format("%.4f", distance));

    // Test threshold-based similarity
    boolean similar = SimilarityFunctions.vectorsSimilar(revenueEmbed, revenueEmbed2, 0.5);
    assertTrue(similar, "Similar revenue texts should pass similarity threshold");

    // Test different contexts produce different embeddings
    String sameTextDiffContext = SecEmbeddingModel.generateEmbedding(revenueText, "Assets");
    double contextDiff = SimilarityFunctions.cosineSimilarity(revenueEmbed, sameTextDiffContext);

    System.out.println("\nContext Impact:");
    System.out.println("Same text, different context: " + String.format("%.4f", contextDiff));
    assertTrue(contextDiff < revenueSelfSim, "Different contexts should produce different embeddings");

    System.out.println("\n=== Real Embedding Test Passed! ===");
  }

  @Test public void testEmbeddingWithParquetIntegration() throws Exception {
    System.out.println("\n=== Testing Parquet Integration with Real Embeddings ===\n");

    // Use the existing test data directory
    File testDataDir = new File("build/apple-edgar-test-data");
    File secDir = new File(testDataDir, "sec");
    File parquetDir = new File(testDataDir, "parquet-embed-test");

    // Clean up test directory
    if (parquetDir.exists()) {
      deleteDirectory(parquetDir);
    }

    // Check if we have XBRL files
    if (!secDir.exists() || secDir.listFiles() == null || secDir.listFiles().length == 0) {
      System.out.println("No XBRL files found, skipping Parquet integration test");
      return;
    }

    // Convert one XBRL file with real embeddings
    File[] secFiles = secDir.listFiles((dir, name) -> name.endsWith(".xml"));
    if (secFiles != null && secFiles.length > 0) {
      File testFile = secFiles[0];
      System.out.println("Processing: " + testFile.getName());

      // Create converter and process
      SecToParquetConverter converter = new SecToParquetConverter(secDir, parquetDir);
      converter.processSecFile(testFile);

      // Check that Parquet files were created
      assertTrue(parquetDir.exists(), "Parquet directory should exist");

      // Look for embeddings table
      File[] parquetFiles = parquetDir.listFiles();
      boolean hasEmbeddings = false;

      if (parquetFiles != null) {
        for (File f : parquetFiles) {
          if (f.getName().contains("document_embeddings") ||
              (f.isDirectory() && f.getName().equals("document_embeddings"))) {
            hasEmbeddings = true;
            System.out.println("Found embeddings: " + f.getAbsolutePath());
          }
        }
      }

      assertTrue(hasEmbeddings, "Should have created document_embeddings table");

      System.out.println("\n=== Parquet Integration Test Passed! ===");
    }
  }

  private void deleteDirectory(File dir) {
    if (dir.exists()) {
      File[] files = dir.listFiles();
      if (files != null) {
        for (File file : files) {
          if (file.isDirectory()) {
            deleteDirectory(file);
          } else {
            file.delete();
          }
        }
      }
      dir.delete();
    }
  }
}
