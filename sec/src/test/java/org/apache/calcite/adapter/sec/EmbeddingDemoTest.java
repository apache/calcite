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

import org.apache.calcite.adapter.file.similarity.SimilarityFunctions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Demonstration of real embeddings with detailed output.
 */
@Tag("unit")
public class EmbeddingDemoTest {

  @Test public void demonstrateEmbeddings() {
    System.out.println("\n" + "=".repeat(80));
    System.out.println("XBRL FINANCIAL TEXT EMBEDDING DEMONSTRATION");

    // Sample financial texts
    String[] texts = {
        "Revenue increased 15% to $394 billion driven by strong iPhone sales",
        "Sales grew 12% reaching $380 billion with robust product demand",
        "Net income declined to $97 billion due to higher operating expenses",
        "Total debt increased to $109 billion following bond issuance",
        "Cash and cash equivalents reached $62 billion at year end"
    };

    String[] contexts = {
        "Revenue", "Revenue", "Profitability", "Debt", "Liquidity"
    };

    // Generate embeddings
    System.out.println("\n1. GENERATING EMBEDDINGS");
    String[] embeddings = new String[texts.length];

    for (int i = 0; i < texts.length; i++) {
      embeddings[i] = SecEmbeddingModel.generateEmbedding(texts[i], contexts[i]);

      // Show first 10 dimensions of each embedding
      String[] dims = embeddings[i].split(",");
      System.out.printf("\n[%s] %s\n", contexts[i], texts[i]);
      System.out.print("   Vector (first 10 dims): [");
      for (int j = 0; j < 10 && j < dims.length; j++) {
        System.out.printf("%s%s", dims[j], j < 9 ? ", " : "");
      }
      System.out.println("...]");
      System.out.println("   Full dimension: " + dims.length);
    }

    // Calculate similarity matrix
    System.out.println("\n2. SIMILARITY MATRIX (Cosine Similarity)");
    System.out.println("        Rev1    Rev2    Prof    Debt    Liq");

    for (int i = 0; i < embeddings.length; i++) {
      System.out.printf("%-7s ", contexts[i] + (i < 2 ? (i+1) : ""));
      for (int j = 0; j < embeddings.length; j++) {
        double sim = SimilarityFunctions.cosineSimilarity(embeddings[i], embeddings[j]);
        System.out.printf("%.3f   ", sim);
      }
    }

    // Demonstrate SQL-like queries
    System.out.println("\n3. SQL QUERY EXAMPLES");

    // Find similar documents to Revenue text 1
    System.out.println("\nQuery: Find documents similar to 'Revenue increased 15%...'");
    System.out.println("SQL: SELECT * FROM documents WHERE COSINE_SIMILARITY(embedding, <vector>) > 0.8");
    System.out.println("\nResults:");

    for (int i = 0; i < texts.length; i++) {
      double sim = SimilarityFunctions.cosineSimilarity(embeddings[0], embeddings[i]);
      if (sim > 0.8) {
        System.out.printf("  ✓ [%.3f] %s\n", sim, texts[i]);
      }
    }

    // Distance-based search (mimics <-> operator)
    System.out.println("\nQuery: Find nearest neighbors using cosine distance");
    System.out.println("SQL: SELECT * FROM documents ORDER BY COSINE_DISTANCE(embedding, <vector>) LIMIT 3");
    System.out.println("\nResults:");

    // Create array of distances and indices
    class DocDistance {
      int index;
      double distance;
      DocDistance(int i, double d) { index = i; distance = d; }
    }

    DocDistance[] distances = new DocDistance[texts.length];
    for (int i = 0; i < texts.length; i++) {
      distances[i] =
          new DocDistance(i, SimilarityFunctions.cosineDistance(embeddings[0], embeddings[i]));
    }

    // Sort by distance
    java.util.Arrays.sort(distances, (a, b) -> Double.compare(a.distance, b.distance));

    // Show top 3
    for (int i = 0; i < 3 && i < distances.length; i++) {
      int idx = distances[i].index;
      System.out.printf("  %d. [dist: %.3f] %s\n", i+1, distances[i].distance, texts[idx]);
    }

    // Threshold-based similarity
    System.out.println("\nQuery: Check if vectors are similar (threshold = 0.9)");
    System.out.println("SQL: SELECT VECTORS_SIMILAR(embedding1, embedding2, 0.9)");

    boolean similar = SimilarityFunctions.vectorsSimilar(embeddings[0], embeddings[1], 0.9);
    System.out.printf("\nRevenue1 vs Revenue2: %s (similarity: %.3f)\n",
        similar ? "SIMILAR" : "NOT SIMILAR",
        SimilarityFunctions.cosineSimilarity(embeddings[0], embeddings[1]));

    // Show how context affects embeddings
    System.out.println("\n4. CONTEXT IMPACT DEMONSTRATION");

    String sameText = "Revenue increased 15% to $394 billion driven by strong iPhone sales";
    String embed1 = SecEmbeddingModel.generateEmbedding(sameText, "Revenue");
    String embed2 = SecEmbeddingModel.generateEmbedding(sameText, "Assets");
    String embed3 = SecEmbeddingModel.generateEmbedding(sameText, "Debt");

    System.out.println("Same text with different contexts:");
    System.out.printf("  Revenue context vs Assets context:  %.3f\n",
        SimilarityFunctions.cosineSimilarity(embed1, embed2));
    System.out.printf("  Revenue context vs Debt context:    %.3f\n",
        SimilarityFunctions.cosineSimilarity(embed1, embed3));
    System.out.printf("  Assets context vs Debt context:     %.3f\n",
        SimilarityFunctions.cosineSimilarity(embed2, embed3));

    // Financial metrics extraction
    System.out.println("\n5. FINANCIAL METRICS DETECTION");

    String[] testTexts = {
        "Revenue grew to $394.3B with gross margin of 44.1%",
        "Operating expenses were $54.8 billion in fiscal 2023",
        "The company holds 231,000 employees worldwide",
        "Quarterly dividend increased to $0.24 per share"
    };

    System.out.println("Text analysis with embeddings:");
    for (String text : testTexts) {
      String embed = SecEmbeddingModel.generateEmbedding(text, "Revenue");

      // Check which financial domain it's most similar to
      String bestContext = "";
      double bestSim = 0;

      for (String ctx : new String[]{"Revenue", "Profitability", "Liquidity", "Debt", "Assets"}) {
        String ctxEmbed = SecEmbeddingModel.generateEmbedding(text, ctx);
        double sim = SimilarityFunctions.cosineSimilarity(embed, ctxEmbed);
        if (sim > bestSim) {
          bestSim = sim;
          bestContext = ctx;
        }
      }

      System.out.printf("\n  Text: \"%s\"\n", text);
      System.out.printf("  Best matching context: %s (similarity: %.3f)\n", bestContext, bestSim);
    }

    System.out.println("\n" + "=".repeat(80));
    System.out.println("DEMONSTRATION COMPLETE");
  }
}
