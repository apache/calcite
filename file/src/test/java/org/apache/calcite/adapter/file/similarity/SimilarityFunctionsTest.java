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
package org.apache.calcite.adapter.file.similarity;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for SimilarityFunctions with real embedding providers.
 */
@Tag("unit")
public class SimilarityFunctionsTest {

  @Test
  void testCosineSimilarity() {
    String vector1 = "0.5,0.5,0.707";
    String vector2 = "0.707,0.707,0.0";
    
    double similarity = SimilarityFunctions.cosineSimilarity(vector1, vector2);
    
    assertTrue(similarity > 0.0 && similarity < 1.0);
    assertEquals(0.707, similarity, 0.01);
  }

  @Test
  void testTextSimilarityWithLocalProvider() {
    // Test that text similarity uses real embedding provider
    String text1 = "Revenue increased by 10% in the fourth quarter";
    String text2 = "Sales grew 10% in Q4 due to strong demand";
    
    double similarity = SimilarityFunctions.textSimilarity(text1, text2);
    
    // Should be greater than 0 since both texts are about revenue/sales growth
    assertTrue(similarity > 0.0, "Similarity should be positive for related financial texts");
    assertTrue(similarity <= 1.0, "Similarity should not exceed 1.0");
  }

  @Test
  void testTextSimilarityDifferentTexts() {
    String text1 = "The company reported strong revenue growth";
    String text2 = "It was raining cats and dogs";
    
    double similarity = SimilarityFunctions.textSimilarity(text1, text2);
    
    // Should be low similarity for unrelated texts
    assertTrue(similarity >= 0.0, "Similarity should be non-negative");
    assertTrue(similarity < 0.5, "Similarity should be low for unrelated texts");
  }

  @Test
  void testEmbedFunction() {
    String text = "Net income increased due to higher margins";
    
    String embedding = SimilarityFunctions.embed(text);
    
    assertNotNull(embedding);
    assertFalse(embedding.isEmpty());
    
    // Should be comma-separated numbers
    String[] parts = embedding.split(",");
    assertTrue(parts.length > 100, "Embedding should have reasonable dimensions");
    
    // Each part should be a valid number
    for (String part : parts) {
      assertDoesNotThrow(() -> Double.parseDouble(part.trim()));
    }
  }

  @Test
  void testVectorNorm() {
    String vector = "3.0,4.0,0.0";
    
    double norm = SimilarityFunctions.vectorNorm(vector);
    
    assertEquals(5.0, norm, 0.001);
  }

  @Test
  void testNormalizeVector() {
    String vector = "3.0,4.0,0.0";
    
    String normalized = SimilarityFunctions.normalizeVector(vector);
    
    assertNotNull(normalized);
    String[] parts = normalized.split(",");
    assertEquals(3, parts.length);
    
    // Normalized vector should have unit length
    double norm = SimilarityFunctions.vectorNorm(normalized);
    assertEquals(1.0, norm, 0.001);
  }
}