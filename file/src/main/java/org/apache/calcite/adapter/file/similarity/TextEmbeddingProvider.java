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

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for text embedding providers.
 * 
 * Supports multiple embedding backends:
 * - Local ONNX models (sentence-transformers, etc.)
 * - Remote APIs (OpenAI, Cohere, Anthropic, etc.)
 * - Cached providers with persistent storage
 * - Custom domain-specific models
 */
public interface TextEmbeddingProvider {
  
  /**
   * Generate embedding for a single text.
   *
   * @param text The input text to embed
   * @return Array of embedding values
   * @throws EmbeddingException if embedding generation fails
   */
  double[] generateEmbedding(String text) throws EmbeddingException;
  
  /**
   * Generate embeddings for multiple texts (batch processing).
   * More efficient than individual calls for large volumes.
   *
   * @param texts List of input texts to embed
   * @return List of embedding arrays
   * @throws EmbeddingException if batch embedding fails
   */
  List<double[]> generateEmbeddings(List<String> texts) throws EmbeddingException;
  
  /**
   * Generate embeddings asynchronously.
   *
   * @param text The input text to embed
   * @return Future containing the embedding array
   */
  CompletableFuture<double[]> generateEmbeddingAsync(String text);
  
  /**
   * Generate embeddings for multiple texts asynchronously.
   *
   * @param texts List of input texts to embed
   * @return Future containing list of embedding arrays
   */
  CompletableFuture<List<double[]>> generateEmbeddingsAsync(List<String> texts);
  
  /**
   * Get the embedding dimension for this provider.
   *
   * @return Number of dimensions in embeddings (e.g., 384, 768, 1536)
   */
  int getDimensions();
  
  /**
   * Get the maximum input length for this provider.
   *
   * @return Maximum characters/tokens supported per text
   */
  int getMaxInputLength();
  
  /**
   * Check if the provider is available and configured.
   *
   * @return true if provider can generate embeddings
   */
  boolean isAvailable();
  
  /**
   * Get a descriptive name for this embedding provider.
   *
   * @return Provider name (e.g., "OpenAI text-embedding-ada-002", "sentence-transformers/all-MiniLM-L6-v2")
   */
  String getProviderName();
  
  /**
   * Get the model identifier used by this provider.
   *
   * @return Model name or path
   */
  String getModelId();
  
  /**
   * Cleanup resources when provider is no longer needed.
   */
  default void close() {
    // Default implementation does nothing
  }
  
  /**
   * Get estimated cost per 1000 tokens (for API-based providers).
   *
   * @return Cost estimate, or 0.0 for local providers
   */
  default double getCostPer1000Tokens() {
    return 0.0;
  }
  
  /**
   * Whether this provider supports batch processing.
   *
   * @return true if batch operations are more efficient than individual calls
   */
  default boolean supportsBatchProcessing() {
    return true;
  }
}