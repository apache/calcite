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

import org.apache.calcite.adapter.file.similarity.EmbeddingException;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for text embedding services.
 * 
 * This abstraction allows plugging in different embedding providers:
 * - OpenAI Embeddings API
 * - Anthropic Claude embeddings
 * - Cohere Embed API
 * - Local models (Sentence-BERT, FinBERT)
 * - Azure OpenAI Service
 * - Google Vertex AI embeddings
 */
public interface EmbeddingService {
  
  /**
   * Generate embeddings for a single text.
   *
   * @param text The input text to embed
   * @return Array of embedding values (typically 128-1536 dimensions)
   * @throws EmbeddingException if embedding generation fails
   */
  double[] generateEmbedding(String text) throws EmbeddingException;
  
  /**
   * Generate embeddings for multiple texts (batch processing).
   * More efficient than individual calls for large volumes.
   *
   * @param texts Array of input texts to embed
   * @return Array of embedding arrays
   * @throws EmbeddingException if batch embedding fails
   */
  double[][] generateEmbeddings(String[] texts) throws EmbeddingException;
  
  /**
   * Generate embeddings asynchronously.
   *
   * @param text The input text to embed
   * @return Future containing the embedding array
   */
  CompletableFuture<double[]> generateEmbeddingAsync(String text);
  
  /**
   * Get the embedding dimension for this service.
   *
   * @return Number of dimensions in embeddings (e.g., 1536 for OpenAI text-embedding-ada-002)
   */
  int getDimensions();
  
  /**
   * Get the maximum input length for this service.
   *
   * @return Maximum characters/tokens supported
   */
  int getMaxInputLength();
  
  /**
   * Check if the service is available and configured.
   *
   * @return true if service can generate embeddings
   */
  boolean isAvailable();
  
  /**
   * Get a descriptive name for this embedding service.
   *
   * @return Service name (e.g., "OpenAI text-embedding-ada-002")
   */
  String getServiceName();
}