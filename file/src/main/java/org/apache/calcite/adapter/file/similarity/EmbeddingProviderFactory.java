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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for creating and managing TextEmbeddingProvider instances.
 * 
 * Supported providers:
 * - "openai": OpenAI embedding API (requires API key)
 * - "local": Local TF-IDF with financial domain knowledge
 * - "placeholder": Placeholder implementation (for testing)
 */
public class EmbeddingProviderFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddingProviderFactory.class);
  
  private static final Map<String, TextEmbeddingProvider> PROVIDER_CACHE = new ConcurrentHashMap<>();
  
  /**
   * Create or retrieve a cached embedding provider.
   * 
   * @param providerType Provider type ("openai", "local", "placeholder")
   * @param config Configuration map
   * @return TextEmbeddingProvider instance
   * @throws EmbeddingException if provider creation fails
   */
  public static TextEmbeddingProvider createProvider(String providerType, Map<String, Object> config) 
      throws EmbeddingException {
    
    if (providerType == null || providerType.trim().isEmpty()) {
      providerType = "local"; // Default to local provider
    }
    
    // Create cache key based on provider type and key config parameters
    String cacheKey = createCacheKey(providerType, config);
    
    // Check cache first
    TextEmbeddingProvider cached = PROVIDER_CACHE.get(cacheKey);
    if (cached != null && cached.isAvailable()) {
      return cached;
    }
    
    // Create new provider
    TextEmbeddingProvider provider = createNewProvider(providerType, config);
    
    // Cache the provider
    PROVIDER_CACHE.put(cacheKey, provider);
    
    LOGGER.info("Created embedding provider: {} with {} dimensions", 
                provider.getProviderName(), provider.getDimensions());
    
    return provider;
  }
  
  /**
   * Create a new provider instance without caching.
   */
  private static TextEmbeddingProvider createNewProvider(String providerType, Map<String, Object> config) 
      throws EmbeddingException {
    
    Map<String, Object> safeConfig = config != null ? config : new HashMap<>();
    
    switch (providerType.toLowerCase()) {
      case "openai":
        return createOpenAIProvider(safeConfig);
        
      case "local":
        return new LocalEmbeddingProvider(safeConfig);
        
      case "placeholder":
        return new PlaceholderEmbeddingProvider(safeConfig);
        
      default:
        throw new EmbeddingException("Unknown embedding provider type: " + providerType + 
                                     ". Supported types: openai, local, placeholder");
    }
  }
  
  private static TextEmbeddingProvider createOpenAIProvider(Map<String, Object> config) throws EmbeddingException {
    try {
      return new OpenAIEmbeddingProvider(config);
    } catch (Exception e) {
      // Fall back to local provider if OpenAI fails
      LOGGER.warn("Failed to create OpenAI provider: {}. Falling back to local provider.", e.getMessage());
      return new LocalEmbeddingProvider(config);
    }
  }
  
  private static String createCacheKey(String providerType, Map<String, Object> config) {
    StringBuilder key = new StringBuilder(providerType);
    
    // Add relevant config parameters to cache key
    if (config != null) {
      Object dimensions = config.get("dimensions");
      if (dimensions != null) {
        key.append("_dim").append(dimensions);
      }
      
      Object model = config.get("model");
      if (model != null) {
        key.append("_model").append(model);
      }
    }
    
    return key.toString();
  }
  
  /**
   * Get configuration from textSimilarity operand with defaults.
   */
  public static Map<String, Object> extractEmbeddingConfig(Map<String, Object> textSimilarityConfig) {
    Map<String, Object> config = new HashMap<>();
    
    if (textSimilarityConfig == null) {
      return config;
    }
    
    // Standard configuration parameters
    config.put("dimensions", textSimilarityConfig.getOrDefault("embeddingDimension", 384));
    config.put("maxInputLength", textSimilarityConfig.getOrDefault("maxTextLength", 8192));
    
    // Provider-specific parameters
    Object apiKey = textSimilarityConfig.get("apiKey");
    if (apiKey != null) {
      config.put("apiKey", apiKey);
    }
    
    Object model = textSimilarityConfig.get("model");
    if (model != null) {
      config.put("model", model);
    }
    
    Object timeout = textSimilarityConfig.get("timeout");
    if (timeout != null) {
      config.put("timeout", timeout);
    }
    
    return config;
  }
  
  /**
   * Clear provider cache (useful for testing or configuration changes).
   */
  public static void clearCache() {
    PROVIDER_CACHE.clear();
    LOGGER.debug("Cleared embedding provider cache");
  }
  
  /**
   * Get cache statistics for monitoring.
   */
  public static Map<String, Object> getCacheStats() {
    Map<String, Object> stats = new HashMap<>();
    stats.put("cacheSize", PROVIDER_CACHE.size());
    stats.put("cachedProviders", PROVIDER_CACHE.keySet());
    return stats;
  }
  
  /**
   * Placeholder provider for backward compatibility and testing.
   */
  private static class PlaceholderEmbeddingProvider implements TextEmbeddingProvider {
    private final int dimensions;
    private final int maxInputLength;
    
    PlaceholderEmbeddingProvider(Map<String, Object> config) {
      this.dimensions = (Integer) config.getOrDefault("dimensions", 128);
      this.maxInputLength = (Integer) config.getOrDefault("maxInputLength", 8192);
    }
    
    @Override
    public double[] generateEmbedding(String text) {
      if (text == null || text.isEmpty()) {
        return new double[dimensions];
      }
      
      // Generate deterministic but meaningless embedding based on text hash
      int hash = text.hashCode();
      java.util.Random random = new java.util.Random(hash);
      double[] embedding = new double[dimensions];
      
      for (int i = 0; i < dimensions; i++) {
        embedding[i] = random.nextGaussian();
      }
      
      return embedding;
    }
    
    @Override
    public java.util.List<double[]> generateEmbeddings(java.util.List<String> texts) {
      java.util.List<double[]> embeddings = new java.util.ArrayList<>();
      for (String text : texts) {
        embeddings.add(generateEmbedding(text));
      }
      return embeddings;
    }
    
    @Override
    public java.util.concurrent.CompletableFuture<double[]> generateEmbeddingAsync(String text) {
      return java.util.concurrent.CompletableFuture.completedFuture(generateEmbedding(text));
    }
    
    @Override
    public java.util.concurrent.CompletableFuture<java.util.List<double[]>> generateEmbeddingsAsync(java.util.List<String> texts) {
      return java.util.concurrent.CompletableFuture.completedFuture(generateEmbeddings(texts));
    }
    
    @Override
    public int getDimensions() {
      return dimensions;
    }
    
    @Override
    public int getMaxInputLength() {
      return maxInputLength;
    }
    
    @Override
    public boolean isAvailable() {
      return true;
    }
    
    @Override
    public String getProviderName() {
      return "Placeholder (hash-based)";
    }
    
    @Override
    public String getModelId() {
      return "placeholder";
    }
  }
}