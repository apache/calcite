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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 * OpenAI embedding provider using the text-embedding-3-small model.
 * 
 * Configuration:
 * - apiKey: OpenAI API key (required)
 * - model: Model name (default: text-embedding-3-small)
 * - dimensions: Embedding dimensions (default: 1536)
 * - timeout: Request timeout in seconds (default: 30)
 */
public class OpenAIEmbeddingProvider implements TextEmbeddingProvider {
  
  private static final String DEFAULT_MODEL = "text-embedding-3-small";
  private static final String OPENAI_API_URL = "https://api.openai.com/v1/embeddings";
  private static final int DEFAULT_DIMENSIONS = 1536;
  private static final int DEFAULT_MAX_INPUT_LENGTH = 8192;
  private static final double COST_PER_1K_TOKENS = 0.00002; // $0.02 per 1M tokens
  
  private final String apiKey;
  private final String model;
  private final int dimensions;
  private final int maxInputLength;
  private final int timeoutSeconds;
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final Executor executor;
  
  public OpenAIEmbeddingProvider(Map<String, Object> config) {
    this.apiKey = getRequiredString(config, "apiKey");
    this.model = (String) config.getOrDefault("model", DEFAULT_MODEL);
    this.dimensions = (Integer) config.getOrDefault("dimensions", DEFAULT_DIMENSIONS);
    this.maxInputLength = (Integer) config.getOrDefault("maxInputLength", DEFAULT_MAX_INPUT_LENGTH);
    this.timeoutSeconds = (Integer) config.getOrDefault("timeout", 30);
    
    this.httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build();
    this.objectMapper = new ObjectMapper();
    this.executor = ForkJoinPool.commonPool();
    
    if (apiKey == null || apiKey.trim().isEmpty()) {
      throw new IllegalArgumentException("OpenAI API key is required");
    }
  }
  
  @Override
  public double[] generateEmbedding(String text) throws EmbeddingException {
    if (text == null || text.trim().isEmpty()) {
      return new double[dimensions];
    }
    
    if (text.length() > maxInputLength) {
      text = text.substring(0, maxInputLength);
    }
    
    try {
      Map<String, Object> requestBody = new HashMap<>();
      requestBody.put("model", model);
      requestBody.put("input", text);
      requestBody.put("dimensions", dimensions);
      
      String jsonBody = objectMapper.writeValueAsString(requestBody);
      
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(OPENAI_API_URL))
          .header("Content-Type", "application/json")
          .header("Authorization", "Bearer " + apiKey)
          .timeout(Duration.ofSeconds(timeoutSeconds))
          .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
          .build();
      
      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      
      if (response.statusCode() != 200) {
        throw new EmbeddingException("OpenAI API error: " + response.statusCode() + " - " + response.body());
      }
      
      JsonNode responseJson = objectMapper.readTree(response.body());
      JsonNode embeddingArray = responseJson.path("data").get(0).path("embedding");
      
      double[] embedding = new double[embeddingArray.size()];
      for (int i = 0; i < embeddingArray.size(); i++) {
        embedding[i] = embeddingArray.get(i).asDouble();
      }
      
      return embedding;
      
    } catch (IOException | InterruptedException e) {
      throw new EmbeddingException("Failed to generate OpenAI embedding", e);
    }
  }
  
  @Override
  public List<double[]> generateEmbeddings(List<String> texts) throws EmbeddingException {
    if (texts == null || texts.isEmpty()) {
      return new ArrayList<>();
    }
    
    // Truncate texts that are too long
    List<String> truncatedTexts = new ArrayList<>();
    for (String text : texts) {
      if (text == null || text.trim().isEmpty()) {
        truncatedTexts.add("");
      } else if (text.length() > maxInputLength) {
        truncatedTexts.add(text.substring(0, maxInputLength));
      } else {
        truncatedTexts.add(text);
      }
    }
    
    try {
      Map<String, Object> requestBody = new HashMap<>();
      requestBody.put("model", model);
      requestBody.put("input", truncatedTexts);
      requestBody.put("dimensions", dimensions);
      
      String jsonBody = objectMapper.writeValueAsString(requestBody);
      
      HttpRequest request = HttpRequest.newBuilder()
          .uri(URI.create(OPENAI_API_URL))
          .header("Content-Type", "application/json")
          .header("Authorization", "Bearer " + apiKey)
          .timeout(Duration.ofSeconds(timeoutSeconds * 2)) // Longer timeout for batch
          .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
          .build();
      
      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      
      if (response.statusCode() != 200) {
        throw new EmbeddingException("OpenAI API error: " + response.statusCode() + " - " + response.body());
      }
      
      JsonNode responseJson = objectMapper.readTree(response.body());
      JsonNode dataArray = responseJson.path("data");
      
      List<double[]> embeddings = new ArrayList<>();
      for (JsonNode item : dataArray) {
        JsonNode embeddingArray = item.path("embedding");
        double[] embedding = new double[embeddingArray.size()];
        for (int i = 0; i < embeddingArray.size(); i++) {
          embedding[i] = embeddingArray.get(i).asDouble();
        }
        embeddings.add(embedding);
      }
      
      return embeddings;
      
    } catch (IOException | InterruptedException e) {
      throw new EmbeddingException("Failed to generate OpenAI embeddings", e);
    }
  }
  
  @Override
  public CompletableFuture<double[]> generateEmbeddingAsync(String text) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        return generateEmbedding(text);
      } catch (EmbeddingException e) {
        throw new RuntimeException(e);
      }
    }, executor);
  }
  
  @Override
  public CompletableFuture<List<double[]>> generateEmbeddingsAsync(List<String> texts) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        return generateEmbeddings(texts);
      } catch (EmbeddingException e) {
        throw new RuntimeException(e);
      }
    }, executor);
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
    return apiKey != null && !apiKey.trim().isEmpty();
  }
  
  @Override
  public String getProviderName() {
    return "OpenAI " + model;
  }
  
  @Override
  public String getModelId() {
    return model;
  }
  
  @Override
  public double getCostPer1000Tokens() {
    return COST_PER_1K_TOKENS;
  }
  
  @Override
  public boolean supportsBatchProcessing() {
    return true;
  }
  
  private String getRequiredString(Map<String, Object> config, String key) {
    Object value = config.get(key);
    if (value == null) {
      // Try environment variable
      String envKey = "OPENAI_" + key.toUpperCase();
      value = System.getenv(envKey);
    }
    return (String) value;
  }
}