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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Local embedding provider using TF-IDF and semantic similarity.
 * 
 * This implementation provides deterministic, meaningful embeddings without
 * requiring external APIs or large model files. It's suitable for:
 * - Development and testing
 * - Privacy-sensitive environments
 * - Offline deployments
 * 
 * Features:
 * - TF-IDF based term weighting
 * - Context-aware semantic expansion
 * - Financial domain knowledge
 * - Deterministic output (same text always gets same embedding)
 * - Fast local computation
 */
public class LocalEmbeddingProvider implements TextEmbeddingProvider {
  
  private static final int DEFAULT_DIMENSIONS = 384;
  private static final int DEFAULT_MAX_INPUT_LENGTH = 8192;
  
  // Financial domain vocabulary for semantic expansion
  private static final Map<String, String[]> FINANCIAL_SYNONYMS = new ConcurrentHashMap<>();
  static {
    // Revenue synonyms
    FINANCIAL_SYNONYMS.put("revenue", new String[]{"sales", "income", "turnover", "receipts", "earnings"});
    FINANCIAL_SYNONYMS.put("profit", new String[]{"earnings", "income", "gain", "surplus", "net"});
    FINANCIAL_SYNONYMS.put("loss", new String[]{"deficit", "shortfall", "decline", "decrease", "negative"});
    
    // Asset synonyms  
    FINANCIAL_SYNONYMS.put("assets", new String[]{"resources", "holdings", "property", "capital", "wealth"});
    FINANCIAL_SYNONYMS.put("cash", new String[]{"money", "liquid", "currency", "funds", "capital"});
    FINANCIAL_SYNONYMS.put("inventory", new String[]{"stock", "goods", "products", "merchandise", "supplies"});
    
    // Liability synonyms
    FINANCIAL_SYNONYMS.put("debt", new String[]{"liability", "obligation", "borrowing", "loan", "credit"});
    FINANCIAL_SYNONYMS.put("expenses", new String[]{"costs", "spending", "outgoings", "expenditure", "charges"});
    
    // Market terms
    FINANCIAL_SYNONYMS.put("growth", new String[]{"increase", "expansion", "rise", "improvement", "gain"});
    FINANCIAL_SYNONYMS.put("decline", new String[]{"decrease", "reduction", "fall", "drop", "downturn"});
    FINANCIAL_SYNONYMS.put("volatility", new String[]{"fluctuation", "instability", "variation", "uncertainty"});
    
    // Performance metrics
    FINANCIAL_SYNONYMS.put("margin", new String[]{"spread", "difference", "gap", "ratio", "percentage"});
    FINANCIAL_SYNONYMS.put("ratio", new String[]{"proportion", "percentage", "rate", "measure", "metric"});
    FINANCIAL_SYNONYMS.put("return", new String[]{"yield", "profit", "gain", "performance", "benefit"});
  }
  
  private final int dimensions;
  private final int maxInputLength;
  private final Map<String, double[]> embeddingCache;
  
  public LocalEmbeddingProvider(Map<String, Object> config) {
    this.dimensions = (Integer) config.getOrDefault("dimensions", DEFAULT_DIMENSIONS);
    this.maxInputLength = (Integer) config.getOrDefault("maxInputLength", DEFAULT_MAX_INPUT_LENGTH);
    this.embeddingCache = new ConcurrentHashMap<>();
  }
  
  @Override
  public double[] generateEmbedding(String text) throws EmbeddingException {
    if (text == null || text.trim().isEmpty()) {
      return new double[dimensions];
    }
    
    // Check cache first
    String cacheKey = text.substring(0, Math.min(text.length(), 100)); // Use first 100 chars as key
    if (embeddingCache.containsKey(cacheKey)) {
      return embeddingCache.get(cacheKey);
    }
    
    if (text.length() > maxInputLength) {
      text = text.substring(0, maxInputLength);
    }
    
    double[] embedding = computeEmbedding(text);
    embeddingCache.put(cacheKey, embedding);
    return embedding;
  }
  
  @Override
  public List<double[]> generateEmbeddings(List<String> texts) throws EmbeddingException {
    List<double[]> embeddings = new ArrayList<>();
    for (String text : texts) {
      embeddings.add(generateEmbedding(text));
    }
    return embeddings;
  }
  
  @Override
  public CompletableFuture<double[]> generateEmbeddingAsync(String text) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        return generateEmbedding(text);
      } catch (EmbeddingException e) {
        throw new RuntimeException(e);
      }
    });
  }
  
  @Override
  public CompletableFuture<List<double[]>> generateEmbeddingsAsync(List<String> texts) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        return generateEmbeddings(texts);
      } catch (EmbeddingException e) {
        throw new RuntimeException(e);
      }
    });
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
    return true; // Always available - no external dependencies
  }
  
  @Override
  public String getProviderName() {
    return "Local TF-IDF with Financial Domain Knowledge";
  }
  
  @Override
  public String getModelId() {
    return "local-tfidf-financial";
  }
  
  @Override
  public double getCostPer1000Tokens() {
    return 0.0; // Free local computation
  }
  
  @Override
  public boolean supportsBatchProcessing() {
    return true;
  }
  
  private double[] computeEmbedding(String text) {
    // Normalize text
    String normalized = text.toLowerCase().trim();
    
    // Extract terms and compute TF-IDF-like weights
    Map<String, Double> termWeights = extractTermWeights(normalized);
    
    // Create embedding vector
    double[] embedding = new double[dimensions];
    
    // Use deterministic hash-based approach for consistent embeddings
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      
      int termIndex = 0;
      for (Map.Entry<String, Double> entry : termWeights.entrySet()) {
        String term = entry.getKey();
        double weight = entry.getValue();
        
        // Generate deterministic features for this term
        byte[] termHash = md.digest(term.getBytes());
        Random termRandom = new Random(Arrays.hashCode(termHash));
        
        // Each term contributes to multiple dimensions
        int contributionCount = Math.min(dimensions / 10, 10); // Each term affects up to 10 dimensions
        for (int i = 0; i < contributionCount; i++) {
          int dim = Math.abs(termRandom.nextInt()) % dimensions;
          double contribution = weight * (termRandom.nextGaussian() * 0.1 + 0.5);
          embedding[dim] += contribution;
        }
        
        // Add semantic expansion for financial terms
        String[] synonyms = FINANCIAL_SYNONYMS.get(term);
        if (synonyms != null) {
          for (String synonym : synonyms) {
            byte[] synonymHash = md.digest(synonym.getBytes());
            Random synonymRandom = new Random(Arrays.hashCode(synonymHash));
            
            // Synonyms contribute with reduced weight
            for (int i = 0; i < contributionCount / 2; i++) {
              int dim = Math.abs(synonymRandom.nextInt()) % dimensions;
              double contribution = weight * 0.3 * (synonymRandom.nextGaussian() * 0.1 + 0.5);
              embedding[dim] += contribution;
            }
          }
        }
        
        termIndex++;
      }
      
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 not available", e);
    }
    
    // Normalize the embedding vector
    double norm = 0.0;
    for (double value : embedding) {
      norm += value * value;
    }
    norm = Math.sqrt(norm);
    
    if (norm > 0) {
      for (int i = 0; i < embedding.length; i++) {
        embedding[i] /= norm;
      }
    }
    
    return embedding;
  }
  
  private Map<String, Double> extractTermWeights(String text) {
    Map<String, Double> termCounts = new ConcurrentHashMap<>();
    Map<String, Double> termWeights = new ConcurrentHashMap<>();
    
    // Simple tokenization and term counting
    String[] words = text.split("[\\s\\p{Punct}]+");
    int totalWords = words.length;
    
    // Count term frequencies
    for (String word : words) {
      word = word.toLowerCase().trim();
      if (word.length() > 2) { // Ignore very short words
        termCounts.put(word, termCounts.getOrDefault(word, 0.0) + 1.0);
      }
    }
    
    // Compute TF-IDF-like weights
    for (Map.Entry<String, Double> entry : termCounts.entrySet()) {
      String term = entry.getKey();
      double tf = entry.getValue() / totalWords; // Term frequency
      
      // Simple IDF approximation based on term length and character patterns
      double idf = computeSimpleIDF(term);
      
      // Boost important financial terms
      double boost = 1.0;
      if (FINANCIAL_SYNONYMS.containsKey(term)) {
        boost = 2.0; // Financial terms get double weight
      } else if (term.matches(".*\\d.*")) {
        boost = 1.5; // Terms with numbers (like amounts) get 1.5x weight
      }
      
      double weight = tf * idf * boost;
      termWeights.put(term, weight);
    }
    
    return termWeights;
  }
  
  private double computeSimpleIDF(String term) {
    // Simple IDF approximation based on term characteristics
    double baseIDF = 1.0;
    
    // Shorter terms are usually more common (lower IDF)
    if (term.length() <= 3) {
      baseIDF = 0.5;
    } else if (term.length() >= 8) {
      baseIDF = 2.0; // Longer terms are usually more specific
    }
    
    // Terms with mixed case or numbers are usually more specific
    if (term.matches(".*[A-Z].*") || term.matches(".*\\d.*")) {
      baseIDF *= 1.5;
    }
    
    // Common English stopwords get lower IDF
    if (isStopWord(term)) {
      baseIDF *= 0.1;
    }
    
    return Math.log(baseIDF + 1.0);
  }
  
  private boolean isStopWord(String term) {
    String[] stopWords = {"the", "and", "or", "but", "in", "on", "at", "to", "for", "of", "with", "by", 
                         "from", "up", "about", "into", "through", "during", "before", "after", "above", 
                         "below", "between", "among", "is", "are", "was", "were", "be", "been", "being", 
                         "have", "has", "had", "do", "does", "did", "will", "would", "could", "should"};
    
    for (String stopWord : stopWords) {
      if (term.equals(stopWord)) {
        return true;
      }
    }
    return false;
  }
}