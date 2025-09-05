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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;

/**
 * Loads embedding models from static files.
 * This allows model updates without code changes.
 */
public class EmbeddingModelLoader {
  private static final Logger LOGGER = Logger.getLogger(EmbeddingModelLoader.class.getName());

  private static EmbeddingModelLoader instance;

  private Map<String, Double> vocabulary;
  private Map<String, Integer> vocabIndex;
  private JsonNode contextMappings;
  private int embeddingDim;
  private int vocabFeatures;
  private int contextFeatures;
  private int statisticalFeatures;

  private EmbeddingModelLoader() {
    loadModels();
  }

  public static synchronized EmbeddingModelLoader getInstance() {
    if (instance == null) {
      instance = new EmbeddingModelLoader();
    }
    return instance;
  }

  /**
   * Load all model files from resources.
   */
  private void loadModels() {
    try {
      // Load vocabulary
      loadVocabulary();

      // Load context mappings and config
      loadContextMappings();

      LOGGER.info("Loaded embedding models: " + vocabulary.size() + " terms, "
          + embeddingDim + " dimensions");

    } catch (Exception e) {
      LOGGER.warning("Failed to load embedding models, using defaults: " + e.getMessage());
      loadDefaults();
    }
  }

  /**
   * Load vocabulary from file.
   */
  private void loadVocabulary() throws IOException {
    vocabulary = new HashMap<>();
    vocabIndex = new HashMap<>();

    try (InputStream is = getClass().getResourceAsStream("/models/financial-vocabulary.txt");
         BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {

      String line;
      int index = 0;

      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.isEmpty() || line.startsWith("#")) {
          continue;
        }

        String[] parts = line.split("\\|");
        if (parts.length == 2) {
          String term = parts[0].trim();
          double weight = Double.parseDouble(parts[1].trim());
          vocabulary.put(term, weight);
          vocabIndex.put(term, index++);
        }
      }
    }
  }

  /**
   * Load context mappings and configuration.
   */
  private void loadContextMappings() throws IOException {
    try (InputStream is = getClass().getResourceAsStream("/models/context-mappings.json")) {
      ObjectMapper mapper = new ObjectMapper();
      contextMappings = mapper.readTree(is);

      JsonNode config = contextMappings.get("modelConfig");
      embeddingDim = config.get("embeddingDim").asInt(128);
      vocabFeatures = config.get("vocabFeatures").asInt(96);
      contextFeatures = config.get("contextFeatures").asInt(16);
      statisticalFeatures = config.get("statisticalFeatures").asInt(16);
    }
  }

  /**
   * Load default values if files are not found.
   */
  private void loadDefaults() {
    vocabulary = new HashMap<>();
    vocabIndex = new HashMap<>();

    // Add some basic terms
    String[] defaultTerms = {
        "revenue", "sales", "income", "profit", "cost", "expense",
        "asset", "liability", "equity", "cash", "debt", "interest"
    };

    for (int i = 0; i < defaultTerms.length; i++) {
      vocabulary.put(defaultTerms[i], 2.0);
      vocabIndex.put(defaultTerms[i], i);
    }

    embeddingDim = 128;
    vocabFeatures = 96;
    contextFeatures = 16;
    statisticalFeatures = 16;
  }

  /**
   * Generate embedding for text using loaded models.
   */
  public String generateEmbedding(String text, String context) {
    if (text == null || text.isEmpty()) {
      return generateZeroVector();
    }

    // Tokenize and normalize
    String normalizedText = text.toLowerCase()
        .replaceAll("[^a-z0-9\\s]", " ")
        .replaceAll("\\s+", " ");

    String[] tokens = normalizedText.split(" ");

    // Initialize embedding vector
    double[] embedding = new double[embeddingDim];

    // Calculate term frequencies with IDF weights
    Map<String, Double> termFreq = new HashMap<>();
    for (String token : tokens) {
      if (vocabulary.containsKey(token)) {
        termFreq.merge(token, 1.0, Double::sum);
      }
    }

    // Normalize by document length and apply IDF
    int docLength = tokens.length;
    if (docLength > 0) {
      for (Map.Entry<String, Double> entry : termFreq.entrySet()) {
        String term = entry.getKey();
        double tf = entry.getValue() / docLength;
        double idf = vocabulary.get(term);

        Integer idx = vocabIndex.get(term);
        if (idx != null && idx < vocabFeatures) {
          embedding[idx] = tf * idf;
        }
      }
    }

    // Add context features
    addContextFeatures(embedding, context);

    // Add statistical features
    addStatisticalFeatures(embedding, text);

    // Normalize to unit vector
    normalizeVector(embedding);

    // Convert to string
    return vectorToString(embedding);
  }

  /**
   * Add context-specific features to embedding.
   */
  private void addContextFeatures(double[] embedding, String context) {
    if (contextMappings == null) return;

    int offset = vocabFeatures;
    JsonNode contexts = contextMappings.get("contexts");

    if (contexts != null && contexts.has(context)) {
      JsonNode contextInfo = contexts.get(context);
      int index = contextInfo.get("index").asInt();
      if (index < contextFeatures) {
        embedding[offset + index] = 1.0;
      }
    }
  }

  /**
   * Add statistical features about the text.
   */
  private void addStatisticalFeatures(double[] embedding, String text) {
    int offset = vocabFeatures + contextFeatures;

    // Extract numeric values
    List<Double> numbers = extractNumbers(text);

    if (!numbers.isEmpty()) {
      // Average magnitude of numbers (log scale)
      double avgMagnitude = numbers.stream()
          .mapToDouble(n -> Math.log10(Math.abs(n) + 1))
          .average()
          .orElse(0.0);
      embedding[offset] = avgMagnitude / 10.0; // Normalize

      // Number count (normalized)
      embedding[offset + 1] = Math.min(numbers.size() / 10.0, 1.0);

      // Presence of large numbers (millions/billions)
      boolean hasLargeNumbers = numbers.stream().anyMatch(n -> Math.abs(n) > 1_000_000);
      embedding[offset + 2] = hasLargeNumbers ? 1.0 : 0.0;
    }

    // Text length feature
    embedding[offset + 3] = Math.min(text.length() / 1000.0, 1.0);

    // Sentiment score
    embedding[offset + 4] = calculateSentiment(text);
  }

  /**
   * Calculate sentiment score from text.
   */
  private double calculateSentiment(String text) {
    if (contextMappings == null) return 0.0;

    String lowerText = text.toLowerCase();
    double sentiment = 0.0;

    JsonNode sentimentWords = contextMappings.get("sentimentWords");
    if (sentimentWords != null) {
      // Check positive words
      JsonNode positive = sentimentWords.get("positive");
      if (positive != null) {
        for (JsonNode word : positive) {
          if (lowerText.contains(word.asText())) {
            sentiment += 0.1;
          }
        }
      }

      // Check negative words
      JsonNode negative = sentimentWords.get("negative");
      if (negative != null) {
        for (JsonNode word : negative) {
          if (lowerText.contains(word.asText())) {
            sentiment -= 0.1;
          }
        }
      }
    }

    return Math.max(-1.0, Math.min(1.0, sentiment));
  }

  /**
   * Extract numeric values from text.
   */
  private List<Double> extractNumbers(String text) {
    List<Double> numbers = new ArrayList<>();

    String[] tokens = text.split("\\s+");
    for (String token : tokens) {
      try {
        // Remove common suffixes and convert
        String cleaned = token.replaceAll("[,$%)]", "");
        if (cleaned.endsWith("M")) {
          numbers.add(Double.parseDouble(cleaned.replace("M", "")) * 1_000_000);
        } else if (cleaned.endsWith("B")) {
          numbers.add(Double.parseDouble(cleaned.replace("B", "")) * 1_000_000_000);
        } else if (cleaned.matches("-?\\d+(\\.\\d+)?")) {
          numbers.add(Double.parseDouble(cleaned));
        }
      } catch (NumberFormatException e) {
        // Skip non-numeric tokens
      }
    }

    return numbers;
  }

  /**
   * Normalize vector to unit length.
   */
  private void normalizeVector(double[] vector) {
    double norm = 0.0;
    for (double v : vector) {
      norm += v * v;
    }

    if (norm > 0) {
      norm = Math.sqrt(norm);
      for (int i = 0; i < vector.length; i++) {
        vector[i] /= norm;
      }
    }
  }

  /**
   * Convert vector to comma-separated string.
   */
  private String vectorToString(double[] vector) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < vector.length; i++) {
      if (i > 0) sb.append(",");
      sb.append(String.format("%.6f", vector[i]));
    }
    return sb.toString();
  }

  /**
   * Generate a zero vector.
   */
  private String generateZeroVector() {
    double[] zeros = new double[embeddingDim];
    return vectorToString(zeros);
  }

  // Getters for model information
  public int getEmbeddingDim() { return embeddingDim; }
  public int getVocabularySize() { return vocabulary.size(); }
  public Set<String> getVocabularyTerms() { return vocabulary.keySet(); }
}
