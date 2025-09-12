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

import java.util.*;

/**
 * Simple embedding model for XBRL documents using TF-IDF and financial domain knowledge.
 * This provides real, meaningful embeddings without requiring large ML models.
 */
public class SecEmbeddingModel {

  // Financial domain vocabulary - these are key terms in financial reporting
  private static final Map<String, Integer> VOCAB_INDEX = new HashMap<>();
  private static final String[] FINANCIAL_TERMS = {
    // Revenue & Sales
    "revenue", "sales", "income", "earnings", "gross", "net", "profit", "margin",
    "product", "service", "subscription", "licensing", "growth", "decline", "increase", "decrease",

    // Costs & Expenses
    "cost", "expense", "operating", "administrative", "marketing", "research", "development",
    "depreciation", "amortization", "interest", "tax", "compensation", "benefits",

    // Assets & Liabilities
    "asset", "liability", "equity", "cash", "receivable", "inventory", "property", "equipment",
    "goodwill", "intangible", "debt", "loan", "payable", "accrued", "deferred",

    // Cash Flow
    "operating", "investing", "financing", "flow", "inflow", "outflow", "capital",
    "dividend", "acquisition", "disposal", "proceeds", "payment", "collection",

    // Metrics & Ratios
    "ratio", "return", "roi", "roe", "eps", "pe", "leverage", "liquidity", "solvency",
    "efficiency", "turnover", "days", "cycle", "margin", "yield", "rate",

    // Risk & Compliance
    "risk", "uncertainty", "volatility", "exposure", "hedge", "derivative", "contingency",
    "litigation", "regulatory", "compliance", "audit", "control", "material", "weakness",

    // Market & Competition
    "market", "competition", "competitor", "industry", "sector", "segment", "geographic",
    "customer", "supplier", "partner", "channel", "distribution", "pricing", "demand",

    // Performance Indicators
    "performance", "target", "goal", "objective", "strategy", "initiative", "outlook",
    "guidance", "forecast", "projection", "expectation", "trend", "momentum", "quarter",

    // Financial Statements
    "balance", "sheet", "statement", "comprehensive", "consolidated", "interim", "annual",
    "quarterly", "fiscal", "period", "year", "month", "date", "ended", "beginning",

    // Accounting Terms
    "gaap", "ifrs", "accounting", "principle", "policy", "estimate", "assumption",
    "recognition", "measurement", "disclosure", "presentation", "reclassification",

    // Corporate Actions
    "merger", "acquisition", "divestiture", "restructuring", "reorganization", "spinoff",
    "ipo", "offering", "issuance", "repurchase", "buyback", "split", "conversion",

    // Contextual Terms
    "significant", "material", "substantial", "critical", "key", "major", "primary",
    "continued", "ongoing", "future", "prior", "current", "subsequent", "compared"
  };

  static {
    // Build vocabulary index
    for (int i = 0; i < FINANCIAL_TERMS.length; i++) {
      VOCAB_INDEX.put(FINANCIAL_TERMS[i], i);
    }
  }

  private static final int EMBEDDING_DIM = 128;

  /**
   * Generate embeddings for a text chunk using TF-IDF with financial vocabulary.
   *
   * @param text The text to embed
   * @param context The context (Revenue, Assets, etc.)
   * @return Comma-separated embedding vector
   */
  public static String generateEmbedding(String text, String context) {
    if (text == null || text.isEmpty()) {
      return generateZeroVector();
    }

    // Tokenize and normalize
    String normalizedText = text.toLowerCase()
        .replaceAll("[^a-z0-9\\s]", " ")
        .replaceAll("\\s+", " ");

    String[] tokens = normalizedText.split(" ");

    // Calculate term frequencies
    Map<String, Double> termFreq = new HashMap<>();
    for (String token : tokens) {
      if (VOCAB_INDEX.containsKey(token)) {
        termFreq.merge(token, 1.0, Double::sum);
      }
    }

    // Normalize by document length
    int docLength = tokens.length;
    if (docLength > 0) {
      termFreq.replaceAll((k, v) -> v / docLength);
    }

    // Initialize embedding vector
    double[] embedding = new double[EMBEDDING_DIM];

    // Fill first part with TF-IDF features
    for (Map.Entry<String, Double> entry : termFreq.entrySet()) {
      int idx = VOCAB_INDEX.get(entry.getKey());
      if (idx < EMBEDDING_DIM) {
        // Apply IDF weighting (simplified - in practice would compute from corpus)
        double idf = Math.log(1000.0 / (10.0 + idx)); // Pseudo-IDF based on term position
        embedding[idx] = entry.getValue() * idf;
      }
    }

    // Add context-specific features
    addContextFeatures(embedding, context, text);

    // Add statistical features
    addStatisticalFeatures(embedding, text);

    // Normalize to unit vector
    normalizeVector(embedding);

    // Convert to string
    return vectorToString(embedding);
  }

  /**
   * Add context-specific features to the embedding.
   */
  private static void addContextFeatures(double[] embedding, String context, String text) {
    int offset = 96; // Start context features at position 96

    // Context type encoding (one-hot style)
    if (context.contains("Revenue")) {
      embedding[offset] = 1.0;
    } else if (context.contains("Profitability")) {
      embedding[offset + 1] = 1.0;
    } else if (context.contains("Assets")) {
      embedding[offset + 2] = 1.0;
    } else if (context.contains("Liquidity")) {
      embedding[offset + 3] = 1.0;
    } else if (context.contains("Debt")) {
      embedding[offset + 4] = 1.0;
    }

    // Sentiment indicators (simplified)
    String lowerText = text.toLowerCase();
    double sentiment = 0.0;

    // Positive indicators
    if (lowerText.contains("increase") || lowerText.contains("growth") ||
        lowerText.contains("improve") || lowerText.contains("strong")) {
      sentiment += 0.5;
    }

    // Negative indicators
    if (lowerText.contains("decrease") || lowerText.contains("decline") ||
        lowerText.contains("loss") || lowerText.contains("weak")) {
      sentiment -= 0.5;
    }

    embedding[offset + 5] = sentiment;
  }

  /**
   * Add statistical features about the text.
   */
  private static void addStatisticalFeatures(double[] embedding, String text) {
    int offset = 112; // Start statistical features at position 112

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

    // Complexity (unique terms ratio)
    String[] words = text.toLowerCase().split("\\s+");
    Set<String> uniqueWords = new HashSet<>(Arrays.asList(words));
    double complexity = words.length > 0 ? (double) uniqueWords.size() / words.length : 0;
    embedding[offset + 4] = complexity;
  }

  /**
   * Extract numeric values from text.
   */
  private static List<Double> extractNumbers(String text) {
    List<Double> numbers = new ArrayList<>();

    // Simple pattern for numbers (including millions/billions notation)
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
  private static void normalizeVector(double[] vector) {
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
  private static String vectorToString(double[] vector) {
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
  private static String generateZeroVector() {
    double[] zeros = new double[EMBEDDING_DIM];
    return vectorToString(zeros);
  }
}
