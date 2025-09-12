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

/**
 * Manages token budgets for text enrichment in vectorization.
 * 
 * Provides simple token estimation and smart text trimming to ensure
 * enriched text stays within model token limits while preserving
 * the most important information.
 */
public class SecTokenManager {
  
  // Maximum tokens for most embedding models (e.g., OpenAI ada-002)
  public static final int MAX_TOKENS = 8192;
  
  // Reserve tokens for main text (50% of budget)
  public static final int MAIN_TEXT_RESERVE = 4096;
  
  // Approximate tokens per character (conservative estimate)
  private static final double CHARS_PER_TOKEN = 4.0;
  
  /**
   * Estimate token count for a text string.
   * Uses simple approximation: 1 token ≈ 4 characters.
   */
  public int estimateTokens(String text) {
    if (text == null || text.isEmpty()) {
      return 0;
    }
    // Conservative estimate to avoid exceeding limits
    return (int) Math.ceil(text.length() / CHARS_PER_TOKEN);
  }
  
  /**
   * Get a smart excerpt that tries to preserve complete sentences.
   * 
   * @param text The text to excerpt
   * @param maxTokens Maximum tokens for the excerpt
   * @return Excerpted text that fits within token budget
   */
  public String getSmartExcerpt(String text, int maxTokens) {
    if (text == null || text.isEmpty()) {
      return "";
    }
    
    int currentTokens = estimateTokens(text);
    if (currentTokens <= maxTokens) {
      return text;
    }
    
    // Split into sentences
    String[] sentences = text.split("(?<=[.!?])\\s+");
    StringBuilder excerpt = new StringBuilder();
    int tokensUsed = 0;
    
    for (String sentence : sentences) {
      int sentenceTokens = estimateTokens(sentence);
      if (tokensUsed + sentenceTokens > maxTokens) {
        if (tokensUsed == 0) {
          // First sentence is too long, truncate it
          int maxChars = (int)(maxTokens * CHARS_PER_TOKEN);
          return text.substring(0, Math.min(maxChars - 3, text.length())) + "...";
        }
        break;
      }
      excerpt.append(sentence).append(" ");
      tokensUsed += sentenceTokens;
    }
    
    String result = excerpt.toString().trim();
    if (result.length() < text.length()) {
      result += " [...]";
    }
    return result;
  }
  
  /**
   * Intelligently trim main text if it exceeds the reserve budget.
   * Preserves beginning and end, summarizes middle.
   */
  public String trimMainText(String text, int maxTokens) {
    if (text == null || text.isEmpty()) {
      return "";
    }
    
    int currentTokens = estimateTokens(text);
    if (currentTokens <= maxTokens) {
      return text;
    }
    
    String[] sentences = text.split("(?<=[.!?])\\s+");
    int totalSentences = sentences.length;
    
    if (totalSentences <= 3) {
      // Too few sentences, just truncate
      return getSmartExcerpt(text, maxTokens);
    }
    
    // Keep first 30% and last 20% of sentences
    int keepStart = Math.max(1, (int)(totalSentences * 0.3));
    int keepEnd = Math.max(1, (int)(totalSentences * 0.2));
    
    StringBuilder result = new StringBuilder();
    int tokensUsed = 0;
    int targetTokens = maxTokens - 50; // Reserve space for trimming marker
    
    // Add beginning sentences
    for (int i = 0; i < keepStart && i < sentences.length; i++) {
      String sentence = sentences[i];
      int sentTokens = estimateTokens(sentence);
      if (tokensUsed + sentTokens > targetTokens / 2) {
        break;
      }
      result.append(sentence).append(" ");
      tokensUsed += sentTokens;
    }
    
    // Add trimming marker
    int trimmedSentences = totalSentences - keepStart - keepEnd;
    String marker = String.format("[... %d sentences trimmed ...] ", trimmedSentences);
    result.append(marker);
    tokensUsed += estimateTokens(marker);
    
    // Add ending sentences
    StringBuilder ending = new StringBuilder();
    for (int i = Math.max(keepStart, totalSentences - keepEnd); i < totalSentences; i++) {
      ending.append(sentences[i]).append(" ");
    }
    
    String endingText = ending.toString().trim();
    int endingTokens = estimateTokens(endingText);
    
    if (tokensUsed + endingTokens > maxTokens) {
      // Ending is too long, truncate it
      endingText = getSmartExcerpt(endingText, maxTokens - tokensUsed);
    }
    
    result.append(endingText);
    return result.toString().trim();
  }
  
  /**
   * Allocate token budget across different relationship types.
   */
  public static class TokenAllocation {
    public final int mainTextBudget;
    public final int parentContextBudget;
    public final int referencesBudget;
    public final int metricsBudget;
    public final int totalBudget;
    
    public TokenAllocation() {
      this.totalBudget = MAX_TOKENS;
      this.mainTextBudget = MAIN_TEXT_RESERVE;
      this.parentContextBudget = 500;  // ~10% for parent/hierarchy
      this.referencesBudget = 2500;    // ~30% for references
      this.metricsBudget = 500;        // ~10% for metrics
    }
    
    public TokenAllocation(int total, double mainPercent, double parentPercent,
                           double referencesPercent, double metricsPercent) {
      this.totalBudget = total;
      this.mainTextBudget = (int)(total * mainPercent);
      this.parentContextBudget = (int)(total * parentPercent);
      this.referencesBudget = (int)(total * referencesPercent);
      this.metricsBudget = (int)(total * metricsPercent);
    }
    
    public int getRemainingBudget(int used) {
      return Math.max(0, totalBudget - used);
    }
  }
  
  /**
   * Extract first N sentences from text.
   */
  public String getFirstSentences(String text, int numSentences) {
    if (text == null || text.isEmpty() || numSentences <= 0) {
      return "";
    }
    
    String[] sentences = text.split("(?<=[.!?])\\s+");
    StringBuilder result = new StringBuilder();
    
    for (int i = 0; i < Math.min(numSentences, sentences.length); i++) {
      result.append(sentences[i]).append(" ");
    }
    
    return result.toString().trim();
  }
  
  /**
   * Check if text is likely to need trimming.
   */
  public boolean needsTrimming(String text) {
    return estimateTokens(text) > MAIN_TEXT_RESERVE;
  }
  
  /**
   * Get a summary of token usage.
   */
  public String getTokenUsageSummary(String enrichedText) {
    int tokens = estimateTokens(enrichedText);
    double percentage = (tokens * 100.0) / MAX_TOKENS;
    return String.format("Tokens: %d/%d (%.1f%%)", tokens, MAX_TOKENS, percentage);
  }
}