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

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Converts XBRL documents to tagged text for vectorization.
 *
 * Creates contextual chunks that preserve semantic relationships between:
 * - Financial line items and their explanatory text
 * - Risk factors and their related metrics
 * - Footnotes and the items they reference
 *
 * Example output:
 * [CONTEXT: Revenue] [METRIC] Revenue [VALUE] 394B [PERIOD] FY2023
 * [MD&A] Revenue decreased 3% due to...
 * [FOOTNOTE] We recognize revenue when...
 * [RISK] Currency fluctuations may impact revenue...
 */
public class SecTextVectorizer {

  // Concept groups for contextual chunking
  private static final Map<String, List<String>> CONCEPT_GROUPS = new HashMap<>();
  static {
    // Revenue-related concepts
    CONCEPT_GROUPS.put(
        "Revenue", Arrays.asList(
        "Revenue", "RevenueFromContractWithCustomerExcludingAssessedTax",
        "ProductRevenue", "ServiceRevenue", "NetRevenue", "GrossProfit"));

    // Profitability concepts
    CONCEPT_GROUPS.put(
        "Profitability", Arrays.asList(
        "NetIncomeLoss", "OperatingIncomeLoss", "GrossProfit",
        "EarningsPerShareBasic", "EarningsPerShareDiluted"));

    // Liquidity concepts
    CONCEPT_GROUPS.put(
        "Liquidity", Arrays.asList(
        "CashAndCashEquivalentsAtCarryingValue", "WorkingCapital",
        "OperatingCashFlow", "FreeCashFlow", "CurrentRatio"));

    // Debt & Obligations
    CONCEPT_GROUPS.put(
        "Debt", Arrays.asList(
        "LongTermDebt", "ShortTermBorrowings", "DebtCurrent",
        "InterestExpense", "DebtToEquityRatio"));

    // Assets
    CONCEPT_GROUPS.put(
        "Assets", Arrays.asList(
        "Assets", "CurrentAssets", "PropertyPlantAndEquipmentNet",
        "IntangibleAssetsNetExcludingGoodwill", "Goodwill"));
  }

  /**
   * Create contextual chunks from XBRL document.
   * Each chunk contains a financial concept and ALL its related narrative.
   */
  public List<ContextualChunk> createContextualChunks(Document sec, File secFile) {
    List<ContextualChunk> chunks = new ArrayList<>();

    // Extract all financial facts
    Map<String, FinancialFact> facts = extractFinancialFacts(sec);

    // Extract narrative sections
    Map<String, String> narratives = extractNarrativeSections(sec);

    // Extract footnotes
    Map<String, String> footnotes = extractFootnotes(sec);

    // Create contextual chunks by concept group
    for (Map.Entry<String, List<String>> group : CONCEPT_GROUPS.entrySet()) {
      String context = group.getKey();
      StringBuilder chunkText = new StringBuilder();

      // Tag the context
      chunkText.append("[CONTEXT: ").append(context).append("] ");
      chunkText.append("[FILING: ").append(extractFilingInfo(secFile)).append("] ");

      // Add all metrics in this group
      boolean hasMetrics = false;
      for (String concept : group.getValue()) {
        if (facts.containsKey(concept)) {
          FinancialFact fact = facts.get(concept);
          chunkText.append("[METRIC] ").append(concept)
                   .append(" [VALUE] ").append(formatValue(fact.value))
                   .append(" [PERIOD] ").append(fact.period)
                   .append(" ");
          hasMetrics = true;
        }
      }

      if (!hasMetrics) continue; // Skip if no metrics found

      // Add related MD&A
      String mdaText = findRelatedText(narratives.get("MD&A"), group.getValue());
      if (mdaText != null) {
        chunkText.append("[MD&A] ").append(mdaText).append(" ");
      }

      // Add related risk factors
      String riskText = findRelatedText(narratives.get("RISKS"), group.getValue());
      if (riskText != null) {
        chunkText.append("[RISK] ").append(riskText).append(" ");
      }

      // Add related footnotes
      for (String concept : group.getValue()) {
        String footnote = findRelatedFootnote(footnotes, concept);
        if (footnote != null) {
          chunkText.append("[FOOTNOTE] ").append(footnote).append(" ");
        }
      }

      chunks.add(new ContextualChunk(context, chunkText.toString()));
    }

    // Also create a full document embedding
    String fullDoc = createFullDocumentText(facts, narratives, footnotes, secFile);
    chunks.add(new ContextualChunk("FULL_DOCUMENT", fullDoc));

    return chunks;
  }

  /**
   * Create full document text with all tags preserved.
   */
  private String createFullDocumentText(Map<String, FinancialFact> facts,
                                        Map<String, String> narratives,
                                        Map<String, String> footnotes,
                                        File secFile) {
    StringBuilder text = new StringBuilder();

    // Header with filing metadata
    text.append("[DOCUMENT_START] ");
    text.append("[FILING_INFO: ").append(extractFilingInfo(secFile)).append("] ");

    // All financial metrics with tags
    text.append("[FINANCIAL_SECTION_START] ");
    for (Map.Entry<String, FinancialFact> entry : facts.entrySet()) {
      FinancialFact fact = entry.getValue();
      text.append("[METRIC:").append(entry.getKey()).append("] ")
          .append(formatValue(fact.value))
          .append(" [PERIOD:").append(fact.period).append("] ");
    }
    text.append("[FINANCIAL_SECTION_END] ");

    // All narratives with section tags
    for (Map.Entry<String, String> entry : narratives.entrySet()) {
      text.append("[").append(entry.getKey()).append("_START] ")
          .append(entry.getValue())
          .append(" [").append(entry.getKey()).append("_END] ");
    }

    // All footnotes
    text.append("[FOOTNOTES_START] ");
    for (Map.Entry<String, String> entry : footnotes.entrySet()) {
      text.append("[FOOTNOTE:").append(entry.getKey()).append("] ")
          .append(entry.getValue()).append(" ");
    }
    text.append("[FOOTNOTES_END] ");

    text.append("[DOCUMENT_END]");

    return text.toString();
  }

  private Map<String, FinancialFact> extractFinancialFacts(Document doc) {
    Map<String, FinancialFact> facts = new HashMap<>();
    NodeList elements = doc.getElementsByTagName("*");

    for (int i = 0; i < elements.getLength(); i++) {
      Element elem = (Element) elements.item(i);

      // Check if this is a numeric fact (has unitRef attribute)
      if (elem.hasAttribute("unitRef")) {
        String concept = elem.getLocalName();
        if (concept == null) concept = elem.getTagName();

        try {
          double value = Double.parseDouble(elem.getTextContent());
          String period = extractPeriod(doc, elem.getAttribute("contextRef"));
          facts.put(concept, new FinancialFact(concept, value, period));
        } catch (NumberFormatException e) {
          // Skip non-numeric values
        }
      }
    }

    return facts;
  }

  private Map<String, String> extractNarrativeSections(Document doc) {
    Map<String, String> sections = new HashMap<>();

    // In real implementation, would extract from HTML exhibits
    // For now, create placeholder narratives
    sections.put("MD&A", extractTextByTag(doc, "ManagementDiscussionAndAnalysis"));
    sections.put("RISKS", extractTextByTag(doc, "RiskFactors"));
    sections.put("BUSINESS", extractTextByTag(doc, "BusinessDescription"));

    return sections;
  }

  private Map<String, String> extractFootnotes(Document doc) {
    Map<String, String> footnotes = new HashMap<>();

    NodeList footnoteElements = doc.getElementsByTagNameNS("*", "footnote");
    for (int i = 0; i < footnoteElements.getLength(); i++) {
      Element footnote = (Element) footnoteElements.item(i);
      String id = footnote.getAttribute("id");
      if (id.isEmpty()) id = "footnote_" + i;
      footnotes.put(id, footnote.getTextContent());
    }

    return footnotes;
  }

  private String extractTextByTag(Document doc, String tagName) {
    NodeList elements = doc.getElementsByTagNameNS("*", tagName);
    if (elements.getLength() > 0) {
      return elements.item(0).getTextContent();
    }
    return "";
  }

  private String findRelatedText(String narrative, List<String> concepts) {
    if (narrative == null || narrative.isEmpty()) return null;

    // Simple keyword matching - in production would use NLP
    for (String concept : concepts) {
      String keyword = conceptToKeyword(concept);
      if (narrative.toLowerCase().contains(keyword.toLowerCase())) {
        // Extract surrounding context (e.g., paragraph)
        int index = narrative.toLowerCase().indexOf(keyword.toLowerCase());
        int start = Math.max(0, index - 200);
        int end = Math.min(narrative.length(), index + 500);
        return narrative.substring(start, end).trim();
      }
    }
    return null;
  }

  private String findRelatedFootnote(Map<String, String> footnotes, String concept) {
    // Look for footnotes that mention this concept
    String keyword = conceptToKeyword(concept);
    for (Map.Entry<String, String> entry : footnotes.entrySet()) {
      if (entry.getValue().toLowerCase().contains(keyword.toLowerCase())) {
        return entry.getValue();
      }
    }
    return null;
  }

  private String conceptToKeyword(String concept) {
    // Convert XBRL concept to searchable keyword
    return concept.replaceAll("([A-Z])", " $1")
                  .replaceAll("Loss$", "")
                  .replaceAll("AtCarryingValue$", "")
                  .trim();
  }

  private String extractPeriod(Document doc, String contextRef) {
    NodeList contexts = doc.getElementsByTagNameNS("*", "context");
    for (int i = 0; i < contexts.getLength(); i++) {
      Element context = (Element) contexts.item(i);
      if (contextRef.equals(context.getAttribute("id"))) {
        NodeList instants = context.getElementsByTagNameNS("*", "instant");
        if (instants.getLength() > 0) {
          return instants.item(0).getTextContent();
        }
        NodeList endDates = context.getElementsByTagNameNS("*", "endDate");
        if (endDates.getLength() > 0) {
          return endDates.item(0).getTextContent();
        }
      }
    }
    return "unknown";
  }

  private String extractFilingInfo(File secFile) {
    // Extract from filename: CIK-DATE-TYPE.xml
    String name = secFile.getName().replace(".xml", "");
    return name;
  }

  private String formatValue(double value) {
    if (value >= 1_000_000_000) {
      return String.format("%.1fB", value / 1_000_000_000);
    } else if (value >= 1_000_000) {
      return String.format("%.1fM", value / 1_000_000);
    } else {
      return String.format("%.0f", value);
    }
  }

  /**
   * Represents a contextual chunk ready for embedding.
   */
  public static class ContextualChunk {
    public final String context;
    public final String text;
    public final String blobType;  // concept_group, footnote, mda_paragraph
    public final String originalBlobId;  // For traceability
    public final Map<String, Object> metadata;  // Relationships, token info, etc.
    public final double[] embedding;  // The actual vector embedding

    public ContextualChunk(String context, String text) {
      this(context, text, "concept_group", null, new HashMap<>(), null);
    }

    public ContextualChunk(String context, String text, String blobType, 
                          String originalBlobId, Map<String, Object> metadata) {
      this(context, text, blobType, originalBlobId, metadata, null);
    }

    public ContextualChunk(String context, String text, String blobType, 
                          String originalBlobId, Map<String, Object> metadata,
                          double[] embedding) {
      this.context = context;
      this.text = text;
      this.blobType = blobType;
      this.originalBlobId = originalBlobId;
      this.metadata = metadata != null ? metadata : new HashMap<>();
      this.embedding = embedding;
    }
  }

  /**
   * Simple container for financial facts.
   */
  public static class FinancialFact {
    public final String concept;
    public final double value;
    public final String period;

    public FinancialFact(String concept, double value, String period) {
      this.concept = concept;
      this.value = value;
      this.period = period;
    }
  }

  // ========== New Relationship-Based Vectorization Methods ==========

  /**
   * Container for text blob with metadata.
   */
  public static class TextBlob {
    public final String id;
    public final String type;  // footnote, mda_paragraph
    public final String text;
    public final String parentSection;
    public final String subsection;
    public final Map<String, String> attributes;

    public TextBlob(String id, String type, String text, String parentSection) {
      this(id, type, text, parentSection, null, new HashMap<>());
    }

    public TextBlob(String id, String type, String text, String parentSection,
                   String subsection, Map<String, String> attributes) {
      this.id = id;
      this.type = type;
      this.text = text;
      this.parentSection = parentSection;
      this.subsection = subsection;
      this.attributes = attributes;
    }
  }

  /**
   * Create individual enriched chunks for footnotes and MD&A paragraphs.
   * Each blob is enriched with its relationships before vectorization.
   */
  public List<ContextualChunk> createIndividualChunks(
      List<TextBlob> footnotes,
      List<TextBlob> mdaParagraphs,
      Map<String, List<String>> references,
      Map<String, FinancialFact> facts) {
    
    List<ContextualChunk> chunks = new ArrayList<>();
    SecTokenManager tokenManager = new SecTokenManager();

    // Vectorize each footnote with its relationships
    for (TextBlob footnote : footnotes) {
      ContextualChunk chunk = vectorizeFootnote(footnote, references, mdaParagraphs, 
                                                facts, tokenManager);
      if (chunk != null) {
        // TODO: Real embedding generation would require calling an actual embedding API
        // For now, using placeholder embeddings - THIS IS NOT REAL VECTORIZATION
        double[] embedding = generateEmbedding(chunk.text, this.embeddingDimension);
        chunk = new ContextualChunk(chunk.context, chunk.text, chunk.blobType,
                                   chunk.originalBlobId, chunk.metadata, embedding);
        chunks.add(chunk);
      }
    }

    // Vectorize each MD&A paragraph with referenced footnotes
    for (TextBlob mdaPara : mdaParagraphs) {
      ContextualChunk chunk = vectorizeMDAParagraph(mdaPara, references, footnotes, 
                                                    facts, tokenManager);
      if (chunk != null) {
        // TODO: Real embedding generation would require calling an actual embedding API
        // For now, using placeholder embeddings - THIS IS NOT REAL VECTORIZATION
        double[] embedding = generateEmbedding(chunk.text, this.embeddingDimension);
        chunk = new ContextualChunk(chunk.context, chunk.text, chunk.blobType,
                                   chunk.originalBlobId, chunk.metadata, embedding);
        chunks.add(chunk);
      }
    }

    return chunks;
  }

  /**
   * Vectorize a footnote with contextual enrichment.
   */
  private ContextualChunk vectorizeFootnote(
      TextBlob footnote,
      Map<String, List<String>> references,
      List<TextBlob> mdaParagraphs,
      Map<String, FinancialFact> facts,
      SecTokenManager tokenManager) {

    StringBuilder enriched = new StringBuilder();
    Map<String, Object> metadata = new HashMap<>();
    
    // Start with main footnote text
    enriched.append("[MAIN:FOOTNOTE:").append(footnote.id).append("] ");
    enriched.append(footnote.text);
    
    int tokensUsed = tokenManager.estimateTokens(enriched.toString());
    int remainingBudget = SecTokenManager.MAX_TOKENS - tokensUsed;

    // Add parent section if exists
    if (footnote.parentSection != null && remainingBudget > 100) {
      String parentText = String.format("\n[PARENT:%s]", footnote.parentSection);
      if (tokenManager.estimateTokens(parentText) < remainingBudget * 0.1) {
        enriched.append(parentText);
        remainingBudget -= tokenManager.estimateTokens(parentText);
      }
    }

    // Add references from MD&A
    List<String> referencedBy = references.get(footnote.id);
    if (referencedBy != null && !referencedBy.isEmpty() && remainingBudget > 200) {
      metadata.put("referenced_by", referencedBy);
      enriched.append("\n[REFERENCED_BY:");
      
      // Add first few referencing paragraphs
      int added = 0;
      for (String refId : referencedBy) {
        if (added >= 3 || remainingBudget < 200) break;
        
        // Find the MD&A paragraph
        TextBlob refPara = findBlobById(mdaParagraphs, refId);
        if (refPara != null) {
          String excerpt = extractRelevantSentence(refPara.text, footnote.id);
          String refText = String.format("\n  %s:%s: %s", 
              refPara.parentSection, refId, excerpt);
          
          int refTokens = tokenManager.estimateTokens(refText);
          if (refTokens < remainingBudget) {
            enriched.append(refText);
            remainingBudget -= refTokens;
            added++;
          }
        }
      }
      enriched.append("]");
    }

    // Add related financial metrics
    List<String> concepts = extractFinancialConcepts(footnote.text);
    if (!concepts.isEmpty() && remainingBudget > 100) {
      metadata.put("financial_concepts", concepts);
      enriched.append("\n[METRICS] ");
      for (String concept : concepts.subList(0, Math.min(3, concepts.size()))) {
        FinancialFact fact = facts.get(concept);
        if (fact != null) {
          enriched.append(concept).append("=").append(formatValue(fact.value)).append(" ");
        }
      }
    }

    // Add metadata about enrichment
    metadata.put("tokens_used", SecTokenManager.MAX_TOKENS - remainingBudget);
    metadata.put("token_budget", SecTokenManager.MAX_TOKENS);
    enriched.append(String.format("\n[ENRICHMENT_META tokens=%d/%d]", 
        SecTokenManager.MAX_TOKENS - remainingBudget, SecTokenManager.MAX_TOKENS));

    return new ContextualChunk(
        "footnote_" + footnote.id,
        enriched.toString(),
        "footnote",
        footnote.id,
        metadata
    );
  }

  /**
   * Vectorize an MD&A paragraph with referenced footnotes.
   */
  private ContextualChunk vectorizeMDAParagraph(
      TextBlob mdaPara,
      Map<String, List<String>> references,
      List<TextBlob> footnotes,
      Map<String, FinancialFact> facts,
      SecTokenManager tokenManager) {

    StringBuilder enriched = new StringBuilder();
    Map<String, Object> metadata = new HashMap<>();
    
    // Start with main paragraph text
    enriched.append("[MAIN:MDA:").append(mdaPara.id).append("] ");
    enriched.append(mdaPara.text);
    
    int tokensUsed = tokenManager.estimateTokens(enriched.toString());
    int remainingBudget = SecTokenManager.MAX_TOKENS - tokensUsed;

    // Add section hierarchy
    if (mdaPara.parentSection != null) {
      String hierarchy = String.format("\n[HIERARCHY: %s", mdaPara.parentSection);
      if (mdaPara.subsection != null) {
        hierarchy += " > " + mdaPara.subsection;
      }
      hierarchy += "]";
      
      if (tokenManager.estimateTokens(hierarchy) < remainingBudget * 0.1) {
        enriched.append(hierarchy);
        remainingBudget -= tokenManager.estimateTokens(hierarchy);
      }
    }

    // Find and add referenced footnotes
    List<String> footnotesReferenced = extractFootnoteReferences(mdaPara.text);
    if (!footnotesReferenced.isEmpty() && remainingBudget > 300) {
      metadata.put("references_footnotes", footnotesReferenced);
      enriched.append("\n[REFERENCED_FOOTNOTES:");
      
      for (String fnId : footnotesReferenced) {
        if (remainingBudget < 200) break;
        
        TextBlob footnote = findBlobById(footnotes, fnId);
        if (footnote != null) {
          // Add first paragraph or up to 500 tokens of footnote
          String excerpt = tokenManager.getSmartExcerpt(footnote.text, 
              Math.min(500, remainingBudget - 100));
          String fnText = String.format("\n  %s: %s", fnId, excerpt);
          
          int fnTokens = tokenManager.estimateTokens(fnText);
          if (fnTokens < remainingBudget) {
            enriched.append(fnText);
            remainingBudget -= fnTokens;
          }
        }
      }
      enriched.append("]");
    }

    // Add related financial metrics
    List<String> concepts = extractFinancialConcepts(mdaPara.text);
    if (!concepts.isEmpty() && remainingBudget > 100) {
      metadata.put("financial_concepts", concepts);
      enriched.append("\n[METRICS] ");
      for (String concept : concepts.subList(0, Math.min(3, concepts.size()))) {
        FinancialFact fact = facts.get(concept);
        if (fact != null) {
          enriched.append(concept).append("=").append(formatValue(fact.value)).append(" ");
        }
      }
    }

    // Add metadata
    metadata.put("tokens_used", SecTokenManager.MAX_TOKENS - remainingBudget);
    enriched.append(String.format("\n[ENRICHMENT_META tokens=%d/%d]", 
        SecTokenManager.MAX_TOKENS - remainingBudget, SecTokenManager.MAX_TOKENS));

    return new ContextualChunk(
        "mda_" + mdaPara.id,
        enriched.toString(),
        "mda_paragraph",
        mdaPara.id,
        metadata
    );
  }

  /**
   * Extract financial concepts mentioned in text.
   */
  private List<String> extractFinancialConcepts(String text) {
    List<String> concepts = new ArrayList<>();
    String lowerText = text.toLowerCase();
    
    // Check against all known concepts
    for (List<String> group : CONCEPT_GROUPS.values()) {
      for (String concept : group) {
        String keyword = conceptToKeyword(concept).toLowerCase();
        if (lowerText.contains(keyword)) {
          concepts.add(concept);
        }
      }
    }
    
    return concepts;
  }

  /**
   * Extract footnote references from text (e.g., "Note 14", "See Note 2").
   */
  private List<String> extractFootnoteReferences(String text) {
    List<String> references = new ArrayList<>();
    java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
        "(?:Note|Footnote)\\s+(\\d+[A-Za-z]?)", java.util.regex.Pattern.CASE_INSENSITIVE);
    java.util.regex.Matcher matcher = pattern.matcher(text);
    
    while (matcher.find()) {
      references.add("footnote_" + matcher.group(1));
    }
    
    return references;
  }

  /**
   * Extract the most relevant sentence mentioning a specific item.
   */
  private String extractRelevantSentence(String text, String itemId) {
    String[] sentences = text.split("(?<=[.!?])\\s+");
    String searchTerm = itemId.replace("footnote_", "Note ");
    
    for (String sentence : sentences) {
      if (sentence.toLowerCase().contains(searchTerm.toLowerCase())) {
        return sentence.length() > 200 ? 
            sentence.substring(0, 197) + "..." : sentence;
      }
    }
    
    // Return first sentence if no specific reference found
    return sentences.length > 0 ? 
        (sentences[0].length() > 200 ? sentences[0].substring(0, 197) + "..." : sentences[0]) 
        : text.substring(0, Math.min(200, text.length()));
  }

  /**
   * Find a text blob by ID.
   */
  private TextBlob findBlobById(List<TextBlob> blobs, String id) {
    for (TextBlob blob : blobs) {
      if (blob.id.equals(id)) {
        return blob;
      }
    }
    return null;
  }
  
  /**
   * PLACEHOLDER: Generate a fake embedding vector for the given text.
   * WARNING: This is NOT a real embedding - just deterministic noise based on text hash.
   * Real implementation would require:
   * - Integration with OpenAI/Anthropic/Cohere embedding API
   * - Or local model like Sentence-BERT
   * - Or financial-specific embeddings like FinBERT
   * 
   * Current implementation is just for testing table structure.
   */
  public double[] generateEmbedding(String text, int dimension) {
    if (text == null || text.isEmpty()) {
      return new double[dimension];
    }
    
    double[] embedding = new double[dimension];
    
    try {
      // Use SHA-256 to create a deterministic hash of the text
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] hashBytes = md.digest(text.getBytes(StandardCharsets.UTF_8));
      
      // Create a deterministic random generator from the hash
      long seed = 0;
      for (int i = 0; i < Math.min(8, hashBytes.length); i++) {
        seed = (seed << 8) | (hashBytes[i] & 0xFF);
      }
      Random random = new Random(seed);
      
      // Generate embedding based on text features
      String[] tokens = text.toLowerCase().split("\\s+");
      
      // Initialize with random values based on text hash
      for (int i = 0; i < dimension; i++) {
        embedding[i] = random.nextGaussian() * 0.1;
      }
      
      // Add semantic features based on financial terms
      for (String token : tokens) {
        int tokenHash = token.hashCode();
        int index = Math.abs(tokenHash) % dimension;
        
        // Boost dimensions for important financial terms
        if (isFinancialTerm(token)) {
          embedding[index] += 0.5;
          if (index > 0) embedding[index - 1] += 0.2;
          if (index < dimension - 1) embedding[index + 1] += 0.2;
        } else if (isNumeric(token)) {
          embedding[index] += 0.3;
        } else {
          embedding[index] += 0.1;
        }
      }
      
      // Normalize the vector to unit length
      double magnitude = 0;
      for (double val : embedding) {
        magnitude += val * val;
      }
      magnitude = Math.sqrt(magnitude);
      
      if (magnitude > 0) {
        for (int i = 0; i < dimension; i++) {
          embedding[i] /= magnitude;
        }
      }
      
    } catch (NoSuchAlgorithmException e) {
      // Fallback to simple hash-based embedding
      Random random = new Random(text.hashCode());
      for (int i = 0; i < dimension; i++) {
        embedding[i] = random.nextGaussian();
      }
    }
    
    return embedding;
  }
  
  /**
   * Check if a token is a financial term.
   */
  private boolean isFinancialTerm(String token) {
    return FINANCIAL_TERMS.contains(token.toLowerCase());
  }
  
  /**
   * Check if a token is numeric.
   */
  private boolean isNumeric(String token) {
    if (token == null || token.isEmpty()) return false;
    token = token.replaceAll("[,$%]", "");
    try {
      Double.parseDouble(token);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }
  
  // Common financial terms for semantic enhancement
  private static final Set<String> FINANCIAL_TERMS = new HashSet<>(Arrays.asList(
    "revenue", "income", "profit", "loss", "earnings", "ebitda", "margin",
    "cash", "debt", "equity", "asset", "liability", "expense", "cost",
    "investment", "dividend", "share", "stock", "bond", "derivative",
    "goodwill", "amortization", "depreciation", "capex", "opex",
    "receivable", "payable", "inventory", "tax", "interest", "principal",
    "acquisition", "merger", "restructuring", "impairment", "provision",
    "segment", "subsidiary", "consolidated", "gaap", "non-gaap",
    "quarter", "fiscal", "year", "annual", "quarterly", "ytd", "qoq", "yoy"
  ));
}
