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

    public ContextualChunk(String context, String text) {
      this.context = context;
      this.text = text;
    }
  }

  /**
   * Simple container for financial facts.
   */
  private static class FinancialFact {
    final String concept;
    final double value;
    final String period;

    FinancialFact(String concept, double value, String period) {
      this.concept = concept;
      this.value = value;
      this.period = period;
    }
  }
}
