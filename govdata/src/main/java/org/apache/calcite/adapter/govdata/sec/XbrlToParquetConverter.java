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

import org.apache.calcite.adapter.file.converters.FileConverter;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.jsoup.Jsoup;
import org.jsoup.select.Elements;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

/**
 * Converter for XBRL files to Parquet format.
 * Implements the file adapter's FileConverter interface for integration
 * with FileConversionManager.
 */
public class XbrlToParquetConverter implements FileConverter {
  private static final Logger LOGGER = Logger.getLogger(XbrlToParquetConverter.class.getName());

  // HTML tag removal pattern
  private static final Pattern HTML_TAG_PATTERN = Pattern.compile("<[^>]+>");
  private static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s+");

  @Override public boolean canConvert(String sourceFormat, String targetFormat) {
    return ("xbrl".equalsIgnoreCase(sourceFormat) || "xml".equalsIgnoreCase(sourceFormat)
        || "html".equalsIgnoreCase(sourceFormat) || "htm".equalsIgnoreCase(sourceFormat))
        && "parquet".equalsIgnoreCase(targetFormat);
  }

  @Override public List<File> convert(File sourceFile, File targetDirectory,
      ConversionMetadata metadata) throws IOException {
    List<File> outputFiles = new ArrayList<>();
    
    // Extract accession from metadata if available
    String accession = null;
    if (metadata != null && metadata.getAllConversions() != null) {
      for (ConversionMetadata.ConversionRecord record : metadata.getAllConversions().values()) {
        if (sourceFile.getAbsolutePath().equals(record.originalFile)) {
          // We stored accession in sourceFile field
          accession = record.sourceFile;
          break;
        }
      }
    }

    try {
      Document doc = null;
      boolean isInlineXbrl = false;

      // Check if it's an HTML file (potential inline XBRL)
      String fileName = sourceFile.getName().toLowerCase();
      if (fileName.endsWith(".htm") || fileName.endsWith(".html")) {
        // Try to parse as inline XBRL
        doc = parseInlineXbrl(sourceFile);
        if (doc != null) {
          isInlineXbrl = true;
          LOGGER.info("Processing inline XBRL from HTML: " + sourceFile.getName());
        }
      }

      // If not inline XBRL or parsing failed, try traditional XBRL
      if (doc == null) {
        // Parse traditional XBRL/XML file
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        DocumentBuilder builder = factory.newDocumentBuilder();
        doc = builder.parse(sourceFile);
      }

      // Extract filing metadata
      String cik = extractCik(doc, sourceFile);
      String filingType = extractFilingType(doc, sourceFile);
      String filingDate = extractFilingDate(doc, sourceFile);
      
      // Skip conversion if we couldn't extract required metadata
      if (cik == null || cik.equals("0000000000")) {
        LOGGER.warning("Skipping conversion due to invalid or missing CIK: " + sourceFile.getName());
        return outputFiles; // Return empty list
      }
      
      // Validate filing date - must be present
      if (filingDate == null) {
        LOGGER.warning("Skipping conversion - could not extract filing date from: " + sourceFile.getName());
        return outputFiles; // Skip conversion
      }
      
      // Validate filing date format and year
      if (filingDate.length() >= 4) {
        try {
          int year = Integer.parseInt(filingDate.substring(0, 4));
          if (year < 1934 || year > java.time.Year.now().getValue()) {
            LOGGER.warning("Invalid year " + year + " in filing date " + filingDate + " for " + sourceFile.getName());
            return outputFiles; // Skip conversion
          }
        } catch (NumberFormatException e) {
          LOGGER.warning("Invalid filing date format: " + filingDate + " for " + sourceFile.getName());
          return outputFiles; // Skip conversion
        }
      } else {
        LOGGER.warning("Filing date too short: " + filingDate + " for " + sourceFile.getName());
        return outputFiles; // Skip conversion
      }
      
      // Check if this is a Form 3, 4, or 5 (insider trading forms)
      if (isInsiderForm(doc, filingType)) {
        return convertInsiderForm(doc, sourceFile, targetDirectory, cik, filingType, filingDate, accession);
      }
      
      // Check if this is an 8-K filing with potential earnings exhibits
      if (is8KFiling(filingType)) {
        List<File> extraFiles = extract8KExhibits(sourceFile, targetDirectory, cik, filingType, filingDate);
        outputFiles.addAll(extraFiles);
      }

      // Create partitioned output path
      // Normalize filing type: remove hyphens and slashes
      String normalizedFilingType = filingType.replace("-", "").replace("/", "");
      File partitionDir =
          new File(
              targetDirectory, String.format("cik=%s/filing_type=%s/year=%s",
              cik, normalizedFilingType,
              filingDate.substring(0, 4)));
      partitionDir.mkdirs();
      
      // Clean up macOS metadata files after creating directories
      cleanupMacOSMetadataFilesRecursive(targetDirectory);

      // Convert financial facts to Parquet
      File factsFile =
          new File(partitionDir, String.format("%s_%s_facts.parquet", cik, filingDate));
      writeFactsToParquet(doc, factsFile, cik, filingType, filingDate);
      outputFiles.add(factsFile);

      // Convert contexts to Parquet
      File contextsFile =
          new File(partitionDir, String.format("%s_%s_contexts.parquet", cik, filingDate));
      writeContextsToParquet(doc, contextsFile, cik, filingType, filingDate);
      outputFiles.add(contextsFile);

      // Extract and write MD&A (Management Discussion & Analysis)
      File mdaFile =
          new File(partitionDir, String.format("%s_%s_mda.parquet", cik, filingDate));
      writeMDAToParquet(doc, mdaFile, cik, filingType, filingDate, sourceFile);
      if (mdaFile.exists()) {
        outputFiles.add(mdaFile);
      }

      // Extract and write XBRL relationships
      File relationshipsFile =
          new File(partitionDir, String.format("%s_%s_relationships.parquet", cik, filingDate));
      writeRelationshipsToParquet(doc, relationshipsFile, cik, filingType, filingDate);
      if (relationshipsFile.exists()) {
        outputFiles.add(relationshipsFile);
      }

      // Create vectorized blobs with contextual enrichment
      File vectorizedFile =
          new File(partitionDir, String.format("%s_%s_vectorized.parquet", cik, filingDate));
      writeVectorizedBlobsToParquet(doc, vectorizedFile, cik, filingType, filingDate, sourceFile);
      if (vectorizedFile.exists()) {
        outputFiles.add(vectorizedFile);
      }

      // Metadata is updated by FileConversionManager after successful conversion

      LOGGER.info("Converted XBRL to Parquet: " + sourceFile.getName() +
          " -> " + outputFiles.size() + " files");
      
      // Final cleanup of macOS metadata files in the entire target directory
      cleanupMacOSMetadataFilesRecursive(targetDirectory);

    } catch (Exception e) {
      throw new IOException("Failed to convert XBRL to Parquet", e);
    }

    return outputFiles;
  }

  private String extractCik(Document doc, File sourceFile) {
    // For ownership documents (Form 3/4/5), extract issuerCik
    NodeList issuerCiks = doc.getElementsByTagName("issuerCik");
    if (issuerCiks.getLength() > 0) {
      String cik = issuerCiks.item(0).getTextContent().trim();
      // Pad to 10 digits
      while (cik.length() < 10) {
        cik = "0" + cik;
      }
      return cik;
    }
    
    // Try to extract from XBRL context
    NodeList contexts = doc.getElementsByTagNameNS("*", "context");
    for (int i = 0; i < contexts.getLength(); i++) {
      Element context = (Element) contexts.item(i);
      NodeList identifiers = context.getElementsByTagNameNS("*", "identifier");
      if (identifiers.getLength() > 0) {
        String identifier = identifiers.item(0).getTextContent();
        if (identifier.matches("\\d{10}")) {
          return identifier;
        }
      }
    }

    // Fall back to directory structure parsing
    // If file is in structure: /CIK/ACCESSION/file.xml
    File parent = sourceFile.getParentFile();
    if (parent != null) {
      File grandparent = parent.getParentFile();
      if (grandparent != null) {
        String dirName = grandparent.getName();
        if (dirName.matches("\\d+")) {
          // Pad to 10 digits
          while (dirName.length() < 10) {
            dirName = "0" + dirName;
          }
          return dirName;
        }
      }
    }

    // Fall back to filename parsing
    String filename = sourceFile.getName();
    if (filename.matches("\\d{10}_.*")) {
      return filename.substring(0, 10);
    }

    // Last resort - but log warning
    LOGGER.warning("Could not extract CIK from " + sourceFile.getName() + ", skipping conversion");
    return null; // Return null to indicate failure
  }

  private String extractFilingType(Document doc, File sourceFile) {
    // For ownership documents (Form 3/4/5), extract documentType
    NodeList docTypes = doc.getElementsByTagName("documentType");
    if (docTypes.getLength() > 0) {
      String docType = docTypes.item(0).getTextContent().trim();
      // Return just the number for forms 3, 4, 5
      if (docType.equals("3") || docType.equals("4") || docType.equals("5")) {
        return docType;
      }
      if (docType.startsWith("3/") || docType.startsWith("4/") || docType.startsWith("5/")) {
        return docType.substring(0, 1); // Just return "3", "4", or "5"
      }
    }
    
    // Try to extract from document type - check both with and without namespace
    NodeList documentTypes = doc.getElementsByTagNameNS("*", "DocumentType");
    if (documentTypes.getLength() > 0) {
      String docType = documentTypes.item(0).getTextContent().trim();
      // Normalize the filing type (remove hyphens for consistency)
      return docType.replace("-", "");
    }

    // Also check for dei:DocumentType (common in inline XBRL)
    NodeList deiDocTypes = doc.getElementsByTagName("dei:DocumentType");
    if (deiDocTypes.getLength() > 0) {
      String docType = deiDocTypes.item(0).getTextContent().trim();
      return docType.replace("-", "");
    }

    // Also check with ix: prefix for inline XBRL (note the capital N in nonNumeric)
    NodeList ixDocTypes = doc.getElementsByTagName("ix:nonNumeric");
    for (int i = 0; i < ixDocTypes.getLength(); i++) {
      Element elem = (Element) ixDocTypes.item(i);
      if ("dei:DocumentType".equals(elem.getAttribute("name"))) {
        String docType = elem.getTextContent().trim();
        return docType.replace("-", "");
      }
    }

    // Fall back to filename parsing
    String filename = sourceFile.getName();
    if (filename.contains("10K") || filename.contains("10-K")) return "10K";
    if (filename.contains("10Q") || filename.contains("10-Q")) return "10Q";
    if (filename.contains("8K") || filename.contains("8-K")) return "8K";

    return "UNKNOWN";
  }

  /**
   * Extract date from HTML filing metadata.
   */
  private String extractDateFromHTML(File htmlFile) {
    try {
      org.jsoup.nodes.Document doc = Jsoup.parse(htmlFile, "UTF-8");
      
      // Look for SEC header metadata
      // Pattern: "CONFORMED PERIOD OF REPORT: 20240930"
      Elements elements = doc.getElementsMatchingOwnText("CONFORMED PERIOD OF REPORT:");
      for (org.jsoup.nodes.Element elem : elements) {
        String text = elem.text();
        Pattern pattern = Pattern.compile("CONFORMED PERIOD OF REPORT:\\s*(\\d{8})");
        Matcher matcher = pattern.matcher(text);
        if (matcher.find()) {
          String dateStr = matcher.group(1);
          // Convert YYYYMMDD to YYYY-MM-DD
          if (dateStr.length() == 8) {
            return dateStr.substring(0, 4) + "-" + dateStr.substring(4, 6) + "-" + dateStr.substring(6, 8);
          }
        }
      }
      
      // Look for FILED AS OF DATE pattern
      elements = doc.getElementsMatchingOwnText("FILED AS OF DATE:");
      for (org.jsoup.nodes.Element elem : elements) {
        String text = elem.text();
        Pattern pattern = Pattern.compile("FILED AS OF DATE:\\s*(\\d{8})");
        Matcher matcher = pattern.matcher(text);
        if (matcher.find()) {
          String dateStr = matcher.group(1);
          // Convert YYYYMMDD to YYYY-MM-DD
          if (dateStr.length() == 8) {
            return dateStr.substring(0, 4) + "-" + dateStr.substring(4, 6) + "-" + dateStr.substring(6, 8);
          }
        }
      }
      
      // Look for inline XBRL elements with date
      elements = doc.select("[name*=DocumentPeriodEndDate], [name*=PeriodEndDate]");
      for (org.jsoup.nodes.Element elem : elements) {
        String date = elem.text().trim();
        if (date.matches("\\d{4}-\\d{2}-\\d{2}")) {
          return date;
        }
        // Handle format like "Sep. 30, 2024" or "September 30, 2024"
        if (date.matches("\\w+\\.?\\s+\\d{1,2},\\s+\\d{4}")) {
          try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("[MMMM][MMM][.][ ]d, yyyy", Locale.ENGLISH);
            LocalDate parsedDate = LocalDate.parse(date.replaceAll("\\.", ""), formatter);
            return parsedDate.format(DateTimeFormatter.ISO_LOCAL_DATE);
          } catch (Exception e) {
            // Ignore parse errors
          }
        }
      }
      
    } catch (IOException e) {
      LOGGER.warning("Failed to parse HTML file for date extraction: " + e.getMessage());
    }
    
    return null;
  }
  
  private String extractFilingDate(Document doc, File sourceFile) {
    // For ownership documents (Form 3/4/5), extract periodOfReport
    NodeList periodOfReports = doc.getElementsByTagName("periodOfReport");
    if (periodOfReports.getLength() > 0) {
      String date = periodOfReports.item(0).getTextContent().trim();
      // Validate date format (should be YYYY-MM-DD)
      if (date.matches("\\d{4}-\\d{2}-\\d{2}")) {
        return date;
      }
    }
    
    // Try to extract from document period end date (standard XBRL)
    NodeList periodEnds = doc.getElementsByTagNameNS("*", "DocumentPeriodEndDate");
    if (periodEnds.getLength() > 0) {
      String date = periodEnds.item(0).getTextContent().trim();
      if (date.matches("\\d{4}-\\d{2}-\\d{2}")) {
        return date;
      }
    }
    
    // Try to extract from dei:DocumentPeriodEndDate (inline XBRL)
    NodeList deiPeriodEnds = doc.getElementsByTagNameNS("*", "dei:DocumentPeriodEndDate");
    if (deiPeriodEnds.getLength() == 0) {
      // Try without namespace prefix
      deiPeriodEnds = doc.getElementsByTagName("dei:DocumentPeriodEndDate");
    }
    if (deiPeriodEnds.getLength() > 0) {
      String date = deiPeriodEnds.item(0).getTextContent().trim();
      if (date.matches("\\d{4}-\\d{2}-\\d{2}")) {
        return date;
      }
    }
    
    // Try to extract from context periods (XBRL contexts)
    NodeList contexts = doc.getElementsByTagNameNS("*", "context");
    if (contexts.getLength() == 0) {
      contexts = doc.getElementsByTagName("context");
    }
    for (int i = 0; i < contexts.getLength(); i++) {
      Node context = contexts.item(i);
      NodeList periods = ((Element) context).getElementsByTagNameNS("*", "instant");
      if (periods.getLength() > 0) {
        String date = periods.item(0).getTextContent().trim();
        if (date.matches("\\d{4}-\\d{2}-\\d{2}")) {
          return date;
        }
      }
      // Also check endDate in period elements
      periods = ((Element) context).getElementsByTagNameNS("*", "endDate");
      if (periods.getLength() > 0) {
        String date = periods.item(0).getTextContent().trim();
        if (date.matches("\\d{4}-\\d{2}-\\d{2}")) {
          return date;
        }
      }
    }
    
    // For HTML/inline XBRL files, try to parse from HTML metadata
    if (sourceFile.getName().endsWith(".htm") || sourceFile.getName().endsWith(".html")) {
      String htmlDate = extractDateFromHTML(sourceFile);
      if (htmlDate != null) {
        return htmlDate;
      }
    }

    // Return null if no date found - let the caller handle this
    LOGGER.warning("Could not extract filing date from: " + sourceFile.getName());
    return null;
  }

  private void writeFactsToParquet(Document doc, File outputFile,
      String cik, String filingType, String filingDate) throws IOException {

    // Create Avro schema for facts
    // NOTE: Partition columns (cik, filing_type, year) are NOT included in the file
    // They are encoded in the directory structure for Hive-style partitioning
    Schema schema = SchemaBuilder.record("XbrlFact")
        .fields()
        .requiredString("filing_date")
        .requiredString("concept")
        .optionalString("context_ref")
        .optionalString("unit_ref")
        .optionalString("value")
        .optionalString("full_text")  // Full text content for TextBlocks
        .optionalDouble("numeric_value")
        .optionalString("period_start")
        .optionalString("period_end")
        .optionalBoolean("is_instant")
        .optionalString("footnote_refs")  // Footnote references if any
        .optionalString("element_id")  // Element ID for linking
        .endRecord();

    // Extract all fact elements
    List<GenericRecord> records = new ArrayList<>();
    NodeList allElements = doc.getElementsByTagName("*");

    for (int i = 0; i < allElements.getLength(); i++) {
      Element element = (Element) allElements.item(i);

      // Skip non-fact elements
      if (element.hasAttribute("contextRef")) {
        GenericRecord record = new GenericData.Record(schema);
        // Partition columns (cik, filing_type) are NOT added - they're in the directory path
        record.put("filing_date", filingDate);

        // For inline XBRL, concept is in 'name' attribute; for regular XBRL, it's the element name
        String concept;
        if (element.hasAttribute("name")) {
          // Inline XBRL: concept is in the 'name' attribute
          concept = element.getAttribute("name");
          // Remove namespace prefix if present (e.g., "us-gaap:NetIncomeLoss" -> "NetIncomeLoss")
          if (concept.contains(":")) {
            concept = concept.substring(concept.indexOf(":") + 1);
          }
        } else {
          // Regular XBRL: concept is the element's local name
          concept = element.getLocalName();
        }
        record.put("concept", concept);
        record.put("context_ref", element.getAttribute("contextRef"));
        record.put("unit_ref", element.getAttribute("unitRef"));

        // Get raw text content
        String rawValue = element.getTextContent().trim();

        // For TextBlocks and narrative content, preserve more formatting
        boolean isTextBlock = concept.contains("TextBlock") ||
                             concept.contains("Disclosure") ||
                             concept.contains("Policy");

        String cleanValue;
        String fullText = null;

        if (isTextBlock) {
          // For text blocks, preserve paragraph structure
          fullText = preserveTextBlockFormatting(rawValue);
          // Still provide a shorter cleaned version for the value field
          cleanValue = extractFirstParagraph(fullText);
        } else {
          // For regular facts, clean normally
          cleanValue = cleanHtmlText(rawValue);
        }

        record.put("value", cleanValue);
        record.put("full_text", fullText);

        // Extract footnote references (e.g., "See Note 14")
        String footnoteRefs = extractFootnoteReferences(rawValue);
        record.put("footnote_refs", footnoteRefs);

        // Store element ID for relationship tracking
        String elementId = element.getAttribute("id");
        record.put("element_id", elementId.isEmpty() ? null : elementId);

        // Try to parse as numeric (using cleaned value)
        try {
          double numValue =
              Double.parseDouble(cleanValue.replaceAll(",", ""));
          record.put("numeric_value", numValue);
        } catch (NumberFormatException e) {
          record.put("numeric_value", null);
        }

        records.add(record);
      }
    }

    // Write to Parquet
    @SuppressWarnings("deprecation")
    ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(new org.apache.hadoop.fs.Path(outputFile.toURI()))
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build();

    try {
      for (GenericRecord record : records) {
        writer.write(record);
      }
    } finally {
      writer.close();
    }

    LOGGER.info("Wrote " + records.size() + " facts to " + outputFile);

    // Clean up macOS metadata files
    cleanupMacOSMetadataFiles(outputFile.getParentFile());
  }

  private void writeContextsToParquet(Document doc, File outputFile,
      String cik, String filingType, String filingDate) throws IOException {

    // Create Avro schema for contexts
    // NOTE: Partition columns (cik, filing_type, year) are NOT included in the file
    Schema schema = SchemaBuilder.record("XbrlContext")
        .fields()
        .requiredString("filing_date")
        .requiredString("context_id")
        .optionalString("entity_identifier")
        .optionalString("entity_scheme")
        .optionalString("period_start")
        .optionalString("period_end")
        .optionalString("period_instant")
        .optionalString("segment")
        .optionalString("scenario")
        .endRecord();

    // Extract context elements
    List<GenericRecord> records = new ArrayList<>();
    NodeList contexts = doc.getElementsByTagNameNS("*", "context");

    for (int i = 0; i < contexts.getLength(); i++) {
      Element context = (Element) contexts.item(i);
      GenericRecord record = new GenericData.Record(schema);

      // Partition columns (cik, filing_type) are NOT added - they're in the directory path
      record.put("filing_date", filingDate);
      record.put("context_id", context.getAttribute("id"));

      // Extract entity information
      NodeList identifiers = context.getElementsByTagNameNS("*", "identifier");
      if (identifiers.getLength() > 0) {
        Element identifier = (Element) identifiers.item(0);
        record.put("entity_identifier", identifier.getTextContent());
        record.put("entity_scheme", identifier.getAttribute("scheme"));
      }

      // Extract period information
      NodeList startDates = context.getElementsByTagNameNS("*", "startDate");
      NodeList endDates = context.getElementsByTagNameNS("*", "endDate");
      NodeList instants = context.getElementsByTagNameNS("*", "instant");

      if (startDates.getLength() > 0) {
        record.put("period_start", startDates.item(0).getTextContent());
      }
      if (endDates.getLength() > 0) {
        record.put("period_end", endDates.item(0).getTextContent());
      }
      if (instants.getLength() > 0) {
        record.put("period_instant", instants.item(0).getTextContent());
      }

      records.add(record);
    }

    // Write to Parquet
    @SuppressWarnings("deprecation")
    ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(new org.apache.hadoop.fs.Path(outputFile.toURI()))
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build();

    try {
      for (GenericRecord record : records) {
        writer.write(record);
      }
    } finally {
      writer.close();
    }

    LOGGER.info("Wrote " + records.size() + " contexts to " + outputFile);

    // Clean up macOS metadata files
    cleanupMacOSMetadataFiles(outputFile.getParentFile());
  }

  @Override public String getSourceFormat() {
    return "xbrl";
  }

  @Override public String getTargetFormat() {
    return "parquet";
  }

  /**
   * Clean HTML tags and entities from text content.
   * This is essential for footnotes, MD&A, risk factors, and other narrative text in XBRL.
   */
  private String cleanHtmlText(String text) {
    if (text == null || text.isEmpty()) {
      return text;
    }

    // First, unescape HTML entities (&amp; &lt; &gt; &nbsp; etc.)
    String unescaped = StringEscapeUtils.unescapeHtml4(text);

    // Remove HTML tags
    String withoutTags = HTML_TAG_PATTERN.matcher(unescaped).replaceAll(" ");

    // Normalize whitespace (multiple spaces/tabs/newlines to single space)
    String normalized = WHITESPACE_PATTERN.matcher(withoutTags).replaceAll(" ");

    // Final trim
    return normalized.trim();
  }

  /**
   * Preserve formatting for TextBlock content while removing HTML.
   * Maintains paragraph breaks and list structure.
   */
  private String preserveTextBlockFormatting(String text) {
    if (text == null || text.isEmpty()) {
      return text;
    }

    // Parse HTML content if present
    if (text.contains("<") && text.contains(">")) {
      org.jsoup.nodes.Document doc = Jsoup.parseBodyFragment(text);

      // Convert <p> tags to double newlines
      doc.select("p").append("\n\n");

      // Convert <br> to single newline
      doc.select("br").append("\n");

      // Convert list items to bullet points
      doc.select("li").prepend("• ");

      // Get text with preserved structure
      String formatted = doc.text();

      // Clean up excessive newlines
      formatted = formatted.replaceAll("\n{3,}", "\n\n");

      return formatted.trim();
    }

    // If no HTML, just unescape entities
    return StringEscapeUtils.unescapeHtml4(text).trim();
  }

  /**
   * Extract first paragraph or first 500 chars for summary.
   */
  private String extractFirstParagraph(String text) {
    if (text == null || text.isEmpty()) {
      return text;
    }

    // Find first paragraph break
    int paragraphEnd = text.indexOf("\n\n");
    if (paragraphEnd > 0 && paragraphEnd < 500) {
      return text.substring(0, paragraphEnd).trim();
    }

    // Otherwise, return first 500 chars
    return text.length() > 500 ?
        text.substring(0, 497) + "..." :
        text;
  }

  /**
   * Extract footnote references from text.
   * Looks for patterns like "See Note 14", "(Note 3)", "Refer to Note 2", etc.
   */
  private String extractFootnoteReferences(String text) {
    if (text == null || text.isEmpty()) {
      return null;
    }

    Pattern footnotePattern =
        Pattern.compile("(?:See|Refer to|Reference|\\()?\\s*Note[s]?\\s+(\\d+[A-Za-z]?(?:\\s*[,&]\\s*\\d+[A-Za-z]?)*)",
        Pattern.CASE_INSENSITIVE);

    Matcher matcher = footnotePattern.matcher(text);
    Set<String> references = new HashSet<>();

    while (matcher.find()) {
      String noteRefs = matcher.group(1);
      // Split on comma or ampersand for multiple references
      String[] notes = noteRefs.split("[,&]");
      for (String note : notes) {
        references.add("Note " + note.trim());
      }
    }

    return references.isEmpty() ? null : String.join("; ", references);
  }

  /**
   * Parse inline XBRL from HTML file.
   * Inline XBRL uses ix: namespace tags embedded in HTML.
   */
  private Document parseInlineXbrl(File htmlFile) {
    try {
      // Read HTML file
      String html = new String(Files.readAllBytes(htmlFile.toPath()));

      // Parse with Jsoup
      org.jsoup.nodes.Document jsoupDoc = Jsoup.parse(html);

      // Look for inline XBRL elements with various namespace prefixes
      // Try multiple selectors for different inline XBRL formats
      org.jsoup.select.Elements ixElements = jsoupDoc.select(
          "ix\\:nonnumeric, ix\\:nonfraction, ix\\:continuation, " +
          "ix\\:nonNumeric, ix\\:nonFraction, " +
          "[name^='us-gaap:'], [name^='dei:'], [name^='srt:'], " +
          "span[contextref], div[contextref], td[contextref]"
      );

      if (ixElements.isEmpty()) {
        // Try alternate inline XBRL format (HTML elements with contextRef)
        ixElements = jsoupDoc.select("[contextRef]");
        if (ixElements.isEmpty()) {
          // No inline XBRL found
          LOGGER.fine("No inline XBRL elements found in: " + htmlFile.getName());
          return null;
        }
      }

      LOGGER.info("Found " + ixElements.size() + " inline XBRL elements in: " + htmlFile.getName());

      // Create a new XML document from inline XBRL elements
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      factory.setNamespaceAware(true);
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document doc = builder.newDocument();

      // Create root element
      Element root = doc.createElement("xbrl");
      doc.appendChild(root);

      // Extract and convert inline XBRL facts to standard XBRL format
      for (org.jsoup.nodes.Element ixElement : ixElements) {
        String name = ixElement.attr("name");
        String contextRef = ixElement.attr("contextref");
        if (contextRef == null || contextRef.isEmpty()) {
          contextRef = ixElement.attr("contextRef");
        }
        
        // Skip if no name attribute - try tag-based extraction
        if (name == null || name.isEmpty()) {
          String tagName = ixElement.tagName();
          // For ix:nonFraction and ix:nonNumeric elements
          if (tagName.equals("ix:nonfraction") || tagName.equals("ix:nonnumeric")) {
            name = ixElement.attr("name");
          } else if (tagName.startsWith("ix:")) {
            // Use the tag name itself as concept
            name = "us-gaap:" + tagName.substring(3);
          } else {
            continue; // Skip elements without proper name
          }
        }
        
        if (name == null || name.isEmpty()) {
          continue;
        }
        
        String value = ixElement.text().trim();
        
        // Create fact element with proper namespace
        if (!value.isEmpty() && contextRef != null && !contextRef.isEmpty()) {
          // Extract namespace and local name
          String namespace = "us-gaap";
          String localName = name;
          if (name.contains(":")) {
            String[] parts = name.split(":", 2);
            namespace = parts[0];
            localName = parts[1];
          }
          
          Element fact = doc.createElementNS(
              "http://fasb.org/us-gaap/2024", 
              namespace + ":" + localName
          );
          fact.setAttribute("contextRef", contextRef);
          
          // Handle numeric values - remove commas and formatting
          if (ixElement.hasAttr("scale") || ixElement.hasAttr("decimals")) {
            value = value.replaceAll("[,$()]", "").trim();
            if (value.startsWith("(") && value.endsWith(")")) {
              value = "-" + value.substring(1, value.length() - 1);
            }
          }
          
          fact.setTextContent(value);
          root.appendChild(fact);
        }
      }

      // Extract contexts from HTML (both ix:context and xbrli:context)
      org.jsoup.select.Elements contexts = jsoupDoc.select(
          "ix\\:context, xbrli\\:context, [id^='c'], [id^='C']"
      );
      Map<String, Element> contextMap = new HashMap<>();
      for (org.jsoup.nodes.Element context : contexts) {
        Element xmlContext = doc.createElement("context");
        xmlContext.setAttribute("id", context.attr("id"));

        // Extract entity, period, etc from context
        org.jsoup.nodes.Element entity = context.selectFirst("[*|entity]");
        if (entity != null) {
          Element xmlEntity = doc.createElement("entity");
          org.jsoup.nodes.Element identifier = entity.selectFirst("[*|identifier]");
          if (identifier != null) {
            Element xmlIdentifier = doc.createElement("identifier");
            xmlIdentifier.setAttribute("scheme", identifier.attr("scheme"));
            xmlIdentifier.setTextContent(identifier.text());
            xmlEntity.appendChild(xmlIdentifier);
          }
          xmlContext.appendChild(xmlEntity);
        }

        // Extract period
        org.jsoup.nodes.Element period = context.selectFirst("[*|period]");
        if (period != null) {
          Element xmlPeriod = doc.createElement("period");
          org.jsoup.nodes.Element instant = period.selectFirst("[*|instant]");
          if (instant != null) {
            Element xmlInstant = doc.createElement("instant");
            xmlInstant.setTextContent(instant.text());
            xmlPeriod.appendChild(xmlInstant);
          }
          org.jsoup.nodes.Element startDate = period.selectFirst("[*|startDate]");
          if (startDate != null) {
            Element xmlStart = doc.createElement("startDate");
            xmlStart.setTextContent(startDate.text());
            xmlPeriod.appendChild(xmlStart);
          }
          org.jsoup.nodes.Element endDate = period.selectFirst("[*|endDate]");
          if (endDate != null) {
            Element xmlEnd = doc.createElement("endDate");
            xmlEnd.setTextContent(endDate.text());
            xmlPeriod.appendChild(xmlEnd);
          }
          xmlContext.appendChild(xmlPeriod);
        }

        root.appendChild(xmlContext);
      }

      // Extract facts from inline XBRL elements
      for (org.jsoup.nodes.Element ixElement : ixElements) {
        String tagName = ixElement.tagName();
        String concept = ixElement.attr("name");
        if (concept.isEmpty()) {
          continue;
        }

        // Create fact element
        Element fact = doc.createElement(concept.replace(":", "_"));
        fact.setAttribute("contextRef", ixElement.attr("contextRef"));
        fact.setAttribute("unitRef", ixElement.attr("unitRef"));
        fact.setAttribute("decimals", ixElement.attr("decimals"));
        fact.setAttribute("scale", ixElement.attr("scale"));
        fact.setTextContent(ixElement.text());

        root.appendChild(fact);
      }

      LOGGER.info("Extracted " + ixElements.size() + " inline XBRL facts from HTML");
      return doc;

    } catch (Exception e) {
      LOGGER.info("Could not parse inline XBRL from HTML: " + e.getMessage());
      return null;
    }
  }

  /**
   * Clean up macOS metadata files (._* files) from a directory.
   * These files are created by macOS and can cause issues with DuckDB and other tools.
   *
   * @param directory The directory to clean
   */
  private void cleanupMacOSMetadataFiles(File directory) {
    if (directory == null || !directory.exists() || !directory.isDirectory()) {
      return;
    }

    File[] metadataFiles = directory.listFiles((dir, name) -> name.startsWith("._"));
    if (metadataFiles != null) {
      for (File metadataFile : metadataFiles) {
        if (metadataFile.delete()) {
          LOGGER.fine("Removed macOS metadata file: " + metadataFile.getName());
        } else {
          LOGGER.warning("Failed to remove macOS metadata file: " + metadataFile.getAbsolutePath());
        }
      }
    }

    // Also clean parent directory (partition directories may have metadata files)
    File parent = directory.getParentFile();
    if (parent != null && parent.exists()) {
      File[] parentMetadataFiles = parent.listFiles((dir, name) -> name.startsWith("._"));
      if (parentMetadataFiles != null) {
        for (File metadataFile : parentMetadataFiles) {
          if (metadataFile.delete()) {
            LOGGER.fine("Removed macOS metadata file from parent: " + metadataFile.getName());
          }
        }
      }
    }
  }
  
  /**
   * Recursively clean up macOS metadata files (._* files) from a directory tree.
   * 
   * @param directory The root directory to clean recursively
   */
  private void cleanupMacOSMetadataFilesRecursive(File directory) {
    if (directory == null || !directory.exists() || !directory.isDirectory()) {
      return;
    }
    
    // Clean this directory
    cleanupMacOSMetadataFiles(directory);
    
    // Recursively clean subdirectories
    File[] subdirs = directory.listFiles(File::isDirectory);
    if (subdirs != null) {
      for (File subdir : subdirs) {
        cleanupMacOSMetadataFilesRecursive(subdir);
      }
    }
  }

  /**
   * Extract and write MD&A (Management Discussion & Analysis) to Parquet.
   * Each paragraph becomes a separate record for better querying.
   */
  private void writeMDAToParquet(Document doc, File outputFile,
      String cik, String filingType, String filingDate, File sourceFile) throws IOException {

    // Create schema for MD&A paragraphs
    Schema schema = SchemaBuilder.record("MDASection")
        .fields()
        .requiredString("filing_date")
        .requiredString("section")  // e.g., "Item 7", "Item 7A"
        .requiredString("subsection")  // e.g., "Overview", "Results of Operations"
        .requiredInt("paragraph_number")
        .requiredString("paragraph_text")
        .optionalString("footnote_refs")
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();

    // Look for MD&A content in different ways
    // 1. Look for Item 7 and Item 7A in inline XBRL
    if (sourceFile.getName().endsWith(".htm") || sourceFile.getName().endsWith(".html")) {
      extractMDAFromHTML(sourceFile, schema, records, filingDate);
    }

    // 2. Look for MD&A-related TextBlocks in XBRL
    NodeList allElements = doc.getElementsByTagName("*");
    for (int i = 0; i < allElements.getLength(); i++) {
      Element element = (Element) allElements.item(i);

      if (element.hasAttribute("contextRef")) {
        String concept = extractConceptName(element);

        // Check if this is an MD&A-related concept
        if (isMDAConcept(concept)) {
          String text = element.getTextContent().trim();
          if (text.length() > 50) {  // Skip if just a title
            extractParagraphs(text, concept, schema, records, filingDate);
          }
        }
      }
    }

    // Only write file if we found MD&A content
    if (!records.isEmpty()) {
      writeRecordsToParquet(records, schema, outputFile);
      LOGGER.info("Wrote " + records.size() + " MD&A paragraphs to " + outputFile);

      // Clean up macOS metadata files in the entire partition directory
      cleanupMacOSMetadataFiles(outputFile.getParentFile());
      // Also clean parent directories
      if (outputFile.getParentFile() != null && outputFile.getParentFile().getParentFile() != null) {
        cleanupMacOSMetadataFiles(outputFile.getParentFile().getParentFile());
        if (outputFile.getParentFile().getParentFile().getParentFile() != null) {
          cleanupMacOSMetadataFiles(outputFile.getParentFile().getParentFile().getParentFile());
        }
      }
    }
  }

  /**
   * Extract MD&A from HTML file by looking for Item 7 and Item 7A sections.
   * Enhanced to handle inline XBRL documents where Item 7 may be embedded in tags.
   */
  private void extractMDAFromHTML(File htmlFile, Schema schema,
      List<GenericRecord> records, String filingDate) {
    try {
      org.jsoup.nodes.Document doc = Jsoup.parse(htmlFile, "UTF-8");

      // Strategy 1: Look for Item 7 text in various formats
      // Match Item 7, Item 7., ITEM 7, Item 7A, etc.
      org.jsoup.select.Elements sections = doc.select("*:matchesOwn((?i)item\\s*7[A]?\\b)");

      // Strategy 2: Also look for Management's Discussion and Analysis directly
      if (sections.isEmpty()) {
        sections = doc.select("*:matchesOwn((?i)management.{0,5}discussion.{0,5}analysis)");
      }

      // Strategy 3: Look for specific HTML patterns common in SEC filings
      if (sections.isEmpty()) {
        // Look for table cells or divs that might contain Item 7
        sections = doc.select("td:matchesOwn((?i)item\\s*7), div:matchesOwn((?i)item\\s*7)");
      }

      for (org.jsoup.nodes.Element section : sections) {
        String sectionText = section.text();

        // Check if this is actually Item 7 or 7A (not Item 17, 27, etc.)
        if (!sectionText.matches("(?i).*item\\s*7[A]?\\b.*") ||
            sectionText.matches("(?i).*item\\s*[1-6]?7[0-9].*")) {
          continue;
        }

        // Check if this is just a table of contents reference
        if (sectionText.length() < 100 &&
            (sectionText.matches("(?i).*page.*") || sectionText.matches(".*\\d+$"))) {
          continue;
        }

        String sectionName = sectionText.contains("7A") || sectionText.contains("7a") ? "Item 7A" : "Item 7";

        // Try to find the actual content
        org.jsoup.nodes.Element contentStart = section;

        // If this element is small, it might just be a header - look for larger content
        if (section.text().length() < 200) {
          // Look at parent and siblings for actual content
          org.jsoup.nodes.Element parent = section.parent();
          if (parent != null) {
            // Check next siblings of parent
            org.jsoup.nodes.Element nextSibling = parent.nextElementSibling();
            if (nextSibling != null && nextSibling.text().length() > 200) {
              contentStart = nextSibling;
            }
          }

          // Also try direct next sibling
          org.jsoup.nodes.Element nextSibling = section.nextElementSibling();
          if (nextSibling != null && nextSibling.text().length() > 200) {
            contentStart = nextSibling;
          }
        }

        // Extract content
        extractMDAContent(contentStart, sectionName, schema, records, filingDate);
      }

      // If we still didn't find any MD&A, try a more aggressive approach
      if (records.isEmpty()) {
        // Look for large text blocks that mention key MD&A terms
        org.jsoup.select.Elements textBlocks = doc.select("div, p, td");
        boolean inMDA = false;
        String currentSection = "";

        for (org.jsoup.nodes.Element block : textBlocks) {
          String text = block.text();

          // Check if we're entering MD&A section
          if (text.matches("(?i).*item\\s*7[^0-9].*management.*discussion.*") ||
              text.matches("(?i).*management.*discussion.*analysis.*")) {
            inMDA = true;
            currentSection = "Item 7";
            continue;
          }

          // Check if we're entering Item 7A
          if (text.matches("(?i).*item\\s*7A.*")) {
            inMDA = true;
            currentSection = "Item 7A";
            continue;
          }

          // Check if we're leaving MD&A section
          if (inMDA && text.matches("(?i).*item\\s*[89]\\b.*")) {
            break;
          }

          // Extract content if we're in MD&A
          if (inMDA && text.length() > 100) {
            extractTextAsParagraphs(text, currentSection, schema, records, filingDate);
          }
        }
      }

    } catch (Exception e) {
      LOGGER.warning("Failed to extract MD&A from HTML: " + e.getMessage());
    }
  }

  /**
   * Extract MD&A content starting from a given element.
   */
  private void extractMDAContent(org.jsoup.nodes.Element startElement, String sectionName,
      Schema schema, List<GenericRecord> records, String filingDate) {

    String subsection = "Overview";
    int paragraphNum = 1;
    org.jsoup.nodes.Element current = startElement;
    int emptyCount = 0;

    while (current != null && !isNextItem(current.text()) && emptyCount < 5) {
      String text = current.text().trim();

      // Skip empty elements but don't give up immediately
      if (text.isEmpty()) {
        emptyCount++;
        current = current.nextElementSibling();
        continue;
      }
      emptyCount = 0;

      // Check for subsection headers
      if (isSubsectionHeader(current)) {
        subsection = cleanSubsectionName(text);
        current = current.nextElementSibling();
        continue;
      }

      // Extract meaningful paragraphs
      if (text.length() > 100 && !text.matches("(?i).*page\\s*\\d+.*")) {
        // Split very long blocks into paragraphs
        String[] paragraphs = text.split("(?<=[.!?])\\s+(?=[A-Z])");

        for (String paragraph : paragraphs) {
          if (paragraph.trim().length() > 50) {
            GenericRecord record = new GenericData.Record(schema);
            record.put("filing_date", filingDate);
            record.put("section", sectionName);
            record.put("subsection", subsection);
            record.put("paragraph_number", paragraphNum++);
            record.put("paragraph_text", paragraph.trim());
            record.put("footnote_refs", extractFootnoteReferences(paragraph));
            records.add(record);
          }
        }
      }

      current = current.nextElementSibling();
    }
  }

  /**
   * Extract text as paragraphs for aggressive MD&A extraction.
   */
  private void extractTextAsParagraphs(String text, String sectionName,
      Schema schema, List<GenericRecord> records, String filingDate) {

    // Split into sentences or natural paragraphs
    String[] paragraphs = text.split("(?<=[.!?])\\s+(?=[A-Z])");

    // Group sentences into reasonable paragraph sizes
    StringBuilder currentParagraph = new StringBuilder();
    int paragraphNum = records.size() + 1;

    for (String sentence : paragraphs) {
      currentParagraph.append(sentence).append(" ");

      // Create a paragraph every few sentences or at natural breaks
      if (currentParagraph.length() > 300 || sentence.endsWith(".")) {
        String paragraphText = currentParagraph.toString().trim();
        if (paragraphText.length() > 100) {
          GenericRecord record = new GenericData.Record(schema);
          record.put("filing_date", filingDate);
          record.put("section", sectionName);
          record.put("subsection", "General");
          record.put("paragraph_number", paragraphNum++);
          record.put("paragraph_text", paragraphText);
          record.put("footnote_refs", extractFootnoteReferences(paragraphText));
          records.add(record);

          currentParagraph = new StringBuilder();
        }
      }
    }

    // Add any remaining text
    String remaining = currentParagraph.toString().trim();
    if (remaining.length() > 100) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("filing_date", filingDate);
      record.put("section", sectionName);
      record.put("subsection", "General");
      record.put("paragraph_number", paragraphNum);
      record.put("paragraph_text", remaining);
      record.put("footnote_refs", extractFootnoteReferences(remaining));
      records.add(record);
    }
  }

  /**
   * Check if text indicates the start of the next Item section.
   */
  private boolean isNextItem(String text) {
    return text != null && text.matches(".*Item\\s+[89]\\b.*");
  }

  /**
   * Check if element is a subsection header.
   */
  private boolean isSubsectionHeader(org.jsoup.nodes.Element element) {
    return element.tagName().matches("h[2-4]") ||
           (element.tagName().equals("p") &&
            element.text().matches("^[A-Z][A-Z\\s]{2,50}$"));
  }

  /**
   * Clean subsection name for storage.
   */
  private String cleanSubsectionName(String text) {
    return text.replaceAll("\\s+", " ").trim();
  }

  /**
   * Check if concept name is MD&A related.
   */
  private boolean isMDAConcept(String concept) {
    return concept != null && (
        concept.contains("ManagementDiscussionAnalysis") ||
        concept.contains("MDA") ||
        concept.contains("OperatingResults") ||
        concept.contains("LiquidityCapitalResources") ||
        concept.contains("CriticalAccountingPolicies"));
  }

  /**
   * Extract paragraphs from text block.
   */
  private void extractParagraphs(String text, String concept, Schema schema,
      List<GenericRecord> records, String filingDate) {

    // Split into paragraphs
    String[] paragraphs = text.split("\\n\\n+");

    for (int i = 0; i < paragraphs.length; i++) {
      String paragraph = paragraphs[i].trim();
      if (paragraph.length() > 50) {  // Skip very short paragraphs
        GenericRecord record = new GenericData.Record(schema);
        record.put("filing_date", filingDate);
        record.put("section", "XBRL MD&A");
        record.put("subsection", concept);
        record.put("paragraph_number", i + 1);
        record.put("paragraph_text", paragraph);
        record.put("footnote_refs", extractFootnoteReferences(paragraph));
        records.add(record);
      }
    }
  }

  /**
   * Extract concept name from element.
   */
  private String extractConceptName(Element element) {
    if (element.hasAttribute("name")) {
      String concept = element.getAttribute("name");
      if (concept.contains(":")) {
        concept = concept.substring(concept.indexOf(":") + 1);
      }
      return concept;
    }
    return element.getLocalName();
  }

  /**
   * Write XBRL relationships to Parquet.
   * Captures presentation, calculation, and definition linkbases.
   */
  private void writeRelationshipsToParquet(Document doc, File outputFile,
      String cik, String filingType, String filingDate) throws IOException {

    // Create schema for relationships
    Schema schema = SchemaBuilder.record("XbrlRelationship")
        .fields()
        .requiredString("filing_date")
        .requiredString("linkbase_type")  // presentation, calculation, definition
        .requiredString("arc_role")  // e.g., parent-child, summation-item
        .requiredString("from_concept")
        .requiredString("to_concept")
        .optionalDouble("weight")  // For calculation linkbase
        .optionalInt("order")  // For presentation linkbase
        .optionalString("preferred_label")
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();

    // Look for linkbase arcs in the document
    // These define relationships between concepts
    NodeList arcs = doc.getElementsByTagNameNS("*", "arc");
    for (int i = 0; i < arcs.getLength(); i++) {
      Element arc = (Element) arcs.item(i);

      GenericRecord record = new GenericData.Record(schema);
      record.put("filing_date", filingDate);

      // Determine linkbase type from namespace or arc role
      String arcRole = arc.getAttribute("arcrole");
      String linkbaseType = determineLinkbaseType(arcRole);
      record.put("linkbase_type", linkbaseType);
      record.put("arc_role", arcRole);

      // Get from and to concepts
      String from = arc.getAttribute("from");
      String to = arc.getAttribute("to");
      record.put("from_concept", cleanConceptName(from));
      record.put("to_concept", cleanConceptName(to));

      // Get weight for calculation linkbase
      String weight = arc.getAttribute("weight");
      if (weight != null && !weight.isEmpty()) {
        try {
          record.put("weight", Double.parseDouble(weight));
        } catch (NumberFormatException e) {
          record.put("weight", null);
        }
      }

      // Get order for presentation linkbase
      String order = arc.getAttribute("order");
      if (order != null && !order.isEmpty()) {
        try {
          record.put("order", Integer.parseInt(order));
        } catch (NumberFormatException e) {
          record.put("order", null);
        }
      }

      // Get preferred label
      record.put("preferred_label", arc.getAttribute("preferredLabel"));

      records.add(record);
    }

    // Also extract relationships from inline XBRL if present
    extractInlineXBRLRelationships(doc, schema, records, filingDate);

    // Only write file if we found relationships
    if (!records.isEmpty()) {
      writeRecordsToParquet(records, schema, outputFile);
      LOGGER.info("Wrote " + records.size() + " relationships to " + outputFile);

      // Clean up macOS metadata files in the entire partition directory
      cleanupMacOSMetadataFiles(outputFile.getParentFile());
      // Also clean parent directories
      if (outputFile.getParentFile() != null && outputFile.getParentFile().getParentFile() != null) {
        cleanupMacOSMetadataFiles(outputFile.getParentFile().getParentFile());
        if (outputFile.getParentFile().getParentFile().getParentFile() != null) {
          cleanupMacOSMetadataFiles(outputFile.getParentFile().getParentFile().getParentFile());
        }
      }
    }
  }

  /**
   * Determine linkbase type from arc role.
   */
  private String determineLinkbaseType(String arcRole) {
    if (arcRole == null) return "unknown";

    if (arcRole.contains("parent-child") || arcRole.contains("presentation")) {
      return "presentation";
    } else if (arcRole.contains("summation") || arcRole.contains("calculation")) {
      return "calculation";
    } else if (arcRole.contains("dimension") || arcRole.contains("definition")) {
      return "definition";
    }

    return "other";
  }

  /**
   * Clean concept name by removing namespace prefix.
   */
  private String cleanConceptName(String concept) {
    if (concept == null) return null;
    if (concept.contains(":")) {
      return concept.substring(concept.indexOf(":") + 1);
    }
    return concept;
  }

  /**
   * Extract relationships from inline XBRL structure.
   */
  private void extractInlineXBRLRelationships(Document doc, Schema schema,
      List<GenericRecord> records, String filingDate) {

    // In inline XBRL, relationships are often implicit in the document structure
    // We can infer parent-child relationships from nesting

    NodeList allElements = doc.getElementsByTagName("*");
    Map<String, String> parentMap = new HashMap<>();

    for (int i = 0; i < allElements.getLength(); i++) {
      Element element = (Element) allElements.item(i);

      if (element.hasAttribute("contextRef")) {
        String concept = extractConceptName(element);

        // Check if this element has a parent with contextRef
        Element parent = (Element) element.getParentNode();
        while (parent != null && !parent.hasAttribute("contextRef")) {
          if (parent.getParentNode() instanceof Element) {
            parent = (Element) parent.getParentNode();
          } else {
            parent = null;
          }
        }

        if (parent != null) {
          String parentConcept = extractConceptName(parent);
          if (!concept.equals(parentConcept)) {
            // Create implicit parent-child relationship
            GenericRecord record = new GenericData.Record(schema);
            record.put("filing_date", filingDate);
            record.put("linkbase_type", "presentation");
            record.put("arc_role", "parent-child-implicit");
            record.put("from_concept", parentConcept);
            record.put("to_concept", concept);
            record.put("weight", null);
            record.put("order", i);  // Use document order
            record.put("preferred_label", null);
            records.add(record);
          }
        }
      }
    }
  }

  /**
   * Generic method to write records to Parquet.
   */
  private void writeRecordsToParquet(List<GenericRecord> records, Schema schema,
      File outputFile) throws IOException {

    @SuppressWarnings("deprecation")
    ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(new org.apache.hadoop.fs.Path(outputFile.toURI()))
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build();

    try {
      for (GenericRecord record : records) {
        writer.write(record);
      }
    } finally {
      writer.close();
    }
    
    // Clean up macOS metadata files after writing
    cleanupMacOSMetadataFiles(outputFile.getParentFile());
  }
  
  /**
   * Check if this is an insider trading form (Form 3, 4, or 5).
   */
  private boolean isInsiderForm(Document doc, String filingType) {
    // Check filing type first
    if (filingType != null && (filingType.equals("3") || filingType.equals("4") 
        || filingType.equals("5") || filingType.startsWith("3/") 
        || filingType.startsWith("4/") || filingType.startsWith("5/"))) {
      return true;
    }
    
    // Also check for ownershipDocument root element
    NodeList ownershipDocs = doc.getElementsByTagName("ownershipDocument");
    return ownershipDocs.getLength() > 0;
  }
  
  /**
   * Convert Form 3/4/5 insider trading forms to Parquet.
   */
  private List<File> convertInsiderForm(Document doc, File sourceFile, File targetDirectory,
      String cik, String filingType, String filingDate, String accession) throws IOException {
    List<File> outputFiles = new ArrayList<>();
    
    try {
      // Create partition directory
      // Validate and parse year from filing date
      int year;
      try {
        year = Integer.parseInt(filingDate.substring(0, 4));
        // Sanity check - SEC filings shouldn't be from before 1934 or in the future
        if (year < 1934 || year > java.time.Year.now().getValue()) {
          LOGGER.warning("Invalid year " + year + " for filing date " + filingDate + ", using current year");
          year = java.time.Year.now().getValue();
        }
      } catch (Exception e) {
        LOGGER.warning("Failed to parse year from filing date: " + filingDate + ", using current year");
        year = java.time.Year.now().getValue();
      }
      
      // Normalize filing type: remove both slashes and hyphens
      String normalizedFilingType = filingType.replace("/", "").replace("-", "");
      File partitionDir = new File(targetDirectory,
          String.format("cik=%s/filing_type=%s/year=%d", cik, normalizedFilingType, year));
      partitionDir.mkdirs();
      
      // Extract insider transactions
      List<GenericRecord> transactions = extractInsiderTransactions(doc, cik, filingType, filingDate);
      
      if (!transactions.isEmpty()) {
        // Write to Parquet - use accession for uniqueness if available
        String uniqueId = (accession != null && !accession.isEmpty()) ? accession : filingDate;
        File outputFile = new File(partitionDir,
            String.format("%s_%s_insider.parquet", cik, uniqueId));
        
        Schema schema = createInsiderTransactionSchema();
        writeParquetFile(transactions, schema, outputFile);
        cleanupMacOSMetadataFiles(outputFile.getParentFile());  // Clean macOS metadata after writing
        outputFiles.add(outputFile);
        
        LOGGER.info("Converted Form " + filingType + " to insider transactions: " 
            + transactions.size() + " records");
      }
      
      // Create vectorized blobs for insider forms if text similarity is enabled
      // Note: For now, we're creating a minimal vectorized file for insider forms
      // This could be enhanced to vectorize transaction narratives or remarks
      String uniqueId = (accession != null && !accession.isEmpty()) ? accession : filingDate;
      File vectorizedFile = new File(partitionDir,
          String.format("%s_%s_vectorized.parquet", cik, uniqueId));
      
      try {
        writeInsiderVectorizedBlobsToParquet(doc, vectorizedFile, cik, filingType, filingDate, sourceFile, accession);
        if (vectorizedFile.exists()) {
          outputFiles.add(vectorizedFile);
          cleanupMacOSMetadataFiles(vectorizedFile.getParentFile());
        }
      } catch (Exception ve) {
        LOGGER.warning("Failed to create vectorized blobs for insider form: " + ve.getMessage());
      }
      
    } catch (Exception e) {
      LOGGER.warning("Failed to convert insider form: " + e.getMessage());
    }
    
    return outputFiles;
  }
  
  /**
   * Extract insider transactions from Form 3/4/5.
   */
  private List<GenericRecord> extractInsiderTransactions(Document doc, String cik, 
      String filingType, String filingDate) {
    List<GenericRecord> records = new ArrayList<>();
    Schema schema = createInsiderTransactionSchema();
    
    // Extract reporting owner information
    String reportingPersonCik = getElementText(doc, "rptOwnerCik");
    String reportingPersonName = getElementText(doc, "rptOwnerName");
    
    // Extract relationship
    boolean isDirector = "1".equals(getElementText(doc, "isDirector"));
    boolean isOfficer = "1".equals(getElementText(doc, "isOfficer"));
    boolean isTenPercentOwner = "1".equals(getElementText(doc, "isTenPercentOwner"));
    String officerTitle = getElementText(doc, "officerTitle");
    
    // Extract non-derivative transactions (common stock)
    NodeList nonDerivTrans = doc.getElementsByTagName("nonDerivativeTransaction");
    for (int i = 0; i < nonDerivTrans.getLength(); i++) {
      Element trans = (Element) nonDerivTrans.item(i);
      
      GenericRecord record = new GenericData.Record(schema);
      record.put("cik", cik);
      record.put("filing_date", filingDate);
      record.put("filing_type", filingType);
      record.put("reporting_person_cik", reportingPersonCik);
      record.put("reporting_person_name", reportingPersonName);
      record.put("is_director", isDirector);
      record.put("is_officer", isOfficer);
      record.put("is_ten_percent_owner", isTenPercentOwner);
      record.put("officer_title", officerTitle);
      
      // Transaction details
      record.put("transaction_date", getElementText(trans, "transactionDate", "value"));
      record.put("transaction_code", getElementText(trans, "transactionCode"));
      record.put("security_title", getElementText(trans, "securityTitle", "value"));
      
      String shares = getElementText(trans, "transactionShares", "value");
      record.put("shares_transacted", shares != null ? Double.parseDouble(shares) : null);
      
      String price = getElementText(trans, "transactionPricePerShare", "value");
      record.put("price_per_share", price != null ? Double.parseDouble(price) : null);
      
      String sharesAfter = getElementText(trans, "sharesOwnedFollowingTransaction", "value");
      record.put("shares_owned_after", sharesAfter != null ? Double.parseDouble(sharesAfter) : null);
      
      String acquiredDisposed = getElementText(trans, "transactionAcquiredDisposedCode", "value");
      record.put("acquired_disposed_code", acquiredDisposed);
      
      String ownership = getElementText(trans, "directOrIndirectOwnership", "value");
      record.put("ownership_type", ownership);
      
      // Extract footnotes if any
      NodeList footnoteIds = trans.getElementsByTagName("footnoteId");
      StringBuilder footnotes = new StringBuilder();
      for (int j = 0; j < footnoteIds.getLength(); j++) {
        String id = ((Element) footnoteIds.item(j)).getAttribute("id");
        String footnoteText = getFootnoteText(doc, id);
        if (footnoteText != null) {
          if (footnotes.length() > 0) footnotes.append(" | ");
          footnotes.append(footnoteText);
        }
      }
      record.put("footnotes", footnotes.length() > 0 ? footnotes.toString() : null);
      
      records.add(record);
    }
    
    // Also extract holdings (non-transactional positions)
    NodeList holdings = doc.getElementsByTagName("nonDerivativeHolding");
    for (int i = 0; i < holdings.getLength(); i++) {
      Element holding = (Element) holdings.item(i);
      
      GenericRecord record = new GenericData.Record(schema);
      record.put("cik", cik);
      record.put("filing_date", filingDate);
      record.put("filing_type", filingType);
      record.put("reporting_person_cik", reportingPersonCik);
      record.put("reporting_person_name", reportingPersonName);
      record.put("is_director", isDirector);
      record.put("is_officer", isOfficer);
      record.put("is_ten_percent_owner", isTenPercentOwner);
      record.put("officer_title", officerTitle);
      
      // Holding details (no transaction)
      record.put("transaction_date", null);
      record.put("transaction_code", "H"); // H for holding
      record.put("security_title", getElementText(holding, "securityTitle", "value"));
      record.put("shares_transacted", null);
      record.put("price_per_share", null);
      
      String shares = getElementText(holding, "sharesOwnedFollowingTransaction", "value");
      record.put("shares_owned_after", shares != null ? Double.parseDouble(shares) : null);
      
      record.put("acquired_disposed_code", null);
      
      String ownership = getElementText(holding, "directOrIndirectOwnership", "value");
      record.put("ownership_type", ownership);
      
      String natureOfOwnership = getElementText(holding, "natureOfOwnership", "value");
      record.put("footnotes", natureOfOwnership);
      
      records.add(record);
    }
    
    return records;
  }
  
  /**
   * Create schema for insider transactions.
   */
  private Schema createInsiderTransactionSchema() {
    return SchemaBuilder.record("InsiderTransaction")
        .namespace("org.apache.calcite.adapter.sec")
        .fields()
        .requiredString("cik")
        .requiredString("filing_date")
        .requiredString("filing_type")
        .optionalString("reporting_person_cik")
        .optionalString("reporting_person_name")
        .requiredBoolean("is_director")
        .requiredBoolean("is_officer")
        .requiredBoolean("is_ten_percent_owner")
        .optionalString("officer_title")
        .optionalString("transaction_date")
        .optionalString("transaction_code")
        .optionalString("security_title")
        .optionalDouble("shares_transacted")
        .optionalDouble("price_per_share")
        .optionalDouble("shares_owned_after")
        .optionalString("acquired_disposed_code")
        .optionalString("ownership_type")
        .optionalString("footnotes")
        .endRecord();
  }
  
  /**
   * Helper to get element text with optional nested element.
   */
  private String getElementText(Element parent, String tagName, String nestedTag) {
    NodeList elements = parent.getElementsByTagName(tagName);
    if (elements.getLength() > 0) {
      Element elem = (Element) elements.item(0);
      if (nestedTag != null) {
        NodeList nested = elem.getElementsByTagName(nestedTag);
        if (nested.getLength() > 0) {
          return nested.item(0).getTextContent().trim();
        }
      } else {
        return elem.getTextContent().trim();
      }
    }
    return null;
  }
  
  /**
   * Helper to get element text.
   */
  private String getElementText(Document doc, String tagName) {
    NodeList elements = doc.getElementsByTagName(tagName);
    if (elements.getLength() > 0) {
      return elements.item(0).getTextContent().trim();
    }
    return null;
  }
  
  /**
   * Helper to get element text from parent.
   */
  private String getElementText(Element parent, String tagName) {
    return getElementText(parent, tagName, null);
  }
  
  /**
   * Get footnote text by ID.
   */
  private String getFootnoteText(Document doc, String footnoteId) {
    NodeList footnotes = doc.getElementsByTagName("footnote");
    for (int i = 0; i < footnotes.getLength(); i++) {
      Element footnote = (Element) footnotes.item(i);
      if (footnoteId.equals(footnote.getAttribute("id"))) {
        return footnote.getTextContent().trim();
      }
    }
    return null;
  }
  
  /**
   * Write Parquet file using Avro Generic Records.
   */
  @SuppressWarnings("deprecation")
  private void writeParquetFile(List<GenericRecord> records, Schema schema, File outputFile) 
      throws IOException {
    if (outputFile.getParentFile() != null) {
      outputFile.getParentFile().mkdirs();
    }
    
    Path outputPath = new Path(outputFile.toURI());
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
        .<GenericRecord>builder(outputPath)
        .withSchema(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {
      
      for (GenericRecord record : records) {
        writer.write(record);
      }
    }
    
    // Always clean up macOS metadata files after writing
    cleanupMacOSMetadataFiles(outputFile.getParentFile());
  }
  
  
  /**
   * Check if this is an 8-K filing.
   */
  private boolean is8KFiling(String filingType) {
    return filingType != null && (filingType.equals("8-K") || filingType.equals("8K") 
        || filingType.startsWith("8-K/"));
  }
  
  /**
   * Extract 8-K exhibits (particularly 99.1 and 99.2 for earnings).
   */
  /**
   * Write vectorized blobs for insider forms (Form 3/4/5).
   * Creates minimal vectors for remarks and transaction descriptions.
   */
  private void writeInsiderVectorizedBlobsToParquet(Document doc, File outputFile,
      String cik, String filingType, String filingDate, File sourceFile, String accession) throws IOException {
    
    // Create schema for vectorized blobs
    Schema embeddingField = SchemaBuilder.array().items().floatType();
    Schema schema = SchemaBuilder.record("VectorizedBlob")
        .fields()
        .requiredString("vector_id")
        .requiredString("original_blob_id")
        .requiredString("blob_type")
        .requiredString("blob_content")
        .name("embedding").type(embeddingField).noDefault()
        .requiredString("cik")
        .requiredString("filing_date")
        .requiredString("filing_type")
        .name("accession_number").type().nullable().stringType().noDefault()
        .endRecord();
    
    List<GenericRecord> records = new ArrayList<>();
    
    // Extract remarks and footnotes from insider forms
    NodeList remarks = doc.getElementsByTagName("remarks");
    NodeList footnotes = doc.getElementsByTagName("footnote");
    
    // Process remarks
    for (int i = 0; i < remarks.getLength(); i++) {
      String remarkText = remarks.item(i).getTextContent().trim();
      if (!remarkText.isEmpty() && remarkText.length() > 20) {
        GenericRecord record = new GenericData.Record(schema);
        String vectorId = UUID.randomUUID().toString();
        record.put("vector_id", vectorId);
        record.put("original_blob_id", "remark_" + i);
        record.put("blob_type", "insider_remark");
        record.put("blob_content", remarkText);
        
        // Generate simple embedding for insider forms
        // For now, use a simple hash-based approach as these are short texts
        List<Float> embedding = generateSimpleEmbedding(remarkText, 384);
        record.put("embedding", embedding);
        
        record.put("cik", cik);
        record.put("filing_date", filingDate);
        record.put("filing_type", filingType);
        record.put("accession_number", accession);
        
        records.add(record);
      }
    }
    
    // Process footnotes
    for (int i = 0; i < footnotes.getLength(); i++) {
      String footnoteText = footnotes.item(i).getTextContent().trim();
      if (!footnoteText.isEmpty() && footnoteText.length() > 20) {
        GenericRecord record = new GenericData.Record(schema);
        String vectorId = UUID.randomUUID().toString();
        record.put("vector_id", vectorId);
        record.put("original_blob_id", "footnote_" + i);
        record.put("blob_type", "insider_footnote");
        record.put("blob_content", footnoteText);
        
        // Generate simple embedding for insider forms
        List<Float> embedding = generateSimpleEmbedding(footnoteText, 384);
        record.put("embedding", embedding);
        
        record.put("cik", cik);
        record.put("filing_date", filingDate);
        record.put("filing_type", filingType);
        record.put("accession_number", accession);
        
        records.add(record);
      }
    }
    
    // Only write if we have some content to vectorize
    if (!records.isEmpty()) {
      writeRecordsToParquet(records, schema, outputFile);
      LOGGER.info("Wrote " + records.size() + " vectorized insider blobs to " + outputFile);
    }
  }
  
  private List<File> extract8KExhibits(File sourceFile, File targetDirectory,
      String cik, String filingType, String filingDate) {
    List<File> outputFiles = new ArrayList<>();
    
    try {
      // Parse the 8-K filing to find exhibits
      String fileContent = new String(Files.readAllBytes(sourceFile.toPath()), java.nio.charset.StandardCharsets.UTF_8);
      
      // Look for Exhibit 99.1 and 99.2 references
      List<GenericRecord> earningsRecords = new ArrayList<>();
      Schema earningsSchema = createEarningsTranscriptSchema();
      
      // Check if this file contains exhibit content directly
      if (fileContent.contains("EX-99.1") || fileContent.contains("EX-99.2")) {
        earningsRecords.addAll(extractEarningsFromExhibit(fileContent, cik, filingType, filingDate));
      }
      
      // Also check for earnings-related content patterns
      if (fileContent.toLowerCase().contains("financial results") 
          || fileContent.toLowerCase().contains("earnings release")
          || fileContent.toLowerCase().contains("conference call")) {
        
        // Extract paragraphs from earnings content
        List<String> paragraphs = extractEarningsParagraphs(fileContent);
        
        for (int i = 0; i < paragraphs.size(); i++) {
          GenericRecord record = new GenericData.Record(earningsSchema);
          record.put("cik", cik);
          record.put("filing_date", filingDate);
          record.put("filing_type", filingType);
          record.put("exhibit_number", detectExhibitNumber(fileContent));
          record.put("section_type", detectSectionType(paragraphs.get(i)));
          record.put("paragraph_number", i + 1);
          record.put("paragraph_text", paragraphs.get(i));
          record.put("speaker_name", extractSpeaker(paragraphs.get(i)));
          record.put("speaker_role", extractSpeakerRole(paragraphs.get(i)));
          
          earningsRecords.add(record);
        }
      }
      
      // Write earnings transcripts to Parquet if we found any
      if (!earningsRecords.isEmpty()) {
        // Create partition directory
        // Validate and parse year
        int year;
        try {
          year = Integer.parseInt(filingDate.substring(0, 4));
          if (year < 1934 || year > java.time.Year.now().getValue()) {
            LOGGER.warning("Invalid year " + year + " for filing date " + filingDate);
            year = java.time.Year.now().getValue();
          }
        } catch (Exception e) {
          LOGGER.warning("Failed to parse year from filing date: " + filingDate);
          year = java.time.Year.now().getValue();
        }
        
        String normalizedFilingType = filingType.replace("/", "").replace("-", "");
        File partitionDir = new File(targetDirectory,
            String.format("cik=%s/filing_type=%s/year=%d", cik, normalizedFilingType, year));
        partitionDir.mkdirs();
        
        File outputFile = new File(partitionDir,
            String.format("%s_%s_earnings.parquet", cik, filingDate));
        
        writeParquetFile(earningsRecords, earningsSchema, outputFile);
        cleanupMacOSMetadataFiles(outputFile.getParentFile());  // Clean macOS metadata after writing
        outputFiles.add(outputFile);
        
        LOGGER.info("Extracted " + earningsRecords.size() + " earnings paragraphs from 8-K");
      }
      
    } catch (Exception e) {
      LOGGER.warning("Failed to extract 8-K exhibits: " + e.getMessage());
    }
    
    return outputFiles;
  }
  
  /**
   * Extract earnings content from exhibit text.
   */
  private List<GenericRecord> extractEarningsFromExhibit(String exhibitContent,
      String cik, String filingType, String filingDate) {
    List<GenericRecord> records = new ArrayList<>();
    Schema schema = createEarningsTranscriptSchema();
    
    // Parse as HTML to extract text content
    org.jsoup.nodes.Document doc = Jsoup.parse(exhibitContent);
    
    // Remove script and style elements
    doc.select("script, style").remove();
    
    // Extract paragraphs
    Elements paragraphs = doc.select("p, div");
    
    int paragraphNum = 0;
    for (org.jsoup.nodes.Element para : paragraphs) {
      String text = para.text().trim();
      
      // Skip empty or very short paragraphs
      if (text.length() < 50) continue;
      
      // Skip boilerplate
      if (text.contains("FORWARD-LOOKING STATEMENTS") 
          || text.contains("Safe Harbor")
          || text.contains("Copyright")) continue;
      
      paragraphNum++;
      
      GenericRecord record = new GenericData.Record(schema);
      record.put("cik", cik);
      record.put("filing_date", filingDate);
      record.put("filing_type", filingType);
      record.put("exhibit_number", detectExhibitNumber(exhibitContent));
      record.put("section_type", detectSectionType(text));
      record.put("paragraph_number", paragraphNum);
      record.put("paragraph_text", text);
      record.put("speaker_name", extractSpeaker(text));
      record.put("speaker_role", extractSpeakerRole(text));
      
      records.add(record);
    }
    
    return records;
  }
  
  /**
   * Extract earnings-related paragraphs from text.
   */
  private List<String> extractEarningsParagraphs(String content) {
    List<String> paragraphs = new ArrayList<>();
    
    // Parse as HTML
    org.jsoup.nodes.Document doc = Jsoup.parse(content);
    
    // Look for earnings-related sections
    Elements relevantElements = doc.select("p, div");
    
    boolean inEarningsSection = false;
    for (org.jsoup.nodes.Element elem : relevantElements) {
      String text = elem.text().trim();
      
      // Start capturing when we find earnings indicators
      if (text.contains("financial results") || text.contains("earnings") 
          || text.contains("revenue") || text.contains("quarter")) {
        inEarningsSection = true;
      }
      
      // Capture relevant paragraphs
      if (inEarningsSection && text.length() > 50) {
        // Skip legal boilerplate
        if (!text.contains("forward-looking") && !text.contains("Safe Harbor")) {
          paragraphs.add(text);
        }
      }
      
      // Stop at certain sections
      if (text.contains("SIGNATURES") || text.contains("EXHIBIT INDEX")) {
        break;
      }
    }
    
    return paragraphs;
  }
  
  /**
   * Detect exhibit number from content.
   */
  private String detectExhibitNumber(String content) {
    if (content.contains("EX-99.1") || content.contains("Exhibit 99.1")) {
      return "99.1";
    } else if (content.contains("EX-99.2") || content.contains("Exhibit 99.2")) {
      return "99.2";
    }
    return null;
  }
  
  /**
   * Detect section type from paragraph content.
   */
  private String detectSectionType(String text) {
    String lowerText = text.toLowerCase();
    
    if (lowerText.contains("prepared remarks") || lowerText.contains("opening remarks")) {
      return "prepared_remarks";
    } else if (lowerText.contains("question") || lowerText.contains("answer")) {
      return "q_and_a";
    } else if (lowerText.contains("financial results") || lowerText.contains("earnings")) {
      return "earnings_release";
    } else if (lowerText.contains("conference call") || lowerText.contains("transcript")) {
      return "transcript";
    }
    
    return "other";
  }
  
  /**
   * Extract speaker name from paragraph (for transcripts).
   */
  private String extractSpeaker(String text) {
    // Look for patterns like "Name - Title:" or "Name:"
    Pattern speakerPattern = Pattern.compile("^([A-Z][a-z]+ [A-Z][a-z]+)\\s*[-:]");
    Matcher matcher = speakerPattern.matcher(text);
    
    if (matcher.find()) {
      return matcher.group(1);
    }
    
    return null;
  }
  
  /**
   * Extract speaker role from paragraph.
   */
  private String extractSpeakerRole(String text) {
    // Look for patterns like "Name - CEO:" or "Name, Chief Executive Officer:"
    Pattern rolePattern = Pattern.compile("[-,]\\s*([^:]+):");
    Matcher matcher = rolePattern.matcher(text);
    
    if (matcher.find()) {
      String role = matcher.group(1).trim();
      
      // Normalize common roles
      if (role.contains("Chief Executive") || role.contains("CEO")) {
        return "CEO";
      } else if (role.contains("Chief Financial") || role.contains("CFO")) {
        return "CFO";
      } else if (role.contains("Analyst")) {
        return "Analyst";
      } else if (role.contains("Operator")) {
        return "Operator";
      }
      
      return role;
    }
    
    return null;
  }
  
  /**
   * Create schema for earnings transcripts.
   */
  private Schema createEarningsTranscriptSchema() {
    return SchemaBuilder.record("EarningsTranscript")
        .namespace("org.apache.calcite.adapter.sec")
        .fields()
        .requiredString("cik")
        .requiredString("filing_date")
        .requiredString("filing_type")
        .optionalString("exhibit_number")
        .optionalString("section_type")
        .requiredInt("paragraph_number")
        .requiredString("paragraph_text")
        .optionalString("speaker_name")
        .optionalString("speaker_role")
        .endRecord();
  }

  /**
   * Write vectorized blobs with contextual enrichment to Parquet.
   * Creates individual vectors for footnotes and MD&A paragraphs with relationships.
   */
  private void writeVectorizedBlobsToParquet(Document doc, File outputFile,
      String cik, String filingType, String filingDate, File sourceFile) throws IOException {

    // Create schema for vectorized blobs
    Schema embeddingField = SchemaBuilder.array().items().floatType();
    Schema schema = SchemaBuilder.record("VectorizedBlob")
        .fields()
        .requiredString("vector_id")
        .requiredString("original_blob_id")
        .requiredString("blob_type")  // footnote, mda_paragraph, concept_group
        .requiredString("filing_date")
        .requiredString("original_text")
        .requiredString("enriched_text")
        .name("embedding").type(embeddingField).noDefault()  // 256-dimensional float array
        .optionalString("parent_section")
        .optionalString("relationships")  // JSON string of relationships
        .optionalString("financial_concepts")  // Comma-separated concepts
        .optionalInt("tokens_used")
        .optionalInt("token_budget")
        .endRecord();

    List<GenericRecord> records = new ArrayList<>();

    // Extract footnotes as TextBlobs
    List<SecTextVectorizer.TextBlob> footnoteBlobs = extractFootnoteBlobs(doc);
    
    // Extract MD&A paragraphs as TextBlobs
    List<SecTextVectorizer.TextBlob> mdaBlobs = extractMDABlobs(doc, sourceFile);
    
    // Build relationship map (who references whom)
    Map<String, List<String>> references = buildReferenceMap(footnoteBlobs, mdaBlobs);
    
    // Extract financial facts for enrichment
    Map<String, SecTextVectorizer.FinancialFact> facts = extractFinancialFactsForVectorization(doc);

    // Create vectorizer instance
    SecTextVectorizer vectorizer = new SecTextVectorizer();
    
    // Generate individual enriched chunks
    List<SecTextVectorizer.ContextualChunk> individualChunks = 
        vectorizer.createIndividualChunks(footnoteBlobs, mdaBlobs, references, facts);

    // Also generate concept group chunks (existing functionality)
    List<SecTextVectorizer.ContextualChunk> conceptChunks = 
        vectorizer.createContextualChunks(doc, sourceFile);
    
    // Combine all chunks
    List<SecTextVectorizer.ContextualChunk> allChunks = new ArrayList<>();
    allChunks.addAll(individualChunks);
    allChunks.addAll(conceptChunks);

    // Convert chunks to Parquet records
    for (SecTextVectorizer.ContextualChunk chunk : allChunks) {
      GenericRecord record = new GenericData.Record(schema);
      
      record.put("vector_id", chunk.context);
      record.put("original_blob_id", chunk.originalBlobId != null ? chunk.originalBlobId : chunk.context);
      record.put("blob_type", chunk.blobType);
      record.put("filing_date", filingDate);
      
      // Truncate texts if they're too long for Parquet
      String originalText = chunk.metadata.containsKey("original_text") ? 
          (String) chunk.metadata.get("original_text") : "";
      record.put("original_text", truncateText(originalText, 32000));
      record.put("enriched_text", truncateText(chunk.text, 32000));
      
      // Extract metadata
      record.put("parent_section", chunk.metadata.get("parent_section"));
      
      // Convert relationships to JSON string
      if (chunk.metadata.containsKey("referenced_by") || chunk.metadata.containsKey("references_footnotes")) {
        Map<String, Object> relationships = new HashMap<>();
        if (chunk.metadata.containsKey("referenced_by")) {
          relationships.put("referenced_by", chunk.metadata.get("referenced_by"));
        }
        if (chunk.metadata.containsKey("references_footnotes")) {
          relationships.put("references", chunk.metadata.get("references_footnotes"));
        }
        record.put("relationships", toJsonString(relationships));
      }
      
      // Financial concepts
      if (chunk.metadata.containsKey("financial_concepts")) {
        List<String> concepts = (List<String>) chunk.metadata.get("financial_concepts");
        record.put("financial_concepts", String.join(",", concepts));
      }
      
      // Token usage
      if (chunk.metadata.containsKey("tokens_used")) {
        record.put("tokens_used", chunk.metadata.get("tokens_used"));
      }
      if (chunk.metadata.containsKey("token_budget")) {
        record.put("token_budget", chunk.metadata.get("token_budget"));
      }
      
      // Add embedding vector (convert double[] to List<Float> for Avro)
      if (chunk.embedding != null && chunk.embedding.length > 0) {
        List<Float> embeddingList = new ArrayList<>();
        for (double value : chunk.embedding) {
          embeddingList.add((float) value);
        }
        record.put("embedding", embeddingList);
      } else {
        throw new IllegalStateException("Chunk missing embedding vector: " + chunk.context + 
            " (type: " + chunk.blobType + ")");
      }
      
      records.add(record);
    }

    // Write to Parquet if we have records
    if (!records.isEmpty()) {
      writeRecordsToParquet(records, schema, outputFile);
      LOGGER.info("Wrote " + records.size() + " vectorized blobs to " + outputFile);
      cleanupMacOSMetadataFiles(outputFile.getParentFile());
    }
  }

  /**
   * Extract footnotes as TextBlob objects for vectorization.
   */
  private List<SecTextVectorizer.TextBlob> extractFootnoteBlobs(Document doc) {
    List<SecTextVectorizer.TextBlob> blobs = new ArrayList<>();
    
    // Extract footnotes from both traditional XBRL and inline XBRL (iXBRL) TextBlock elements
    // These contain actual narrative text in XBRL filings
    NodeList allElements = doc.getElementsByTagName("*");
    int footnoteCounter = 0;
    
    for (int i = 0; i < allElements.getLength(); i++) {
      Element element = (Element) allElements.item(i);
      String localName = element.getLocalName();
      String namespaceURI = element.getNamespaceURI();
      
      String concept = null;
      String text = null;
      
      // Check for traditional XBRL TextBlock elements
      if (localName != null && localName.endsWith("TextBlock")) {
        text = element.getTextContent().trim();
        concept = extractConceptName(element);
      }
      // Check for inline XBRL (iXBRL) TextBlock elements
      else if ("nonNumeric".equals(localName) && namespaceURI != null 
               && namespaceURI.contains("xbrl")) {
        String name = element.getAttribute("name");
        if (name != null && name.contains("TextBlock")) {
          // For inline XBRL, the text content is in the element
          text = element.getTextContent().trim();
          // Extract concept from the name attribute (e.g., "us-gaap:RevenueRecognitionPolicyTextBlock")
          concept = name.contains(":") ? name.substring(name.indexOf(":") + 1) : name;
        }
      }
      
      // Process extracted text if found
      if (text != null && text.length() > 200 && !text.startsWith("<")) {
        String contextRef = element.getAttribute("contextRef");
        
        // Determine footnote section from concept name
        String parentSection = determineFootnoteSection(concept);
        
        if (parentSection != null) {
          String id = "footnote_" + (++footnoteCounter);
          
          // Create footnote blob with metadata
          Map<String, String> attributes = new HashMap<>();
          attributes.put("concept", concept != null ? concept : "");
          attributes.put("contextRef", contextRef != null ? contextRef : "");
          
          SecTextVectorizer.TextBlob blob = new SecTextVectorizer.TextBlob(
              id, "footnote", text, parentSection, null, attributes);
          blobs.add(blob);
          
          LOGGER.fine("Extracted footnote: " + id + " from concept: " + concept);
        }
      }
    }
    
    LOGGER.info("Extracted " + blobs.size() + " footnote blobs from XBRL");
    return blobs;
  }
  
  /**
   * Determine the section for a footnote based on its concept name.
   */
  private String determineFootnoteSection(String concept) {
    if (concept == null) return "Notes to Financial Statements";
    
    String lowerConcept = concept.toLowerCase();
    
    // Map common footnote concepts to sections
    if (lowerConcept.contains("accountingpolicy") || lowerConcept.contains("significantaccounting")) {
      return "Significant Accounting Policies";
    } else if (lowerConcept.contains("revenue")) {
      return "Revenue Recognition";
    } else if (lowerConcept.contains("debt") || lowerConcept.contains("borrowing")) {
      return "Debt and Credit Facilities";
    } else if (lowerConcept.contains("equity") || lowerConcept.contains("stock")) {
      return "Stockholders Equity";
    } else if (lowerConcept.contains("segment")) {
      return "Segment Information";  
    } else if (lowerConcept.contains("commitment") || lowerConcept.contains("contingenc")) {
      return "Commitments and Contingencies";
    } else if (lowerConcept.contains("acquisition") || lowerConcept.contains("businesscombination")) {
      return "Business Combinations";
    } else if (lowerConcept.contains("incometax") || lowerConcept.contains("tax")) {
      return "Income Taxes";
    } else if (lowerConcept.contains("pension") || lowerConcept.contains("retirement")) {
      return "Employee Benefits";
    } else if (lowerConcept.contains("derivative") || lowerConcept.contains("hedge")) {
      return "Derivatives and Hedging";
    } else if (lowerConcept.contains("fairvalue")) {
      return "Fair Value Measurements";
    } else if (lowerConcept.contains("lease")) {
      return "Leases";
    } else if (lowerConcept.contains("textblock") || lowerConcept.contains("disclosure")) {
      return "Notes to Financial Statements";
    }
    
    return "Notes to Financial Statements";  // Default section
  }

  /**
   * Extract MD&A paragraphs as TextBlob objects.
   */
  private List<SecTextVectorizer.TextBlob> extractMDABlobs(Document xbrlDoc, File sourceFile) {
    List<SecTextVectorizer.TextBlob> blobs = new ArrayList<>();
    
    // Extract MD&A from both traditional XBRL and inline XBRL (iXBRL)
    NodeList mdaElements = xbrlDoc.getElementsByTagName("*");
    int mdaCounter = 0;
    
    for (int i = 0; i < mdaElements.getLength(); i++) {
      Element element = (Element) mdaElements.item(i);
      String localName = element.getLocalName();
      String namespaceURI = element.getNamespaceURI();
      
      String elementName = null;
      String text = null;
      
      // Check for traditional XBRL MD&A elements
      if (localName != null && 
          (localName.contains("ManagementDiscussionAndAnalysis") ||
           localName.contains("MDAndA") ||
           localName.contains("Item7"))) {
        text = element.getTextContent().trim();
        elementName = localName;
      }
      // Check for inline XBRL (iXBRL) MD&A elements
      else if ("nonNumeric".equals(localName) && namespaceURI != null 
               && namespaceURI.contains("xbrl")) {
        String name = element.getAttribute("name");
        if (name != null && 
            (name.contains("ManagementDiscussionAndAnalysis") ||
             name.contains("MDAndA") ||
             name.contains("Item7"))) {
          text = element.getTextContent().trim();
          // Extract element name from the name attribute
          elementName = name.contains(":") ? name.substring(name.indexOf(":") + 1) : name;
        }
      }
      
      // Process extracted text if found
      if (text != null && text.length() > 200) {
        // Split into paragraphs for better vectorization
        String[] paragraphs = text.split("\\n\\n+|(?<=\\.)\\s+(?=[A-Z])");
        
        for (String paragraph : paragraphs) {
          paragraph = paragraph.trim();
          if (paragraph.length() > 100 && !paragraph.startsWith("<")) {
            String id = "mda_para_" + (++mdaCounter);
            String parentSection = "Management Discussion and Analysis";
            String subsection = detectMDASubsection(paragraph);
            
            Map<String, String> attributes = new HashMap<>();
            attributes.put("source", "xbrl");
            attributes.put("element", elementName != null ? elementName : "");
            
            SecTextVectorizer.TextBlob blob = new SecTextVectorizer.TextBlob(
                id, "mda_paragraph", paragraph, parentSection, subsection, attributes);
            blobs.add(blob);
          }
        }
      }
    }
    
    // If no MD&A in XBRL and source is HTML, extract from HTML
    if (blobs.isEmpty() && (sourceFile.getName().endsWith(".htm") || 
                            sourceFile.getName().endsWith(".html"))) {
      blobs.addAll(extractMDAFromHtml(sourceFile));
    }
    
    // If still no MD&A and this is XBRL, look for associated HTML filing
    if (blobs.isEmpty() && sourceFile.getName().endsWith(".xml")) {
      File htmlFile = findAssociatedHtmlFiling(sourceFile);
      if (htmlFile != null && htmlFile.exists()) {
        blobs.addAll(extractMDAFromHtml(htmlFile));
      }
    }
    
    LOGGER.info("Extracted " + blobs.size() + " MD&A paragraphs");
    return blobs;
  }
  
  /**
   * Find the associated HTML filing for an XBRL file.
   */
  private File findAssociatedHtmlFiling(File xbrlFile) {
    // Look for 10-K or 10-Q HTML in same directory
    File parentDir = xbrlFile.getParentFile();
    if (parentDir != null) {
      File[] htmlFiles = parentDir.listFiles((dir, name) -> 
        (name.endsWith(".htm") || name.endsWith(".html")) &&
        !name.contains("_cal") && !name.contains("_def") && !name.contains("_lab") &&
        !name.contains("_pre") && !name.contains("_ref"));
      
      if (htmlFiles != null && htmlFiles.length > 0) {
        // Prefer files with 10-K or 10-Q in name
        for (File file : htmlFiles) {
          String name = file.getName().toLowerCase();
          if (name.contains("10-k") || name.contains("10k") ||
              name.contains("10-q") || name.contains("10q")) {
            return file;
          }
        }
        return htmlFiles[0];  // Return first HTML file if no specific match
      }
    }
    return null;
  }
  
  /**
   * Extract MD&A from HTML filing.
   */
  private List<SecTextVectorizer.TextBlob> extractMDAFromHtml(File htmlFile) {
    List<SecTextVectorizer.TextBlob> blobs = new ArrayList<>();
    
    try {
      org.jsoup.nodes.Document htmlDoc = Jsoup.parse(htmlFile, "UTF-8");
      
      // Find Item 7 or MD&A sections
      Elements mdaSections = htmlDoc.select(
        "*:matchesOwn((?i)(item\\s*7[^0-9]|management.*discussion.*analysis))");
      
      int paraCounter = 0;
      for (org.jsoup.nodes.Element section : mdaSections) {
        // Skip if this is table of contents
        if (section.parents().select("table").size() > 0) continue;
        
        // Extract following content until next major item
        org.jsoup.nodes.Element current = section;
        int localCounter = 0;
        boolean inMDA = true;
        
        while (current != null && localCounter < 200 && inMDA) {
          current = current.nextElementSibling();
          if (current == null) break;
          
          String text = current.text().trim();
          
          // Stop at next item
          if (isNextItem(text)) {
            inMDA = false;
            break;
          }
          
          // Extract meaningful paragraphs
          if (text.length() > 100 && !isBoilerplate(text)) {
            String id = "mda_para_" + (++paraCounter);
            String parentSection = "Management Discussion and Analysis";
            String subsection = detectMDASubsection(text);
            
            Map<String, String> attributes = new HashMap<>();
            attributes.put("source", "html");
            attributes.put("file", htmlFile.getName());
            
            SecTextVectorizer.TextBlob blob = new SecTextVectorizer.TextBlob(
                id, "mda_paragraph", text, parentSection, subsection, attributes);
            blobs.add(blob);
          }
          
          localCounter++;
        }
      }
    } catch (IOException e) {
      LOGGER.warning("Failed to parse HTML for MD&A: " + e.getMessage());
    }
    
    return blobs;
  }

  /**
   * Detect MD&A subsection from paragraph content.
   */
  private String detectMDASubsection(String text) {
    if (text == null || text.length() < 20) return null;
    
    String lower = text.toLowerCase();
    
    // Check for common MD&A subsections
    if (lower.contains("overview") || lower.contains("executive summary")) {
      return "Overview";
    } else if (lower.contains("results") && lower.contains("operation")) {
      return "Results of Operations";
    } else if (lower.contains("liquidity") && lower.contains("capital")) {
      return "Liquidity and Capital Resources";
    } else if (lower.contains("liquidity")) {
      return "Liquidity";
    } else if (lower.contains("capital") && (lower.contains("resource") || lower.contains("cash flow"))) {
      return "Capital Resources";
    } else if (lower.contains("critical") && lower.contains("accounting")) {
      return "Critical Accounting Policies";
    } else if (lower.contains("risk factor") || lower.contains("uncertainties")) {
      return "Risk Factors";
    } else if (lower.contains("outlook") || lower.contains("guidance")) {
      return "Outlook";
    } else if (lower.contains("segment") || lower.contains("geographic")) {
      return "Segment Analysis";
    }
    
    return null;  // No specific subsection detected
  }
  
  // Method already exists above - removing duplicate
  
  /**
   * Check if text is boilerplate that should be skipped.
   */
  private boolean isBoilerplate(String text) {
    if (text == null || text.isEmpty()) return true;
    String lower = text.toLowerCase();
    return lower.contains("forward-looking statements") ||
           lower.contains("safe harbor") ||
           lower.contains("table of contents") ||
           lower.contains("exhibit index") ||
           lower.startsWith("page ") ||
           (lower.length() < 20 && lower.matches("\\d+"));
  }

  /**
   * Build a map of footnote references from MD&A paragraphs.
   */
  private Map<String, List<String>> buildReferenceMap(
      List<SecTextVectorizer.TextBlob> footnotes,
      List<SecTextVectorizer.TextBlob> mdaBlobs) {
    
    Map<String, List<String>> references = new HashMap<>();
    
    // Check each MD&A paragraph for footnote references
    for (SecTextVectorizer.TextBlob mdaBlob : mdaBlobs) {
      java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
          "(?:Note|Footnote)\\s+(\\d+[A-Za-z]?)", java.util.regex.Pattern.CASE_INSENSITIVE);
      java.util.regex.Matcher matcher = pattern.matcher(mdaBlob.text);
      
      while (matcher.find()) {
        String footnoteRef = "footnote_" + matcher.group(1);
        
        // Add MD&A paragraph to the footnote's reference list
        references.computeIfAbsent(footnoteRef, k -> new ArrayList<>()).add(mdaBlob.id);
      }
    }
    
    return references;
  }

  /**
   * Extract financial facts for vectorization enrichment.
   */
  private Map<String, SecTextVectorizer.FinancialFact> extractFinancialFactsForVectorization(Document doc) {
    // This method would need to be made accessible from SecTextVectorizer
    // For now, return empty map - the vectorizer will handle this internally
    return new HashMap<>();
  }

  /**
   * Generate a simple embedding for short text (used for insider forms).
   * This is a placeholder implementation that creates deterministic vectors.
   */
  private List<Float> generateSimpleEmbedding(String text, int dimension) {
    List<Float> embedding = new ArrayList<>(dimension);
    
    // Simple hash-based approach for deterministic embeddings
    int hash = text.hashCode();
    java.util.Random random = new java.util.Random(hash);
    
    for (int i = 0; i < dimension; i++) {
      embedding.add(random.nextFloat() * 2.0f - 1.0f); // Range [-1, 1]
    }
    
    // Normalize the vector
    float sum = 0;
    for (float val : embedding) {
      sum += val * val;
    }
    float norm = (float) Math.sqrt(sum);
    if (norm > 0) {
      for (int i = 0; i < embedding.size(); i++) {
        embedding.set(i, embedding.get(i) / norm);
      }
    }
    
    return embedding;
  }
  
  /**
   * Convert object to JSON string.
   */
  private String toJsonString(Object obj) {
    // Simple JSON conversion - in production would use Jackson or Gson
    if (obj instanceof Map) {
      Map<String, Object> map = (Map<String, Object>) obj;
      StringBuilder json = new StringBuilder("{");
      boolean first = true;
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        if (!first) json.append(",");
        json.append("\"").append(entry.getKey()).append("\":");
        if (entry.getValue() instanceof List) {
          json.append("[");
          List<?> list = (List<?>) entry.getValue();
          for (int i = 0; i < list.size(); i++) {
            if (i > 0) json.append(",");
            json.append("\"").append(list.get(i)).append("\"");
          }
          json.append("]");
        } else {
          json.append("\"").append(entry.getValue()).append("\"");
        }
        first = false;
      }
      json.append("}");
      return json.toString();
    }
    return obj != null ? obj.toString() : null;
  }

  /**
   * Truncate text to fit Parquet string limits.
   */
  private String truncateText(String text, int maxLength) {
    if (text == null || text.length() <= maxLength) {
      return text;
    }
    return text.substring(0, maxLength - 3) + "...";
  }
}
