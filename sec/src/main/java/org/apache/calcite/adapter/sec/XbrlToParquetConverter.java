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

import org.apache.calcite.adapter.file.converters.FileConverter;
import org.apache.calcite.adapter.file.metadata.ConversionMetadata;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.commons.text.StringEscapeUtils;
import org.jsoup.Jsoup;

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
      
      // Create partitioned output path
      File partitionDir = new File(targetDirectory, 
          String.format("cik=%s/filing_type=%s/year=%s", 
              cik, filingType.replace("-", ""), 
              filingDate.substring(0, 4)));
      partitionDir.mkdirs();
      
      // Convert financial facts to Parquet
      File factsFile = new File(partitionDir, 
          String.format("%s_%s_facts.parquet", cik, filingDate));
      writeFactsToParquet(doc, factsFile, cik, filingType, filingDate);
      outputFiles.add(factsFile);
      
      // Convert contexts to Parquet
      File contextsFile = new File(partitionDir, 
          String.format("%s_%s_contexts.parquet", cik, filingDate));
      writeContextsToParquet(doc, contextsFile, cik, filingType, filingDate);
      outputFiles.add(contextsFile);
      
      // Metadata is updated by FileConversionManager after successful conversion
      
      LOGGER.info("Converted XBRL to Parquet: " + sourceFile.getName() + 
          " -> " + outputFiles.size() + " files");
      
    } catch (Exception e) {
      throw new IOException("Failed to convert XBRL to Parquet", e);
    }
    
    return outputFiles;
  }

  private String extractCik(Document doc, File sourceFile) {
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
    
    // Fall back to filename parsing
    String filename = sourceFile.getName();
    if (filename.matches("\\d{10}_.*")) {
      return filename.substring(0, 10);
    }
    
    return "0000000000"; // Unknown CIK
  }

  private String extractFilingType(Document doc, File sourceFile) {
    // Try to extract from document type
    NodeList documentTypes = doc.getElementsByTagNameNS("*", "DocumentType");
    if (documentTypes.getLength() > 0) {
      return documentTypes.item(0).getTextContent();
    }
    
    // Fall back to filename parsing
    String filename = sourceFile.getName();
    if (filename.contains("10K") || filename.contains("10-K")) return "10-K";
    if (filename.contains("10Q") || filename.contains("10-Q")) return "10-Q";
    if (filename.contains("8K") || filename.contains("8-K")) return "8-K";
    
    return "UNKNOWN";
  }

  private String extractFilingDate(Document doc, File sourceFile) {
    // Try to extract from document period end date
    NodeList periodEnds = doc.getElementsByTagNameNS("*", "DocumentPeriodEndDate");
    if (periodEnds.getLength() > 0) {
      return periodEnds.item(0).getTextContent();
    }
    
    // Fall back to filename parsing or current date
    String filename = sourceFile.getName();
    if (filename.matches(".*\\d{8}.*")) {
      // Extract YYYYMMDD pattern
      return filename.replaceAll(".*?(\\d{8}).*", "$1");
    }
    
    return LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);
  }

  private void writeFactsToParquet(Document doc, File outputFile, 
      String cik, String filingType, String filingDate) throws IOException {
    
    // Create Avro schema for facts
    Schema schema = SchemaBuilder.record("XbrlFact")
        .fields()
        .requiredString("cik")
        .requiredString("filing_type")
        .requiredString("filing_date")
        .requiredString("concept")
        .optionalString("context_ref")
        .optionalString("unit_ref")
        .optionalString("value")
        .optionalDouble("numeric_value")
        .optionalString("period_start")
        .optionalString("period_end")
        .optionalBoolean("is_instant")
        .endRecord();
    
    // Extract all fact elements
    List<GenericRecord> records = new ArrayList<>();
    NodeList allElements = doc.getElementsByTagName("*");
    
    for (int i = 0; i < allElements.getLength(); i++) {
      Element element = (Element) allElements.item(i);
      
      // Skip non-fact elements
      if (element.hasAttribute("contextRef")) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("cik", cik);
        record.put("filing_type", filingType);
        record.put("filing_date", filingDate);
        record.put("concept", element.getLocalName());
        record.put("context_ref", element.getAttribute("contextRef"));
        record.put("unit_ref", element.getAttribute("unitRef"));
        
        // Clean HTML from text content (for footnotes, MD&A, etc.)
        String rawValue = element.getTextContent().trim();
        String cleanValue = cleanHtmlText(rawValue);
        record.put("value", cleanValue);
        
        // Try to parse as numeric (using cleaned value)
        try {
          double numValue = Double.parseDouble(
              cleanValue.replaceAll(",", ""));
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
  }

  private void writeContextsToParquet(Document doc, File outputFile,
      String cik, String filingType, String filingDate) throws IOException {
    
    // Create Avro schema for contexts
    Schema schema = SchemaBuilder.record("XbrlContext")
        .fields()
        .requiredString("cik")
        .requiredString("filing_type")
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
      
      record.put("cik", cik);
      record.put("filing_type", filingType);
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
   * Parse inline XBRL from HTML file.
   * Inline XBRL uses ix: namespace tags embedded in HTML.
   */
  private Document parseInlineXbrl(File htmlFile) {
    try {
      // Read HTML file
      String html = new String(Files.readAllBytes(htmlFile.toPath()));
      
      // Parse with Jsoup
      org.jsoup.nodes.Document jsoupDoc = Jsoup.parse(html);
      
      // Look for inline XBRL elements (ix: namespace)
      // Common tags: ix:nonnumeric, ix:nonfraction, ix:continuation
      org.jsoup.select.Elements ixElements = jsoupDoc.select("[*|nonnumeric], [*|nonfraction], [*|continuation]");
      
      if (ixElements.isEmpty()) {
        // No inline XBRL found
        return null;
      }
      
      // Create a new XML document from inline XBRL elements
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      factory.setNamespaceAware(true);
      DocumentBuilder builder = factory.newDocumentBuilder();
      Document doc = builder.newDocument();
      
      // Create root element
      Element root = doc.createElement("xbrl");
      doc.appendChild(root);
      
      // Extract contexts from HTML
      org.jsoup.select.Elements contexts = jsoupDoc.select("[*|context]");
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
}