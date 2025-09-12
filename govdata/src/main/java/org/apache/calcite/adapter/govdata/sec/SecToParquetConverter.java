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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

/**
 * Converts XBRL instance documents to partitioned Parquet files.
 *
 * <p>Partitioning strategy: CIK/filing-type/date
 * <p>Output tables:
 * <ul>
 *   <li>financial_line_items - Financial statement line items with values</li>
 *   <li>footnotes - Footnotes linked to line items</li>
 *   <li>company_info - Company metadata</li>
 * </ul>
 */
public class SecToParquetConverter {
  private static final Logger LOGGER = Logger.getLogger(SecToParquetConverter.class.getName());

  private final File sourceDirectory;
  private final File targetDirectory;
  private final EdgarPartitionStrategy partitioner;
  private final SecTextVectorizer vectorizer = new SecTextVectorizer();

  // Avro schemas for output tables
  private static final String FINANCIAL_LINE_ITEMS_SCHEMA = "{"
      + "\"type\": \"record\","
      + "\"name\": \"FinancialLineItem\","
      + "\"fields\": ["
      + "  {\"name\": \"cik\", \"type\": \"string\"},"
      + "  {\"name\": \"filing_type\", \"type\": \"string\"},"
      + "  {\"name\": \"filing_date\", \"type\": \"string\"},"
      + "  {\"name\": \"period_end\", \"type\": \"string\"},"
      + "  {\"name\": \"concept\", \"type\": \"string\"},"
      + "  {\"name\": \"label\", \"type\": [\"null\", \"string\"], \"default\": null},"
      + "  {\"name\": \"value\", \"type\": [\"null\", \"double\"], \"default\": null},"
      + "  {\"name\": \"unit\", \"type\": [\"null\", \"string\"], \"default\": null},"
      + "  {\"name\": \"decimals\", \"type\": [\"null\", \"int\"], \"default\": null},"
      + "  {\"name\": \"parent_concept\", \"type\": [\"null\", \"string\"], \"default\": null},"
      + "  {\"name\": \"hierarchy_level\", \"type\": \"int\"},"
      + "  {\"name\": \"presentation_order\", \"type\": \"int\"}"
      + "]}";

  private static final String FOOTNOTES_SCHEMA = "{"
      + "\"type\": \"record\","
      + "\"name\": \"Footnote\","
      + "\"fields\": ["
      + "  {\"name\": \"cik\", \"type\": \"string\"},"
      + "  {\"name\": \"filing_type\", \"type\": \"string\"},"
      + "  {\"name\": \"filing_date\", \"type\": \"string\"},"
      + "  {\"name\": \"footnote_id\", \"type\": \"string\"},"
      + "  {\"name\": \"line_item_concept\", \"type\": [\"null\", \"string\"], \"default\": null},"
      + "  {\"name\": \"footnote_text\", \"type\": \"string\"},"
      + "  {\"name\": \"footnote_type\", \"type\": [\"null\", \"string\"], \"default\": null}"
      + "]}";

  private static final String COMPANY_INFO_SCHEMA = "{"
      + "\"type\": \"record\","
      + "\"name\": \"CompanyInfo\","
      + "\"fields\": ["
      + "  {\"name\": \"cik\", \"type\": \"string\"},"
      + "  {\"name\": \"filing_type\", \"type\": \"string\"},"
      + "  {\"name\": \"filing_date\", \"type\": \"string\"},"
      + "  {\"name\": \"company_name\", \"type\": \"string\"},"
      + "  {\"name\": \"sic_code\", \"type\": [\"null\", \"string\"], \"default\": null},"
      + "  {\"name\": \"state_of_incorporation\", \"type\": [\"null\", \"string\"], \"default\": null},"
      + "  {\"name\": \"fiscal_year_end\", \"type\": [\"null\", \"string\"], \"default\": null},"
      + "  {\"name\": \"business_address\", \"type\": [\"null\", \"string\"], \"default\": null},"
      + "  {\"name\": \"business_phone\", \"type\": [\"null\", \"string\"], \"default\": null}"
      + "]}";

  private static final String EMBEDDINGS_SCHEMA = "{"
      + "\"type\": \"record\","
      + "\"name\": \"DocumentEmbedding\","
      + "\"fields\": ["
      + "  {\"name\": \"cik\", \"type\": \"string\"},"
      + "  {\"name\": \"filing_type\", \"type\": \"string\"},"
      + "  {\"name\": \"filing_date\", \"type\": \"string\"},"
      + "  {\"name\": \"chunk_context\", \"type\": \"string\"},"
      + "  {\"name\": \"chunk_text\", \"type\": \"string\"},"
      + "  {\"name\": \"chunk_length\", \"type\": \"int\"},"
      + "  {\"name\": \"embedding_vector\", \"type\": \"string\"},"
      + "  {\"name\": \"has_revenue_data\", \"type\": \"boolean\"},"
      + "  {\"name\": \"has_risk_factors\", \"type\": \"boolean\"},"
      + "  {\"name\": \"has_footnotes\", \"type\": \"boolean\"}"
      + "]}";

  public SecToParquetConverter(File sourceDirectory, File targetDirectory) {
    this.sourceDirectory = sourceDirectory;
    this.targetDirectory = targetDirectory;
    this.partitioner = new EdgarPartitionStrategy();

    // Ensure target directory exists
    targetDirectory.mkdirs();
  }

  public File getSourceDirectory() {
    return sourceDirectory;
  }

  /**
   * Process all XBRL files in the source directory.
   */
  public int processAllSecFiles() throws IOException {
    if (!sourceDirectory.exists() || !sourceDirectory.isDirectory()) {
      LOGGER.warning("XBRL source directory does not exist: " + sourceDirectory);
      return 0;
    }

    File[] secFiles = sourceDirectory.listFiles((dir, name) ->
        !name.startsWith(".") && // Skip hidden files (Mac ._ files)
        (name.toLowerCase().endsWith(".xml") || name.toLowerCase().endsWith(".sec")));

    if (secFiles == null || secFiles.length == 0) {
      LOGGER.info("No XBRL files found in: " + sourceDirectory);
      return 0;
    }

    int processed = 0;
    for (File secFile : secFiles) {
      try {
        processSecFile(secFile);
        processed++;
      } catch (Exception e) {
        LOGGER.warning("Failed to process XBRL file " + secFile.getName() + ": " + e.getMessage());
      }
    }

    return processed;
  }

  /**
   * Process a single XBRL file and convert to partitioned Parquet.
   */
  public void processSecFile(File secFile) throws Exception {
    LOGGER.fine("Processing XBRL file: " + secFile.getName());

    // Parse XBRL document
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    DocumentBuilder builder = factory.newDocumentBuilder();
    Document doc = builder.parse(secFile);

    // Extract filing metadata
    FilingMetadata metadata = extractFilingMetadata(doc, secFile.getName());

    // Extract data from XBRL
    List<GenericRecord> lineItems = extractFinancialLineItems(doc, metadata);
    List<GenericRecord> footnotes = extractFootnotes(doc, metadata);
    GenericRecord companyInfo = extractCompanyInfo(doc, metadata);

    // Create contextual chunks for embeddings
    List<SecTextVectorizer.ContextualChunk> chunks = vectorizer.createContextualChunks(doc, secFile);
    List<GenericRecord> embeddings = createEmbeddingRecords(chunks, metadata);

    // Write to partitioned Parquet files
    writePartitionedParquet(lineItems, "financial_line_items", metadata,
        new Schema.Parser().parse(FINANCIAL_LINE_ITEMS_SCHEMA));
    writePartitionedParquet(footnotes, "footnotes", metadata,
        new Schema.Parser().parse(FOOTNOTES_SCHEMA));

    List<GenericRecord> companyList = new ArrayList<>();
    if (companyInfo != null) {
      companyList.add(companyInfo);
    }
    writePartitionedParquet(companyList, "company_info", metadata,
        new Schema.Parser().parse(COMPANY_INFO_SCHEMA));

    // Write embeddings
    writePartitionedParquet(embeddings, "document_embeddings", metadata,
        new Schema.Parser().parse(EMBEDDINGS_SCHEMA));
  }

  private FilingMetadata extractFilingMetadata(Document doc, String filename) {
    // Extract from XBRL context or filename pattern
    // Format expected: CIK-YYYYMMDD-FormType.xml
    FilingMetadata metadata = new FilingMetadata();

    // Try to extract from filename first
    // Handle format: 0000320193-20230930-10-K.xml or 0000320193-20230930-DEF 14A.xml
    String nameWithoutExt = filename.replace(".xml", "").replace(".sec", "");

    // Split only on first two hyphens to preserve filing type
    int firstHyphen = nameWithoutExt.indexOf('-');
    int secondHyphen = nameWithoutExt.indexOf('-', firstHyphen + 1);

    if (firstHyphen > 0 && secondHyphen > firstHyphen) {
      metadata.cik = nameWithoutExt.substring(0, firstHyphen);
      metadata.filingDate = nameWithoutExt.substring(firstHyphen + 1, secondHyphen);
      metadata.filingType = nameWithoutExt.substring(secondHyphen + 1);
    } else {
      // If filename doesn't match expected pattern, skip the file
      // Don't create invalid partitions with default values
      LOGGER.warning("Filename doesn't match expected pattern, will skip partition creation: " + filename);
      metadata.cik = null;  // Will check this later and skip if null
      metadata.filingDate = null;
      metadata.filingType = "UNKNOWN";
    }

    // Try to extract from XBRL document
    NodeList contexts = doc.getElementsByTagNameNS("*", "context");
    if (contexts.getLength() > 0) {
      Element context = (Element) contexts.item(0);
      NodeList entities = context.getElementsByTagNameNS("*", "identifier");
      if (entities.getLength() > 0) {
        String identifier = entities.item(0).getTextContent();
        if (identifier.length() == 10 && identifier.matches("\\d+")) {
          metadata.cik = identifier;
        }
      }
    }

    return metadata;
  }

  private List<GenericRecord> extractFinancialLineItems(Document doc, FilingMetadata metadata) {
    List<GenericRecord> records = new ArrayList<>();
    Schema schema = new Schema.Parser().parse(FINANCIAL_LINE_ITEMS_SCHEMA);

    // Extract all numeric facts from XBRL
    NodeList facts = doc.getElementsByTagName("*");
    int presentationOrder = 0;

    for (int i = 0; i < facts.getLength(); i++) {
      Element fact = (Element) facts.item(i);

      // Check if this is a numeric fact (has unitRef attribute)
      if (fact.hasAttribute("unitRef")) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("cik", metadata.cik);
        record.put("filing_type", metadata.filingType);
        record.put("filing_date", metadata.filingDate);

        // Extract period from contextRef
        String contextRef = fact.getAttribute("contextRef");
        record.put("period_end", extractPeriodEnd(doc, contextRef));

        // Concept name is the tag name
        String concept = fact.getLocalName();
        if (concept == null) {
          concept = fact.getTagName();
        }
        record.put("concept", concept);

        // Extract label if available
        record.put("label", getConceptLabel(doc, concept));

        // Extract numeric value
        try {
          double value = Double.parseDouble(fact.getTextContent());
          record.put("value", value);
        } catch (NumberFormatException e) {
          record.put("value", null);
        }

        // Extract unit
        record.put("unit", fact.getAttribute("unitRef"));

        // Extract decimals
        String decimals = fact.getAttribute("decimals");
        if (!decimals.isEmpty()) {
          try {
            record.put("decimals", Integer.parseInt(decimals));
          } catch (NumberFormatException e) {
            record.put("decimals", null);
          }
        } else {
          record.put("decimals", null);
        }

        // Parent concept and hierarchy would come from presentation linkbase
        record.put("parent_concept", null);
        record.put("hierarchy_level", 0);
        record.put("presentation_order", presentationOrder++);

        records.add(record);
      }
    }

    return records;
  }

  private List<GenericRecord> extractFootnotes(Document doc, FilingMetadata metadata) {
    List<GenericRecord> records = new ArrayList<>();
    Schema schema = new Schema.Parser().parse(FOOTNOTES_SCHEMA);

    // Look for footnote elements
    NodeList footnotes = doc.getElementsByTagNameNS("*", "footnote");

    for (int i = 0; i < footnotes.getLength(); i++) {
      Element footnote = (Element) footnotes.item(i);

      GenericRecord record = new GenericData.Record(schema);
      record.put("cik", metadata.cik);
      record.put("filing_type", metadata.filingType);
      record.put("filing_date", metadata.filingDate);

      String footnoteId = footnote.getAttribute("id");
      if (footnoteId.isEmpty()) {
        footnoteId = "footnote_" + i;
      }
      record.put("footnote_id", footnoteId);

      // Try to link to concept via footnoteLink
      String linkedConcept = footnote.getAttribute("xlink:label");
      record.put("line_item_concept", linkedConcept.isEmpty() ? null : linkedConcept);

      record.put("footnote_text", footnote.getTextContent());
      record.put("footnote_type", footnote.getAttribute("xlink:role"));

      records.add(record);
    }

    return records;
  }

  private GenericRecord extractCompanyInfo(Document doc, FilingMetadata metadata) {
    Schema schema = new Schema.Parser().parse(COMPANY_INFO_SCHEMA);
    GenericRecord record = new GenericData.Record(schema);

    record.put("cik", metadata.cik);
    record.put("filing_type", metadata.filingType);
    record.put("filing_date", metadata.filingDate);

    // Extract company name
    String companyName = extractElementValue(doc, "EntityRegistrantName", "RegistrantName");
    record.put("company_name", companyName != null ? companyName : "Unknown");

    // Extract other company info
    record.put("sic_code", extractElementValue(doc, "EntityCentralIndexKey", "SicCode"));
    record.put("state_of_incorporation", extractElementValue(doc, "EntityIncorporationStateCountryCode", "StateOfIncorporation"));
    record.put("fiscal_year_end", extractElementValue(doc, "CurrentFiscalYearEndDate", "FiscalYearEnd"));
    record.put("business_address", extractElementValue(doc, "BusinessAddress", "Address"));
    record.put("business_phone", extractElementValue(doc, "PhoneNumber", "Phone"));

    return record;
  }

  private String extractElementValue(Document doc, String... tagNames) {
    for (String tagName : tagNames) {
      NodeList elements = doc.getElementsByTagNameNS("*", tagName);
      if (elements.getLength() > 0) {
        return elements.item(0).getTextContent();
      }
    }
    return null;
  }

  private String extractPeriodEnd(Document doc, String contextRef) {
    if (contextRef == null || contextRef.isEmpty()) {
      return LocalDate.now().toString();
    }

    NodeList contexts = doc.getElementsByTagNameNS("*", "context");
    for (int i = 0; i < contexts.getLength(); i++) {
      Element context = (Element) contexts.item(i);
      if (contextRef.equals(context.getAttribute("id"))) {
        NodeList periods = context.getElementsByTagNameNS("*", "instant");
        if (periods.getLength() > 0) {
          return periods.item(0).getTextContent();
        }
        periods = context.getElementsByTagNameNS("*", "endDate");
        if (periods.getLength() > 0) {
          return periods.item(0).getTextContent();
        }
      }
    }

    return LocalDate.now().toString();
  }

  private String getConceptLabel(Document doc, String concept) {
    // In a full implementation, this would look up the label from the label linkbase
    // For now, convert camelCase to readable format
    return concept.replaceAll("([A-Z])", " $1").trim();
  }

  private void writePartitionedParquet(List<GenericRecord> records, String tableName,
                                        FilingMetadata metadata, Schema schema) throws IOException {
    if (records.isEmpty()) {
      return;
    }

    // Skip if we don't have valid metadata for partitioning
    if (metadata.cik == null || metadata.filingDate == null) {
      LOGGER.warning("Skipping partition creation due to invalid metadata: CIK=" + metadata.cik + ", date=" + metadata.filingDate);
      return;
    }

    // Create partition directory structure: table/cik=X/filing_type=Y/filing_date=Z/
    String partitionPath = partitioner.getPartitionPath(metadata.cik, metadata.filingType, metadata.filingDate);
    File partitionDir = new File(targetDirectory, tableName + "/" + partitionPath);
    partitionDir.mkdirs();

    // Generate unique filename
    String filename =
        String.format("%s_%s_%s_%d.parquet", metadata.cik, metadata.filingType, metadata.filingDate, System.currentTimeMillis());
    File outputFile = new File(partitionDir, filename);

    // Write Parquet file
    Configuration conf = new Configuration();
    Path path = new Path(outputFile.getAbsolutePath());

    ParquetWriter<GenericRecord> writer = null;
    try {
      @SuppressWarnings("deprecation")
      ParquetWriter<GenericRecord> w = AvroParquetWriter.<GenericRecord>builder(path)
          .withSchema(schema)
          .withConf(conf)
          .withCompressionCodec(CompressionCodecName.SNAPPY)
          .build();
      writer = w;

      for (GenericRecord record : records) {
        writer.write(record);
      }
    } finally {
      if (writer != null) {
        writer.close();
      }
    }

    LOGGER.fine("Wrote " + records.size() + " records to " + outputFile.getAbsolutePath());
  }

  /**
   * Create embedding records from contextual chunks.
   */
  private List<GenericRecord> createEmbeddingRecords(List<SecTextVectorizer.ContextualChunk> chunks,
                                                      FilingMetadata metadata) {
    List<GenericRecord> records = new ArrayList<>();
    Schema schema = new Schema.Parser().parse(EMBEDDINGS_SCHEMA);

    for (SecTextVectorizer.ContextualChunk chunk : chunks) {
      GenericRecord record = new GenericData.Record(schema);
      record.put("cik", metadata.cik);
      record.put("filing_type", metadata.filingType);
      record.put("filing_date", metadata.filingDate);
      record.put("chunk_context", chunk.context);
      record.put("chunk_text", chunk.text);
      record.put("chunk_length", chunk.text.length());

      // Generate embedding vector using static model files
      EmbeddingModelLoader modelLoader = EmbeddingModelLoader.getInstance();
      String embeddingVector = modelLoader.generateEmbedding(chunk.text, chunk.context);
      record.put("embedding_vector", embeddingVector);

      // Analyze chunk content
      String text = chunk.text.toLowerCase();
      record.put("has_revenue_data", text.contains("revenue") || text.contains("sales"));
      record.put("has_risk_factors", text.contains("[risk]") || text.contains("risk factor"));
      record.put("has_footnotes", text.contains("[footnote]"));

      records.add(record);
    }

    return records;
  }

  /**
   * Filing metadata extracted from XBRL document.
   */
  static class FilingMetadata {
    String cik;
    String filingType;
    String filingDate;
  }
}
