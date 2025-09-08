# SEC Full-Text Extraction Solutions

## Overview
Based on investigation of SEC's APIs and third-party solutions, there are multiple approaches to extract full text from SEC filings, particularly for narrative sections like MD&A that aren't XBRL-tagged.

## Current Challenge
Modern SEC filings use inline XBRL (iXBRL) format where:
- Financial data is XBRL-tagged within HTML
- Narrative sections (MD&A, business description) are plain HTML without XBRL tags
- HTML structure varies significantly between filers
- MD&A extraction requires HTML parsing with multiple fallback strategies

## Solution Options

### 1. Official SEC APIs (data.sec.gov)

**Capabilities:**
- Provides structured XBRL data via REST APIs
- Company metadata and financial facts in JSON format
- Submissions history with accession numbers
- Bulk download via companyfacts.zip

**Limitations:**
- No full-text extraction API
- Only provides XBRL-tagged data
- Narrative sections not accessible via API
- Must download full HTML filings and parse locally

**Implementation:**
```java
// Current approach - download full filing HTML
String filingUrl = "https://www.sec.gov/Archives/edgar/data/{CIK}/{accession}.htm";
// Then parse HTML locally with enhanced extraction logic
```

### 2. SEC Full-Text Search (EDGAR Search)

**Capabilities:**
- Web interface for full-text search since 2001
- Searches filing content and attachments
- Returns filing metadata with search snippets

**Limitations:**
- No programmatic API
- Web scraping would violate terms of service
- Not suitable for bulk extraction

### 3. Third-Party Solutions

#### SEC-API.io (Commercial)

**Capabilities:**
- Full-text extraction API for all sections
- Supports 10-K, 10-Q, 8-K since 1994
- Returns cleaned text or HTML
- 300ms latency after EDGAR publication
- Section-specific extraction

**API Endpoints:**
```
GET /extractor?url={filing_url}&item=7
GET /extractor?url={filing_url}&item=7A
GET /extractor?url={filing_url}&type=10-K&sections=all
```

**Pricing:**
- Subscription-based
- Usage limits apply
- Not suitable for open-source project

#### Python sec-api Package

**Capabilities:**
- Open-source Python library
- Wraps SEC-API.io services
- Full-text search and extraction
- XBRL-to-JSON conversion

**Installation:**
```bash
pip install sec-api
```

**Limitations:**
- Requires API key from SEC-API.io
- Same commercial limitations

### 4. Enhanced Local Extraction (Recommended)

**Current Implementation Status:**
Successfully enhanced `XbrlToParquetConverter.extractMDAFromHTML()` with:

1. **Multiple Search Strategies:**
   - Case-insensitive regex for "Item 7" variations
   - Direct search for "Management's Discussion and Analysis"
   - HTML element-specific searches (td, div)
   - Aggressive fallback for large text blocks

2. **Results:**
   - Apple 2023: 22 paragraphs extracted (was 0)
   - Apple 2022: 6 paragraphs extracted
   - Microsoft 2023: 8 paragraphs extracted
   - Microsoft 2022: 223 paragraphs extracted

**Code Location:**
`sec/src/main/java/org/apache/calcite/adapter/sec/XbrlToParquetConverter.java`
Lines 742-949

### 5. Alternative Data Sources

#### SEC RSS Feeds
- Real-time filing notifications
- Metadata only, no full text

#### EDGAR Bulk Data
- FTP access to all filings
- Complete historical archive
- Requires significant storage
- Full HTML/XML parsing needed

#### Financial Data Vendors
- Bloomberg, Refinitiv, S&P Capital IQ
- Pre-processed and cleaned data
- Expensive commercial licenses
- Not suitable for open-source

## Recommended Approach

### Short Term (Implemented)
Continue with enhanced local HTML parsing:
- ✅ Multiple extraction strategies
- ✅ Case-insensitive matching
- ✅ Fallback approaches
- ✅ Successfully extracting MD&A from test filings

### Medium Term
1. **Improve HTML parsing patterns:**
   - Study more filing structures
   - Add filer-specific templates
   - Machine learning for section detection

2. **Cache extracted text:**
   - Store processed MD&A in database
   - Avoid re-parsing same filings
   - Build corpus for pattern analysis

3. **Hybrid approach:**
   - Use XBRL for financial data
   - Enhanced HTML parsing for narratives
   - Optional third-party API for difficult cases

### Long Term
1. **Build extraction model:**
   - Train on successfully extracted filings
   - Use NLP to identify section boundaries
   - Handle various HTML structures

2. **Community contribution:**
   - Open-source extraction patterns
   - Crowd-source difficult cases
   - Share parsing templates

## Implementation Guidelines

### File Adapter Integration
Following SEC adapter standards, integrate with file adapter:

```java
public class SecTextExtractor extends HtmlToJsonConverter {
    @Override
    protected void extractSections(Document doc, JsonGenerator gen) {
        // Use file adapter's HTML processing
        super.extractSections(doc, gen);
        
        // Add SEC-specific extraction
        extractMDA(doc, gen);
        extractRiskFactors(doc, gen);
        extractBusinessDescription(doc, gen);
    }
}
```

### Error Handling
```java
try {
    // Primary extraction
    text = extractWithPrimaryStrategy(doc);
} catch (Exception e) {
    if (config.getBoolean("sec.fallback.enabled", false)) {
        logger.warning("Primary extraction failed, using fallback: " + e.getMessage());
        text = extractWithFallbackStrategy(doc);
    } else {
        throw new ExtractionException("Failed to extract text", e);
    }
}
```

## Testing Strategy

### Unit Tests
- Mock HTML structures
- Test each extraction strategy
- Verify fallback behavior

### Integration Tests
```java
@Test
@Tag("integration")
public void testMDAExtraction() throws Exception {
    // Test with real filings
    String[] testFilings = {
        "AAPL/2023/10-K",
        "MSFT/2023/10-K",
        "GOOGL/2023/10-K"
    };
    
    for (String filing : testFilings) {
        List<String> mda = extractMDA(filing);
        assertFalse(mda.isEmpty(), 
            "Should extract MD&A from " + filing);
    }
}
```

## Metrics and Monitoring

### Track Extraction Success
```java
public class ExtractionMetrics {
    private final Counter successCount;
    private final Counter fallbackCount;
    private final Counter failureCount;
    private final Histogram paragraphCount;
    
    public void recordExtraction(ExtractionResult result) {
        if (result.isSuccess()) {
            successCount.increment();
            paragraphCount.update(result.getParagraphCount());
        }
        if (result.usedFallback()) {
            fallbackCount.increment();
        }
        if (result.isFailed()) {
            failureCount.increment();
        }
    }
}
```

## Conclusion

The enhanced local extraction approach is working well and should be the primary strategy. The SEC doesn't provide a full-text API, making local HTML parsing necessary. Third-party APIs exist but require commercial licenses unsuitable for open-source projects.

The current implementation successfully extracts MD&A from various filing structures and can be further improved with additional patterns and machine learning approaches.