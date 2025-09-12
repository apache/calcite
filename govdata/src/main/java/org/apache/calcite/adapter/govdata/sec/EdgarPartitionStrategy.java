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

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Partition strategy for SEC EDGAR XBRL data using CIK/filing-type/date hierarchy.
 *
 * <p>This strategy creates a three-level partition structure:
 * <ul>
 *   <li>Level 1: CIK (Central Index Key) - groups by company</li>
 *   <li>Level 2: Filing Type (10-K, 10-Q, 8-K, etc.) - groups by report type</li>
 *   <li>Level 3: Filing Date (YYYY-MM-DD) - groups by date</li>
 * </ul>
 *
 * <p>Benefits:
 * <ul>
 *   <li>Efficient queries by company (WHERE cik = '0000320193')</li>
 *   <li>Efficient queries by filing type (WHERE filing_type = '10-K')</li>
 *   <li>Efficient time-range queries (WHERE filing_date BETWEEN '2023-01-01' AND '2023-12-31')</li>
 *   <li>Partition pruning eliminates irrelevant data early in query planning</li>
 * </ul>
 */
public class EdgarPartitionStrategy {

  private static final Pattern CIK_PATTERN = Pattern.compile("\\d{10}");
  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
  private static final DateTimeFormatter BASIC_DATE_FORMATTER = DateTimeFormatter.BASIC_ISO_DATE;

  /**
   * Standard SEC filing types.
   */
  public enum FilingType {
    FORM_10K("10-K", "Annual report"),
    FORM_10Q("10-Q", "Quarterly report"),
    FORM_8K("8-K", "Current report"),
    FORM_20F("20-F", "Annual report for foreign private issuers"),
    FORM_40F("40-F", "Annual report for Canadian issuers"),
    FORM_11K("11-K", "Annual report of employee stock purchase savings and similar plans"),
    FORM_DEF14A("DEF 14A", "Definitive proxy statement"),
    FORM_S1("S-1", "Registration statement"),
    FORM_S3("S-3", "Registration statement for securities offerings"),
    FORM_S4("S-4", "Registration statement for M&A transactions"),
    FORM_S8("S-8", "Registration statement for employee benefit plans"),
    FORM_424B("424B", "Prospectus"),
    OTHER("OTHER", "Other filing types");

    private final String code;
    private final String description;

    FilingType(String code, String description) {
      this.code = code;
      this.description = description;
    }

    public String getCode() {
      return code;
    }

    public String getDescription() {
      return description;
    }

    public static FilingType fromCode(String code) {
      if (code == null) {
        return OTHER;
      }

      // Direct match first
      for (FilingType type : values()) {
        if (type.code.equalsIgnoreCase(code)) {
          return type;
        }
      }

      // Try normalized match (remove hyphens, spaces, underscores)
      String normalized = code.toUpperCase().replace("-", "").replace("_", "").replace(" ", "");
      for (FilingType type : values()) {
        if (type.code.replace("-", "").replace(" ", "").equals(normalized)) {
          return type;
        }
      }

      // Check for variations
      if (code.startsWith("424B")) {
        return FORM_424B;
      }

      // Check for DEF 14A variations
      if (normalized.equals("DEF14A") || normalized.equals("DEFPROXY")) {
        return FORM_DEF14A;
      }

      return OTHER;
    }
  }

  /**
   * Get the partition path for given filing metadata.
   *
   * Partitioning strategy based on filing type semantics:
   *
   * 1. Periodic financial reports (10-K, 10-Q, 20-F, 11-K):
   *    - Partitioned by fiscal_year and fiscal_period
   *    - Example: cik=0000320193/filing_type=10-K/fiscal_year=2023/fiscal_period=FY/
   *    - Example: cik=0000320193/filing_type=10-Q/fiscal_year=2023/fiscal_period=Q2/
   *
   * 2. All other filings (8-K, S-*, DEF 14A, 424B*, etc.):
   *    - Partitioned by filing_year and filing_month only (not day, to reduce cardinality)
   *    - Example: cik=0000320193/filing_type=8-K/filing_year=2023/filing_month=06/
   *    - Example: cik=0000320193/filing_type=S-1/filing_year=2023/filing_month=03/
   *
   * @param cik Central Index Key (10-digit identifier)
   * @param filingType Type of filing (10-K, 10-Q, 8-K, S-1, etc.)
   * @param date Either period end date (for financials) or filing date (for others)
   * @return Partition path based on filing type semantics
   */
  public String getPartitionPath(String cik, String filingType, String date) {
    String normalizedCik = normalizeCik(cik);
    String normalizedType = normalizeFilingType(filingType);
    String normalizedDate = normalizeFilingDate(date);

    // Determine if this filing type has meaningful fiscal periods
    boolean hasFiscalPeriod = isPeriodicFinancialReport(filingType);

    String[] dateParts = normalizedDate.split("-");
    if (dateParts.length < 2) {
      // Fallback for malformed dates
      return String.format("cik=%s/filing_type=%s/date=%s/",
          normalizedCik, normalizedType, normalizedDate);
    }

    if (hasFiscalPeriod) {
      // For periodic financial reports, partition by fiscal year and period
      String fiscalYear = dateParts[0];
      String fiscalPeriod = determineFiscalPeriod(filingType, dateParts[1]);

      return String.format("cik=%s/filing_type=%s/fiscal_year=%s/fiscal_period=%s/",
          normalizedCik, normalizedType, fiscalYear, fiscalPeriod);
    } else {
      // For all other filings, partition by filing year and month
      return String.format("cik=%s/filing_type=%s/filing_year=%s/filing_month=%s/",
          normalizedCik, normalizedType, dateParts[0], dateParts[1]);
    }
  }

  /**
   * Check if filing type represents a periodic financial report with fiscal periods.
   */
  private boolean isPeriodicFinancialReport(String filingType) {
    return filingType.equals("10-K") ||
           filingType.equals("10-Q") ||
           filingType.equals("20-F") ||
           filingType.equals("11-K") ||
           filingType.equals("40-F");
  }

  /**
   * Determine fiscal period from filing type and month.
   * For annual reports: returns "FY" (fiscal year)
   * For quarterly reports: returns appropriate quarter based on month
   */
  private String determineFiscalPeriod(String filingType, String month) {
    // Annual reports use FY (Fiscal Year)
    if (filingType.equals("10-K") || filingType.equals("20-F") ||
        filingType.equals("11-K") || filingType.equals("40-F")) {
      return "FY";
    }

    // Quarterly reports use Q1-Q4
    if (filingType.equals("10-Q")) {
      int monthNum = Integer.parseInt(month);
      // Standard calendar quarters
      // (Note: Companies may have different fiscal years, this is a simplification)
      if (monthNum <= 3) return "Q1";
      if (monthNum <= 6) return "Q2";
      if (monthNum <= 9) return "Q3";
      return "Q4";
    }

    // Shouldn't reach here if isPeriodicFinancialReport is correct
    return "UNKNOWN";
  }

  /**
   * Parse partition values from a partition path.
   *
   * @param partitionPath Path like "cik=0000320193/filing_type=10-K/fiscal_year=2023/fiscal_period=FY/"
   * @return Map of partition column names to values
   */
  public Map<String, String> parsePartitionPath(String partitionPath) {
    Map<String, String> partitions = new HashMap<>();

    String[] parts = partitionPath.split("/");
    for (String part : parts) {
      if (part.contains("=")) {
        String[] keyValue = part.split("=", 2);
        if (keyValue.length == 2) {
          partitions.put(keyValue[0], keyValue[1]);
        }
      }
    }

    // For fiscal year/period partitions, construct a period_end_date
    if (partitions.containsKey("fiscal_year") && partitions.containsKey("fiscal_period")) {
      String periodEnd =
          constructPeriodEndDate(partitions.get("fiscal_year"),
          partitions.get("fiscal_period"));
      partitions.put("period_end_date", periodEnd);
    }

    // For filing year/month partitions, construct a filing_date
    if (partitions.containsKey("filing_year") && partitions.containsKey("filing_month")) {
      String filingDate =
          String.format("%s-%s-01", partitions.get("filing_year"), partitions.get("filing_month"));
      partitions.put("filing_date", filingDate);
    }

    return partitions;
  }

  /**
   * Construct period end date from fiscal year and period.
   */
  private String constructPeriodEndDate(String fiscalYear, String fiscalPeriod) {
    switch (fiscalPeriod) {
      case "Q1":
        return fiscalYear + "-03-31";
      case "Q2":
        return fiscalYear + "-06-30";
      case "Q3":
        return fiscalYear + "-09-30";
      case "Q4":
        return fiscalYear + "-12-31";
      case "FY":
        // For Apple, fiscal year ends September 30
        // In a real system, this would be company-specific
        return fiscalYear + "-09-30";
      default:
        return fiscalYear + "-12-31";
    }
  }

  /**
   * Normalize CIK to 10-digit format with leading zeros.
   */
  private String normalizeCik(String cik) {
    if (cik == null || cik.isEmpty()) {
      return "0000000000";
    }

    // Remove any non-numeric characters
    String numeric = cik.replaceAll("[^0-9]", "");

    // Pad with leading zeros to 10 digits
    if (numeric.length() > 10) {
      numeric = numeric.substring(0, 10);
    } else if (numeric.length() < 10) {
      numeric = String.format("%010d", Long.parseLong(numeric));
    }

    return numeric;
  }

  /**
   * Normalize filing type to standard format.
   */
  private String normalizeFilingType(String filingType) {
    if (filingType == null || filingType.isEmpty()) {
      return FilingType.OTHER.getCode();
    }

    FilingType type = FilingType.fromCode(filingType);
    return type.getCode().replace("/", "_").replace(" ", "_");
  }

  /**
   * Normalize filing date to YYYY-MM-DD format.
   */
  private String normalizeFilingDate(String filingDate) {
    if (filingDate == null || filingDate.isEmpty()) {
      return LocalDate.now().format(DATE_FORMATTER);
    }

    // Handle YYYYMMDD format
    if (filingDate.length() == 8 && filingDate.matches("\\d{8}")) {
      try {
        LocalDate date = LocalDate.parse(filingDate, BASIC_DATE_FORMATTER);
        return date.format(DATE_FORMATTER);
      } catch (Exception e) {
        // Fall through to default
      }
    }

    // Handle YYYY-MM-DD format
    if (filingDate.matches("\\d{4}-\\d{2}-\\d{2}")) {
      return filingDate;
    }

    // Handle YYYY/MM/DD format
    if (filingDate.matches("\\d{4}/\\d{2}/\\d{2}")) {
      return filingDate.replace("/", "-");
    }

    // Default to current date if unparseable
    return LocalDate.now().format(DATE_FORMATTER);
  }

  /**
   * Get partition predicate for SQL WHERE clause.
   *
   * @param cik Optional CIK filter
   * @param filingType Optional filing type filter
   * @param startDate Optional start date for range query
   * @param endDate Optional end date for range query
   * @return SQL predicate string
   */
  public String getPartitionPredicate(String cik, String filingType,
                                       LocalDate startDate, LocalDate endDate) {
    StringBuilder predicate = new StringBuilder();

    if (cik != null) {
      if (predicate.length() > 0) {
        predicate.append(" AND ");
      }
      predicate.append("cik = '").append(normalizeCik(cik)).append("'");
    }

    if (filingType != null) {
      if (predicate.length() > 0) {
        predicate.append(" AND ");
      }
      predicate.append("filing_type = '").append(normalizeFilingType(filingType)).append("'");
    }

    if (startDate != null) {
      if (predicate.length() > 0) {
        predicate.append(" AND ");
      }
      predicate.append("filing_date >= '").append(startDate.format(DATE_FORMATTER)).append("'");
    }

    if (endDate != null) {
      if (predicate.length() > 0) {
        predicate.append(" AND ");
      }
      predicate.append("filing_date <= '").append(endDate.format(DATE_FORMATTER)).append("'");
    }

    return predicate.toString();
  }
}
