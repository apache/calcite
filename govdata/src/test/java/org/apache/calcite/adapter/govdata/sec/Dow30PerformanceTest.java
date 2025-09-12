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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Performance test for downloading Dow 30 companies' SEC filings.
 *
 * <p>This test downloads all filings for Dow 30 companies over the past 5 years
 * and measures performance metrics including:
 * <ul>
 *   <li>Time per company-year</li>
 *   <li>Files downloaded per company</li>
 *   <li>Average download speed</li>
 *   <li>Total data volume processed</li>
 * </ul>
 *
 * <p>Enable this test by removing @Disabled and run with:
 * <pre>
 * export SEC_DATA_DIRECTORY=/Volumes/T9/sec-data
 * ./gradlew :sec:test --tests "*.Dow30PerformanceTest" --info
 * </pre>
 */
@Tag("performance")
@Tag("integration")
public class Dow30PerformanceTest {
  private static final Logger LOGGER = Logger.getLogger(Dow30PerformanceTest.class.getName());

  // Performance tracking
  private static final Map<String, List<Duration>> companyYearDurations = new HashMap<>();
  private static final Map<String, Integer> companyFileCounts = new HashMap<>();
  private static final Map<String, Long> companyDataSizes = new HashMap<>();

  // Configuration
  private static final String BASE_DIRECTORY = "/Volumes/T9/sec-data";
  private static final int YEARS_TO_DOWNLOAD = 5;
  private static final DecimalFormat df = new DecimalFormat("#.##");

  @BeforeAll
  public static void setup() {
    // Ensure base directory exists
    File baseDir = new File(BASE_DIRECTORY);
    if (!baseDir.exists()) {
      baseDir.mkdirs();
      LOGGER.info("Created base directory: " + BASE_DIRECTORY);
    }

    // Set environment variable for SEC adapter
    System.setProperty("SEC_DATA_DIRECTORY", BASE_DIRECTORY);
  }

  @Test public void testDow30FullDownloadWithMetrics() throws Exception {
    LOGGER.info("==========================================================");
    LOGGER.info("Starting Dow 30 SEC Filing Download Performance Test");
    LOGGER.info("Base Directory: " + BASE_DIRECTORY);
    LOGGER.info("Years to Download: " + YEARS_TO_DOWNLOAD);
    LOGGER.info("==========================================================\n");

    Instant testStart = Instant.now();

    // Calculate date range
    LocalDate endDate = LocalDate.now();
    LocalDate startDate = endDate.minusYears(YEARS_TO_DOWNLOAD);

    // Create JDBC connection with configuration for Dow 30 download
    String modelPath = createModelFile(startDate, endDate);
    Properties props = new Properties();
    props.put("model", modelPath);

    String jdbcUrl = "jdbc:calcite:";

    LOGGER.info("Connecting to SEC adapter...");
    LOGGER.info("Date Range: " + startDate + " to " + endDate);

    // Register driver
    Class.forName("org.apache.calcite.jdbc.Driver");
    try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
      // Query to trigger downloads and track per-company metrics
      processCompanyFilings(conn, startDate, endDate);

      // Generate performance report
      generatePerformanceReport(testStart);

    } catch (Exception e) {
      LOGGER.severe("Test failed: " + e.getMessage());
      e.printStackTrace();
      throw e;
    }
  }

  private void processCompanyFilings(Connection conn, LocalDate startDate, LocalDate endDate)
      throws Exception {

    // Get list of Dow 30 CIKs from registry
    List<String> dow30Ciks = CikRegistry.resolveCiks("DOW30");
    LOGGER.info("Found " + dow30Ciks.size() + " Dow 30 companies\n");

    int companyNum = 0;
    for (String cik : dow30Ciks) {
      companyNum++;
      String companyName = "CIK-" + cik;

      LOGGER.info("========================================");
      LOGGER.info("Processing Company " + companyNum + "/" + dow30Ciks.size() +
                  ": " + companyName + " (CIK: " + cik + ")");
      LOGGER.info("========================================");

      // Process each year separately to get year-by-year metrics
      for (int year = startDate.getYear(); year <= endDate.getYear(); year++) {
        LocalDate yearStart = LocalDate.of(year, 1, 1);
        LocalDate yearEnd = LocalDate.of(year, 12, 31);

        // Adjust for actual date range
        if (yearStart.isBefore(startDate)) {
          yearStart = startDate;
        }
        if (yearEnd.isAfter(endDate)) {
          yearEnd = endDate;
        }

        Instant yearStartTime = Instant.now();

        // Query to download filings for this company-year
        String sql =
          String.format("SELECT filing_date, filing_type, accession_number " +
          "FROM sec.financial_line_items " +
          "WHERE cik = '%s' " +
          "  AND filing_date >= '%s' " +
          "  AND filing_date <= '%s' " +
          "GROUP BY filing_date, filing_type, accession_number " +
          "ORDER BY filing_date DESC",
          cik, yearStart, yearEnd);

        int filingCount = 0;
        long dataSize = 0;

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

          while (rs.next()) {
            filingCount++;
            // Estimate data size (this is approximate)
            dataSize += 500_000; // Assume 500KB average per filing
          }

        } catch (Exception e) {
          LOGGER.warning("Failed to process " + companyName + " for year " + year +
                        ": " + e.getMessage());
        }

        Duration yearDuration = Duration.between(yearStartTime, Instant.now());

        // Track metrics
        String companyKey = companyName + " (" + cik + ")";
        companyYearDurations.computeIfAbsent(companyKey, k -> new ArrayList<>())
            .add(yearDuration);
        companyFileCounts.merge(companyKey, filingCount, Integer::sum);
        companyDataSizes.merge(companyKey, dataSize, Long::sum);

        LOGGER.info(
            String.format("  Year %d: %d filings downloaded in %s seconds",
            year, filingCount, df.format(yearDuration.toMillis() / 1000.0)));
      }

      LOGGER.info("");
    }
  }

  private void generatePerformanceReport(Instant testStart) {
    Duration totalDuration = Duration.between(testStart, Instant.now());

    LOGGER.info("\n==========================================================");
    LOGGER.info("PERFORMANCE METRICS REPORT");
    LOGGER.info("==========================================================\n");

    // Overall statistics
    long totalFiles = companyFileCounts.values().stream().mapToInt(Integer::intValue).sum();
    long totalDataMB = companyDataSizes.values().stream().mapToLong(Long::longValue).sum() / 1_048_576;
    double totalMinutes = totalDuration.toMillis() / 60000.0;

    LOGGER.info("OVERALL STATISTICS:");
    LOGGER.info("  Total Test Duration: " + formatDuration(totalDuration));
    LOGGER.info("  Total Companies: " + companyYearDurations.size());
    LOGGER.info("  Total Filings Downloaded: " + totalFiles);
    LOGGER.info("  Estimated Total Data: " + totalDataMB + " MB");
    LOGGER.info("  Average Speed: " + df.format(totalFiles / totalMinutes) + " filings/minute");
    LOGGER.info("");

    // Per company-year metrics
    LOGGER.info("PER COMPANY-YEAR METRICS:");
    double totalCompanyYearSeconds = 0;
    int totalCompanyYears = 0;

    for (Map.Entry<String, List<Duration>> entry : companyYearDurations.entrySet()) {
      String company = entry.getKey();
      List<Duration> yearDurations = entry.getValue();

      double avgSecondsPerYear = yearDurations.stream()
          .mapToLong(Duration::toMillis)
          .average()
          .orElse(0) / 1000.0;

      totalCompanyYearSeconds += avgSecondsPerYear * yearDurations.size();
      totalCompanyYears += yearDurations.size();

      int totalFilings = companyFileCounts.getOrDefault(company, 0);
      double avgFilingsPerYear = (double) totalFilings / yearDurations.size();

      LOGGER.info(String.format("  %s:", company));
      LOGGER.info(String.format("    Avg time per year: %s seconds", df.format(avgSecondsPerYear)));
      LOGGER.info(
          String.format("    Total filings: %d (avg %.1f per year)",
                                totalFilings, avgFilingsPerYear));
    }

    LOGGER.info("");

    // Key metric: Average seconds per company-year
    double avgSecondsPerCompanyYear = totalCompanyYearSeconds / totalCompanyYears;
    LOGGER.info("==========================================================");
    LOGGER.info("KEY METRIC: " + df.format(avgSecondsPerCompanyYear) +
                " seconds to download 1 year for 1 company");
    LOGGER.info("==========================================================");

    // Extrapolations
    LOGGER.info("\nEXTRAPOLATIONS:");
    double sp500Time = (500 * YEARS_TO_DOWNLOAD * avgSecondsPerCompanyYear) / 3600;
    double russell2000Time = (2000 * YEARS_TO_DOWNLOAD * avgSecondsPerCompanyYear) / 3600;

    LOGGER.info("  S&P 500 (5 years): ~" + df.format(sp500Time) + " hours");
    LOGGER.info("  Russell 2000 (5 years): ~" + df.format(russell2000Time) + " hours");

    // Performance recommendations
    LOGGER.info("\nPERFORMANCE NOTES:");
    if (avgSecondsPerCompanyYear > 60) {
      LOGGER.info("  - Consider parallelizing downloads across companies");
      LOGGER.info("  - Check network bandwidth and SEC rate limits");
    }
    if (avgSecondsPerCompanyYear > 30) {
      LOGGER.info("  - Consider caching parsed XBRL data");
      LOGGER.info("  - Optimize Parquet conversion pipeline");
    }
    LOGGER.info("  - Current rate allows ~" +
                df.format(3600 / avgSecondsPerCompanyYear) +
                " company-years per hour");
  }

  private String createModelFile(LocalDate startDate, LocalDate endDate) throws Exception {
    File modelFile = new File(BASE_DIRECTORY, "dow30-model.json");

    String modelContent =
      String.format("{\n"
  +
      "  \"version\": \"1.0\",\n"
  +
      "  \"defaultSchema\": \"sec\",\n"
  +
      "  \"schemas\": [\n"
  +
      "    {\n"
  +
      "      \"name\": \"sec\",\n"
  +
      "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",\n"
  +
      "      \"operand\": {\n"
  +
      "        \"directory\": \"%s\",\n"
  +
      "        \"enableSecProcessing\": true,\n"
  +
      "        \"processSecOnInit\": true,\n"
  +
      "        \"edgarSource\": {\n"
  +
      "          \"autoDownload\": true,\n"
  +
      "          \"ciks\": \"DOW30\",\n"
  +
      "          \"startDate\": \"%s\",\n"
  +
      "          \"endDate\": \"%s\",\n"
  +
      "          \"filingTypes\": [\n"
  +
      "            \"10-K\", \"10-Q\", \"8-K\", \"20-F\", \"DEF 14A\",\n"
  +
      "            \"S-1\", \"S-3\", \"S-4\", \"S-8\", \"424B2\",\n"
  +
      "            \"11-K\", \"10-K/A\", \"10-Q/A\", \"8-K/A\"\n"
  +
      "          ]\n"
  +
      "        }\n"
  +
      "      }\n"
  +
      "    }\n"
  +
      "  ]\n"
  +
      "}",
      BASE_DIRECTORY, startDate, endDate);

    java.nio.file.Files.write(modelFile.toPath(), modelContent.getBytes());
    return modelFile.getAbsolutePath();
  }

  private String formatDuration(Duration duration) {
    long hours = duration.toHours();
    long minutes = duration.toMinutesPart();
    long seconds = duration.toSecondsPart();

    if (hours > 0) {
      return String.format("%d hours, %d minutes, %d seconds", hours, minutes, seconds);
    } else if (minutes > 0) {
      return String.format("%d minutes, %d seconds", minutes, seconds);
    } else {
      return String.format("%d seconds", seconds);
    }
  }
}
