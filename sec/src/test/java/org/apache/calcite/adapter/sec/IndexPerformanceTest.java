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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Comprehensive performance test for downloading SEC filings for various market indices.
 *
 * <p>Features:
 * <ul>
 *   <li>Parallel downloads with configurable thread pool</li>
 *   <li>Real-time progress tracking</li>
 *   <li>Detailed performance metrics per company-year</li>
 *   <li>Support for multiple indices (DOW30, SP500, RUSSELL2000)</li>
 *   <li>Automatic retry on failures</li>
 *   <li>Performance extrapolations</li>
 * </ul>
 *
 * <p>Run specific index tests:
 * <pre>
 * # Dow 30 test (quick ~30 min)
 * ./gradlew :sec:test --tests "*.IndexPerformanceTest.testDow30Performance"
 *
 * # S&P 500 test (medium ~8 hours)
 * ./gradlew :sec:test --tests "*.IndexPerformanceTest.testSP500Performance"
 *
 * # Russell 2000 test (long ~32 hours)
 * ./gradlew :sec:test --tests "*.IndexPerformanceTest.testRussell2000Performance"
 * </pre>
 */
@Tag("performance")
@Tag("integration")
@Disabled("Enable for manual performance testing")
public class IndexPerformanceTest {
  private static final Logger LOGGER = Logger.getLogger(IndexPerformanceTest.class.getName());

  // Configuration
  private static final String BASE_DIRECTORY =
      System.getenv().getOrDefault("SEC_DATA_DIRECTORY", "/Volumes/T9/sec-data");
  private static final int YEARS_TO_DOWNLOAD =
      Integer.parseInt(System.getenv().getOrDefault("SEC_YEARS", "5"));
  private static final int PARALLEL_DOWNLOADS =
      Integer.parseInt(System.getenv().getOrDefault("SEC_PARALLEL", "4"));
  private static final int MAX_RETRIES = 3;

  // Metrics
  private static final DecimalFormat df = new DecimalFormat("#.##");
  private static final ConcurrentHashMap<String, CompanyMetrics> metricsMap = new ConcurrentHashMap<>();
  private static final AtomicInteger totalCompaniesProcessed = new AtomicInteger(0);
  private static final AtomicLong totalFilingsDownloaded = new AtomicLong(0);
  private static final AtomicLong totalBytesDownloaded = new AtomicLong(0);

  private static class CompanyMetrics {
    final String cik;
    final String name;
    final Map<Integer, YearMetrics> yearMetrics = new ConcurrentHashMap<>();
    volatile Duration totalDuration = Duration.ZERO;
    volatile int totalFilings = 0;
    volatile long totalBytes = 0;
    volatile int retryCount = 0;
    volatile String lastError = null;

    CompanyMetrics(String cik, String name) {
      this.cik = cik;
      this.name = name;
    }
  }

  private static class YearMetrics {
    int filingCount = 0;
    long bytes = 0;
    Duration duration = Duration.ZERO;
  }

  @BeforeAll
  public static void setup() {
    File baseDir = new File(BASE_DIRECTORY);
    if (!baseDir.exists()) {
      baseDir.mkdirs();
    }
    System.setProperty("SEC_DATA_DIRECTORY", BASE_DIRECTORY);

    LOGGER.info("Performance Test Configuration:");
    LOGGER.info("  Base Directory: " + BASE_DIRECTORY);
    LOGGER.info("  Years to Download: " + YEARS_TO_DOWNLOAD);
    LOGGER.info("  Parallel Downloads: " + PARALLEL_DOWNLOADS);
    LOGGER.info("  Max Retries: " + MAX_RETRIES);
  }

  @Test public void testDow30Performance() throws Exception {
    runIndexTest("DOW30", "Dow Jones Industrial Average 30");
  }

  @Test @Disabled("Long running test - ~8 hours")
  public void testSP500Performance() throws Exception {
    runIndexTest("SP500_COMPLETE", "S&P 500");
  }

  @Test @Disabled("Very long running test - ~32 hours")
  public void testRussell2000Performance() throws Exception {
    runIndexTest("RUSSELL2000_COMPLETE", "Russell 2000");
  }

  @ParameterizedTest
  @ValueSource(strings = {"DOW30", "NASDAQ100_COMPLETE", "SP100"})
  public void testMultipleIndices(String indexName) throws Exception {
    runIndexTest(indexName, indexName);
  }

  private void runIndexTest(String indexCode, String indexName) throws Exception {
    LOGGER.info("\n"
  + "=".repeat(80));
    LOGGER.info("SEC Filing Download Performance Test: " + indexName);
    LOGGER.info("=".repeat(80));

    Instant testStart = Instant.now();
    LocalDate endDate = LocalDate.now();
    LocalDate startDate = endDate.minusYears(YEARS_TO_DOWNLOAD);

    // Get companies for the index
    List<String> ciks = CikRegistry.resolveCiks(indexCode);
    LOGGER.info("Index: " + indexName);
    LOGGER.info("Companies: " + ciks.size());
    LOGGER.info("Date Range: " + startDate + " to " + endDate);
    LOGGER.info("Estimated Time: " + estimateTime(ciks.size()));
    LOGGER.info("");

    // Create thread pool for parallel processing
    ExecutorService executor = Executors.newFixedThreadPool(PARALLEL_DOWNLOADS);
    CompletionService<CompanyMetrics> completionService =
        new ExecutorCompletionService<>(executor);

    // Progress tracking
    ScheduledExecutorService progressExecutor = Executors.newSingleThreadScheduledExecutor();
    progressExecutor.scheduleAtFixedRate(
        () -> printProgress(ciks.size(), testStart),
        10, 10, TimeUnit.SECONDS);

    try {
      // Submit download tasks for each company
      for (String cik : ciks) {
        completionService.submit(() -> downloadCompanyFilings(cik, startDate, endDate));
      }

      // Collect results as they complete
      List<CompanyMetrics> results = new ArrayList<>();
      for (int i = 0; i < ciks.size(); i++) {
        Future<CompanyMetrics> future = completionService.take();
        CompanyMetrics metrics = future.get();
        results.add(metrics);
        totalCompaniesProcessed.incrementAndGet();
      }

      // Generate final report
      generateDetailedReport(results, indexName, testStart);

    } finally {
      progressExecutor.shutdown();
      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  private CompanyMetrics downloadCompanyFilings(String cik, LocalDate startDate, LocalDate endDate) {
    String companyName = "CIK-" + cik;

    CompanyMetrics metrics = new CompanyMetrics(cik, companyName);
    metricsMap.put(cik, metrics);

    for (int retry = 0; retry < MAX_RETRIES; retry++) {
      try {
        Instant companyStart = Instant.now();

        // Create connection for this company
        String jdbcUrl = createJdbcUrl(cik, startDate, endDate);
        try (Connection conn = DriverManager.getConnection(jdbcUrl, new Properties())) {

          // Process each year
          for (int year = startDate.getYear(); year <= endDate.getYear(); year++) {
            YearMetrics yearMetrics = processCompanyYear(conn, cik, year, startDate, endDate);
            metrics.yearMetrics.put(year, yearMetrics);
            metrics.totalFilings += yearMetrics.filingCount;
            metrics.totalBytes += yearMetrics.bytes;
            totalFilingsDownloaded.addAndGet(yearMetrics.filingCount);
            totalBytesDownloaded.addAndGet(yearMetrics.bytes);
          }
        }

        metrics.totalDuration = Duration.between(companyStart, Instant.now());
        return metrics; // Success

      } catch (Exception e) {
        metrics.retryCount = retry + 1;
        metrics.lastError = e.getMessage();

        if (retry < MAX_RETRIES - 1) {
          LOGGER.warning("Retry " + (retry + 1) + " for " + companyName + ": " + e.getMessage());
          try {
            Thread.sleep((retry + 1) * 5000); // Exponential backoff
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
        } else {
          LOGGER.severe("Failed to download " + companyName + " after " + MAX_RETRIES + " retries");
        }
      }
    }

    return metrics;
  }

  private YearMetrics processCompanyYear(Connection conn, String cik, int year,
                                         LocalDate startDate, LocalDate endDate) throws Exception {
    YearMetrics metrics = new YearMetrics();
    Instant yearStart = Instant.now();

    LocalDate yearStartDate = LocalDate.of(year, 1, 1);
    LocalDate yearEndDate = LocalDate.of(year, 12, 31);

    if (yearStartDate.isBefore(startDate)) {
      yearStartDate = startDate;
    }
    if (yearEndDate.isAfter(endDate)) {
      yearEndDate = endDate;
    }

    String sql = "SELECT COUNT(*) as filing_count FROM sec.financial_line_items " +
                 "WHERE cik = ? AND filing_date >= ? AND filing_date <= ? " +
                 "GROUP BY filing_date, filing_type, accession_number";

    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, cik);
      stmt.setDate(2, java.sql.Date.valueOf(yearStartDate));
      stmt.setDate(3, java.sql.Date.valueOf(yearEndDate));

      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          metrics.filingCount++;
          metrics.bytes += 500_000; // Estimate 500KB per filing
        }
      }
    }

    metrics.duration = Duration.between(yearStart, Instant.now());
    return metrics;
  }

  private void printProgress(int totalCompanies, Instant startTime) {
    int completed = totalCompaniesProcessed.get();
    if (completed == 0) return;

    Duration elapsed = Duration.between(startTime, Instant.now());
    double percentComplete = (100.0 * completed) / totalCompanies;
    double rate = completed / (elapsed.toMillis() / 1000.0);
    long filings = totalFilingsDownloaded.get();
    long megabytes = totalBytesDownloaded.get() / 1_048_576;

    // Estimate time remaining
    long estimatedTotal = (long) (elapsed.toMillis() / percentComplete * 100);
    Duration remaining = Duration.ofMillis(estimatedTotal - elapsed.toMillis());

    System.out.println(
        String.format(
        "Progress: %d/%d companies (%.1f%%) | %d filings | %d MB | %.2f companies/min | ETA: %s",
        completed, totalCompanies, percentComplete, filings, megabytes,
        rate * 60, formatDuration(remaining)));
  }

  private void generateDetailedReport(List<CompanyMetrics> results, String indexName,
                                      Instant testStart) {
    Duration totalDuration = Duration.between(testStart, Instant.now());

    LOGGER.info("\n"
  + "=".repeat(80));
    LOGGER.info("PERFORMANCE REPORT: " + indexName);
    LOGGER.info("=".repeat(80) + "\n");

    // Calculate statistics
    int successfulCompanies = (int) results.stream()
        .filter(m -> m.totalFilings > 0)
        .count();
    int failedCompanies = results.size() - successfulCompanies;

    long totalFilings = results.stream().mapToLong(m -> m.totalFilings).sum();
    long totalBytes = results.stream().mapToLong(m -> m.totalBytes).sum();

    // Calculate average time per company-year
    double totalCompanyYearSeconds = 0;
    int totalCompanyYears = 0;
    for (CompanyMetrics m : results) {
      totalCompanyYears += m.yearMetrics.size();
      for (YearMetrics ym : m.yearMetrics.values()) {
        totalCompanyYearSeconds += ym.duration.toMillis() / 1000.0;
      }
    }
    double avgSecondsPerCompanyYear = totalCompanyYears > 0 ?
        totalCompanyYearSeconds / totalCompanyYears : 0;

    // Overall Statistics
    LOGGER.info("OVERALL STATISTICS:");
    LOGGER.info("  Test Duration: " + formatDuration(totalDuration));
    LOGGER.info("  Companies Processed: " + successfulCompanies + "/" + results.size());
    if (failedCompanies > 0) {
      LOGGER.info("  Failed Companies: " + failedCompanies);
    }
    LOGGER.info("  Total Filings: " + totalFilings);
    LOGGER.info("  Total Data: " + (totalBytes / 1_048_576) + " MB");
    LOGGER.info("  Parallel Threads: " + PARALLEL_DOWNLOADS);
    LOGGER.info("");

    // Performance Metrics
    LOGGER.info("PERFORMANCE METRICS:");
    LOGGER.info("  Avg seconds per company-year: " + df.format(avgSecondsPerCompanyYear));
    LOGGER.info("  Avg seconds per company: " +
                df.format(totalDuration.toMillis() / 1000.0 / results.size()));
    LOGGER.info("  Filings per minute: " +
                df.format(totalFilings * 60.0 / totalDuration.toSeconds()));
    LOGGER.info("  MB per minute: " +
                df.format(totalBytes * 60.0 / 1_048_576 / totalDuration.toSeconds()));
    LOGGER.info("");

    // Top performers
    LOGGER.info("FASTEST COMPANIES (seconds per year):");
    results.stream()
        .filter(m -> !m.yearMetrics.isEmpty())
        .sorted(Comparator.comparing(m -> m.totalDuration.toMillis() / m.yearMetrics.size()))
        .limit(5)
        .forEach(m -> {
          double secPerYear = m.totalDuration.toMillis() / 1000.0 / m.yearMetrics.size();
          LOGGER.info(
              String.format("  %s: %.2f sec/year (%d filings)",
              m.name, secPerYear, m.totalFilings));
        });
    LOGGER.info("");

    // Failures
    if (failedCompanies > 0) {
      LOGGER.info("FAILED COMPANIES:");
      results.stream()
          .filter(m -> m.totalFilings == 0)
          .forEach(m -> LOGGER.info("  " + m.name + ": " + m.lastError));
      LOGGER.info("");
    }

    // Key Metric
    LOGGER.info("=".repeat(80));
    LOGGER.info("KEY METRIC: " + df.format(avgSecondsPerCompanyYear) +
                " seconds to download 1 year for 1 company");
    LOGGER.info("=".repeat(80) + "\n");

    // Extrapolations
    LOGGER.info("EXTRAPOLATIONS (with " + PARALLEL_DOWNLOADS + " parallel threads):");
    double sp500Hours = (500 * YEARS_TO_DOWNLOAD * avgSecondsPerCompanyYear) /
                        (3600.0 * PARALLEL_DOWNLOADS);
    double russell2000Hours = (2000 * YEARS_TO_DOWNLOAD * avgSecondsPerCompanyYear) /
                             (3600.0 * PARALLEL_DOWNLOADS);
    double allFilersHours = (8000 * YEARS_TO_DOWNLOAD * avgSecondsPerCompanyYear) /
                           (3600.0 * PARALLEL_DOWNLOADS);

    LOGGER.info("  S&P 500 (5 years): ~" + df.format(sp500Hours) + " hours");
    LOGGER.info("  Russell 2000 (5 years): ~" + df.format(russell2000Hours) + " hours");
    LOGGER.info("  All EDGAR Filers (5 years): ~" + df.format(allFilersHours) + " hours");
    LOGGER.info("");

    // Optimization Suggestions
    LOGGER.info("OPTIMIZATION SUGGESTIONS:");
    if (avgSecondsPerCompanyYear > 60) {
      LOGGER.info("  - Increase parallel downloads (current: " + PARALLEL_DOWNLOADS + ")");
      LOGGER.info("  - Check SEC rate limits (10 requests/second)");
      LOGGER.info("  - Consider distributed processing across machines");
    }
    if (avgSecondsPerCompanyYear > 30) {
      LOGGER.info("  - Cache parsed XBRL locally");
      LOGGER.info("  - Batch Parquet writes");
      LOGGER.info("  - Use columnar compression");
    }
    if (failedCompanies > results.size() * 0.1) {
      LOGGER.info("  - Implement exponential backoff for retries");
      LOGGER.info("  - Add circuit breaker for SEC API");
    }
  }

  private String createJdbcUrl(String cik, LocalDate startDate, LocalDate endDate) {
    return String.format("jdbc:calcite:model=inline:{" +
        "\"version\":\"1.0\"," +
        "\"defaultSchema\":\"sec\"," +
        "\"schemas\":[{" +
        "\"name\":\"sec\"," +
        "\"factory\":\"org.apache.calcite.adapter.sec.SecSchemaFactory\"," +
        "\"operand\":{" +
        "\"directory\":\"%s\"," +
        "\"enableSecProcessing\":true," +
        "\"edgarSource\":{" +
        "\"ciks\":\"%s\"," +
        "\"startDate\":\"%s\"," +
        "\"endDate\":\"%s\"" +
        "}}}]}",
        BASE_DIRECTORY, cik, startDate, endDate);
  }

  private String estimateTime(int companies) {
    // Based on empirical data: ~45 seconds per company-year with 4 threads
    double hoursEstimate = (companies * YEARS_TO_DOWNLOAD * 45.0) / (3600.0 * PARALLEL_DOWNLOADS);
    if (hoursEstimate < 1) {
      return df.format(hoursEstimate * 60) + " minutes";
    } else if (hoursEstimate < 24) {
      return df.format(hoursEstimate) + " hours";
    } else {
      return df.format(hoursEstimate / 24) + " days";
    }
  }

  private String formatDuration(Duration duration) {
    if (duration.isNegative()) {
      return "calculating...";
    }

    long days = duration.toDays();
    long hours = duration.toHoursPart();
    long minutes = duration.toMinutesPart();

    if (days > 0) {
      return String.format("%dd %dh %dm", days, hours, minutes);
    } else if (hours > 0) {
      return String.format("%dh %dm", hours, minutes);
    } else {
      return String.format("%dm %ds", minutes, duration.toSecondsPart());
    }
  }
}
