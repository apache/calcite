package org.apache.calcite.adapter.file.performance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.PrintWriter;
import java.util.*;

/**
 * Generates a markdown performance report from JSON test results.
 * 
 * Usage: java PerformanceReportGenerator <input.json> <output.md>
 */
public class PerformanceReportGenerator {
    
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: PerformanceReportGenerator <input.json> <output.md>");
            System.exit(1);
        }
        
        File inputFile = new File(args[0]);
        File outputFile = new File(args[1]);
        
        ObjectMapper mapper = new ObjectMapper();
        JsonNode results = mapper.readTree(inputFile);
        
        // Organize results by scenario, row count, and engine
        Map<String, Map<Integer, Map<String, TestResult>>> organized = organizeResults(results);
        
        // Generate report
        try (PrintWriter writer = new PrintWriter(outputFile)) {
            generateReport(writer, organized);
        }
        
        System.out.println("Report generated: " + outputFile.getAbsolutePath());
    }
    
    private static Map<String, Map<Integer, Map<String, TestResult>>> organizeResults(JsonNode results) {
        Map<String, Map<Integer, Map<String, TestResult>>> organized = new TreeMap<>();
        
        for (JsonNode result : results) {
            String scenario = result.get("scenario").asText();
            int rows = result.get("rows").asInt();
            String engine = result.get("engine").asText();
            long execTime = result.get("executionTimeMs").asLong();
            String warning = result.has("warning") ? result.get("warning").asText() : null;
            
            organized
                .computeIfAbsent(scenario, k -> new TreeMap<>())
                .computeIfAbsent(rows, k -> new LinkedHashMap<>())
                .put(engine, new TestResult(execTime, warning));
        }
        
        return organized;
    }
    
    private static void generateReport(PrintWriter writer, Map<String, Map<Integer, Map<String, TestResult>>> organized) {
        writer.println("# Apache Calcite File Adapter - Performance Test Results");
        writer.println();
        writer.println("**Generated:** " + new Date());
        writer.println("**Test Configuration:**");
        writer.println("- Each engine runs in isolated JVM");
        writer.println("- PreparedStatements used (execution time only, no planning)");
        writer.println("- Minimum time from 10 runs after 3 warmup runs");
        writer.println();
        
        // Get list of engines and row counts
        Set<String> engines = new LinkedHashSet<>();
        Set<Integer> rowCounts = new TreeSet<>();
        for (Map<Integer, Map<String, TestResult>> byRows : organized.values()) {
            rowCounts.addAll(byRows.keySet());
            for (Map<String, TestResult> byEngine : byRows.values()) {
                engines.addAll(byEngine.keySet());
            }
        }
        
        writer.println("**Engines tested:** " + String.join(", ", engines));
        writer.println("**Data sizes:** " + rowCounts);
        writer.println();
        
        // Generate section for each scenario
        for (Map.Entry<String, Map<Integer, Map<String, TestResult>>> scenarioEntry : organized.entrySet()) {
            String scenario = scenarioEntry.getKey();
            Map<Integer, Map<String, TestResult>> byRows = scenarioEntry.getValue();
            
            writer.println("## " + formatScenario(scenario));
            writer.println();
            
            // Check for HLL issues
            boolean hllIssue = false;
            for (Map<String, TestResult> engineResults : byRows.values()) {
                if (engineResults.containsKey("parquet+hll")) {
                    TestResult hllResult = engineResults.get("parquet+hll");
                    if (hllResult.warning != null && scenario.contains("count_distinct")) {
                        hllIssue = true;
                        break;
                    }
                }
            }
            
            if (hllIssue) {
                writer.println("⚠️ **WARNING: HLL optimization not working for this scenario!**");
                writer.println();
            }
            
            // Generate table
            writer.print("| Rows |");
            for (String engine : engines) {
                writer.print(" " + formatEngine(engine) + " |");
            }
            writer.println();
            
            writer.print("|------|");
            for (int i = 0; i < engines.size(); i++) {
                writer.print("--------|");
            }
            writer.println();
            
            for (Integer rows : rowCounts) {
                if (!byRows.containsKey(rows)) continue;
                
                Map<String, TestResult> engineResults = byRows.get(rows);
                writer.print("| " + formatRowCount(rows) + " |");
                
                // Find minimum time for speedup calculation
                long minTime = engineResults.values().stream()
                    .mapToLong(r -> r.executionTime)
                    .min()
                    .orElse(Long.MAX_VALUE);
                
                for (String engine : engines) {
                    TestResult result = engineResults.get(engine);
                    if (result == null) {
                        writer.print(" N/A |");
                    } else {
                        String cell = result.executionTime + " ms";
                        
                        // Add speedup if not the slowest
                        if (result.executionTime > minTime && minTime > 0) {
                            double speedup = (double) result.executionTime / minTime;
                            if (speedup > 1.1) {
                                cell += String.format(" (%.1fx slower)", speedup);
                            }
                        } else if (result.executionTime == minTime) {
                            cell = "**" + cell + "**";
                        }
                        
                        // Add warning for HLL
                        if (result.warning != null) {
                            cell += " ⚠️";
                        }
                        
                        writer.print(" " + cell + " |");
                    }
                }
                writer.println();
            }
            
            writer.println();
            
            // Best performers summary
            writer.println("**Best performers:**");
            for (Integer rows : rowCounts) {
                if (!byRows.containsKey(rows)) continue;
                
                Map<String, TestResult> engineResults = byRows.get(rows);
                Map.Entry<String, TestResult> best = engineResults.entrySet().stream()
                    .min((e1, e2) -> Long.compare(e1.getValue().executionTime, e2.getValue().executionTime))
                    .orElse(null);
                
                if (best != null) {
                    writer.println("- " + formatRowCount(rows) + ": " + 
                        formatEngine(best.getKey()) + " (" + best.getValue().executionTime + " ms)");
                }
            }
            writer.println();
        }
        
        // Summary section
        writer.println("## Summary");
        writer.println();
        
        // Check overall HLL status
        boolean hllWorking = false;
        for (Map<Integer, Map<String, TestResult>> byRows : organized.values()) {
            for (Map<String, TestResult> engineResults : byRows.values()) {
                TestResult hllResult = engineResults.get("parquet+hll");
                if (hllResult != null && hllResult.executionTime < 1) {
                    hllWorking = true;
                    break;
                }
            }
        }
        
        if (hllWorking) {
            writer.println("✅ **HLL optimization is working** - sub-millisecond COUNT(DISTINCT) performance achieved");
        } else {
            writer.println("❌ **HLL optimization is NOT working** - COUNT(DISTINCT) queries are scanning data instead of using pre-computed sketches");
        }
        writer.println();
        
        writer.println("**Key Findings:**");
        writer.println("- Each engine ran in complete isolation (separate JVM)");
        writer.println("- File caches were cleared between each test");
        writer.println("- Results show execution time only (planning time excluded via PreparedStatement)");
        if (!hllWorking) {
            writer.println("- **CRITICAL: HLL should provide < 1ms performance for COUNT(DISTINCT) but is not working**");
        }
        writer.println();
        
        writer.println("**Engine Descriptions:**");
        writer.println("- **LINQ4J**: Default row-by-row processing engine");
        writer.println("- **PARQUET**: Native Parquet reader with basic optimizations");
        writer.println("- **PARQUET+HLL**: Parquet with HyperLogLog sketches for COUNT(DISTINCT)");
        writer.println("- **PARQUET+VEC**: Parquet with vectorized/columnar batch reading");
        writer.println("- **PARQUET+ALL**: All optimizations combined");
        writer.println("- **ARROW**: Apache Arrow columnar format");
        writer.println("- **VECTORIZED**: Legacy vectorized engine");
    }
    
    private static String formatScenario(String scenario) {
        String[] words = scenario.replace("_", " ").split(" ");
        StringBuilder result = new StringBuilder();
        for (String word : words) {
            if (word.length() > 0) {
                if (result.length() > 0) result.append(" ");
                result.append(Character.toUpperCase(word.charAt(0)))
                      .append(word.substring(1).toLowerCase());
            }
        }
        return result.toString();
    }
    
    private static String formatEngine(String engine) {
        return engine.toUpperCase().replace("+", " + ");
    }
    
    private static String formatRowCount(int count) {
        if (count >= 1000000) {
            return String.format("%.1fM", count / 1000000.0);
        } else if (count >= 1000) {
            return (count / 1000) + "K";
        } else {
            return String.valueOf(count);
        }
    }
    
    private static class TestResult {
        final long executionTime;
        final String warning;
        
        TestResult(long executionTime, String warning) {
            this.executionTime = executionTime;
            this.warning = warning;
        }
    }
}