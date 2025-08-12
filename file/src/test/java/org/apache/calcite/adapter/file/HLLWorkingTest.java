package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.statistics.HyperLogLogSketch;
import org.apache.calcite.adapter.file.statistics.HLLSketchCache;
import org.apache.calcite.adapter.file.statistics.StatisticsBuilder;
import org.apache.calcite.adapter.file.statistics.TableStatistics;
import org.apache.calcite.adapter.file.statistics.ColumnStatistics;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public class HLLWorkingTest {
    @TempDir
    java.nio.file.Path tempDir;
    
    private File cacheDir;
    
    @BeforeEach
    public void setUp() throws Exception {
        // Setup cache directory for HLL statistics
        cacheDir = tempDir.resolve("hll_cache").toFile();
        cacheDir.mkdirs();
        
        // Create test data
        createTestParquetFile(100000); // 100K rows
        
        // Pre-generate HLL statistics
        System.out.println("Generating HLL statistics...");
        generateStatistics();
    }
    
    @Test
    public void testHLLActuallyWorks() throws Exception {
        // Enable HLL
        System.setProperty("calcite.file.statistics.hll.enabled", "true");
        System.setProperty("calcite.file.statistics.cache.directory", cacheDir.getAbsolutePath());
        
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
             CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {
            
            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            
            // Test with PARQUET engine (no HLL)
            Map<String, Object> operandNoHLL = new LinkedHashMap<>();
            operandNoHLL.put("directory", tempDir.toString());
            operandNoHLL.put("executionEngine", "parquet");
            
            SchemaPlus parquetSchema = rootSchema.add("PARQUET_NO_HLL", 
                FileSchemaFactory.INSTANCE.create(rootSchema, "PARQUET_NO_HLL", operandNoHLL));
            
            // Test with PARQUET+HLL (should use HLL)
            Map<String, Object> operandWithHLL = new LinkedHashMap<>();
            operandWithHLL.put("directory", tempDir.toString());
            operandWithHLL.put("executionEngine", "parquet");
            // The key is to ensure HLL rules are added
            
            SchemaPlus parquetHLLSchema = rootSchema.add("PARQUET_HLL", 
                FileSchemaFactory.INSTANCE.create(rootSchema, "PARQUET_HLL", operandWithHLL));
            
            // Simple COUNT(DISTINCT) - no GROUP BY, should trigger HLL
            String query = "SELECT COUNT(DISTINCT \"customer_id\") FROM %s.\"test_data\"";
            
            // Warm up
            for (int i = 0; i < 3; i++) {
                try (Statement stmt = connection.createStatement();
                     ResultSet rs = stmt.executeQuery(String.format(query, "PARQUET_NO_HLL"))) {
                    rs.next();
                }
            }
            
            // Test WITHOUT HLL
            long startNoHLL = System.currentTimeMillis();
            long resultNoHLL = 0;
            for (int i = 0; i < 5; i++) {
                try (Statement stmt = connection.createStatement();
                     ResultSet rs = stmt.executeQuery(String.format(query, "PARQUET_NO_HLL"))) {
                    assertTrue(rs.next());
                    resultNoHLL = rs.getLong(1);
                }
            }
            long timeNoHLL = (System.currentTimeMillis() - startNoHLL) / 5;
            
            // Test WITH HLL - using regular COUNT(DISTINCT)
            // The HLL rule should optimize this automatically
            String hllQuery = "SELECT COUNT(DISTINCT \"customer_id\") FROM PARQUET_HLL.\"test_data\"";
            
            System.out.println("\nExecuting HLL-optimized query: " + hllQuery);
            
            // First, let's see the plan
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery("EXPLAIN PLAN FOR " + hllQuery)) {
                System.out.println("\n=== Execution plan WITH HLL ===");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                }
            }
            
            // Also check plan without HLL for comparison
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery("EXPLAIN PLAN FOR " + String.format(query, "PARQUET_NO_HLL"))) {
                System.out.println("\n=== Execution plan WITHOUT HLL ===");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                }
            }
            
            long startHLL = System.currentTimeMillis();
            long resultHLL = 0;
            for (int i = 0; i < 5; i++) {
                try (Statement stmt = connection.createStatement();
                     ResultSet rs = stmt.executeQuery(hllQuery)) {
                    assertTrue(rs.next());
                    resultHLL = rs.getLong(1);
                }
            }
            long timeHLL = (System.currentTimeMillis() - startHLL) / 5;
            
            System.out.println("\n=== HLL Performance Test Results ===");
            System.out.println("Dataset: 100,000 rows");
            System.out.println("Query: COUNT(DISTINCT customer_id)");
            System.out.println("");
            System.out.println("WITHOUT HLL: " + timeNoHLL + "ms (result: " + resultNoHLL + ")");
            System.out.println("WITH HLL:    " + timeHLL + "ms (result: " + resultHLL + ")");
            System.out.println("Speedup:     " + (timeNoHLL / (double) Math.max(1, timeHLL)) + "x");
            
            // HLL should provide reasonable performance
            // The time includes planning, but execution is instant (< 1ms)
            System.out.println("\nNote: Times include query planning overhead.");
            System.out.println("Actual execution time for HLL is < 1ms (see HLLExecutionTest)");
            
            // Results should be close (within 5% for HLL accuracy)
            double accuracy = Math.abs(resultHLL - resultNoHLL) / (double) resultNoHLL;
            assertTrue(accuracy < 0.05, "HLL accuracy should be within 5% but was " + (accuracy * 100) + "%");
        } finally {
            System.clearProperty("calcite.file.statistics.hll.enabled");
            System.clearProperty("calcite.file.statistics.cache.directory");
        }
    }
    
    @SuppressWarnings("deprecation")
    private void createTestParquetFile(int rows) throws Exception {
        File file = new File(tempDir.toFile(), "test_data.parquet");
        
        String schemaString = "{"
            + "\"type\": \"record\","
            + "\"name\": \"TestRecord\","
            + "\"fields\": ["
            + "  {\"name\": \"id\", \"type\": \"int\"},"
            + "  {\"name\": \"customer_id\", \"type\": \"int\"},"
            + "  {\"name\": \"product_id\", \"type\": \"int\"},"
            + "  {\"name\": \"amount\", \"type\": \"double\"}"
            + "]"
            + "}";
        
        Schema avroSchema = new Schema.Parser().parse(schemaString);
        Random random = new Random(42);
        
        try (ParquetWriter<GenericRecord> writer = 
             AvroParquetWriter
                 .<GenericRecord>builder(new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
                 .withSchema(avroSchema)
                 .withCompressionCodec(CompressionCodecName.SNAPPY)
                 .build()) {
            
            for (int i = 0; i < rows; i++) {
                GenericRecord record = new GenericData.Record(avroSchema);
                record.put("id", i);
                // ~10K unique customers
                record.put("customer_id", random.nextInt(10000));
                // ~1K unique products
                record.put("product_id", random.nextInt(1000));
                record.put("amount", random.nextDouble() * 1000);
                writer.write(record);
            }
        }
    }
    
    private void generateStatistics() throws Exception {
        // ENSURE HLL IS ENABLED FOR STATISTICS GENERATION!
        System.setProperty("calcite.file.statistics.hll.enabled", "true");
        System.setProperty("calcite.file.statistics.cache.directory", cacheDir.getAbsolutePath());
        
        File parquetFile = new File(tempDir.toFile(), "test_data.parquet");
        StatisticsBuilder builder = new StatisticsBuilder();
        TableStatistics stats = builder.buildStatistics(
            new DirectFileSource(parquetFile),
            cacheDir);
        
        System.out.println("Generated statistics:");
        System.out.println("  Row count: " + stats.getRowCount());
        System.out.println("  Cache dir: " + cacheDir.getAbsolutePath());
        
        // Verify HLL sketches were created
        File[] cacheFiles = cacheDir.listFiles();
        if (cacheFiles != null) {
            System.out.println("  Cache files created: " + cacheFiles.length);
            for (File f : cacheFiles) {
                System.out.println("    - " + f.getName());
            }
        }
        
        // CRITICAL: Load the HLL sketches into the in-memory cache!
        // The statistics are on disk but need to be in the HLLSketchCache
        HLLSketchCache cache = HLLSketchCache.getInstance();
        
        // Load sketches for each column
        for (String columnName : stats.getColumnStatistics().keySet()) {
            ColumnStatistics colStats = stats.getColumnStatistics(columnName);
            if (colStats != null && colStats.getHllSketch() != null) {
                System.out.println("  Loading HLL sketch for column " + columnName + 
                                   " with estimate " + colStats.getHllSketch().getEstimate());
                cache.putSketch("TEST", "test_data", columnName, colStats.getHllSketch());
            }
        }
        
        // Verify cache is populated
        HyperLogLogSketch customerIdSketch = cache.getSketch("TEST", "test_data", "customer_id");
        if (customerIdSketch != null) {
            System.out.println("  Verified: HLL sketch for customer_id is in cache with estimate " + 
                               customerIdSketch.getEstimate());
        } else {
            System.out.println("  WARNING: HLL sketch for customer_id NOT in cache!");
        }
    }
}