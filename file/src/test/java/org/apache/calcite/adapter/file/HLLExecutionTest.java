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
import org.junit.jupiter.api.Tag;import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Tag;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test that focuses on execution time, not planning time.
 */
@Tag("unit")public class HLLExecutionTest {
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
        generateStatistics();
    }
    
    @Test
    public void testPreparedStatementPerformance() throws Exception {
        // Enable HLL
        System.setProperty("calcite.file.statistics.hll.enabled", "true");
        System.setProperty("calcite.file.statistics.cache.directory", cacheDir.getAbsolutePath());
        
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
             CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {
            
            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            
            Map<String, Object> operand = new LinkedHashMap<>();
            operand.put("directory", tempDir.toString());
            operand.put("executionEngine", "parquet");
            
            SchemaPlus schema = rootSchema.add("PARQUET", 
                FileSchemaFactory.INSTANCE.create(rootSchema, "PARQUET", operand));
            
            // First execution to warm up and plan
            String query = "SELECT COUNT(DISTINCT \"customer_id\") FROM PARQUET.\"test_data\"";
            
            // Use prepared statement to avoid re-planning
            try (PreparedStatement pstmt = connection.prepareStatement(query)) {
                // Warm up
                for (int i = 0; i < 5; i++) {
                    try (ResultSet rs = pstmt.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(10343, rs.getLong(1));
                    }
                }
                
                // Measure execution time only (no planning)
                long startExec = System.nanoTime();
                for (int i = 0; i < 100; i++) {
                    try (ResultSet rs = pstmt.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(10343, rs.getLong(1));
                    }
                }
                long execNanos = (System.nanoTime() - startExec) / 100;
                double execMillis = execNanos / 1_000_000.0;
                
                System.out.println("\n=== HLL EXECUTION Performance ===");
                System.out.println("Query: " + query);
                System.out.println("Execution time (no planning): " + String.format("%.3f ms", execMillis));
                System.out.println("Execution time in microseconds: " + (execNanos / 1000) + " μs");
                
                // HLL execution should be sub-millisecond
                assertTrue(execMillis < 1.0, 
                    "HLL execution (without planning) should be < 1ms but was " + execMillis + "ms");
            }
            
            // Compare with direct VALUES query
            String valuesQuery = "SELECT * FROM (VALUES (10343))";
            try (PreparedStatement pstmt = connection.prepareStatement(valuesQuery)) {
                // Warm up
                for (int i = 0; i < 5; i++) {
                    try (ResultSet rs = pstmt.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(10343, rs.getLong(1));
                    }
                }
                
                // Measure
                long startValues = System.nanoTime();
                for (int i = 0; i < 100; i++) {
                    try (ResultSet rs = pstmt.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(10343, rs.getLong(1));
                    }
                }
                long valuesNanos = (System.nanoTime() - startValues) / 100;
                double valuesMillis = valuesNanos / 1_000_000.0;
                
                System.out.println("\nDirect VALUES query execution: " + String.format("%.3f ms", valuesMillis));
                System.out.println("Direct VALUES in microseconds: " + (valuesNanos / 1000) + " μs");
            }
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
        
        // Load the HLL sketches into the in-memory cache
        HLLSketchCache cache = HLLSketchCache.getInstance();
        
        for (String columnName : stats.getColumnStatistics().keySet()) {
            ColumnStatistics colStats = stats.getColumnStatistics(columnName);
            if (colStats != null && colStats.getHllSketch() != null) {
                cache.putSketch("test_data", columnName, colStats.getHllSketch());
            }
        }
    }
}