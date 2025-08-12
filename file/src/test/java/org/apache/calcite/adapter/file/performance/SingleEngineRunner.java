package org.apache.calcite.adapter.file.performance;

import org.apache.calcite.adapter.file.DirectFileSource;
import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.adapter.file.statistics.ColumnStatistics;
import org.apache.calcite.adapter.file.statistics.HLLSketchCache;
import org.apache.calcite.adapter.file.statistics.StatisticsBuilder;
import org.apache.calcite.adapter.file.statistics.TableStatistics;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

/**
 * Runs a single engine test in complete isolation.
 * Designed to be invoked from command line with a fresh JVM.
 * 
 * Usage: java SingleEngineRunner <engine> <rows> <scenario> <dataDir> <cacheDir>
 * 
 * Output: JSON results to stdout
 */
public class SingleEngineRunner {
    private static final int WARMUP_RUNS = 3;
    private static final int TEST_RUNS = 10;
    
    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: SingleEngineRunner <engine> <rows> <scenario> <dataDir> <cacheDir>");
            System.exit(1);
        }
        
        String engine = args[0];
        int rows = Integer.parseInt(args[1]);
        String scenario = args[2];
        File dataDir = new File(args[3]);
        File cacheDir = new File(args[4]);
        
        // Clean up any existing caches
        cleanupCaches();
        
        // Configure engine
        configureEngine(engine, cacheDir);
        
        // Ensure data exists or create it
        ensureTestData(dataDir, rows);
        
        // Check if cache pre-loading is enabled (default: true for HLL engines)
        boolean preloadCache = Boolean.parseBoolean(
            System.getenv().getOrDefault("PRELOAD_HLL_CACHE", "true"));
        
        // Generate HLL statistics if needed and enabled
        if (preloadCache && (engine.contains("hll") || engine.equals("parquet+all"))) {
            System.err.println("Pre-loading HLL cache for " + engine + " engine...");
            generateStatistics(dataDir, cacheDir, rows);
        } else if (engine.contains("hll") || engine.equals("parquet+all")) {
            System.err.println("HLL cache pre-loading disabled. HLL will generate on first query.");
        }
        
        // Run the test
        long executionTime = runTest(engine, scenario, rows, dataDir, cacheDir);
        
        // Output JSON result
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode result = mapper.createObjectNode();
        result.put("engine", engine);
        result.put("rows", rows);
        result.put("scenario", scenario);
        result.put("executionTimeMs", executionTime);
        result.put("executionTimeMicros", executionTime * 1000);
        result.put("timestamp", System.currentTimeMillis());
        
        // Add warning if HLL should be fast but isn't
        if (engine.contains("hll") && scenario.contains("COUNT(DISTINCT)") && executionTime > 1) {
            result.put("warning", "HLL should be < 1ms but was " + executionTime + "ms");
        }
        
        System.out.println(mapper.writeValueAsString(result));
    }
    
    private static void cleanupCaches() {
        // Clean Parquet cache
        File parquetCache = new File(System.getProperty("java.io.tmpdir"), ".parquet_cache");
        if (parquetCache.exists()) {
            deleteRecursively(parquetCache);
        }
        
        // Clean any other temporary caches
        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        for (File file : tempDir.listFiles()) {
            if (file.getName().endsWith(".apericio_stats") || 
                file.getName().endsWith(".hll") ||
                file.getName().endsWith(".stats")) {
                file.delete();
            }
        }
    }
    
    private static void deleteRecursively(File file) {
        if (file.isDirectory()) {
            for (File child : file.listFiles()) {
                deleteRecursively(child);
            }
        }
        file.delete();
    }
    
    private static void configureEngine(String engine, File cacheDir) {
        // Always start with HLL enabled for statistics generation
        System.setProperty("calcite.file.statistics.hll.enabled", "true");
        
        // Configure HLL threshold - for test data with small cardinality, use lower threshold
        // In production, the default 1000 is appropriate
        String hllThreshold = System.getenv("HLL_THRESHOLD");
        if (hllThreshold == null || hllThreshold.isEmpty()) {
            // For test data with 1K-1M rows, we need lower threshold to trigger HLL
            hllThreshold = "10"; 
        }
        System.setProperty("calcite.file.statistics.hll.threshold", hllThreshold);
        System.setProperty("calcite.file.statistics.cache.directory", cacheDir.getAbsolutePath());
        
        if (engine.equals("parquet+hll") || engine.equals("parquet+all")) {
            System.setProperty("calcite.file.statistics.filter.enabled", "true");
            System.setProperty("calcite.file.statistics.join.reorder.enabled", "true");
            System.setProperty("calcite.file.statistics.column.pruning.enabled", "true");
        }
        
        if (engine.equals("parquet+vec") || engine.equals("parquet+all")) {
            System.setProperty("parquet.enable.vectorized.reader", "true");
        } else {
            System.setProperty("parquet.enable.vectorized.reader", "false");
        }
        
        // Disable HLL for engines that shouldn't use it
        if (!engine.contains("hll") && !engine.equals("parquet+all")) {
            System.setProperty("calcite.file.statistics.hll.enabled", "false");
        }
    }
    
    private static void ensureTestData(File dataDir, int rows) throws Exception {
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }
        
        File salesFile = new File(dataDir, "sales_" + rows + ".parquet");
        if (!salesFile.exists()) {
            createSalesParquetFile(salesFile, rows);
        }
        
        File customersFile = new File(dataDir, "customers_" + (rows / 10) + ".parquet");
        if (!customersFile.exists()) {
            createCustomersParquetFile(customersFile, rows / 10);
        }
    }
    
    @SuppressWarnings("deprecation")
    private static void createSalesParquetFile(File file, int rows) throws Exception {
        String schemaString = "{"
            + "\"type\": \"record\","
            + "\"name\": \"SalesRecord\","
            + "\"fields\": ["
            + "  {\"name\": \"order_id\", \"type\": \"int\"},"
            + "  {\"name\": \"customer_id\", \"type\": \"int\"},"
            + "  {\"name\": \"product_id\", \"type\": \"int\"},"
            + "  {\"name\": \"category\", \"type\": \"string\"},"
            + "  {\"name\": \"quantity\", \"type\": \"int\"},"
            + "  {\"name\": \"unit_price\", \"type\": \"double\"},"
            + "  {\"name\": \"total\", \"type\": \"double\"},"
            + "  {\"name\": \"order_date\", \"type\": \"string\"},"
            + "  {\"name\": \"region\", \"type\": \"string\"}"
            + "]"
            + "}";
        
        Schema avroSchema = new Schema.Parser().parse(schemaString);
        Random random = new Random(42);
        String[] categories = {"Electronics", "Clothing", "Books", "Food", "Sports"};
        String[] regions = {"North", "South", "East", "West", "Central"};
        
        try (ParquetWriter<GenericRecord> writer = 
             AvroParquetWriter
                 .<GenericRecord>builder(new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
                 .withSchema(avroSchema)
                 .withCompressionCodec(CompressionCodecName.SNAPPY)
                 .build()) {
            
            for (int i = 0; i < rows; i++) {
                GenericRecord record = new GenericData.Record(avroSchema);
                record.put("order_id", i);
                record.put("customer_id", random.nextInt(rows / 10));
                record.put("product_id", random.nextInt(1000));
                record.put("category", categories[random.nextInt(categories.length)]);
                record.put("quantity", 1 + random.nextInt(10));
                double price = 10 + random.nextDouble() * 990;
                record.put("unit_price", price);
                record.put("total", price * (Integer) record.get("quantity"));
                record.put("order_date", "2024-01-" + String.format("%02d", 1 + random.nextInt(28)));
                record.put("region", regions[random.nextInt(regions.length)]);
                writer.write(record);
            }
        }
    }
    
    @SuppressWarnings("deprecation")
    private static void createCustomersParquetFile(File file, int rows) throws Exception {
        String schemaString = "{"
            + "\"type\": \"record\","
            + "\"name\": \"CustomerRecord\","
            + "\"fields\": ["
            + "  {\"name\": \"customer_id\", \"type\": \"int\"},"
            + "  {\"name\": \"name\", \"type\": \"string\"},"
            + "  {\"name\": \"age\", \"type\": \"int\"},"
            + "  {\"name\": \"city\", \"type\": \"string\"},"
            + "  {\"name\": \"state\", \"type\": \"string\"},"
            + "  {\"name\": \"loyalty_score\", \"type\": \"double\"}"
            + "]"
            + "}";
        
        Schema avroSchema = new Schema.Parser().parse(schemaString);
        Random random = new Random(42);
        String[] cities = {"New York", "Los Angeles", "Chicago", "Houston", "Phoenix"};
        String[] states = {"NY", "CA", "IL", "TX", "AZ"};
        
        try (ParquetWriter<GenericRecord> writer = 
             AvroParquetWriter
                 .<GenericRecord>builder(new org.apache.hadoop.fs.Path(file.getAbsolutePath()))
                 .withSchema(avroSchema)
                 .withCompressionCodec(CompressionCodecName.SNAPPY)
                 .build()) {
            
            for (int i = 0; i < rows; i++) {
                GenericRecord record = new GenericData.Record(avroSchema);
                record.put("customer_id", i);
                record.put("name", "Customer_" + i);
                record.put("age", 18 + random.nextInt(62));
                int cityIdx = random.nextInt(cities.length);
                record.put("city", cities[cityIdx]);
                record.put("state", states[cityIdx]);
                record.put("loyalty_score", random.nextDouble() * 100);
                writer.write(record);
            }
        }
    }
    
    private static void generateStatistics(File dataDir, File cacheDir, int rows) throws Exception {
        if (!cacheDir.exists()) {
            cacheDir.mkdirs();
        }
        
        // Generate for sales table
        generateTableStatistics(dataDir, cacheDir, "sales_" + rows);
        
        // Generate for customers table
        generateTableStatistics(dataDir, cacheDir, "customers_" + (rows / 10));
    }
    
    private static void generateTableStatistics(File dataDir, File cacheDir, String tableName) throws Exception {
        File parquetFile = new File(dataDir, tableName + ".parquet");
        if (!parquetFile.exists()) {
            return;
        }
        
        StatisticsBuilder builder = new StatisticsBuilder();
        TableStatistics stats = builder.buildStatistics(
            new DirectFileSource(parquetFile), cacheDir);
        
        // Load HLL sketches into memory cache
        HLLSketchCache cache = HLLSketchCache.getInstance();
        for (String columnName : stats.getColumnStatistics().keySet()) {
            ColumnStatistics colStats = stats.getColumnStatistics(columnName);
            if (colStats != null && colStats.getHllSketch() != null) {
                cache.putSketch(tableName, columnName, colStats.getHllSketch());
            }
        }
        
        System.err.println("Generated statistics for " + tableName + ": " + stats.getRowCount() + " rows");
    }
    
    private static long runTest(String engine, String scenario, int rows, File dataDir, File cacheDir) throws Exception {
        String actualEngine = engine.startsWith("parquet") ? "parquet" : engine;
        
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
             CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {
            
            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            Map<String, Object> operand = new LinkedHashMap<>();
            operand.put("directory", dataDir.getAbsolutePath());
            operand.put("executionEngine", actualEngine);
            
            rootSchema.add("TEST", 
                FileSchemaFactory.INSTANCE.create(rootSchema, "TEST", operand));
            
            String query = getQueryForScenario(scenario, rows);
            
            try (PreparedStatement pstmt = connection.prepareStatement(query)) {
                // Warmup
                for (int i = 0; i < WARMUP_RUNS; i++) {
                    try (ResultSet rs = pstmt.executeQuery()) {
                        while (rs.next()) {
                            for (int j = 1; j <= rs.getMetaData().getColumnCount(); j++) {
                                rs.getObject(j);
                            }
                        }
                    }
                }
                
                // Test runs
                long minTime = Long.MAX_VALUE;
                for (int i = 0; i < TEST_RUNS; i++) {
                    long start = System.nanoTime();
                    try (ResultSet rs = pstmt.executeQuery()) {
                        while (rs.next()) {
                            for (int j = 1; j <= rs.getMetaData().getColumnCount(); j++) {
                                rs.getObject(j);
                            }
                        }
                    }
                    long elapsed = (System.nanoTime() - start) / 1_000_000; // to ms
                    minTime = Math.min(minTime, elapsed);
                }
                
                return minTime;
            }
        }
    }
    
    private static String getQueryForScenario(String scenario, int rows) {
        switch (scenario) {
            case "simple_aggregation":
                return String.format("SELECT COUNT(*), SUM(\"total\"), AVG(\"unit_price\") FROM TEST.\"sales_%d\"", rows);
                
            case "count_distinct_single":
                return String.format("SELECT COUNT(DISTINCT \"customer_id\") FROM TEST.\"sales_%d\"", rows);
                
            case "count_distinct_multiple":
                return String.format("SELECT COUNT(DISTINCT \"customer_id\"), COUNT(DISTINCT \"product_id\"), COUNT(DISTINCT \"category\") FROM TEST.\"sales_%d\"", rows);
                
            case "filtered_aggregation":
                return String.format("SELECT COUNT(DISTINCT \"customer_id\"), SUM(\"total\") FROM TEST.\"sales_%d\" WHERE \"category\" = 'Electronics' AND \"quantity\" > 2", rows);
                
            case "group_by_count_distinct":
                return String.format("SELECT \"category\", COUNT(DISTINCT \"customer_id\"), AVG(\"total\") FROM TEST.\"sales_%d\" GROUP BY \"category\"", rows);
                
            case "complex_join":
                return String.format("SELECT COUNT(DISTINCT s.\"customer_id\"), SUM(s.\"total\") FROM TEST.\"sales_%d\" s JOIN TEST.\"customers_%d\" c ON s.\"customer_id\" = c.\"customer_id\" WHERE c.\"age\" > 30", rows, rows / 10);
                
            default:
                throw new IllegalArgumentException("Unknown scenario: " + scenario);
        }
    }
}