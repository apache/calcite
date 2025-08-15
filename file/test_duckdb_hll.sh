#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
echo "=== Testing DuckDB HLL Optimization ==="
echo "This test will definitively show if COUNT(DISTINCT) is being intercepted by HLL"
echo ""

# Compile
echo "Compiling..."
../gradlew :file:compileJava :file:compileTestJava --console=plain > /dev/null 2>&1

# Run a simple test with DuckDB
echo "Running COUNT(DISTINCT) test with DuckDB..."
echo ""

# Create a simple test that shows all logs
cat > TestDuckDBHLL.java << 'EOF'
import org.apache.calcite.adapter.file.FileSchemaFactory;
import org.apache.calcite.adapter.file.statistics.*;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import java.io.*;
import java.sql.*;
import java.util.*;

public class TestDuckDBHLL {
    public static void main(String[] args) throws Exception {
        // Create test data
        File tempDir = new File("/tmp/duckdb_hll_test");
        tempDir.mkdirs();
        File csvFile = new File(tempDir, "test_table.csv");

        try (PrintWriter writer = new PrintWriter(csvFile)) {
            writer.println("id,value");
            for (int i = 1; i <= 10000; i++) {
                writer.println(i + "," + (i % 100));
            }
        }

        // Create and populate HLL cache
        HLLSketchCache cache = HLLSketchCache.getInstance();
        cache.invalidateAll();

        HyperLogLogSketch sketch = new HyperLogLogSketch(12);
        for (int i = 1; i <= 10000; i++) {
            sketch.add(i);
        }

        System.err.println("[TEST] Storing HLL sketch: TEST.test_table.id with estimate=" + sketch.getEstimate());
        cache.putSketch("TEST", "test_table", "id", sketch);

        // Test with DuckDB
        try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
             CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class)) {

            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            Map<String, Object> operand = new LinkedHashMap<>();
            operand.put("directory", tempDir.getAbsolutePath());
            operand.put("executionEngine", "duckdb");

            rootSchema.add("TEST",
                FileSchemaFactory.INSTANCE.create(rootSchema, "TEST", operand));

            String query = "SELECT COUNT(DISTINCT id) FROM TEST.test_table";

            System.err.println("[TEST] Executing: " + query);

            long start = System.nanoTime();
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {
                rs.next();
                long result = rs.getLong(1);
                long elapsed = System.nanoTime() - start;
                double elapsedMs = elapsed / 1_000_000.0;

                System.err.println("[TEST] Result: " + result);
                System.err.println("[TEST] Execution time: " + String.format("%.3f ms", elapsedMs));

                if (elapsedMs < 0.1) {
                    System.err.println("[TEST] ✅ SUCCESS! HLL optimization is working (< 0.1ms)");
                } else {
                    System.err.println("[TEST] ❌ FAILED! Query was pushed to DuckDB (" + elapsedMs + " ms)");
                }
            }
        }
    }
}
EOF

# Compile and run the test
echo "Compiling test..."
javac -cp "build/classes/java/main:build/classes/java/test:$(../gradlew -q :file:dependencies --configuration testRuntimeClasspath | grep -E '\.jar' | head -100 | tr '\n' ':')" TestDuckDBHLL.java

echo ""
echo "Running test..."
java -cp ".:build/classes/java/main:build/classes/java/test:$(../gradlew -q :file:dependencies --configuration testRuntimeClasspath | grep -E '\.jar' | head -100 | tr '\n' ':')" \
    -Dcalcite.file.statistics.hll.enabled=true \
    TestDuckDBHLL 2>&1 | grep "\[TEST\]\|\[DUCKDB\]\|\[HLL\]\|\[INTERCEPT\]"

echo ""
echo "=== Test Complete ==="
