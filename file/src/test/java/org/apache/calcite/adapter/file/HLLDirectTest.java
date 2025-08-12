package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.statistics.HyperLogLogSketch;
import org.apache.calcite.adapter.file.statistics.HLLSketchCache;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Direct test showing HLL provides instant results when used correctly.
 */
public class HLLDirectTest {
    
    @Test
    public void testHLLIsInstantaneous() {
        // Create an HLL sketch with 100K unique values
        HyperLogLogSketch sketch = new HyperLogLogSketch(14);
        
        // Add 100K unique values
        System.out.println("Building HLL sketch with 100K unique values...");
        long startBuild = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            sketch.add("customer_" + i);
        }
        long buildTime = System.currentTimeMillis() - startBuild;
        System.out.println("  Build time: " + buildTime + "ms");
        
        // Store in cache
        HLLSketchCache cache = HLLSketchCache.getInstance();
        cache.putSketch("test_table", "customer_id", sketch);
        
        // Now simulate COUNT(DISTINCT) queries
        System.out.println("\nSimulating COUNT(DISTINCT) queries:");
        
        // Without HLL - simulate scanning 100K rows
        long startScan = System.currentTimeMillis();
        int count = 0;
        for (int run = 0; run < 100; run++) {
            // Simulate row scan
            for (int i = 0; i < 100000; i++) {
                count++; // Simulate processing
            }
            count = count / 100000; // Reset
        }
        long scanTime = (System.currentTimeMillis() - startScan) / 100;
        
        // With HLL - just retrieve estimate
        long startHLL = System.currentTimeMillis();
        long estimate = 0;
        for (int run = 0; run < 100; run++) {
            HyperLogLogSketch cachedSketch = cache.getSketch("test_table", "customer_id");
            if (cachedSketch != null) {
                estimate = cachedSketch.getEstimate();
            }
        }
        long hllTime = (System.currentTimeMillis() - startHLL) / 100;
        
        System.out.println("  Simulated scan time: " + scanTime + "ms");
        System.out.println("  HLL lookup time:     " + hllTime + "ms");
        System.out.println("  HLL estimate:        " + estimate);
        System.out.println("  Speedup:             " + (scanTime > 0 ? scanTime / Math.max(1, hllTime) : "N/A") + "x");
        
        // HLL should be essentially instant (0-1ms)
        assertTrue(hllTime <= 1, "HLL lookup should be instant (<=1ms) but was " + hllTime + "ms");
        
        // Estimate should be close to 100K
        assertTrue(Math.abs(estimate - 100000) < 5000, 
            "HLL estimate should be within 5% of 100K but was " + estimate);
    }
    
    @Test
    public void testHLLWithMillionValues() {
        // Test with 1M unique values to show HLL scales
        HyperLogLogSketch sketch = new HyperLogLogSketch(14);
        
        System.out.println("\nTesting HLL with 1M unique values:");
        
        // Add 1M unique values
        long startBuild = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            sketch.add("id_" + i);
        }
        long buildTime = System.currentTimeMillis() - startBuild;
        
        // Get estimate instantly
        long startEstimate = System.nanoTime();
        long estimate = sketch.getEstimate();
        long estimateNanos = System.nanoTime() - startEstimate;
        
        System.out.println("  Build time:     " + buildTime + "ms");
        System.out.println("  Estimate time:  " + (estimateNanos / 1000) + " microseconds");
        System.out.println("  Estimate:       " + estimate);
        System.out.println("  Actual:         1000000");
        System.out.println("  Error:          " + String.format("%.2f%%", 
            Math.abs(estimate - 1000000) * 100.0 / 1000000));
        
        // Getting estimate should be sub-millisecond
        assertTrue(estimateNanos < 1_000_000, 
            "Getting HLL estimate should be sub-millisecond but was " + 
            (estimateNanos / 1_000_000) + "ms");
    }
}