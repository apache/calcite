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
package org.apache.calcite.adapter.file.execution.duckdb;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * Creates proxy Object[] arrays that only fetch column values when accessed.
 * 
 * <p>This allows operations like COUNT(*) to iterate through rows without
 * ever fetching the actual column data.
 */
public class LazyRowProxy {
  
  /**
   * Creates a lazy Object[] proxy that defers column fetching.
   * 
   * @param resultSet The current ResultSet positioned at a row
   * @param columnCount Number of columns
   * @param accessTracker Optional tracker for column access patterns
   * @return A proxy that acts like Object[] but fetches lazily
   */
  public static Object[] createLazyRow(ResultSet resultSet, int columnCount, 
                                       ColumnAccessTracker accessTracker) {
    return (Object[]) Proxy.newProxyInstance(
        Object[].class.getClassLoader(),
        new Class<?>[] { Object[].class },
        new LazyRowHandler(resultSet, columnCount, accessTracker)
    );
  }
  
  /**
   * Handler that intercepts array access and lazily loads values.
   */
  private static class LazyRowHandler implements InvocationHandler {
    private final ResultSet resultSet;
    private final int columnCount;
    private final Object[] cache;
    private final boolean[] cached;
    private final ColumnAccessTracker accessTracker;
    
    LazyRowHandler(ResultSet resultSet, int columnCount, ColumnAccessTracker accessTracker) {
      this.resultSet = resultSet;
      this.columnCount = columnCount;
      this.cache = new Object[columnCount];
      this.cached = new boolean[columnCount];
      this.accessTracker = accessTracker;
    }
    
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      String methodName = method.getName();
      
      // Handle array access methods
      if ("get".equals(methodName) && args != null && args.length == 1) {
        int index = (Integer) args[0];
        return getElement(index);
      }
      
      if ("length".equals(methodName) || "size".equals(methodName)) {
        return columnCount;
      }
      
      if ("clone".equals(methodName)) {
        // Force materialization for clone
        Object[] result = new Object[columnCount];
        for (int i = 0; i < columnCount; i++) {
          result[i] = getElement(i);
        }
        return result;
      }
      
      if ("toString".equals(methodName)) {
        return toString();
      }
      
      // Default: delegate to Object methods
      return method.invoke(this, args);
    }
    
    private Object getElement(int index) throws SQLException {
      if (index < 0 || index >= columnCount) {
        throw new ArrayIndexOutOfBoundsException(index);
      }
      
      // Only fetch if not cached
      if (!cached[index]) {
        cache[index] = resultSet.getObject(index + 1);
        cached[index] = true;
        
        if (accessTracker != null) {
          accessTracker.recordAccess(index);
        }
      }
      
      return cache[index];
    }
    
    @Override
    public String toString() {
      int loadedCount = 0;
      for (boolean c : cached) {
        if (c) loadedCount++;
      }
      return "LazyRow[" + loadedCount + "/" + columnCount + " loaded]";
    }
  }
  
  /**
   * Tracks column access patterns for optimization insights.
   */
  public static class ColumnAccessTracker {
    private final boolean[] accessed;
    private final int columnCount;
    private int totalAccesses = 0;
    private int firstAccessedColumn = -1;
    
    public ColumnAccessTracker(int columnCount) {
      this.columnCount = columnCount;
      this.accessed = new boolean[columnCount];
    }
    
    public void recordAccess(int columnIndex) {
      if (!accessed[columnIndex]) {
        accessed[columnIndex] = true;
        if (firstAccessedColumn == -1) {
          firstAccessedColumn = columnIndex;
        }
      }
      totalAccesses++;
    }
    
    public void logStatistics() {
      int accessedCount = 0;
      for (boolean a : accessed) {
        if (a) accessedCount++;
      }
      
      if (totalAccesses > 0) {
        System.out.println("Column access statistics:");
        System.out.println("  Columns accessed: " + accessedCount + "/" + columnCount);
        System.out.println("  Total accesses: " + totalAccesses);
        System.out.println("  First accessed column: " + firstAccessedColumn);
        
        if (accessedCount == 0) {
          System.out.println("  OPTIMIZATION: No columns were accessed (e.g., COUNT(*))");
        } else if (accessedCount < columnCount / 2) {
          System.out.println("  OPTIMIZATION: Only " + (accessedCount * 100 / columnCount) + 
                           "% of columns accessed - consider projection pushdown");
        }
      }
    }
    
    public boolean[] getAccessedColumns() {
      return Arrays.copyOf(accessed, accessed.length);
    }
    
    public int getTotalAccesses() {
      return totalAccesses;
    }
  }
}