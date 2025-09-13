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
package org.apache.calcite.adapter.govdata.geo;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collections;
import java.util.Iterator;

/**
 * TIGER States table.
 */
public class TigerStatesTable extends AbstractTable implements ScannableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TigerStatesTable.class);
  
  private final TigerDataDownloader tigerDownloader;
  
  public TigerStatesTable(TigerDataDownloader tigerDownloader) {
    this.tigerDownloader = tigerDownloader;
  }
  
  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        .add("state_fips", SqlTypeName.VARCHAR)
        .add("state_code", SqlTypeName.VARCHAR)  // 2-letter state code for FK from SEC
        .add("state_name", SqlTypeName.VARCHAR)
        .add("state_abbr", SqlTypeName.VARCHAR)
        .add("land_area", SqlTypeName.DOUBLE)
        .add("water_area", SqlTypeName.DOUBLE)
        .build();
  }
  
  @Override public Enumerable<Object[]> scan(DataContext root) {
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        try {
          LOGGER.info("Fetching TIGER states data");
          
          // Download states shapefile
          File statesDir = tigerDownloader.downloadStates();
          
          // For now, return stub data - would need shapefile parsing
          LOGGER.warn("TIGER shapefile parsing not yet implemented, returning empty result");
          return new TigerStatesEnumerator();
          
        } catch (Exception e) {
          LOGGER.error("Error fetching TIGER states data", e);
          throw new RuntimeException("Failed to fetch TIGER states data", e);
        }
      }
    };
  }
  
  private static class TigerStatesEnumerator implements Enumerator<Object[]> {
    private final Iterator<Object[]> iterator;
    private Object[] current;
    
    TigerStatesEnumerator() {
      this.iterator = Collections.emptyIterator();
    }
    
    @Override public Object[] current() {
      return current;
    }
    
    @Override public boolean moveNext() {
      if (iterator.hasNext()) {
        current = iterator.next();
        return true;
      }
      return false;
    }
    
    @Override public void reset() {
      throw new UnsupportedOperationException("Reset not supported");
    }
    
    @Override public void close() {
      // Nothing to close
    }
  }
}