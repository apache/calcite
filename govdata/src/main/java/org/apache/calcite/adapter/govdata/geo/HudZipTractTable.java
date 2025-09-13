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

import java.util.Iterator;
import java.util.List;

/**
 * HUD ZIP-Tract crosswalk table.
 */
public class HudZipTractTable extends AbstractTable implements ScannableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(HudZipTractTable.class);
  
  private final HudCrosswalkFetcher hudFetcher;
  
  public HudZipTractTable(HudCrosswalkFetcher hudFetcher) {
    this.hudFetcher = hudFetcher;
  }
  
  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        .add("zip", SqlTypeName.VARCHAR)
        .add("tract_fips", SqlTypeName.VARCHAR)
        .add("res_ratio", SqlTypeName.DOUBLE)
        .add("bus_ratio", SqlTypeName.DOUBLE)
        .add("oth_ratio", SqlTypeName.DOUBLE)
        .add("tot_ratio", SqlTypeName.DOUBLE)
        .add("city", SqlTypeName.VARCHAR)
        .add("state", SqlTypeName.VARCHAR)
        .build();
  }
  
  @Override public Enumerable<Object[]> scan(DataContext root) {
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        try {
          LOGGER.info("Fetching HUD ZIP-Tract crosswalk data for Q2 2024");
          
          // Download latest crosswalk data from HUD API
          java.io.File csvFile = hudFetcher.downloadZipToTract("2", 2024);
          
          // Load the actual crosswalk records from the CSV file
          List<HudCrosswalkFetcher.CrosswalkRecord> records = hudFetcher.loadCrosswalkData(csvFile);
          return new HudZipTractEnumerator(records);
          
        } catch (Exception e) {
          LOGGER.error("Error fetching HUD ZIP-Tract data", e);
          throw new RuntimeException("Failed to fetch HUD ZIP-Tract data", e);
        }
      }
    };
  }
  
  private static class HudZipTractEnumerator implements Enumerator<Object[]> {
    private final Iterator<Object[]> iterator;
    private Object[] current;
    
    HudZipTractEnumerator(List<HudCrosswalkFetcher.CrosswalkRecord> records) {
      this.iterator = records.stream()
          .map(record -> new Object[] {
              record.zip,
              record.geoCode, // tract FIPS
              record.resRatio,
              record.busRatio,
              record.othRatio,
              record.totRatio,
              record.city,
              record.state
          })
          .iterator();
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