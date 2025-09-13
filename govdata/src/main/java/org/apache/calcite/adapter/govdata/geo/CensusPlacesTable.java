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

import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Census Places (cities/towns) table with population data.
 */
public class CensusPlacesTable extends AbstractTable implements ScannableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(CensusPlacesTable.class);
  
  private final CensusApiClient censusClient;
  
  public CensusPlacesTable(CensusApiClient censusClient) {
    this.censusClient = censusClient;
  }
  
  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        .add("place_name", SqlTypeName.VARCHAR)
        .add("place_fips", SqlTypeName.VARCHAR) 
        .add("state_fips", SqlTypeName.VARCHAR)
        .add("population", SqlTypeName.INTEGER)
        .build();
  }
  
  @Override public Enumerable<Object[]> scan(DataContext root) {
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        try {
          LOGGER.info("Fetching Census places data for all states");
          
          // Get places (cities) population data for all states
          JsonNode data = censusClient.getAcsData(2022, "NAME,B01003_001E", "place:*");
          
          List<Object[]> places = new ArrayList<>();
          
          if (data.has("data") && data.get("data").isArray()) {
            JsonNode dataArray = data.get("data");
            
            // Skip header row (index 0)
            for (int i = 1; i < dataArray.size(); i++) {
              JsonNode row = dataArray.get(i);
              if (row.isArray() && row.size() >= 4) {
                String fullName = row.get(0).asText(); // e.g., "Aberdeen town, North Carolina"
                String population = row.get(1).asText();
                String stateFips = row.get(2).asText();
                String placeFips = row.get(3).asText();
                
                // Extract place name (remove state suffix and type suffix)
                String placeName = fullName;
                // Remove state suffix (e.g., ", North Carolina", ", California", etc.)
                if (placeName.contains(", ")) {
                  int commaIndex = placeName.lastIndexOf(", ");
                  placeName = placeName.substring(0, commaIndex);
                }
                // Remove type suffixes
                placeName = placeName.replaceAll(" (town|city|village|CDP)$", "");
                
                try {
                  int pop = Integer.parseInt(population);
                  places.add(new Object[] {placeName, placeFips, stateFips, pop});
                } catch (NumberFormatException e) {
                  // Skip places with invalid population data
                }
              }
            }
          }
          
          LOGGER.info("Loaded {} Census places", places.size());
          return new CensusPlacesEnumerator(places);
          
        } catch (Exception e) {
          LOGGER.error("Error fetching Census places data", e);
          throw new RuntimeException("Failed to fetch Census places data", e);
        }
      }
    };
  }
  
  private static class CensusPlacesEnumerator implements Enumerator<Object[]> {
    private final Iterator<Object[]> iterator;
    private Object[] current;
    
    CensusPlacesEnumerator(List<Object[]> places) {
      this.iterator = places.iterator();
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