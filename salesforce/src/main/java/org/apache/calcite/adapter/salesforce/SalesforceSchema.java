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
package org.apache.calcite.adapter.salesforce;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Schema for Salesforce sObjects.
 */
public class SalesforceSchema extends AbstractSchema {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(SalesforceSchema.class);
  
  private final SalesforceConnection connection;
  private final LoadingCache<String, SalesforceConnection.SObjectDescription> descriptionCache;
  private Map<String, Table> tableMap;
  
  public SalesforceSchema(SalesforceConnection connection, int cacheMaxSize) {
    this.connection = connection;
    this.descriptionCache = CacheBuilder.newBuilder()
        .maximumSize(cacheMaxSize)
        .expireAfterWrite(1, TimeUnit.HOURS)
        .build(new CacheLoader<String, SalesforceConnection.SObjectDescription>() {
          @Override
          public SalesforceConnection.SObjectDescription load(String sObjectType) 
              throws IOException {
            return connection.describeSObject(sObjectType);
          }
        });
  }
  
  @Override
  protected Map<String, Table> getTableMap() {
    if (tableMap == null) {
      tableMap = createTableMap();
    }
    return tableMap;
  }
  
  private Map<String, Table> createTableMap() {
    try {
      List<SalesforceConnection.SObjectBasicInfo> sObjects = connection.listSObjects();
      ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
      
      for (SalesforceConnection.SObjectBasicInfo sObject : sObjects) {
        if (sObject.queryable) {
          builder.put(sObject.name, new SalesforceTable(this, sObject.name));
          
          // Also add with lowercase name for case-insensitive matching
          String lowerName = sObject.name.toLowerCase();
          if (!lowerName.equals(sObject.name)) {
            builder.put(lowerName, new SalesforceTable(this, sObject.name));
          }
        }
      }
      
      return builder.build();
    } catch (IOException e) {
      LOGGER.error("Failed to list Salesforce sObjects", e);
      throw new RuntimeException("Failed to list Salesforce sObjects", e);
    }
  }
  
  /**
   * Get the cached description for an sObject.
   */
  public SalesforceConnection.SObjectDescription getDescription(String sObjectType) {
    try {
      return descriptionCache.get(sObjectType);
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to describe sObject: " + sObjectType, e);
    }
  }
  
  /**
   * Get the connection to Salesforce.
   */
  public SalesforceConnection getConnection() {
    return connection;
  }
}