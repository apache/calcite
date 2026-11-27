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
package org.apache.calcite.adapter.milvus.operation;

import org.apache.calcite.util.Pair;

import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.response.QueryResp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Iterator for reading rows from a Milvus collection with pagination support.
 */
public class MilvusRowIterator implements Iterator<Row> {
  private final MilvusClientV2 client;
  private final String collectionName;
  private final List<String> project;
  private final String filterExpression;
  private final int pageSize;
  private final Map<Integer, MilvusProjectExpression> projectRowTypeMap;

  private Iterator<Row> currentPageIterator = null;
  private long offset = 0;
  private boolean hasMore = true;

  MilvusRowIterator(
      MilvusClientV2 client,
      String collectionName,
      String filterExpression,
      int pageSize,
      List<Pair<Integer, MilvusProjectExpression>> projectRowTypeMapForEnumerator) {
    this.client = client;
    this.collectionName = collectionName;
    this.filterExpression = filterExpression;
    this.pageSize = pageSize;

    // Build projectRowTypeMap and extract field names for querying Milvus
    if (projectRowTypeMapForEnumerator != null && !projectRowTypeMapForEnumerator.isEmpty()) {
      this.projectRowTypeMap = new LinkedHashMap<>();
      this.project = new ArrayList<>();
      for (Pair<Integer, MilvusProjectExpression> pair : projectRowTypeMapForEnumerator) {
        MilvusProjectExpression expr = pair.right;
        this.projectRowTypeMap.put(pair.left, expr);

        // Extract InputField names for Milvus query
        if (expr instanceof MilvusProjectExpression.InputField) {
          String fieldName = ((MilvusProjectExpression.InputField) expr).getFieldName();
          this.project.add(fieldName);
        }
      }
    } else {
      this.project = null;
      this.projectRowTypeMap = null;
    }
  }

  @Override public boolean hasNext() {
    // Check if we have more data in the current page
    if (currentPageIterator != null && currentPageIterator.hasNext()) {
      return true;
    }

    // No more data in current page, try to load next page
    if (hasMore) {
      loadNextPage();
      return currentPageIterator != null && currentPageIterator.hasNext();
    }

    return false;
  }

  @Override public Row next() {
    if (currentPageIterator == null || !currentPageIterator.hasNext()) {
      if (hasMore) {
        loadNextPage();
      }
      if (currentPageIterator == null || !currentPageIterator.hasNext()) {
        throw new java.util.NoSuchElementException();
      }
    }
    return currentPageIterator.next();
  }

  private void loadNextPage() {
    if (!hasMore) {
      return;
    }

    try {
      // Query a page of records from Milvus
      QueryReq.QueryReqBuilder queryBuilder = QueryReq.builder()
          .collectionName(collectionName)
          .outputFields(project)
          .limit(pageSize);

      if (filterExpression != null && !filterExpression.isEmpty()) {
        queryBuilder.filter(filterExpression);
      }

      if (offset > 0) {
        queryBuilder.offset(offset);
      }

      QueryReq queryReq = queryBuilder.build();
      QueryResp response = client.query(queryReq);

      List<QueryResp.QueryResult> queryResults = response.getQueryResults();

      if (queryResults == null || queryResults.isEmpty()) {
        hasMore = false;
        currentPageIterator = Collections.emptyIterator();
        return;
      }

      int rowCount = queryResults.size();

      if (rowCount == 0) {
        hasMore = false;
        currentPageIterator = Collections.emptyIterator();
        return;
      }

      currentPageIterator = createIteratorFromQueryResults(queryResults, projectRowTypeMap);

      // Update offset and hasMore for next page
      if (rowCount < pageSize) {
        hasMore = false;
      } else {
        offset += rowCount;
      }
    } catch (Exception e) {
      hasMore = false;
      throw new RuntimeException("Error loading next page from Milvus: " + e.getMessage(), e);
    }
  }

  private static Object[] fillProjectRow(
      Map<String, Object> entity,
      Map<Integer, MilvusProjectExpression> projectRowTypeMap) {
    if (projectRowTypeMap == null) {
      return entity.values().toArray();
    }
    Object[] rowValues = new Object[projectRowTypeMap.size()];
    for (Map.Entry<Integer, MilvusProjectExpression> entry : projectRowTypeMap.entrySet()) {
      Integer position = entry.getKey();
      MilvusProjectExpression expr = entry.getValue();
      if (expr.getType() == MilvusProjectExpression.ExpressionType.CONSTANT) {
        rowValues[position] = ((MilvusProjectExpression.Constant) expr).getValue();
      } else if (expr.getType() == MilvusProjectExpression.ExpressionType.INPUT_FIELD
          && expr instanceof MilvusProjectExpression.InputField) {
        rowValues[position] =
            entity.get(((MilvusProjectExpression.InputField) expr).getFieldName());
      } else {
        rowValues[position] = null;
      }
    }
    return rowValues;
  }

  private Iterator<Row> createIteratorFromQueryResults(
      List<QueryResp.QueryResult> queryResults,
      Map<Integer, MilvusProjectExpression> projectRowTypeMap) {
    List<Row> pageRows = new ArrayList<>();

    for (QueryResp.QueryResult result : queryResults) {
      Map<String, Object> entity = result.getEntity();
      Object[] rowValues = fillProjectRow(entity, projectRowTypeMap);
      pageRows.add(new Row(rowValues));
    }

    return pageRows.iterator();
  }
}
