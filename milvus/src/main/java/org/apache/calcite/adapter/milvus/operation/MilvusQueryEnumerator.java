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

import org.apache.calcite.adapter.milvus.factory.MilvusSchema;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.util.Pair;

import io.milvus.v2.client.MilvusClientV2;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.List;

/**
 * Enumerator that reads from a Milvus collection with pagination support.
 */
public class MilvusQueryEnumerator implements Enumerator<Object> {
  private static final int DEFAULT_PAGINATION_SIZE = 5000;
  private static final Object NO_CURRENT = new Object();
  private final MilvusClientV2 client;
  private final Iterator<Row> iterator;
  private Object current;

  private MilvusSchema milvusSchema;

  public MilvusQueryEnumerator(
          MilvusSchema schema,
          String collectionName,
          @Nullable String filterExpression,
          @Nullable List<Pair<Integer, MilvusProjectExpression>> projectRowTypeMapForEnumerator) {
    this.milvusSchema = schema;
    this.client = schema.borrowClient();
    this.iterator =
            createIterator(this.client, collectionName, filterExpression, DEFAULT_PAGINATION_SIZE,
                    projectRowTypeMapForEnumerator);
    this.current = NO_CURRENT;
  }

  static Iterator<Row> createIterator(
      MilvusClientV2 client,
      String collectionName,
      @Nullable String filterExpression,
      int batchSize,
      @Nullable List<Pair<Integer, MilvusProjectExpression>> projectRowTypeMapForEnumerator) {
    return new MilvusRowIterator(client, collectionName, filterExpression, batchSize,
        projectRowTypeMapForEnumerator);
  }


  @Override public Object current() {
    if (current == NO_CURRENT) {
      throw new IllegalStateException();
    }
    return current;
  }

  @Override public boolean moveNext() {
    if (iterator.hasNext()) {
      Row row = iterator.next();
      if (row.values.length == 1) {
        current = row.values[0];
      } else {
        current = row.values;
      }
      return true;
    } else {
      current = NO_CURRENT;
      return false;
    }
  }

  @Override public void reset() {

  }

  @Override public void close() {
    try {
      this.milvusSchema.returnClient(this.client);
    } catch (Exception ignore) {
      // ignore
    }
  }

}
