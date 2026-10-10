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
package org.apache.calcite.adapter.milvus.util;

import com.google.common.collect.Lists;

import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.grpc.MutationResult;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.CollectionSchemaParam;
import io.milvus.param.collection.CreateCollectionParam;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.HasCollectionParam;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.index.CreateIndexParam;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for setting up Milvus test environment.
 */
public class TestEnvUtil {
  private String collectionName;
  private String databaseName;
  private MilvusServiceClient milvusServiceClient;
  private final MetricType metricType;

  public TestEnvUtil(String collectionName, MilvusServiceClient milvusServiceClient) {
    this(collectionName, "default", milvusServiceClient, MetricType.L2);
  }

  public TestEnvUtil(String collectionName, String databaseName,
      MilvusServiceClient milvusServiceClient) {
    this(collectionName, databaseName, milvusServiceClient, MetricType.L2);
  }

  public TestEnvUtil(String collectionName, MilvusServiceClient milvusServiceClient,
      MetricType metricType) {
    this(collectionName, "default", milvusServiceClient, metricType);
  }

  public TestEnvUtil(String collectionName, String databaseName,
      MilvusServiceClient milvusServiceClient, MetricType metricType) {
    this.collectionName = collectionName;
    this.databaseName = databaseName != null ? databaseName : "default";
    this.milvusServiceClient = milvusServiceClient;
    this.metricType = metricType;
  }

  public void createExampleCollection() {
    dropCollectionIfExists();
    createFloatVectorCollection();
    generateVectorExampleData();
  }

  private void generateVectorExampleData() {
    List<String> bookNames =
        Lists.newArrayList("TheLittlePrince",
            "ABriefHistoryOfTime",
            "OneHundredYearsOfSolitude",
            "ToLive",
            "FortressBesieged",
            "OrdinaryWorld",
            "ADreamOfRedMansions",
            "ThreeBody",
            "NorwegianWood",
            "TheKiteRunner");

    List<String> bookContents =
        Lists.newArrayList(
            "Once upon a time there was a little prince who lived on a very small"
                + " planet, where there was a rose he cherished very much.",
            "Time is a mysterious phenomenon that is everywhere yet hard to grasp.",
            "Macondo is a small town full of magical colors, where many incredible stories happened.",
            "Life is like a play, and we are all actors in it, experiencing joy and sorrow.",
            "Those inside the fortress want to get out, while those outside want to get in. This is the paradox of life.",
            "Although life is ordinary, everyone has their own dreams and pursuits.",
            "A Dream of Red Mansions is a great work depicting the rise and fall of feudal society.",
            "The arrival of the Trisolaran civilization changed humanity's understanding of the universe.",
            "Norwegian Wood is filled with the confusion and hesitation of youth.",
            "The Kite Runner tells a moving story about friendship and redemption.");

    // Generate float vectors (4 dimensions for testing)
    List<List<Float>> vectors = generateDeterministicVectors(bookNames.size(), 4, 0.1f);

    // Create fields for insertion
    List<InsertParam.Field> fields = new ArrayList<>();
    fields.add(new InsertParam.Field("book_name", bookNames));
    fields.add(new InsertParam.Field("book_content", bookContents));
    fields.add(new InsertParam.Field("VectorFieldAutoTest", vectors));

    // Insert data
    InsertParam insertParam = InsertParam.newBuilder()
        .withCollectionName(collectionName)
        .withDatabaseName(databaseName)
        .withFields(fields)
        .build();

    R<MutationResult> response =
        milvusServiceClient.insert(insertParam);
    if (response.getStatus() != 0) {
      throw new RuntimeException("Failed to insert data: " + response.getMessage());
    }

    CreateIndexParam indexParam = CreateIndexParam.newBuilder()
        .withCollectionName(collectionName)
        .withDatabaseName(databaseName)
        .withFieldName("VectorFieldAutoTest")
        .withIndexName("float_vector_idx")
        .withIndexType(IndexType.IVF_FLAT)
        .withMetricType(metricType)
        .withExtraParam("{\"nlist\":1024}")
        .withSyncMode(Boolean.TRUE)
        .build();

    R<RpcStatus> indexResponse = milvusServiceClient.createIndex(indexParam);
    if (indexResponse.getStatus() != 0) {
      System.out.println("Warning: Failed to create index: " + indexResponse.getMessage());
    }

    // Load collection
    LoadCollectionParam loadParam = LoadCollectionParam.newBuilder()
        .withCollectionName(collectionName)
        .withDatabaseName(databaseName)
        .withSyncLoad(Boolean.TRUE)
        .build();

    R<?> loadResponse = milvusServiceClient.loadCollection(loadParam);
    if (loadResponse.getStatus() != R.Status.Success.getCode()) {
      System.out.println("Warning: Failed to load collection: " + loadResponse.getMessage());
    }
  }


  private List<List<Float>> generateDeterministicVectors(int count, int dimension, float scale) {
    List<List<Float>> vectors = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      List<Float> vector = new ArrayList<>(dimension);
      float base = (i + 1) * scale;
      for (int j = 0; j < dimension; j++) {
        vector.add(base * (j + 1));
      }
      vectors.add(vector);
    }
    return vectors;
  }

  private void dropCollectionIfExists() {
    if (collectionExists(collectionName)) {
      dropCollection();
    }
  }


  private boolean collectionExists(String collectionName) {
    try {
      HasCollectionParam.Builder paramBuilder = HasCollectionParam.newBuilder()
          .withCollectionName(collectionName);
      if (databaseName != null && !"default".equals(databaseName)) {
        paramBuilder.withDatabaseName(databaseName);
      }
      R<Boolean> response = milvusServiceClient.hasCollection(paramBuilder.build());
      return response.getData();
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Creates a collection with FloatVector field specifically for vector search testing.
   * This does not modify the base test class and is specific to vector search scenarios.
   */
  private void createFloatVectorCollection() {
    List<FieldType> fieldsSchema = new ArrayList<>();

    // Primary key field
    fieldsSchema.add(
        FieldType.newBuilder()
            .withName("book_name")
            .withDataType(DataType.VarChar)
            .withMaxLength(200)
            .withPrimaryKey(true)
            .withAutoID(false)
            .build());

    // Content field
    fieldsSchema.add(
        FieldType.newBuilder()
            .withName("book_content")
            .withDataType(DataType.VarChar)
            .withMaxLength(200)
            .build());

    // Float vector field for L2 distance testing
    fieldsSchema.add(
        FieldType.newBuilder()
            .withName("VectorFieldAutoTest")
            .withDataType(DataType.FloatVector)
            .withDimension(4)  // Small dimension for ease of testing
            .build());

    CollectionSchemaParam schemaParam = CollectionSchemaParam.newBuilder()
        .withFieldTypes(fieldsSchema)
        .build();

    CreateCollectionParam.Builder createCollectionReqBuilder = CreateCollectionParam.newBuilder()
        .withCollectionName(collectionName)
        .withDescription("Collection for vector search testing")
        .withShardsNum(2)
        .withSchema(schemaParam);
    if (databaseName != null && !"default".equals(databaseName)) {
      createCollectionReqBuilder.withDatabaseName(databaseName);
    }
    CreateCollectionParam createCollectionReq = createCollectionReqBuilder.build();

    R<RpcStatus> response = milvusServiceClient.createCollection(createCollectionReq);
    if (response.getStatus() != 0) {
      throw new RuntimeException("Failed to create collection: " + response.getMessage());
    }
  }

  private void dropCollection() {
    DropCollectionParam.Builder dropParamBuilder = DropCollectionParam.newBuilder()
        .withCollectionName(collectionName);
    if (databaseName != null && !"default".equals(databaseName)) {
      dropParamBuilder.withDatabaseName(databaseName);
    }
    milvusServiceClient.dropCollection(dropParamBuilder.build());
  }
}
