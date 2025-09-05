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
package org.apache.calcite.adapter.sec;

import org.apache.calcite.schema.impl.ScalarFunctionImpl;

/**
 * Vector similarity functions for XBRL embeddings.
 * These functions can be used in SQL queries to find similar documents
 * or text chunks based on their embeddings.
 */
public class SecVectorFunctions {

  /**
   * Computes cosine similarity between two vectors.
   * Returns a value between -1 and 1, where 1 means identical direction,
   * 0 means orthogonal, and -1 means opposite direction.
   *
   * @param vector1 First vector as comma-separated string
   * @param vector2 Second vector as comma-separated string
   * @return Cosine similarity score
   */
  public static double cosineSimilarity(String vector1, String vector2) {
    if (vector1 == null || vector2 == null) {
      return 0.0;
    }

    double[] v1 = parseVector(vector1);
    double[] v2 = parseVector(vector2);

    if (v1.length != v2.length) {
      throw new IllegalArgumentException(
          "Vectors must have the same dimension: " + v1.length + " vs " + v2.length);
    }

    double dotProduct = 0.0;
    double norm1 = 0.0;
    double norm2 = 0.0;

    for (int i = 0; i < v1.length; i++) {
      dotProduct += v1[i] * v2[i];
      norm1 += v1[i] * v1[i];
      norm2 += v2[i] * v2[i];
    }

    if (norm1 == 0.0 || norm2 == 0.0) {
      return 0.0;
    }

    return dotProduct / (Math.sqrt(norm1) * Math.sqrt(norm2));
  }

  /**
   * Computes cosine distance between two vectors.
   * This is 1 - cosine_similarity, ranging from 0 (identical) to 2 (opposite).
   * This mimics the PostgreSQL <-> operator behavior.
   *
   * @param vector1 First vector as comma-separated string
   * @param vector2 Second vector as comma-separated string
   * @return Cosine distance score
   */
  public static double cosineDistance(String vector1, String vector2) {
    return 1.0 - cosineSimilarity(vector1, vector2);
  }

  /**
   * Computes Euclidean distance between two vectors.
   *
   * @param vector1 First vector as comma-separated string
   * @param vector2 Second vector as comma-separated string
   * @return Euclidean distance
   */
  public static double euclideanDistance(String vector1, String vector2) {
    if (vector1 == null || vector2 == null) {
      return Double.MAX_VALUE;
    }

    double[] v1 = parseVector(vector1);
    double[] v2 = parseVector(vector2);

    if (v1.length != v2.length) {
      throw new IllegalArgumentException(
          "Vectors must have the same dimension: " + v1.length + " vs " + v2.length);
    }

    double sum = 0.0;
    for (int i = 0; i < v1.length; i++) {
      double diff = v1[i] - v2[i];
      sum += diff * diff;
    }

    return Math.sqrt(sum);
  }

  /**
   * Computes dot product between two vectors.
   *
   * @param vector1 First vector as comma-separated string
   * @param vector2 Second vector as comma-separated string
   * @return Dot product
   */
  public static double dotProduct(String vector1, String vector2) {
    if (vector1 == null || vector2 == null) {
      return 0.0;
    }

    double[] v1 = parseVector(vector1);
    double[] v2 = parseVector(vector2);

    if (v1.length != v2.length) {
      throw new IllegalArgumentException(
          "Vectors must have the same dimension: " + v1.length + " vs " + v2.length);
    }

    double product = 0.0;
    for (int i = 0; i < v1.length; i++) {
      product += v1[i] * v2[i];
    }

    return product;
  }

  /**
   * Finds whether two vectors are similar based on a threshold.
   * Uses cosine similarity with a default threshold of 0.7.
   *
   * @param vector1 First vector
   * @param vector2 Second vector
   * @param threshold Similarity threshold (default 0.7)
   * @return true if vectors are similar
   */
  public static boolean vectorsSimilar(String vector1, String vector2, Double threshold) {
    double thresh = threshold != null ? threshold : 0.7;
    return cosineSimilarity(vector1, vector2) >= thresh;
  }

  /**
   * Computes the magnitude (L2 norm) of a vector.
   *
   * @param vector Vector as comma-separated string
   * @return Vector magnitude
   */
  public static double vectorNorm(String vector) {
    if (vector == null) {
      return 0.0;
    }

    double[] v = parseVector(vector);
    double sum = 0.0;
    for (double val : v) {
      sum += val * val;
    }

    return Math.sqrt(sum);
  }

  /**
   * Normalizes a vector to unit length.
   *
   * @param vector Vector as comma-separated string
   * @return Normalized vector as comma-separated string
   */
  public static String normalizeVector(String vector) {
    if (vector == null) {
      return null;
    }

    double[] v = parseVector(vector);
    double norm = 0.0;

    for (double val : v) {
      norm += val * val;
    }
    norm = Math.sqrt(norm);

    if (norm == 0.0) {
      return vector;
    }

    StringBuilder result = new StringBuilder();
    for (int i = 0; i < v.length; i++) {
      if (i > 0) {
        result.append(",");
      }
      result.append(v[i] / norm);
    }

    return result.toString();
  }

  /**
   * Parses a comma-separated string into a double array.
   */
  private static double[] parseVector(String vector) {
    String[] parts = vector.split(",");
    double[] result = new double[parts.length];

    for (int i = 0; i < parts.length; i++) {
      try {
        result[i] = Double.parseDouble(parts[i].trim());
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
            "Invalid vector format at position " + i + ": " + parts[i]);
      }
    }

    return result;
  }

  /**
   * Registers all vector functions with a schema.
   */
  public static void registerFunctions(
      org.apache.calcite.schema.SchemaPlus schema) {
    schema.add("COSINE_SIMILARITY",
        ScalarFunctionImpl.create(SecVectorFunctions.class, "cosineSimilarity"));
    schema.add("COSINE_DISTANCE",
        ScalarFunctionImpl.create(SecVectorFunctions.class, "cosineDistance"));
    schema.add("EUCLIDEAN_DISTANCE",
        ScalarFunctionImpl.create(SecVectorFunctions.class, "euclideanDistance"));
    schema.add("DOT_PRODUCT",
        ScalarFunctionImpl.create(SecVectorFunctions.class, "dotProduct"));
    schema.add("VECTORS_SIMILAR",
        ScalarFunctionImpl.create(SecVectorFunctions.class, "vectorsSimilar"));
    schema.add("VECTOR_NORM",
        ScalarFunctionImpl.create(SecVectorFunctions.class, "vectorNorm"));
    schema.add("NORMALIZE_VECTOR",
        ScalarFunctionImpl.create(SecVectorFunctions.class, "normalizeVector"));
  }
}
