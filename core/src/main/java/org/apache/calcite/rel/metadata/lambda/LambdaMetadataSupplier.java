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
package org.apache.calcite.rel.metadata.lambda;

import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.google.common.base.Suppliers;

import org.apiguardian.api.API;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Entry point for creating a Lambda-based RelMetadataQuery supplier.
 */
@API(since = "1.29", status = API.Status.EXPERIMENTAL)
public class LambdaMetadataSupplier implements Supplier<RelMetadataQuery> {

  // lazy initialize a supplier so we don't maintain multiple caches
  // but pay no overhead if people want to use their own supplier
  private static final Supplier<LambdaMetadataSupplier> INSTANCE =
      Suppliers.memoize(() -> new LambdaMetadataSupplier());

  /**
   * Get a supplier with a static backing cache.
   *
   * Create a RelMetadataQuery supplier. Note that this lazily creates a static cache of
   * reflection based lambda references so that future calls will avoid recreation
   * of the global cache. Thus the first call is expected to take longer since a number
   * of Lambdas are generated as part of this invocation.
   *
   * @return A supplier that can be provided to RelOptCluster.setMetadataQuerySupplier()
   */
  public static LambdaMetadataSupplier instance() {
    return INSTANCE.get();
  }

  private final LambdaProvider lambdaProvider;

  /**
   * Create a supplier with a default ReflectionToLambdaProvider wrapped in a CachingLambdaProvider.
   */
  public LambdaMetadataSupplier() {
    this(new CachingLambdaProvider(new ReflectionToLambdaProvider()));
  }

  /**
   * Create a supplier with the provided lambda provider. No additional caching is done.
   *
   * For performance purposes, users should wrap the provider with a CachingLambdaProvider to avoid
   * duplicate lookups.
   *
   * @param provider The provider that will be used.
   */
  public LambdaMetadataSupplier(LambdaProvider provider) {
    lambdaProvider = provider;
  }

  /**
   * Create a new unique query object.
   * @return Instance of RelMetadataQuery backed by the classes LambdaProvider.
   */
  @Override public RelMetadataQuery get() {
    Function<RelMetadataQuery, IRelMetadataQuery> supplier = rmq -> {
      LambdaIRMQ lambdas = new LambdaIRMQ(lambdaProvider, rmq);
      CanonicalizingIRMQ nullCanonicalizingRMQ = new CanonicalizingIRMQ(lambdas);
      return nullCanonicalizingRMQ;
    };
    return new DelegatingRelMetadataQuery(supplier);
  }

}
