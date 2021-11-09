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

import org.apache.calcite.rel.RelNode;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A LambdaProvider that maintains a cache for each RelNode class/Lambda class combination,
 * delegating to an underlying LambdaProvider to do actual work.
 */
@ThreadSafe
class CachingLambdaProvider implements LambdaProvider {

  private final LambdaProvider provider;
  private final LoadingCache<Class<? extends RelNode>, LoadingCache<Class, Object>> cache;

  CachingLambdaProvider(final LambdaProvider provider) {
    this.provider = provider;
    this.cache = CacheBuilder.newBuilder()
        .build(new CacheLoader<Class, LoadingCache<Class, Object>>() {
          @Override public LoadingCache<Class, Object> load(final Class relNodeClazz)
              throws Exception {
            return CacheBuilder.<Class, Object>newBuilder().build(
                new CacheLoader<Class, Object>() {
                  @Override public Object load(final Class lambdaClazz) throws Exception {
                    return provider.get((Class<? extends RelNode>) relNodeClazz, lambdaClazz);
                  }
                });
          }
        });
  }

  public <R extends RelNode, T extends MetadataLambda> List<T> get(
      Class<R> relNodeClazz,
      Class<T> lambdaClazz) throws ExecutionException {
    return (List<T>) cache.get(relNodeClazz).get(lambdaClazz);
  }

}
