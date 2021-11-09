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

import org.apiguardian.api.API;

import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A method for returning Lambdas associated with a RelNode class for a particular Lambda class.
 */
@API(status = API.Status.EXPERIMENTAL)
@ThreadSafe
@FunctionalInterface
public interface LambdaProvider {

  /**
   * Get a list of handlers for the requested RelNode/Lambda class combination.
   * @param relNodeClass The class of the RelNode that you want a list of lambdas for.
   * @param handlerClass The specific type MetadataLambda you expect.
   * @param <R> The class of the RelNode that you want a handler for.
   * @param <T> The class of the RelNode that you want a list of lambdas for.
   * @return A list of Lambdas
   * @throws ExecutionException Thrown if a failure occurs while trying to find or build a lambda.
   */
  <R extends RelNode, T extends MetadataLambda> List<T> get(
      Class<R> relNodeClass, Class<T> handlerClass) throws ExecutionException;
}
