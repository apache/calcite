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
package org.apache.calcite.util;

import org.apache.calcite.config.CalciteSystemProperty;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

/**
 * Looking up methods relating to reflective visitation. Caching reflect
 * method globally, every instance of the class shares the global cache to
 * mitigate java reflection invocation overhead.
 *
 * @param <E> Argument type
 * @param <R> Return type
 */
public class ReflectVisitDispatcherImpl<R extends ReflectiveVisitor, E>
    implements ReflectiveVisitDispatcher<R, E> {

  /**
   * Global singleton cache with limited size used to cache methods. Every instance
   * of the class will share this cache to mitigate java reflection invocation
   * overhead when looking up methods.
   * <p>Note that there should be multiple ways to implement caching, for example,
   * <p>1) Passing <em>Cache</em> between objects and hopefully using Java's
   * <em>RAII (Resource Acquisition Is Initialization)</em> like try-finally to
   * garbage collect.
   * <p>2) Using <code>ThreadLocal</code> to store cache per thread.
   * <p>3) Use global caching.
   * <p>Solution 1) and 2) could introduce complexity to the main course and it only
   * works per instance or per thread.
   * Solution 3) which introduces a controllable global small sized cache is a
   * better solution to balance memory usage and performance.
   */
  private static final Cache<List<Object>, Method> GLOBAL_METHOD_CACHE =
      CacheBuilder.newBuilder()
          .maximumSize(CalciteSystemProperty.REFLECT_VISIT_DISPATCHER_METHOD_CACHE_MAX_SIZE.value())
          .build();

  /**
   * visitE class
   */
  private Class<E> visiteeBaseClazz;

  /**
   * Create an instance.
   *
   * @param visiteeBaseClazz visitee base class
   */
  public ReflectVisitDispatcherImpl(Class<E> visiteeBaseClazz) {
    this.visiteeBaseClazz = visiteeBaseClazz;
  }

  public Method lookupVisitMethod(
      Class<? extends R> visitorClass,
      Class<? extends E> visiteeClass,
      String visitMethodName) {
    return lookupVisitMethod(
        visitorClass,
        visiteeClass,
        visitMethodName,
        Collections.emptyList());
  }

  public Method lookupVisitMethod(
      Class<? extends R> visitorClass,
      Class<? extends E> visiteeClass,
      String visitMethodName,
      List<Class> additionalParameterTypes) {
    final List<Object> key =
        ImmutableList.of(
            visitorClass,
            visiteeClass,
            visitMethodName,
            additionalParameterTypes);
    Method method = GLOBAL_METHOD_CACHE.getIfPresent(key);
    if (method == null) {
      //  Maybe we already looked for the method but found nothing,
      //  which usually will not happen. Because Guava cache does
      //  not support containsKey method and does not allow null value.
      //  So we will look up whenever not found.
      method = ReflectUtil.lookupVisitMethod(
          visitorClass,
          visiteeClass,
          visitMethodName,
          additionalParameterTypes);
      if (method != null) {
        GLOBAL_METHOD_CACHE.put(key, method);
      }
    }
    return method;
  }

  public boolean invokeVisitor(
      R visitor,
      E visitee,
      String visitMethodName) {
    return ReflectUtil.invokeVisitor(
        visitor,
        visitee,
        visiteeBaseClazz,
        visitMethodName);
  }
}
