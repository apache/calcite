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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Looking up methods relating to reflective visitation. Use caching technique
 * to implement, two caching strategy:
 * <ul>
 *   <li>
 *   1. Caching reflect method globally, every instance of the class shares the
 *   global cache to mitigating java reflection invocation overhead.
 *   </li>
 *   <li>
 *   2. Caching reflect method per {@link ReflectiveVisitDispatcher} instance,
 *   caching takes effect between invocation on the same instance.
 *   </li>
 * </ul>
 *
 * @param <E> Argument type
 * @param <R> Return type
 */
public class ReflectVisitDispatcherImpl<R extends ReflectiveVisitor, E>
    implements ReflectiveVisitDispatcher<R, E> {

  /**
   * Global singleton cache with limited size used to cache methods. Every instance
   * of the class will share this cache to mitigating java reflection invocation
   * overhead when looking up methods.
   * <p/>
   * Note that there should be multiple ways to implement caching, for example,
   * 1) Passing <em>Cache</em> between objects and hopefully using
   * <em>RAII (Resource Acquisition Is Initialization)</em> to garbage collect.
   * 2) Using <code>ThreadLocal</code> to store cache.
   * 3) Use global caching.
   * Solution 1) and 2) could introduce complexity to the main course, so the 3)
   * option which introduce a relatively controllable global small sized cache
   * may be a good solution.
   */
  private static final Cache<List<Object>, Method> GLOBAL_METHOD_CACHE =
      CacheBuilder.newBuilder()
          .maximumSize(CalciteSystemProperty.REFLECT_VISIT_DISPATCHER_METHOD_CACHE_MAX_SIZE.value())
          .build();

  /** Use global caching or not */
  private boolean useGlobalMethodCache;

  /** visitE class */
  private Class<E> visiteeBaseClazz;

  /**
   * If caching methods between invocations is preferred, every instance will create a new
   * hash map to cache methods independently.
   */
  private Map<List<Object>, Method> methodMap;

  /**
   * Create an instance.
   *
   * @param visiteeBaseClazz  visitee base class
   * @param useMethodLruCache If set to true, a globally cache with limited size
   *                          will be used to cache methods. If set to false, a new
   *                          {@link HashMap} will used instead.
   */
  public ReflectVisitDispatcherImpl(Class<E> visiteeBaseClazz,
                                    boolean useMethodLruCache) {
    this.visiteeBaseClazz = visiteeBaseClazz;
    this.useGlobalMethodCache = useMethodLruCache;
    if (!useMethodLruCache) {
      this.methodMap = new HashMap<>();
    }
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
    Method method = internalGet(key);
    if (method == null) {
      if (internalContains(key)) {
        // We already looked for the method and found nothing.
      } else {
        method = ReflectUtil.lookupVisitMethod(
            visitorClass,
            visiteeClass,
            visitMethodName,
            additionalParameterTypes);
        internalPut(key, method);
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

  private boolean internalContains(List<Object> key) {
    if (useGlobalMethodCache) {
      return GLOBAL_METHOD_CACHE.getIfPresent(key) != null;
    } else {
      return methodMap.containsKey(key);
    }
  }

  private Method internalGet(List<Object> key) {
    return useGlobalMethodCache ? GLOBAL_METHOD_CACHE.getIfPresent(key) : methodMap.get(key);
  }

  private void internalPut(List<Object> key, Method method) {
    if (useGlobalMethodCache) {
      // Cache needs non null key
      if (method != null) {
        GLOBAL_METHOD_CACHE.put(key, method);
      }
    } else {
      methodMap.put(key, method);
    }
  }
}
