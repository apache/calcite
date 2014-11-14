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

import java.lang.reflect.Method;
import java.util.List;

/**
 * Interface for looking up methods relating to reflective visitation. One
 * possible implementation would cache the results.
 *
 * <p>Type parameter 'R' is the base class of visitoR class; type parameter 'E'
 * is the base class of visiteE class.
 *
 * <p>TODO: obsolete {@link ReflectUtil#lookupVisitMethod}, and use caching in
 * implementing that method.
 *
 * @param <E> Argument type
 * @param <R> Return type
 */
public interface ReflectiveVisitDispatcher<R extends ReflectiveVisitor, E> {
  //~ Methods ----------------------------------------------------------------

  /**
   * Looks up a visit method taking additional parameters beyond the
   * overloaded visitee type.
   *
   * @param visitorClass             class of object whose visit method is to be
   *                                 invoked
   * @param visiteeClass             class of object to be passed as a parameter
   *                                 to the visit method
   * @param visitMethodName          name of visit method
   * @param additionalParameterTypes list of additional parameter types
   * @return method found, or null if none found
   */
  Method lookupVisitMethod(
      Class<? extends R> visitorClass,
      Class<? extends E> visiteeClass,
      String visitMethodName,
      List<Class> additionalParameterTypes);

  /**
   * Looks up a visit method.
   *
   * @param visitorClass    class of object whose visit method is to be invoked
   * @param visiteeClass    class of object to be passed as a parameter to the
   *                        visit method
   * @param visitMethodName name of visit method
   * @return method found, or null if none found
   */
  Method lookupVisitMethod(
      Class<? extends R> visitorClass,
      Class<? extends E> visiteeClass,
      String visitMethodName);

  /**
   * Implements the {@link org.apache.calcite.util.Glossary#VISITOR_PATTERN} via
   * reflection. The basic technique is taken from <a
   * href="http://www.javaworld.com/javaworld/javatips/jw-javatip98.html">a
   * Javaworld article</a>. For an example of how to use it, see
   * {@code ReflectVisitorTest}.
   *
   * <p>Visit method lookup follows the same rules as if compile-time resolution
   * for VisitorClass.visit(VisiteeClass) were performed. An ambiguous match due
   * to multiple interface inheritance results in an IllegalArgumentException. A
   * non-match is indicated by returning false.</p>
   *
   * @param visitor         object whose visit method is to be invoked
   * @param visitee         object to be passed as a parameter to the visit
   *                        method
   * @param visitMethodName name of visit method, e.g. "visit"
   * @return true if a matching visit method was found and invoked
   */
  boolean invokeVisitor(
      R visitor,
      E visitee,
      String visitMethodName);
}

// End ReflectiveVisitDispatcher.java
