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
package org.apache.calcite.sql.fun;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation that is read by {@link SqlLibraryOperatorTableFactory} to
 * add functions and operators to a library.
 *
 * <p>Typically, such collections are associated with a particular dialect or
 * database. For example, {@link SqlLibrary#ORACLE} is a collection of functions
 * that are in the Oracle database but not the SQL standard.
 *
 * <p>In {@link SqlLibraryOperatorTableFactory} this annotation is applied to
 * function definitions to include them in a particular library. It allows
 * an operator to belong to more than one library.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface LibraryOperator {

  /** The set of libraries that this function or operator belongs to.
   * Must not be null or empty. */
  SqlLibrary[] libraries();
}

// End LibraryOperator.java
