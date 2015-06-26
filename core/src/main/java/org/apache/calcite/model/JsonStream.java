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
package org.apache.calcite.model;

/**
 * Information about whether a table allows streaming.
 *
 * <p>Occurs within {@link JsonTable#stream}.
 *
 * @see org.apache.calcite.model.JsonRoot Description of schema elements
 * @see org.apache.calcite.model.JsonTable#stream
 */
public class JsonStream {
  /** Whether the table allows streaming.
   *
   * <p>Optional; default true.
   */
  public boolean stream = true;

  /** Whether the history of the table is available.
   *
   * <p>Optional; default false.
   */
  public boolean history = false;
}

// End JsonStream.java
