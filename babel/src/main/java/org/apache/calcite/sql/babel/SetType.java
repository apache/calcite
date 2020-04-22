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
package org.apache.calcite.sql.babel;

/**
 * Enumerates the types of sets.
 */
public enum SetType {
  /**
   * Set type not specified. Defaults to MULTISET in ANSI mode and SET in
   * Teradata mode. The way to set the session mode in Teradata depends on what
   * client software is being used. More information can be found at:
   * https://docs.teradata.com/reader/Daz9Bt8GiwSdtthYFn~vdw/jDfW4crLGusqMwxGjie8Ww
   */
  UNSPECIFIED,

  /**
   * Duplicate rows are not permitted.
   */
  SET,

  /**
   * Duplicate rows are permitted, in compliance with the ANSI SQL:2011 standard.
   */
  MULTISET,
}
