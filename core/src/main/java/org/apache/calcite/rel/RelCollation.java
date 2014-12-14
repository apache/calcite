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
package org.apache.calcite.rel;

import org.apache.calcite.plan.RelMultipleTrait;

import java.util.List;

/**
 * Description of the physical ordering of a relational expression.
 *
 * <p>An ordering consists of a list of one or more column ordinals and the
 * direction of the ordering.
 */
public interface RelCollation extends RelMultipleTrait {
  //~ Methods ----------------------------------------------------------------

  /**
   * Returns the ordinals and directions of the columns in this ordering.
   */
  List<RelFieldCollation> getFieldCollations();
}

// End RelCollation.java
