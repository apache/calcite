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
package org.eigenbase.sql2rel;

import java.util.List;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;

/**
 * DefaultValueFactory supplies default values for INSERT, UPDATE, and NEW.
 *
 * <p>TODO jvs 26-Feb-2005: rename this to InitializerExpressionFactory, since
 * it is in the process of being generalized to handle constructor invocations
 * and eventually generated columns.
 */
public interface DefaultValueFactory {
  //~ Methods ----------------------------------------------------------------

  /**
   * Whether a column is always generated. If a column is always generated,
   * then non-generated values cannot be inserted into the column.
   */
  boolean isGeneratedAlways(
      RelOptTable table,
      int iColumn);

  /**
   * Creates an expression which evaluates to the default value for a
   * particular column.
   *
   * @param table   the table containing the column
   * @param iColumn the 0-based offset of the column in the table
   * @return default value expression
   */
  RexNode newColumnDefaultValue(
      RelOptTable table,
      int iColumn);

  /**
   * Creates an expression which evaluates to the initializer expression for a
   * particular attribute of a structured type.
   *
   * @param type            the structured type
   * @param constructor     the constructor invoked to initialize the type
   * @param iAttribute      the 0-based offset of the attribute in the type
   * @param constructorArgs arguments passed to the constructor invocation
   * @return default value expression
   */
  RexNode newAttributeInitializer(
      RelDataType type,
      SqlFunction constructor,
      int iAttribute,
      List<RexNode> constructorArgs);
}

// End DefaultValueFactory.java
