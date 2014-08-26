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

/**
 * Provides an SQL parser and object model. This package, and the dependent
 * <code>org.eigenbase.sql.parser</code> package, are independent of the other
 * saffron packages, so may be used standalone.<p>&nbsp;</p>
 *
 * <h2>Parser</h2>
 *
 * <p>{@link org.eigenbase.sql.parser.SqlParser} parses a SQL string to a parse
 *     tree. It
 *     only performs the most basic syntactic validation.</p>
 *
 * <h2>Object model</h2>
 *
 * <p>Every node in the parse tree is a {@link org.eigenbase.sql.SqlNode}.
 *     Sub-types are:</p>
 * <ul>
 *
 *     <li>{@link org.eigenbase.sql.SqlLiteral} represents a boolean, numeric,
 *         string, or
 *         date constant, or the value <code>NULL</code>.
 *     </li>
 *
 *     <li>{@link org.eigenbase.sql.SqlIdentifier} represents an identifier, such
 *         as <code>
 *             EMPNO</code> or <code>emp.deptno</code>.
 *     </li>
 *
 *     <li>{@link org.eigenbase.sql.SqlCall} is a call to an operator or
 *         function.  By means of special operators, we can use this construct
 *         to represent virtually every non-leaf node in the tree. For example,
 *         a <code>select</code> statement is a call to the 'select'
 *         operator.</li>
 *
 *     <li>{@link org.eigenbase.sql.SqlNodeList} is a list of nodes.</li>
 *
 * </ul>
 *
 * <p>A {@link org.eigenbase.sql.SqlOperator} describes the behavior of a node
 *     in the tree, such as how to un-parse a {@link org.eigenbase.sql.SqlCall}
 *     into a SQL string.  It is important to note that operators are metadata,
 *     not data: there is only one <code>SqlOperator</code> instance
 *     representing the '=' operator, even though there may be many calls to
 *     it.</p>
 *
 * <p><code>SqlOperator</code> has several derived classes which make it easy to
 *     define new operators: {@link org.eigenbase.sql.SqlFunction},
 *     {@link org.eigenbase.sql.SqlBinaryOperator},
 *     {@link org.eigenbase.sql.SqlPrefixOperator},
 *     {@link org.eigenbase.sql.SqlPostfixOperator}.
 *
 *     And there are singleton classes for special syntactic constructs
 *     {@link org.eigenbase.sql.SqlSelectOperator}

 *     and {@link org.eigenbase.sql.SqlJoin.SqlJoinOperator}. (These special operators
 *     even have their own sub-types of {@link org.eigenbase.sql.SqlCall}:
 *     {@link org.eigenbase.sql.SqlSelect}
 *     and {@link org.eigenbase.sql.SqlJoin}.)</p>
 *
 * <p>A {@link org.eigenbase.sql.SqlOperatorTable} is a collection of operators. By
 *     supplying your own operator table, you can customize the dialect of SQL
 *     without
 *     modifying the parser.</p>
 *
 * <h2>Validation</h2>
 *
 * <p>{@link org.eigenbase.sql.validate.SqlValidator} checks that
 *     a tree of {@link org.eigenbase.sql.SqlNode}s is
 *     semantically valid. You supply a {@link org.eigenbase.sql.SqlOperatorTable}
 *     to
 *     describe the available functions and operators, and a
 *     {@link org.eigenbase.sql.validate.SqlValidatorCatalogReader}
 *     for access to the database's catalog.</p>
 *
 * <h2>Generating SQL</h2>
 *
 * <p>A {@link org.eigenbase.sql.SqlWriter} converts a tree of {@link
 *     org.eigenbase.sql.SqlNode}s into a
 *     SQL string. A {@link org.eigenbase.sql.SqlDialect} defines how this
 *     happens.</p>
 *
 */
package org.eigenbase.sql;

// End package-info.java
