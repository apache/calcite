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
package org.apache.calcite.rel.type;

/**
 * Describes a policy for resolving fields in record types.
 *
 * <p>The usual value is {@link #FULLY_QUALIFIED}.
 *
 * <p>A field whose record type is labeled {@link #PEEK_FIELDS} can be omitted.
 * In Phoenix, column families are represented by fields like this.
 * {@link #PEEK_FIELDS_DEFAULT} is similar, but represents the default column
 * family, so it will win in the event of a tie.
 *
 * <p>SQL usually disallows a record type. For instance,
 *
 * <blockquote><pre>SELECT address.zip FROM Emp AS e</pre></blockquote>
 *
 * <p>is disallowed because {@code address} "looks like" a table alias. You'd
 * have to write
 *
 * <blockquote><pre>SELECT e.address.zip FROM Emp AS e</pre></blockquote>
 *
 * <p>But if a table has one or more columns that are record-typed and are
 * labeled {@link #PEEK_FIELDS} or {@link #PEEK_FIELDS_DEFAULT} we suspend that
 * rule and would allow {@code address.zip}.
 *
 * <p>If there are multiple matches, we choose the one that is:
 * <ol>
 *   <li>Shorter. If you write {@code zipcode}, {@code address.zipcode} will
 *   be preferred over {@code product.supplier.zipcode}.
 *   <li>Uses as little skipping as possible. A match that is fully-qualified
 *   will beat one that uses {@code PEEK_FIELDS_DEFAULT} at some point, which
 *   will beat one that uses {@code PEEK_FIELDS} at some point.
 * </ol>
 */
public enum StructKind {
  /** This is not a structured type. */
  NONE,

  /** This is a traditional structured type, where each field must be
   * referenced explicitly.
   *
   * <p>Also, when referencing a struct column, you
   * need to qualify it with the table alias, per standard SQL. For instance,
   * {@code SELECT c.address.zipcode FROM customer AS c}
   * is valid but
   * {@code SELECT address.zipcode FROM customer}
   * it not valid.
   */
  FULLY_QUALIFIED,

  /** As {@link #PEEK_FIELDS}, but takes priority if another struct-typed
   * field also has a field of the name being sought.
   *
   * <p>In Phoenix, only one of a table's columns is labeled
   * {@code PEEK_FIELDS_DEFAULT} - the default column family - but in principle
   * there could be more than one. */
  PEEK_FIELDS_DEFAULT,

  /** If a field has this type, you can see its fields without qualifying them
   * with the name of this field.
   *
   * <p>For example, if {@code address} is labeled {@code PEEK_FIELDS}, you
   * could write {@code zipcode} as shorthand for {@code address.zipcode}. */
  PEEK_FIELDS,

  /** As {@link #PEEK_FIELDS}, but fields are not expanded in "SELECT *".
   *
   * <p>Used in Flink, not Phoenix. */
  PEEK_FIELDS_NO_EXPAND,
}

// End StructKind.java
