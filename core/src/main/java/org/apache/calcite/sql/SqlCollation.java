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
package org.apache.calcite.sql;

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.util.Glossary;
import org.apache.calcite.util.SerializableCharset;
import org.apache.calcite.util.Util;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Locale;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * A <code>SqlCollation</code> is an object representing a <code>Collate</code>
 * statement. It is immutable.
 */
public class SqlCollation implements Serializable {
  public static final SqlCollation COERCIBLE =
      new SqlCollation(Coercibility.COERCIBLE);
  public static final SqlCollation IMPLICIT =
      new SqlCollation(Coercibility.IMPLICIT);

  //~ Enums ------------------------------------------------------------------

  /**
   * <blockquote>A &lt;character value expression&gt; consisting of a column
   * reference has the coercibility characteristic Implicit, with collating
   * sequence as defined when the column was created. A &lt;character value
   * expression&gt; consisting of a value other than a column (e.g., a host
   * variable or a literal) has the coercibility characteristic Coercible,
   * with the default collation for its character repertoire. A &lt;character
   * value expression&gt; simply containing a &lt;collate clause&gt; has the
   * coercibility characteristic Explicit, with the collating sequence
   * specified in the &lt;collate clause&gt;.</blockquote>
   *
   * @see Glossary#SQL99 SQL:1999 Part 2 Section 4.2.3
   */
  public enum Coercibility {
    /** Strongest coercibility. */
    EXPLICIT,
    IMPLICIT,
    COERCIBLE,
    /** Weakest coercibility. */
    NONE
  }

  //~ Instance fields --------------------------------------------------------

  protected final String collationName;
  protected final SerializableCharset wrappedCharset;
  protected final Locale locale;
  protected final String strength;
  private final Coercibility coercibility;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a Collation by its name and its coercibility
   *
   * @param collation    Collation specification
   * @param coercibility Coercibility
   */
  public SqlCollation(
      String collation,
      Coercibility coercibility) {
    this.coercibility = coercibility;
    SqlParserUtil.ParsedCollation parseValues =
        SqlParserUtil.parseCollation(collation);
    Charset charset = parseValues.getCharset();
    this.wrappedCharset = SerializableCharset.forCharset(charset);
    locale = parseValues.getLocale();
    strength = parseValues.getStrength();
    String c =
        charset.name().toUpperCase(Locale.ROOT) + "$" + locale.toString();
    if ((strength != null) && (strength.length() > 0)) {
      c += "$" + strength;
    }
    collationName = c;
  }

  /**
   * Creates a SqlCollation with the default collation name and the given
   * coercibility.
   *
   * @param coercibility Coercibility
   */
  public SqlCollation(Coercibility coercibility) {
    this(
        CalciteSystemProperty.DEFAULT_COLLATION.value(),
        coercibility);
  }

  //~ Methods ----------------------------------------------------------------

  public boolean equals(Object o) {
    return this == o
        || o instanceof SqlCollation
        && collationName.equals(((SqlCollation) o).collationName);
  }

  @Override public int hashCode() {
    return collationName.hashCode();
  }

  /**
   * Returns the collating sequence (the collation name) and the coercibility
   * for the resulting value of a dyadic operator.
   *
   * @param col1 first operand for the dyadic operation
   * @param col2 second operand for the dyadic operation
   * @return the resulting collation sequence. The "no collating sequence"
   * result is returned as null.
   *
   * @see Glossary#SQL99 SQL:1999 Part 2 Section 4.2.3 Table 2
   */
  public static SqlCollation getCoercibilityDyadicOperator(
      SqlCollation col1,
      SqlCollation col2) {
    return getCoercibilityDyadic(col1, col2);
  }

  /**
   * Returns the collating sequence (the collation name) and the coercibility
   * for the resulting value of a dyadic operator.
   *
   * @param col1 first operand for the dyadic operation
   * @param col2 second operand for the dyadic operation
   * @return the resulting collation sequence
   *
   * @throws org.apache.calcite.runtime.CalciteException from
   *   {@link org.apache.calcite.runtime.CalciteResource#invalidCompare} or
   *   {@link org.apache.calcite.runtime.CalciteResource#differentCollations}
   *   if no collating sequence can be deduced
   *
   * @see Glossary#SQL99 SQL:1999 Part 2 Section 4.2.3 Table 2
   */
  public static SqlCollation getCoercibilityDyadicOperatorThrows(
      SqlCollation col1,
      SqlCollation col2) {
    SqlCollation ret = getCoercibilityDyadic(col1, col2);
    if (null == ret) {
      throw RESOURCE.invalidCompare(
          col1.collationName,
          "" + col1.coercibility,
          col2.collationName,
          "" + col2.coercibility).ex();
    }
    return ret;
  }

  /**
   * Returns the collating sequence (the collation name) to use for the
   * resulting value of a comparison.
   *
   * @param col1 first operand for the dyadic operation
   * @param col2 second operand for the dyadic operation
   *
   * @return the resulting collation sequence. If no collating
   * sequence could be deduced throws a
   * {@link org.apache.calcite.runtime.CalciteResource#invalidCompare}
   *
   * @see Glossary#SQL99 SQL:1999 Part 2 Section 4.2.3 Table 3
   */
  public static String getCoercibilityDyadicComparison(
      SqlCollation col1,
      SqlCollation col2) {
    return getCoercibilityDyadicOperatorThrows(col1, col2).collationName;
  }

  /**
   * Returns the result for {@link #getCoercibilityDyadicComparison} and
   * {@link #getCoercibilityDyadicOperator}.
   */
  protected static SqlCollation getCoercibilityDyadic(
      SqlCollation col1,
      SqlCollation col2) {
    assert null != col1;
    assert null != col2;
    final Coercibility coercibility1 = col1.getCoercibility();
    final Coercibility coercibility2 = col2.getCoercibility();
    switch (coercibility1) {
    case COERCIBLE:
      switch (coercibility2) {
      case COERCIBLE:
        return col2;
      case IMPLICIT:
        return col2;
      case NONE:
        return null;
      case EXPLICIT:
        return col2;
      default:
        throw Util.unexpected(coercibility2);
      }
    case IMPLICIT:
      switch (coercibility2) {
      case COERCIBLE:
        return col1;
      case IMPLICIT:
        if (col1.collationName.equals(col2.collationName)) {
          return col2;
        }
        return null;
      case NONE:
        return null;
      case EXPLICIT:
        return col2;
      default:
        throw Util.unexpected(coercibility2);
      }
    case NONE:
      switch (coercibility2) {
      case COERCIBLE:
      case IMPLICIT:
      case NONE:
        return null;
      case EXPLICIT:
        return col2;
      default:
        throw Util.unexpected(coercibility2);
      }
    case EXPLICIT:
      switch (coercibility2) {
      case COERCIBLE:
      case IMPLICIT:
      case NONE:
        return col1;
      case EXPLICIT:
        if (col1.collationName.equals(col2.collationName)) {
          return col2;
        }
        throw RESOURCE.differentCollations(
            col1.collationName,
            col2.collationName).ex();
      default:
        throw Util.unexpected(coercibility2);
      }
    default:
      throw Util.unexpected(coercibility1);
    }
  }

  public String toString() {
    return "COLLATE " + collationName;
  }

  public void unparse(
      SqlWriter writer) {
    writer.keyword("COLLATE");
    writer.identifier(collationName, false);
  }

  public Charset getCharset() {
    return wrappedCharset.getCharset();
  }

  public final String getCollationName() {
    return collationName;
  }

  public final SqlCollation.Coercibility getCoercibility() {
    return coercibility;
  }
}

// End SqlCollation.java
