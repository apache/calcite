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

import org.apache.calcite.sql.dialect.CalciteSqlDialect;

/**
 * Specification of a SQL sample.
 *
 * <p>For example, the query</p>
 *
 * <blockquote>
 * <pre>SELECT *
 * FROM emp TABLESAMPLE SUBSTITUTE('medium')</pre>
 * </blockquote>
 *
 * <p>declares a sample which is created using {@link #createNamed}.</p>
 *
 * <p>A sample is not a {@link SqlNode}. To include it in a parse tree, wrap it
 * as a literal, viz:
 * {@link SqlLiteral#createSample(SqlSampleSpec, org.apache.calcite.sql.parser.SqlParserPos)}.
 */
public abstract class SqlSampleSpec {
  //~ Constructors -----------------------------------------------------------

  protected SqlSampleSpec() {
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Creates a sample which substitutes one relation for another.
   */
  public static SqlSampleSpec createNamed(String name) {
    return new SqlSubstitutionSampleSpec(name);
  }

  /**
   * Creates a table sample without repeatability.
   *
   * @param isBernoulli      true if Bernoulli style sampling is to be used;
   *                         false for implementation specific sampling
   * @param samplePercentage likelihood of a row appearing in the sample
   */
  public static SqlSampleSpec createTableSample(
      boolean isBernoulli,
      float samplePercentage) {
    return new SqlTableSampleSpec(isBernoulli, samplePercentage);
  }

  /**
   * Creates a table sample with repeatability.
   *
   * @param isBernoulli      true if Bernoulli style sampling is to be used;
   *                         false for implementation specific sampling
   * @param samplePercentage likelihood of a row appearing in the sample
   * @param repeatableSeed   seed value used to reproduce the same sample
   */
  public static SqlSampleSpec createTableSample(
      boolean isBernoulli,
      float samplePercentage,
      int repeatableSeed) {
    return new SqlTableSampleSpec(
        isBernoulli,
        samplePercentage,
        repeatableSeed);
  }

  //~ Inner Classes ----------------------------------------------------------

  /** Sample specification that orders substitution. */
  public static class SqlSubstitutionSampleSpec extends SqlSampleSpec {
    private final String name;

    private SqlSubstitutionSampleSpec(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public String toString() {
      return "SUBSTITUTE("
          + CalciteSqlDialect.DEFAULT.quoteStringLiteral(name)
          + ")";
    }
  }

  /** Sample specification. */
  public static class SqlTableSampleSpec extends SqlSampleSpec {
    private final boolean isBernoulli;
    private final float samplePercentage;
    private final boolean isRepeatable;
    private final int repeatableSeed;

    private SqlTableSampleSpec(boolean isBernoulli, float samplePercentage) {
      this.isBernoulli = isBernoulli;
      this.samplePercentage = samplePercentage;
      this.isRepeatable = false;
      this.repeatableSeed = 0;
    }

    private SqlTableSampleSpec(
        boolean isBernoulli,
        float samplePercentage,
        int repeatableSeed) {
      this.isBernoulli = isBernoulli;
      this.samplePercentage = samplePercentage;
      this.isRepeatable = true;
      this.repeatableSeed = repeatableSeed;
    }

    /**
     * Indicates Bernoulli vs. System sampling.
     */
    public boolean isBernoulli() {
      return isBernoulli;
    }

    /**
     * Returns sampling percentage. Range is 0.0 to 1.0, exclusive
     */
    public float getSamplePercentage() {
      return samplePercentage;
    }

    /**
     * Indicates whether repeatable seed should be used.
     */
    public boolean isRepeatable() {
      return isRepeatable;
    }

    /**
     * Seed to produce repeatable samples.
     */
    public int getRepeatableSeed() {
      return repeatableSeed;
    }

    public String toString() {
      StringBuilder b = new StringBuilder();
      b.append(isBernoulli ? "BERNOULLI" : "SYSTEM");
      b.append('(');
      b.append(samplePercentage * 100.0);
      b.append(')');

      if (isRepeatable) {
        b.append(" REPEATABLE(");
        b.append(repeatableSeed);
        b.append(')');
      }
      return b.toString();
    }
  }
}

// End SqlSampleSpec.java
