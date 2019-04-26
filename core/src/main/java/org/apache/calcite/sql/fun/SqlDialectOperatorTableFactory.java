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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.util.ListSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.util.Util;

import com.google.common.base.Preconditions;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.sql.fun.SqlFlavor.Flavor;

/**
 * Operator table factory for multiple sql dialects. You can use method
 * {@link #getOperatorTable} to fetch dialect specific operator table.
 *
 * <p>If you want to extend the sql dialect, add the sql dialect in {@link SqlFlavor},
 * if the sql dialect already exists, and you want to add a sql function, annotate the function
 * with the specific sql dialects that function belongs.
 */
public class SqlDialectOperatorTableFactory {

  //~ Instance fields --------------------------------------------------------

  // The singleton SqlDialectOperatorTableFactory instance.
  private static SqlDialectOperatorTableFactory instance = null;

  // Flag to indicate if this operator table has been initialized.
  private boolean initialized = false;

  // A container holding the operators for all the sql dialects.
  private final Map<Flavor, ListSqlOperatorTable> dialectSqlOperatorTableMapping =
      new HashMap<>();

  /** Return type inference for {@code DECODE}. */
  protected static final SqlReturnTypeInference DECODE_RETURN_TYPE =
      opBinding -> {
        final List<RelDataType> list = new ArrayList<>();
        for (int i = 1, n = opBinding.getOperandCount(); i < n; i++) {
          if (i < n - 1) {
            ++i;
          }
          list.add(opBinding.getOperandType(i));
        }
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        RelDataType type = typeFactory.leastRestrictive(list);
        if (opBinding.getOperandCount() % 2 == 1) {
          type = typeFactory.createTypeWithNullability(type, true);
        }
        return type;
      };

  /** The "DECODE(v, v1, result1, [v2, result2, ...], resultN)" function. */
  @SqlFlavor(flavors = {Flavor.ORACLE})
  public static final SqlFunction DECODE =
      new SqlFunction("DECODE", SqlKind.DECODE, DECODE_RETURN_TYPE, null,
          OperandTypes.VARIADIC, SqlFunctionCategory.SYSTEM);

  /** The "NVL(value, value)" function. */
  @SqlFlavor(flavors = {Flavor.ORACLE})
  public static final SqlFunction NVL =
      new SqlFunction("NVL", SqlKind.NVL,
          ReturnTypes.cascade(ReturnTypes.LEAST_RESTRICTIVE,
              SqlTypeTransforms.TO_NULLABLE_ALL),
          null, OperandTypes.SAME_SAME, SqlFunctionCategory.SYSTEM);

  /** The "LTRIM(string)" function. */
  @SqlFlavor(flavors = {Flavor.ORACLE})
  public static final SqlFunction LTRIM =
      new SqlFunction("LTRIM", SqlKind.LTRIM,
          ReturnTypes.cascade(ReturnTypes.ARG0, SqlTypeTransforms.TO_NULLABLE,
              SqlTypeTransforms.TO_VARYING), null,
          OperandTypes.STRING, SqlFunctionCategory.STRING);

  /** The "RTRIM(string)" function. */
  @SqlFlavor(flavors = {Flavor.ORACLE})
  public static final SqlFunction RTRIM =
      new SqlFunction("RTRIM", SqlKind.RTRIM,
          ReturnTypes.cascade(ReturnTypes.ARG0, SqlTypeTransforms.TO_NULLABLE,
              SqlTypeTransforms.TO_VARYING), null,
          OperandTypes.STRING, SqlFunctionCategory.STRING);

  /** Oracle's "SUBSTR(string, position [, substringLength ])" function.
   *
   * <p>It has similar semantics to standard SQL's
   * {@link SqlStdOperatorTable#SUBSTRING} function but different syntax. */
  @SqlFlavor(flavors = {SqlFlavor.Flavor.ORACLE})
  public static final SqlFunction SUBSTR =
      new SqlFunction("SUBSTR", SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0_NULLABLE_VARYING, null, null,
          SqlFunctionCategory.STRING);

  /** The "GREATEST(value, value)" function. */
  @SqlFlavor(flavors = {SqlFlavor.Flavor.ORACLE})
  public static final SqlFunction GREATEST =
      new SqlFunction("GREATEST", SqlKind.GREATEST,
          ReturnTypes.cascade(ReturnTypes.LEAST_RESTRICTIVE,
              SqlTypeTransforms.TO_NULLABLE), null,
          OperandTypes.SAME_VARIADIC, SqlFunctionCategory.SYSTEM);

  /** The "LEAST(value, value)" function. */
  @SqlFlavor(flavors = {Flavor.ORACLE})
  public static final SqlFunction LEAST =
      new SqlFunction("LEAST", SqlKind.LEAST,
          ReturnTypes.cascade(ReturnTypes.LEAST_RESTRICTIVE,
              SqlTypeTransforms.TO_NULLABLE), null,
          OperandTypes.SAME_VARIADIC, SqlFunctionCategory.SYSTEM);

  /**
   * The <code>TRANSLATE(<i>string_expr</i>, <i>search_chars</i>, <i>replacement_chars</i>)</code>
   * function returns <i>string_expr</i> with all occurrences of each character in
   * <i>search_chars</i> replaced by its corresponding character in <i>replacement_chars</i>.
   *
   * <p>It is not defined in the SQL standard, but occurs in Oracle and PostgreSQL.
   */
  @SqlFlavor(flavors = {Flavor.ORACLE, Flavor.POSTGRES})
  public static final SqlFunction TRANSLATE3 = new SqlTranslate3Function();

  /**
   * Returns the sql dialect operator table factory, creating it if necessary.
   */
  public static synchronized SqlDialectOperatorTableFactory instance() {
    if (instance == null) {
      // Creates and initializes this table factory.
      instance = new SqlDialectOperatorTableFactory();
      instance.init();
    }
    return instance;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Performs post-constructor initialization of an operator table. It can't
   * be part of the constructor, because the subclass constructor needs to
   * complete first.
   */
  public final void init() {
    // Initialize the dialect to SqlOperator mapping.
    for (Flavor flavor : Flavor.values()) {
      dialectSqlOperatorTableMapping.put(flavor, new ListSqlOperatorTable());
    }

    // Use reflection to register the expressions stored in public fields.
    for (Field field : getClass().getFields()) {
      try {
        if (SqlFunction.class.isAssignableFrom(field.getType())) {
          SqlFunction op = (SqlFunction) field.get(this);
          if (op != null) {
            register(op, getSqlDialectsFromField(op.getName(), field));
          }
        } else if (
            SqlOperator.class.isAssignableFrom(field.getType())) {
          SqlOperator op = (SqlOperator) field.get(this);
          register(op, getSqlDialectsFromField(op.getName(), field));
        }
      } catch (IllegalArgumentException | IllegalAccessException e) {
        Util.throwIfUnchecked(e.getCause());
        throw new RuntimeException(e.getCause());
      }
    }
    this.initialized = true;
  }

  /** Extract supported sql dialects from the field annotation. */
  private Flavor[] getSqlDialectsFromField(String operatorName, Field field) {
    SqlFlavor sqlFlavor = field.getAnnotation(SqlFlavor.class);
    assert sqlFlavor != null : "Must annotate with SqlDialect annotation for operator "
        + operatorName;
    SqlFlavor.Flavor[] flavors = sqlFlavor.flavors();
    assert flavors.length > 0 : "Must specify at least one dialect for operator "
        + operatorName;
    return flavors;
  }

  private void register(SqlOperator operator, Flavor[] flavors) {
    for (Flavor flavor : flavors) {
      dialectSqlOperatorTableMapping.get(flavor).add(operator);
    }
  }

  /** Returns an immutable sql operator table by specific sql dialect. */
  public SqlOperatorTable getOperatorTable(Flavor flavor) {
    assert initialized : "Please invoke init() to initialize this operator table first.";
    return new ImmutableSqlOperatorTable(dialectSqlOperatorTableMapping.get(flavor));
  }

  /**
   * Operator table that can not modify the operator list.
   */
  class ImmutableSqlOperatorTable implements SqlOperatorTable {
    private final ListSqlOperatorTable operatorTable;

    ImmutableSqlOperatorTable(ListSqlOperatorTable operatorTable) {
      Preconditions.checkArgument(operatorTable != null,
          "The operator table passed in should not be null.");
      this.operatorTable = operatorTable;
    }

    @Override public void lookupOperatorOverloads(
        SqlIdentifier opName, SqlFunctionCategory category,
        SqlSyntax syntax, List<SqlOperator> operatorList,
        SqlNameMatcher nameMatcher) {
      this.operatorTable.lookupOperatorOverloads(opName, category, syntax,
          operatorList, nameMatcher);
    }

    @Override public List<SqlOperator> getOperatorList() {
      return this.operatorTable.getOperatorList();
    }
  }
}

// End SqlDialectOperatorTableFactory.java
