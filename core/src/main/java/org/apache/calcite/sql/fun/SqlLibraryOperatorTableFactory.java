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

import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.util.Util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static java.util.Objects.requireNonNull;

/**
 * Factory that creates operator tables that consist of functions and operators
 * for particular named libraries. For example, the following code will return
 * an operator table that contains operators for both Oracle and MySQL:
 *
 * <blockquote>
 *   <pre>SqlOperatorTable opTab =
 *     SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
 *         EnumSet.of(SqlLibrary.ORACLE, SqlLibrary.MYSQL))</pre>
 * </blockquote>
 *
 * <p>To define a new library, add a value to enum {@link SqlLibrary}.
 *
 * <p>To add functions to a library, add the {@link LibraryOperator} annotation
 * to fields that of type {@link SqlOperator}, and the library name to its
 * {@link LibraryOperator#libraries()} field.
 */
public class SqlLibraryOperatorTableFactory {

  /** List of classes to scan for operators. */
  private final ImmutableList<Class> classes;

  /** The singleton instance. */
  public static final SqlLibraryOperatorTableFactory INSTANCE =
      new SqlLibraryOperatorTableFactory(SqlLibraryOperators.class);

  private SqlLibraryOperatorTableFactory(Class... classes) {
    this.classes = ImmutableList.copyOf(classes);
  }

  //~ Instance fields --------------------------------------------------------

  /** A cache that returns an operator table for a given library (or set of
   * libraries). */
  @SuppressWarnings("methodref.receiver.bound.invalid")
  private final LoadingCache<ImmutableSet<SqlLibrary>, SqlOperatorTable> cache =
      CacheBuilder.newBuilder().build(CacheLoader.from(this::create));

  //~ Methods ----------------------------------------------------------------

  /** Creates an operator table that contains operators in the given set of
   * libraries. */
  private SqlOperatorTable create(ImmutableSet<SqlLibrary> librarySet) {
    final List<SqlOperator> list = new ArrayList<>();
    boolean custom = false;
    boolean standard = false;
    for (SqlLibrary library : librarySet) {
      switch (library) {
      case STANDARD:
        standard = true;
        break;
      case SPATIAL:
        list.addAll(SqlOperatorTables.spatialInstance().getOperatorList());
        break;
      default:
        custom = true;
      }
    }

    // Use reflection to register the expressions stored in public fields.
    // Skip if the only libraries asked for are "standard" or "spatial".
    if (custom) {
      for (Class aClass : classes) {
        for (Field field : aClass.getFields()) {
          try {
            if (SqlOperator.class.isAssignableFrom(field.getType())) {
              final SqlOperator op = (SqlOperator) requireNonNull(field.get(this),
                  () -> "null value of " + field + " for " + this);
              if (operatorIsInLibrary(op.getName(), field, librarySet)) {
                list.add(op);
              }
            }
          } catch (IllegalArgumentException | IllegalAccessException e) {
            throw Util.throwAsRuntime(Util.causeOrSelf(e));
          }
        }
      }
    }
    SqlOperatorTable operatorTable = SqlOperatorTables.of(list);
    if (standard) {
      operatorTable =
          SqlOperatorTables.chain(SqlStdOperatorTable.instance(),
              operatorTable);
    }
    return operatorTable;
  }

  /** Returns whether an operator is in one or more of the given libraries. */
  private static boolean operatorIsInLibrary(String operatorName, Field field,
      Set<SqlLibrary> seekLibrarySet) {
    LibraryOperator libraryOperator =
        field.getAnnotation(LibraryOperator.class);
    if (libraryOperator == null) {
      throw new AssertionError("Operator must have annotation: "
          + operatorName);
    }
    SqlLibrary[] librarySet = libraryOperator.libraries();
    if (librarySet.length <= 0) {
      throw new AssertionError("Operator must belong to at least one library: "
          + operatorName);
    }
    for (SqlLibrary library : librarySet) {
      if (seekLibrarySet.contains(library)) {
        return true;
      }
    }
    return false;
  }

  /** Returns a SQL operator table that contains operators in the given library
   * or libraries. */
  public SqlOperatorTable getOperatorTable(SqlLibrary... libraries) {
    return getOperatorTable(ImmutableSet.copyOf(libraries));
  }

  /** Returns a SQL operator table that contains operators in the given set of
   * libraries. */
  public SqlOperatorTable getOperatorTable(Iterable<SqlLibrary> librarySet) {
    try {
      return cache.get(ImmutableSet.copyOf(librarySet));
    } catch (ExecutionException e) {
      throw Util.throwAsRuntime("populating SqlOperatorTable for library "
          + librarySet, Util.causeOrSelf(e));
    }
  }
}
