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
package org.apache.calcite.sql.util;

import org.apache.calcite.runtime.ImmutablePairList;
import org.apache.calcite.runtime.PairList;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.LibraryOperator;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Field;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * ReflectiveSqlOperatorTable implements the {@link SqlOperatorTable} interface
 * by reflecting the public fields of a subclass.
 */
public abstract class ReflectiveSqlOperatorTable implements SqlOperatorTable {
  public static final String IS_NAME = "INFORMATION_SCHEMA";

  //~ Instance fields --------------------------------------------------------

  private ImmutableSortedPairList<String, SqlOperator> operators =
      ImmutableSortedPairList.of(ImmutableList.of(), String.CASE_INSENSITIVE_ORDER);

  //~ Constructors -----------------------------------------------------------

  protected ReflectiveSqlOperatorTable() {
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Performs post-constructor initialization of an operator table. It can't
   * be part of the constructor, because the subclass constructor needs to
   * complete first.
   */
  public final SqlOperatorTable init() {
    // Use reflection to register the expressions stored in public fields.
    final Initializer initializer = new Initializer();
    for (Field field : getClass().getFields()) {
      try {
        final Object o = field.get(this);
        if (o instanceof SqlOperator) {
          // Fields do not need the LibraryOperator tag, but if they have it,
          // we index them only if they contain STANDARD library.
          LibraryOperator libraryOperator =
              field.getAnnotation(LibraryOperator.class);
          if (libraryOperator != null) {
            if (Arrays.stream(libraryOperator.libraries())
                .noneMatch(library -> library == SqlLibrary.STANDARD)) {
              continue;
            }
          }

          initializer.add((SqlOperator) o);
        }
      } catch (IllegalArgumentException | IllegalAccessException e) {
        throw Util.throwAsRuntime(Util.causeOrSelf(e));
      }
    }
    initializer.done();
    return this;
  }

  @Override public void lookupOperatorOverloads(SqlIdentifier opName,
      @Nullable SqlFunctionCategory category, SqlSyntax syntax,
      List<SqlOperator> operatorList, SqlNameMatcher nameMatcher) {
    // NOTE jvs 3-Mar-2005:  ignore category until someone cares

    String simpleName;
    if (opName.names.size() > 1) {
      if (opName.names.get(opName.names.size() - 2).equals(IS_NAME)) {
        // per SQL99 Part 2 Section 10.4 Syntax Rule 7.b.ii.1
        simpleName = Util.last(opName.names);
      } else {
        return;
      }
    } else {
      simpleName = opName.getSimple();
    }

    lookUpOperators(simpleName, nameMatcher.isCaseSensitive(), op -> {
      if (op.getSyntax() == syntax) {
        operatorList.add(op);
      } else if (syntax == SqlSyntax.FUNCTION
          && op instanceof SqlFunction) {
        // this special case is needed for operators like CAST,
        // which are treated as functions but have special syntax
        operatorList.add(op);
      }
    });

    // REVIEW jvs 1-Jan-2005:  why is this extra lookup required?
    // Shouldn't it be covered by search above?
    switch (syntax) {
    case BINARY:
    case PREFIX:
    case POSTFIX:
      lookUpOperators(simpleName, nameMatcher.isCaseSensitive(), extra -> {
        // REVIEW: should only search operators added during this method?
        if (extra != null && !operatorList.contains(extra)) {
          operatorList.add(extra);
        }
      });
      break;
    default:
      break;
    }
  }

  /** Looks up operators, optionally matching case-sensitively. */
  protected void lookUpOperators(String name,
      boolean caseSensitive, Consumer<SqlOperator> consumer) {
    // Only UDFs are looked up using case-sensitive search.
    // Always look up built-in operators case-insensitively. Even in sessions
    // with unquotedCasing=UNCHANGED and caseSensitive=true.
    operators.forEachBetween(name,
        (name2, operator) -> consumer.accept(operator),
        caseSensitive
            ? Comparator.naturalOrder()
            : String.CASE_INSENSITIVE_ORDER);
  }

  /**
   * Registers a function or operator in the table.
   *
   * @deprecated This table is designed to be initialized from the fields of
   * a class, and adding operators is not efficient
   */
  @Deprecated
  public void register(SqlOperator op) {
    // Rebuild the immutable collections with their current contents plus one.
    final List<SqlOperator> list = getOperatorList();
    final Initializer initializer = new Initializer();
    list.forEach(initializer::add);
    initializer.add(op);
    initializer.done();
  }

  @Override public List<SqlOperator> getOperatorList() {
    return operators.pairList.rightList();
  }

  /** Builds a list of operators, and writes it back into the operator table
   * when done. */
  private class Initializer {
    final PairList<String, SqlOperator> pairList = PairList.of();

    void add(SqlOperator op) {
      pairList.add(op.getName(), op);
    }

    void done() {
      operators =
          ImmutableSortedPairList.of(pairList, String.CASE_INSENSITIVE_ORDER);
    }
  }

  /** List of pairs that is sorted by applying a comparator to the left part of
   * each pair.
   *
   * @param <T> First type
   * @param <U> Second type
   */
  public static class ImmutableSortedPairList<@NonNull T, @NonNull U>
      extends AbstractList<Map.Entry<T, U>> {
    private final ImmutablePairList<@NonNull T, @NonNull U> pairList;
    private final Comparator<T> comparator;

    /** Creates an ImmutableSortedPairList. */
    public static <@NonNull T, @NonNull U> ImmutableSortedPairList<T, U> of(
        Collection<? extends Map.Entry<@NonNull T, @NonNull U>> entries,
        Comparator<T> comparator) {
      final PairList<@NonNull T, @NonNull U> pairList = PairList.of();
      pairList.addAll(entries);
      pairList.sort(Map.Entry.comparingByKey(comparator));
      return new ImmutableSortedPairList<>(
          ImmutablePairList.copyOf(pairList), comparator);
    }

    private ImmutableSortedPairList(ImmutablePairList<@NonNull T, @NonNull U> pairList,
        Comparator<T> comparator) {
      this.pairList = requireNonNull(pairList, "pairList");
      this.comparator = requireNonNull(comparator, "comparator");
    }

    @Override public Map.Entry<@NonNull T, @NonNull U> get(int index) {
      return pairList.get(index);
    }

    @Override public int size() {
      return pairList.size();
    }

    /** For each entry whose key is equal to {@code t}, calls the consumer. */
    public void forEachBetween(T t, BiConsumer<T, U> consumer) {
      forEachBetween(t, consumer, comparator);
    }

    /** For each entry whose key is equal to {@code t}
     * according to a given comparator, calls the consumer.
     *
     * <p>The comparator must be the same as, or weaker than, the comparator
     * used to sort the list. */
    public void forEachBetween(T t, BiConsumer<T, U> consumer,
        Comparator<T> comparator) {
      final List<@NonNull T> leftList = pairList.leftList();
      final List<@NonNull U> rightList = pairList.rightList();
      int i = Collections.binarySearch(leftList, t, comparator);
      if (i >= 0) {
        // Back-track so that "i" points to the last entry less than "k".
        do {
          --i;
        } while (i >= 0
            && comparator.compare(leftList.get(i), t) >= 0);
      } else {
        i = -(i + 1);
      }

      // Output values for all keys equal to "t"
      for (;;) {
        ++i;
        if (i >= leftList.size()
            || comparator.compare(leftList.get(i), t) > 0) {
          break;
        }
        consumer.accept(leftList.get(i), rightList.get(i));
      }
    }
  }

}
