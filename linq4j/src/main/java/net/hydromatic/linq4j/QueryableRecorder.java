/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.linq4j;

import net.hydromatic.linq4j.expressions.FunctionExpression;
import net.hydromatic.linq4j.function.*;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.Comparator;

import static net.hydromatic.linq4j.QueryableDefaults.NonLeafReplayableQueryable;

/**
 * Implementation of {@link QueryableFactory} that records each event
 * and returns an object that can replay the event when you call its
 * {@link net.hydromatic.linq4j.QueryableDefaults.ReplayableQueryable#replay(QueryableFactory)}
 * method.
 *
 * @param <T> Element type
 */
public class QueryableRecorder<T> implements QueryableFactory<T> {
  private static final QueryableRecorder INSTANCE = new QueryableRecorder();

  @SuppressWarnings("unchecked")
  public static <T> QueryableRecorder<T> instance() {
    return INSTANCE;
  }

  public T aggregate(final Queryable<T> source,
      final FunctionExpression<Function2<T, T, T>> func) {
    return new QueryableDefaults.NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.aggregate(source, func);
      }
    }.single(); // CHECKSTYLE: IGNORE 0
  }

  public <TAccumulate> TAccumulate aggregate(final Queryable<T> source,
      final TAccumulate seed,
      final FunctionExpression<Function2<TAccumulate, T, TAccumulate>> func) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.aggregate(source, seed, func);
      }
    }.<TAccumulate>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public <TAccumulate, TResult> TResult aggregate(final Queryable<T> source,
      final TAccumulate seed,
      final FunctionExpression<Function2<TAccumulate, T, TAccumulate>> func,
      final FunctionExpression<Function1<TAccumulate, TResult>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.aggregate(source, seed, func, selector);
      }
    }.<TResult>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public boolean all(final Queryable<T> source,
      final FunctionExpression<Predicate1<T>> predicate) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.all(source, predicate);
      }
    }.<Boolean>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public boolean any(final Queryable<T> source) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.any(source);
      }
    }.<Boolean>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public boolean any(final Queryable<T> source,
      final FunctionExpression<Predicate1<T>> predicate) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.any(source, predicate);
      }
    }.<Boolean>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public BigDecimal averageBigDecimal(final Queryable<T> source,
      final FunctionExpression<BigDecimalFunction1<T>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.averageBigDecimal(source, selector);
      }
    }.castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public BigDecimal averageNullableBigDecimal(final Queryable<T> source,
      final FunctionExpression<NullableBigDecimalFunction1<T>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.averageNullableBigDecimal(source, selector);
      }
    }.castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public double averageDouble(final Queryable<T> source,
      final FunctionExpression<DoubleFunction1<T>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.averageDouble(source, selector);
      }
    }.<Double>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public Double averageNullableDouble(final Queryable<T> source,
      final FunctionExpression<NullableDoubleFunction1<T>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.averageNullableDouble(source, selector);
      }
    }.<Double>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public int averageInteger(final Queryable<T> source,
      final FunctionExpression<IntegerFunction1<T>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.averageInteger(source, selector);
      }
    }.<Integer>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public Integer averageNullableInteger(final Queryable<T> source,
      final FunctionExpression<NullableIntegerFunction1<T>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.averageNullableInteger(source, selector);
      }
    }.<Integer>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public float averageFloat(final Queryable<T> source,
      final FunctionExpression<FloatFunction1<T>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.averageFloat(source, selector);
      }
    }.<Float>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public Float averageNullableFloat(final Queryable<T> source,
      final FunctionExpression<NullableFloatFunction1<T>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.averageNullableFloat(source, selector);
      }
    }.<Float>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public long averageLong(final Queryable<T> source,
      final FunctionExpression<LongFunction1<T>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.averageLong(source, selector);
      }
    }.<Long>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public Long averageNullableLong(final Queryable<T> source,
      final FunctionExpression<NullableLongFunction1<T>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.averageNullableLong(source, selector);
      }
    }.<Long>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public <T2> Queryable<T2> cast(final Queryable<T> source,
      final Class<T2> clazz) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.cast(source, clazz);
      }
    }.castQueryable(); // CHECKSTYLE: IGNORE 0
  }

  public Queryable<T> concat(final Queryable<T> source,
      final Enumerable<T> source2) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.concat(source, source2);
      }
    };
  }

  public boolean contains(final Queryable<T> source, final T element) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.contains(source, element);
      }
    }.<Boolean>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public boolean contains(final Queryable<T> source, final T element,
      final EqualityComparer<T> comparer) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.contains(source, element, comparer);
      }
    }.<Boolean>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public int count(final Queryable<T> source) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.count(source);
      }
    }.<Integer>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public int count(final Queryable<T> source,
      final FunctionExpression<Predicate1<T>> func) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.count(source, func);
      }
    }.<Integer>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public Queryable<T> defaultIfEmpty(final Queryable<T> source) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.defaultIfEmpty(source);
      }
    };
  }

  public Queryable<T> defaultIfEmpty(final Queryable<T> source, final T value) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.defaultIfEmpty(source, value);
      }
    };
  }

  public Queryable<T> distinct(final Queryable<T> source) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.distinct(source);
      }
    };
  }

  public Queryable<T> distinct(final Queryable<T> source,
      final EqualityComparer<T> comparer) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.distinct(source, comparer);
      }
    };
  }

  public T elementAt(final Queryable<T> source, final int index) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.elementAt(source, index);
      }
    }.<T>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public T elementAtOrDefault(final Queryable<T> source, final int index) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.elementAtOrDefault(source, index);
      }
    }.<T>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public Queryable<T> except(final Queryable<T> source,
      final Enumerable<T> enumerable) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.except(source, enumerable);
      }
    };
  }

  public Queryable<T> except(final Queryable<T> source,
      final Enumerable<T> enumerable, final EqualityComparer<T> comparer) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.except(source, enumerable, comparer);
      }
    };
  }

  public T first(final Queryable<T> source) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.first(source);
      }
    }.single(); // CHECKSTYLE: IGNORE 0
  }

  public T first(final Queryable<T> source,
      final FunctionExpression<Predicate1<T>> predicate) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.first(source, predicate);
      }
    }.single(); // CHECKSTYLE: IGNORE 0
  }

  public T firstOrDefault(final Queryable<T> source) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.firstOrDefault(source);
      }
    }.single(); // CHECKSTYLE: IGNORE 0
  }

  public T firstOrDefault(final Queryable<T> source,
      final FunctionExpression<Predicate1<T>> predicate) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.firstOrDefault(source, predicate);
      }
    }.single(); // CHECKSTYLE: IGNORE 0
  }

  public <TKey> Queryable<Grouping<TKey, T>> groupBy(final Queryable<T> source,
      final FunctionExpression<Function1<T, TKey>> keySelector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.groupBy(source, keySelector);
      }
    }.castQueryable(); // CHECKSTYLE: IGNORE 0
  }

  public <TKey> Queryable<Grouping<TKey, T>> groupBy(final Queryable<T> source,
      final FunctionExpression<Function1<T, TKey>> keySelector,
      final EqualityComparer<TKey> comparer) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.groupBy(source, keySelector, comparer);
      }
    }.castQueryable(); // CHECKSTYLE: IGNORE 0
  }

  public <TKey, TElement> Queryable<Grouping<TKey, TElement>> groupBy(
      final Queryable<T> source,
      final FunctionExpression<Function1<T, TKey>> keySelector,
      final FunctionExpression<Function1<T, TElement>> elementSelector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.groupBy(source, keySelector, elementSelector);
      }
    }.castQueryable(); // CHECKSTYLE: IGNORE 0
  }

  public <TKey, TResult> Queryable<Grouping<TKey, TResult>> groupByK(
      final Queryable<T> source,
      final FunctionExpression<Function1<T, TKey>> keySelector,
      final FunctionExpression<Function2<TKey, Enumerable<T>, TResult>>
        elementSelector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.groupByK(source, keySelector, elementSelector);
      }
    }.castQueryable(); // CHECKSTYLE: IGNORE 0
  }

  public <TKey, TElement> Queryable<Grouping<TKey, TElement>> groupBy(
      final Queryable<T> source,
      final FunctionExpression<Function1<T, TKey>> keySelector,
      final FunctionExpression<Function1<T, TElement>> elementSelector,
      final EqualityComparer<TKey> comparer) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.groupBy(source, keySelector, elementSelector, comparer);
      }
    }.castQueryable(); // CHECKSTYLE: IGNORE 0
  }

  public <TKey, TResult> Queryable<TResult> groupByK(final Queryable<T> source,
      final FunctionExpression<Function1<T, TKey>> keySelector,
      final FunctionExpression<Function2<TKey, Enumerable<T>, TResult>>
        elementSelector,
      final EqualityComparer<TKey> comparer) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.groupByK(source, keySelector, elementSelector, comparer);
      }
    }.castQueryable(); // CHECKSTYLE: IGNORE 0
  }

  public <TKey, TElement, TResult> Queryable<TResult> groupBy(
      final Queryable<T> source,
      final FunctionExpression<Function1<T, TKey>> keySelector,
      final FunctionExpression<Function1<T, TElement>> elementSelector,
      final FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>>
        resultSelector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.groupBy(source, keySelector, elementSelector, resultSelector);
      }
    }.castQueryable(); // CHECKSTYLE: IGNORE 0
  }

  public <TKey, TElement, TResult> Queryable<TResult> groupBy(
      final Queryable<T> source,
      final FunctionExpression<Function1<T, TKey>> keySelector,
      final FunctionExpression<Function1<T, TElement>> elementSelector,
      final FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>>
        resultSelector,
      final EqualityComparer<TKey> comparer) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.groupBy(source, keySelector, elementSelector, resultSelector,
            comparer);
      }
    }.castQueryable(); // CHECKSTYLE: IGNORE 0
  }

  public <TInner, TKey, TResult> Queryable<TResult> groupJoin(
      final Queryable<T> source, final Enumerable<TInner> inner,
      final FunctionExpression<Function1<T, TKey>> outerKeySelector,
      final FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      final FunctionExpression<Function2<T, Enumerable<TInner>, TResult>>
        resultSelector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.groupJoin(source, inner, outerKeySelector, innerKeySelector,
            resultSelector);
      }
    }.castQueryable(); // CHECKSTYLE: IGNORE 0
  }

  public <TInner, TKey, TResult> Queryable<TResult> groupJoin(
      final Queryable<T> source, final Enumerable<TInner> inner,
      final FunctionExpression<Function1<T, TKey>> outerKeySelector,
      final FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      final FunctionExpression<Function2<T, Enumerable<TInner>, TResult>>
        resultSelector,
      final EqualityComparer<TKey> comparer) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.groupJoin(source, inner, outerKeySelector, innerKeySelector,
            resultSelector, comparer);
      }
    }.castQueryable(); // CHECKSTYLE: IGNORE 0
  }

  public Queryable<T> intersect(final Queryable<T> source,
      final Enumerable<T> enumerable) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.intersect(source, enumerable);
      }
    };
  }

  public Queryable<T> intersect(final Queryable<T> source,
      final Enumerable<T> enumerable, final EqualityComparer<T> comparer) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.intersect(source, enumerable, comparer);
      }
    };
  }

  public <TInner, TKey, TResult> Queryable<TResult> join(
      final Queryable<T> source, final Enumerable<TInner> inner,
      final FunctionExpression<Function1<T, TKey>> outerKeySelector,
      final FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      final FunctionExpression<Function2<T, TInner, TResult>> resultSelector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.join(source, inner, outerKeySelector, innerKeySelector,
            resultSelector);
      }
    }.castQueryable(); // CHECKSTYLE: IGNORE 0
  }

  public <TInner, TKey, TResult> Queryable<TResult> join(
      final Queryable<T> source, final Enumerable<TInner> inner,
      final FunctionExpression<Function1<T, TKey>> outerKeySelector,
      final FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
      final FunctionExpression<Function2<T, TInner, TResult>> resultSelector,
      final EqualityComparer<TKey> comparer) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.join(source, inner, outerKeySelector, innerKeySelector,
            resultSelector, comparer);
      }
    }.castQueryable(); // CHECKSTYLE: IGNORE 0
  }

  public T last(final Queryable<T> source) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.last(source);
      }
    }.single(); // CHECKSTYLE: IGNORE 0
  }

  public T last(final Queryable<T> source,
      final FunctionExpression<Predicate1<T>> predicate) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.last(source, predicate);
      }
    }.single(); // CHECKSTYLE: IGNORE 0
  }

  public T lastOrDefault(final Queryable<T> source) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.lastOrDefault(source);
      }
    }.single(); // CHECKSTYLE: IGNORE 0
  }

  public T lastOrDefault(final Queryable<T> source,
      final FunctionExpression<Predicate1<T>> predicate) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.lastOrDefault(source, predicate);
      }
    }.single(); // CHECKSTYLE: IGNORE 0
  }

  public long longCount(final Queryable<T> source) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.longCount(source);
      }
    }.<Long>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public long longCount(final Queryable<T> source,
      final FunctionExpression<Predicate1<T>> predicate) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.longCount(source, predicate);
      }
    }.longCount(); // CHECKSTYLE: IGNORE 0
  }

  public T max(final Queryable<T> source) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.max(source);
      }
    }.<T>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public <TResult extends Comparable<TResult>> TResult max(
      final Queryable<T> source,
      final FunctionExpression<Function1<T, TResult>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.max(source, selector);
      }
    }.<TResult>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public T min(final Queryable<T> source) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.min(source);
      }
    }.<T>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public <TResult extends Comparable<TResult>> TResult min(
      final Queryable<T> source,
      final FunctionExpression<Function1<T, TResult>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.min(source, selector);
      }
    }.<TResult>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public <TResult> Queryable<TResult> ofType(final Queryable<T> source,
      final Class<TResult> clazz) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.ofType(source, clazz);
      }
    }.castQueryable(); // CHECKSTYLE: IGNORE 0
  }

  public <TKey extends Comparable> OrderedQueryable<T> orderBy(
      final Queryable<T> source,
      final FunctionExpression<Function1<T, TKey>> keySelector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.orderBy(source, keySelector);
      }
    };
  }

  public <TKey> OrderedQueryable<T> orderBy(final Queryable<T> source,
      final FunctionExpression<Function1<T, TKey>> keySelector,
      final Comparator<TKey> comparator) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.orderBy(source, keySelector, comparator);
      }
    };
  }

  public <TKey extends Comparable> OrderedQueryable<T> orderByDescending(
      final Queryable<T> source,
      final FunctionExpression<Function1<T, TKey>> keySelector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.orderByDescending(source, keySelector);
      }
    };
  }

  public <TKey> OrderedQueryable<T> orderByDescending(final Queryable<T> source,
      final FunctionExpression<Function1<T, TKey>> keySelector,
      final Comparator<TKey> comparator) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.orderByDescending(source, keySelector, comparator);
      }
    };
  }

  public Queryable<T> reverse(final Queryable<T> source) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.reverse(source);
      }
    };
  }

  public <TResult> Queryable<TResult> select(final Queryable<T> source,
      final FunctionExpression<Function1<T, TResult>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.select(source, selector);
      }

      @Override
      public Type getElementType() {
        return selector.body.type;
      }
    }.castQueryable(); // CHECKSTYLE: IGNORE 0
  }

  public <TResult> Queryable<TResult> selectN(final Queryable<T> source,
      final FunctionExpression<Function2<T, Integer, TResult>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.selectN(source, selector);
      }
    }.castQueryable(); // CHECKSTYLE: IGNORE 0
  }

  public <TResult> Queryable<TResult> selectMany(final Queryable<T> source,
      final FunctionExpression<Function1<T, Enumerable<TResult>>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.selectMany(source, selector);
      }
    }.castQueryable(); // CHECKSTYLE: IGNORE 0
  }

  public <TResult> Queryable<TResult> selectManyN(final Queryable<T> source,
      final FunctionExpression<Function2<T, Integer, Enumerable<TResult>>>
        selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.selectManyN(source, selector);
      }
    }.castQueryable(); // CHECKSTYLE: IGNORE 0
  }

  public <TCollection, TResult> Queryable<TResult> selectMany(
      final Queryable<T> source,
      final FunctionExpression<Function2<T, Integer, Enumerable<TCollection>>>
        collectionSelector,
      final FunctionExpression<Function2<T, TCollection, TResult>>
        resultSelector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.selectMany(source, collectionSelector, resultSelector);
      }
    }.castQueryable(); // CHECKSTYLE: IGNORE 0
  }

  public <TCollection, TResult> Queryable<TResult> selectManyN(
      final Queryable<T> source,
      final FunctionExpression<Function1<T, Enumerable<TCollection>>>
        collectionSelector,
      final FunctionExpression<Function2<T, TCollection, TResult>>
        resultSelector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.selectManyN(source, collectionSelector, resultSelector);
      }
    }.castQueryable(); // CHECKSTYLE: IGNORE 0
  }

  public boolean sequenceEqual(final Queryable<T> source,
      final Enumerable<T> enumerable) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.sequenceEqual(source, enumerable);
      }
    }.<Boolean>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public boolean sequenceEqual(final Queryable<T> source,
      final Enumerable<T> enumerable, final EqualityComparer<T> comparer) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.sequenceEqual(source, enumerable, comparer);
      }
    }.<Boolean>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public T single(final Queryable<T> source) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.single(source);
      }
    }.single(); // CHECKSTYLE: IGNORE 0
  }

  public T single(final Queryable<T> source,
      final FunctionExpression<Predicate1<T>> predicate) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.single(source, predicate);
      }
    }.single(); // CHECKSTYLE: IGNORE 0
  }

  public T singleOrDefault(final Queryable<T> source) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.singleOrDefault(source);
      }
    }.single(); // CHECKSTYLE: IGNORE 0
  }

  public T singleOrDefault(final Queryable<T> source,
      final FunctionExpression<Predicate1<T>> predicate) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.singleOrDefault(source, predicate);
      }
    }.single(); // CHECKSTYLE: IGNORE 0
  }

  public Queryable<T> skip(final Queryable<T> source, final int count) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.skip(source, count);
      }
    };
  }

  public Queryable<T> skipWhile(final Queryable<T> source,
      final FunctionExpression<Predicate1<T>> predicate) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.skipWhile(source, predicate);
      }
    };
  }

  public Queryable<T> skipWhileN(final Queryable<T> source,
      final FunctionExpression<Predicate2<T, Integer>> predicate) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.skipWhileN(source, predicate);
      }
    };
  }

  public BigDecimal sumBigDecimal(final Queryable<T> source,
      final FunctionExpression<BigDecimalFunction1<T>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.sumBigDecimal(source, selector);
      }
    }.castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public BigDecimal sumNullableBigDecimal(final Queryable<T> source,
      final FunctionExpression<NullableBigDecimalFunction1<T>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.sumNullableBigDecimal(source, selector);
      }
    }.castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public double sumDouble(final Queryable<T> source,
      final FunctionExpression<DoubleFunction1<T>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.sumDouble(source, selector);
      }
    }.<Double>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public Double sumNullableDouble(final Queryable<T> source,
      final FunctionExpression<NullableDoubleFunction1<T>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.sumNullableDouble(source, selector);
      }
    }.<Double>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public int sumInteger(final Queryable<T> source,
      final FunctionExpression<IntegerFunction1<T>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.sumInteger(source, selector);
      }
    }.<Integer>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public Integer sumNullableInteger(final Queryable<T> source,
      final FunctionExpression<NullableIntegerFunction1<T>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.sumNullableInteger(source, selector);
      }
    }.<Integer>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public long sumLong(final Queryable<T> source,
      final FunctionExpression<LongFunction1<T>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.sumLong(source, selector);
      }
    }.<Long>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public Long sumNullableLong(final Queryable<T> source,
      final FunctionExpression<NullableLongFunction1<T>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.sumNullableLong(source, selector);
      }
    }.<Long>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public float sumFloat(final Queryable<T> source,
      final FunctionExpression<FloatFunction1<T>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.sumFloat(source, selector);
      }
    }.<Float>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public Float sumNullableFloat(final Queryable<T> source,
      final FunctionExpression<NullableFloatFunction1<T>> selector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.sumNullableFloat(source, selector);
      }
    }.<Float>castSingle(); // CHECKSTYLE: IGNORE 0
  }

  public Queryable<T> take(final Queryable<T> source, final int count) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.take(source, count);
      }
    };
  }

  public Queryable<T> takeWhile(final Queryable<T> source,
      final FunctionExpression<Predicate1<T>> predicate) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.takeWhile(source, predicate);
      }
    };
  }

  public Queryable<T> takeWhileN(final Queryable<T> source,
      final FunctionExpression<Predicate2<T, Integer>> predicate) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.takeWhileN(source, predicate);
      }
    };
  }

  public <TKey extends Comparable<TKey>> OrderedQueryable<T> thenBy(
      final OrderedQueryable<T> source,
      final FunctionExpression<Function1<T, TKey>> keySelector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.thenBy(source, keySelector);
      }
    };
  }

  public <TKey> OrderedQueryable<T> thenBy(final OrderedQueryable<T> source,
      final FunctionExpression<Function1<T, TKey>> keySelector,
      final Comparator<TKey> comparator) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.thenBy(source, keySelector, comparator);
      }
    };
  }

  public <TKey extends Comparable<TKey>> OrderedQueryable<T> thenByDescending(
      final OrderedQueryable<T> source,
      final FunctionExpression<Function1<T, TKey>> keySelector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.thenByDescending(source, keySelector);
      }
    };
  }

  public <TKey> OrderedQueryable<T> thenByDescending(
      final OrderedQueryable<T> source,
      final FunctionExpression<Function1<T, TKey>> keySelector,
      final Comparator<TKey> comparator) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.thenByDescending(source, keySelector, comparator);
      }
    };
  }

  public Queryable<T> union(final Queryable<T> source,
      final Enumerable<T> source1) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.union(source, source1);
      }
    };
  }

  public Queryable<T> union(final Queryable<T> source,
      final Enumerable<T> source1, final EqualityComparer<T> comparer) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.union(source, source1, comparer);
      }
    };
  }

  public Queryable<T> where(final Queryable<T> source,
      final FunctionExpression<? extends Predicate1<T>> predicate) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.where(source, predicate);
      }
    };
  }

  public Queryable<T> whereN(final Queryable<T> source,
      final FunctionExpression<? extends Predicate2<T, Integer>> predicate) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.whereN(source, predicate);
      }
    };
  }

  public <T1, TResult> Queryable<TResult> zip(final Queryable<T> source,
      final Enumerable<T1> source1,
      final FunctionExpression<Function2<T, T1, TResult>> resultSelector) {
    return new NonLeafReplayableQueryable<T>(source) {
      public void replay(QueryableFactory<T> factory) {
        factory.zip(source, source1, resultSelector);
      }
    }.castQueryable(); // CHECKSTYLE: IGNORE 0
  }
}

// End QueryableRecorder.java
