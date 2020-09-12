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
package org.apache.calcite.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.annotation.Nonnull;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.junit.jupiter.api.Assertions.fail;

/** Unit test for {@link ImmutableBeans}. */
class ImmutableBeanTest {

  @Test void testSimple() {
    final MyBean b = ImmutableBeans.create(MyBean.class);
    assertThat(b.withFoo(1).getFoo(), is(1));
    assertThat(b.withBar(false).isBar(), is(false));
    assertThat(b.withBaz("a").getBaz(), is("a"));
    assertThat(b.withBaz("a").withBaz("a").getBaz(), is("a"));

    // Calling "with" on b2 does not change the "foo" property
    final MyBean b2 = b.withFoo(2);
    final MyBean b3 = b2.withFoo(3);
    assertThat(b3.getFoo(), is(3));
    assertThat(b2.getFoo(), is(2));

    final MyBean b4 = b2.withFoo(3).withBar(true).withBaz("xyz");
    final Map<String, Object> map = new TreeMap<>();
    map.put("Foo", b4.getFoo());
    map.put("Bar", b4.isBar());
    map.put("Baz", b4.getBaz());
    assertThat(b4.toString(), is(map.toString()));
    assertThat(b4.hashCode(), is(map.hashCode()));
    final MyBean b5 = b2.withFoo(3).withBar(true).withBaz("xyz");
    assertThat(b4.equals(b5), is(true));
    assertThat(b4.equals(b), is(false));
    assertThat(b4.equals(b2), is(false));
    assertThat(b4.equals(b3), is(false));
  }

  @Test void testDefault() {
    final Bean2 b = ImmutableBeans.create(Bean2.class);

    // int, no default
    try {
      final int v = b.getIntSansDefault();
      throw new AssertionError("expected error, got " + v);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("property 'IntSansDefault' is required and has no default value"));
    }
    assertThat(b.withIntSansDefault(4).getIntSansDefault(), is(4));

    // int, with default
    assertThat(b.getIntWithDefault(), is(1));
    assertThat(b.withIntWithDefault(10).getIntWithDefault(), is(10));
    assertThat(b.withIntWithDefault(1).getIntWithDefault(), is(1));

    // boolean, no default
    try {
      final boolean v = b.isBooleanSansDefault();
      throw new AssertionError("expected error, got " + v);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("property 'BooleanSansDefault' is required and has no default "
              + "value"));
    }
    assertThat(b.withBooleanSansDefault(false).isBooleanSansDefault(),
        is(false));

    // boolean, with default
    assertThat(b.isBooleanWithDefault(), is(true));
    assertThat(b.withBooleanWithDefault(false).isBooleanWithDefault(),
        is(false));
    assertThat(b.withBooleanWithDefault(true).isBooleanWithDefault(),
        is(true));

    // string, no default
    try {
      final String v = b.getStringSansDefault();
      throw new AssertionError("expected error, got " + v);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("property 'StringSansDefault' is required and has no default "
              + "value"));
    }
    assertThat(b.withStringSansDefault("a").getStringSansDefault(), is("a"));

    // string, no default
    try {
      final String v = b.getNonnullString();
      throw new AssertionError("expected error, got " + v);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("property 'NonnullString' is required and has no default value"));
    }
    assertThat(b.withNonnullString("a").getNonnullString(), is("a"));

    // string, with default
    assertThat(b.getStringWithDefault(), is("abc"));
    assertThat(b.withStringWithDefault("").getStringWithDefault(), is(""));
    assertThat(b.withStringWithDefault("x").getStringWithDefault(), is("x"));
    assertThat(b.withStringWithDefault("abc").getStringWithDefault(),
        is("abc"));
    try {
      final Bean2 v = b.withStringWithDefault(null);
      throw new AssertionError("expected error, got " + v);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("cannot set required property 'StringWithDefault' to null"));
    }

    // string, optional
    assertThat(b.getOptionalString(), nullValue());
    assertThat(b.withOptionalString("").getOptionalString(), is(""));
    assertThat(b.withOptionalString("x").getOptionalString(), is("x"));
    assertThat(b.withOptionalString("abc").getOptionalString(), is("abc"));
    assertThat(b.withOptionalString(null).getOptionalString(), nullValue());

    // string, optional
    assertThat(b.getStringWithNullDefault(), nullValue());
    assertThat(b.withStringWithNullDefault("").getStringWithNullDefault(),
        is(""));
    assertThat(b.withStringWithNullDefault("x").getStringWithNullDefault(),
        is("x"));
    assertThat(b.withStringWithNullDefault("abc").getStringWithNullDefault(),
        is("abc"));
    assertThat(b.withStringWithNullDefault(null).getStringWithNullDefault(),
        nullValue());

    // enum, with default
    assertThat(b.getColorWithDefault(), is(Color.RED));
    assertThat(b.withColorWithDefault(Color.GREEN).getColorWithDefault(),
        is(Color.GREEN));
    assertThat(b.withColorWithDefault(Color.RED).getColorWithDefault(),
        is(Color.RED));
    try {
      final Bean2 v = b.withColorWithDefault(null);
      throw new AssertionError("expected error, got " + v);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(),
          is("cannot set required property 'ColorWithDefault' to null"));
    }

    // color, optional
    assertThat(b.getColorOptional(), nullValue());
    assertThat(b.withColorOptional(Color.RED).getColorOptional(),
        is(Color.RED));
    assertThat(b.withColorOptional(Color.RED).withColorOptional(null)
        .getColorOptional(), nullValue());
    assertThat(b.withColorOptional(null).getColorOptional(), nullValue());
    assertThat(b.withColorOptional(Color.RED).withColorOptional(Color.GREEN)
        .getColorOptional(), is(Color.GREEN));

    // color, optional with null default
    assertThat(b.getColorWithNullDefault(), nullValue());
    assertThat(b.withColorWithNullDefault(null).getColorWithNullDefault(),
        nullValue());
    assertThat(b.withColorWithNullDefault(Color.RED).getColorWithNullDefault(),
        is(Color.RED));
    assertThat(b.withColorWithNullDefault(Color.RED)
        .withColorWithNullDefault(null).getColorWithNullDefault(), nullValue());
    assertThat(b.withColorWithNullDefault(Color.RED)
        .withColorWithNullDefault(Color.GREEN).getColorWithNullDefault(),
        is(Color.GREEN));

    // Default values do not appear in toString().
    // (Maybe they should... but then they'd be initial values?)
    assertThat(b.toString(), is("{}"));

    // Beans with values explicitly set are not equal to
    // beans with the same values via defaults.
    // (I could be persuaded that this is the wrong behavior.)
    assertThat(b.equals(b.withIntWithDefault(1)), is(false));
    assertThat(b.withIntWithDefault(1).equals(b.withIntWithDefault(1)),
        is(true));
    assertThat(b.withIntWithDefault(1).equals(b.withIntWithDefault(2)),
        is(false));
  }

  private void check(Class<?> beanClass, Matcher<String> matcher) {
    try {
      final Object v = ImmutableBeans.create(beanClass);
      fail("expected error, got " + v);
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), matcher);
    }
  }

  @Test void testValidate() {
    check(BeanWhoseDefaultIsBadEnumValue.class,
        is("property 'Color' is an enum but its default value YELLOW is not a "
            + "valid enum constant"));
    check(BeanWhoseWithMethodHasBadReturnType.class,
        is("method 'withFoo' should return the bean class 'interface "
            + "org.apache.calcite.util.ImmutableBeanTest$"
            + "BeanWhoseWithMethodHasBadReturnType', actually returns "
            + "'interface org.apache.calcite.util.ImmutableBeanTest$MyBean'"));
    check(BeanWhoseWithMethodDoesNotMatchProperty.class,
        is("method 'withFoo' should return the bean class 'interface "
            + "org.apache.calcite.util.ImmutableBeanTest$"
            + "BeanWhoseWithMethodDoesNotMatchProperty', actually returns "
            + "'interface org.apache.calcite.util.ImmutableBeanTest$MyBean'"));
    check(BeanWhoseWithMethodHasArgOfWrongType.class,
        is("method 'withFoo' should return the bean class 'interface "
            + "org.apache.calcite.util.ImmutableBeanTest$"
            + "BeanWhoseWithMethodHasArgOfWrongType', actually returns "
            + "'interface org.apache.calcite.util.ImmutableBeanTest$"
            + "BeanWhoseWithMethodHasTooManyArgs'"));
    check(BeanWhoseWithMethodHasTooManyArgs.class,
        is("method 'withFoo' should have one parameter, actually has 2"));
    check(BeanWhoseWithMethodHasTooFewArgs.class,
        is("method 'withFoo' should have one parameter, actually has 0"));
    check(BeanWhoseSetMethodHasBadReturnType.class,
        is("method 'setFoo' should return void, actually returns "
            + "'interface org.apache.calcite.util.ImmutableBeanTest$MyBean'"));
    check(BeanWhoseGetMethodHasTooManyArgs.class,
        is("method 'getFoo' has too many parameters"));
    check(BeanWhoseSetMethodDoesNotMatchProperty.class,
        is("cannot find property 'Foo' for method 'setFoo'; maybe add a method "
            + "'getFoo'?'"));
    check(BeanWhoseSetMethodHasArgOfWrongType.class,
        is("method 'setFoo' should have parameter of type int, actually has "
            + "float"));
    check(BeanWhoseSetMethodHasTooManyArgs.class,
        is("method 'setFoo' should have one parameter, actually has 2"));
    check(BeanWhoseSetMethodHasTooFewArgs.class,
        is("method 'setFoo' should have one parameter, actually has 0"));
  }

  @Test void testDefaultMethod() {
    assertThat(ImmutableBeans.create(BeanWithDefault.class)
        .withChar('a').nTimes(2), is("aa"));
  }

  @Test void testImmutableCollection() {
    final List<String> list = Arrays.asList("Jimi", "Noel", "Mitch");
    final List<String> immutableList = ImmutableList.copyOf(list);
    final Set<String> set = new TreeSet<>(list);
    final ImmutableSet<String> immutableSet = ImmutableSet.copyOf(set);
    final Map<String, Integer> map = new HashMap<>();
    list.forEach(name -> map.put(name, name.length()));
    final ImmutableMap<String, Integer> immutableMap = ImmutableMap.copyOf(map);

    final CollectionBean bean = ImmutableBeans.create(CollectionBean.class);

    // list: the non-copying method never makes a copy
    final List<String> list2 = bean.withList(list).list();
    assertThat(list2, sameInstance(list));

    // list: the copying method makes a copy if the original is not immutable
    final List<String> list3 =
        bean.withImmutableList(list).immutableList();
    assertThat(list3, instanceOf(ImmutableList.class));
    assertThat(list3, not(sameInstance(list)));
    assertThat(list3, is(list));

    // list: if the original is immutable, no need to make a copy
    final List<String> list4 =
        bean.withImmutableList(immutableList).immutableList();
    assertThat(list4, sameInstance(immutableList));
    assertThat(list3, not(sameInstance(list)));
    assertThat(list3, is(list));

    // list: empty
    final List<String> emptyList = Collections.emptyList();
    assertThat(bean.withImmutableList(emptyList).immutableList(),
        is(emptyList));

    // list: no need to copy the singleton list
    final List<String> singletonList = Collections.singletonList("Elvis");
    assertThat(bean.withImmutableList(singletonList).immutableList(),
        is(singletonList));

    final List<String> singletonNullList = Collections.singletonList(null);
    assertThat(bean.withImmutableList(singletonNullList).immutableList(),
        is(singletonNullList));

    // set: the non-copying method never makes a copy
    final Set<String> set2 = bean.withSet(set).set();
    assertThat(set2, sameInstance(set));

    // set: the copying method makes a copy if the original is not immutable
    final Set<String> set3 =
        bean.withImmutableSet(set).immutableSet();
    assertThat(set3, instanceOf(ImmutableSet.class));
    assertThat(set3, not(sameInstance(set)));
    assertThat(set3, is(set));

    // set: if the original is immutable, no need to make a copy
    final Set<String> set4 =
        bean.withImmutableSet(immutableSet).immutableSet();
    assertThat(set4, sameInstance(immutableSet));
    assertThat(set3, not(sameInstance(set)));
    assertThat(set3, is(set));

    // set: empty
    final Set<String> emptySet = Collections.emptySet();
    assertThat(bean.withImmutableSet(emptySet).immutableSet(),
        is(emptySet));
    assertThat(bean.withImmutableSet(emptySet).immutableSet(),
        sameInstance(emptySet));

    // set: other empty
    final Set<String> emptySet2 = new HashSet<>();
    assertThat(bean.withImmutableSet(emptySet2).immutableSet(),
        is(emptySet));
    assertThat(bean.withImmutableSet(emptySet2).immutableSet(),
        instanceOf(ImmutableSet.class));

    // set: singleton
    final Set<String> singletonSet = Collections.singleton("Elvis");
    assertThat(bean.withImmutableSet(singletonSet).immutableSet(),
        is(singletonSet));
    assertThat(bean.withImmutableSet(singletonSet).immutableSet(),
        sameInstance(singletonSet));

    // set: other singleton
    final Set<String> singletonSet2 =
        new HashSet<>(Collections.singletonList("Elvis"));
    assertThat(bean.withImmutableSet(singletonSet2).immutableSet(),
        is(singletonSet2));
    assertThat(bean.withImmutableSet(singletonSet2).immutableSet(),
        instanceOf(ImmutableSet.class));

    // set: singleton null set
    final Set<String> singletonNullSet = Collections.singleton(null);
    assertThat(bean.withImmutableSet(singletonNullSet).immutableSet(),
        is(singletonNullSet));
    assertThat(bean.withImmutableSet(singletonNullSet).immutableSet(),
        sameInstance(singletonNullSet));

    // set: other singleton null set
    final Set<String> singletonNullSet2 =
        new HashSet<>(Collections.singleton(null));
    assertThat(bean.withImmutableSet(singletonNullSet2).immutableSet(),
        is(singletonNullSet2));
    assertThat(bean.withImmutableSet(singletonNullSet2).immutableSet(),
        instanceOf(ImmutableNullableSet.class));

    // map: the non-copying method never makes a copy
    final Map<String, Integer> map2 = bean.withMap(map).map();
    assertThat(map2, sameInstance(map));

    // map: the copying method makes a copy if the original is not immutable
    final Map<String, Integer> map3 =
        bean.withImmutableMap(map).immutableMap();
    assertThat(map3, instanceOf(ImmutableMap.class));
    assertThat(map3, not(sameInstance(map)));
    assertThat(map3, is(map));

    // map: if the original is immutable, no need to make a copy
    final Map<String, Integer> map4 =
        bean.withImmutableMap(immutableMap).immutableMap();
    assertThat(map4, sameInstance(immutableMap));
    assertThat(map3, not(sameInstance(map)));
    assertThat(map3, is(map));

    // map: no need to copy the empty map
    final Map<String, Integer> emptyMap = Collections.emptyMap();
    assertThat(bean.withImmutableMap(emptyMap).immutableMap(),
        sameInstance(emptyMap));

    // map: no need to copy the singleton map
    final Map<String, Integer> singletonMap =
        Collections.singletonMap("Elvis", "Elvis".length());
    assertThat(bean.withImmutableMap(singletonMap).immutableMap(),
        sameInstance(singletonMap));
  }

  @Test void testSubBean() {
    assertThat(ImmutableBeans.create(SubBean.class)
            .withBuzz(7).withBaz("x").withBar(true).toString(),
        is("{Bar=true, Baz=x, Buzz=7}"));

    assertThat(ImmutableBeans.create(MyBean.class)
            .withBar(true).as(SubBean.class)
            .withBuzz(7).withBaz("x").toString(),
        is("{Bar=true, Baz=x, Buzz=7}"));

    // Up-casting to MyBean does not discard value of sub-class property,
    // "buzz". This is a feature, not a bug. It will allow us to down-cast
    // later. (If we down-cast to a different sub-class where "buzz" has a
    // different type, that would be a problem.)
    assertThat(ImmutableBeans.create(SubBean.class)
            .withBuzz(5).withBar(false).as(MyBean.class)
            .withBaz("z").toString(),
        is("{Bar=false, Baz=z, Buzz=5}"));
  }

  /** Bean whose default value is not a valid value for the enum;
   * used in {@link #testValidate()}. */
  interface BeanWhoseDefaultIsBadEnumValue {
    @ImmutableBeans.Property
    @ImmutableBeans.EnumDefault("YELLOW")
    Color getColor();
    BeanWhoseDefaultIsBadEnumValue withColor(Color color);
  }

  /** Bean that has a 'with' method that has a bad return type;
   * used in {@link #testValidate()}. */
  interface BeanWhoseWithMethodHasBadReturnType {
    @ImmutableBeans.Property int getFoo();
    MyBean withFoo(int x);
  }

  /** Bean that has a 'with' method that does not correspond to a property
   * (declared using a {@link ImmutableBeans.Property} annotation on a
   * 'get' method;
   * used in {@link #testValidate()}. */
  interface BeanWhoseWithMethodDoesNotMatchProperty {
    @ImmutableBeans.Property int getFoo();
    MyBean withFoo(int x);
  }

  /** Bean that has a 'with' method whose argument type is not the same as the
   * type of the property (the return type of a 'get{PropertyName}' method);
   * used in {@link #testValidate()}. */
  interface BeanWhoseWithMethodHasArgOfWrongType {
    @ImmutableBeans.Property int getFoo();
    BeanWhoseWithMethodHasTooManyArgs withFoo(float x);
  }

  /** Bean that has a 'with' method that has too many arguments;
   * it should have just one;
   * used in {@link #testValidate()}. */
  interface BeanWhoseWithMethodHasTooManyArgs {
    @ImmutableBeans.Property int getFoo();
    BeanWhoseWithMethodHasTooManyArgs withFoo(int x, int y);
  }

  /** Bean that has a 'with' method that has too few arguments;
   * it should have just one;
   * used in {@link #testValidate()}. */
  interface BeanWhoseWithMethodHasTooFewArgs {
    @ImmutableBeans.Property int getFoo();
    BeanWhoseWithMethodHasTooFewArgs withFoo();
  }

  /** Bean that has a 'set' method that has a bad return type;
   * used in {@link #testValidate()}. */
  interface BeanWhoseSetMethodHasBadReturnType {
    @ImmutableBeans.Property int getFoo();
    MyBean setFoo(int x);
  }

  /** Bean that has a 'get' method that has one arg, whereas 'get' must have no
   * args;
   * used in {@link #testValidate()}. */
  interface BeanWhoseGetMethodHasTooManyArgs {
    @ImmutableBeans.Property int getFoo(int x);
    void setFoo(int x);
  }

  /** Bean that has a 'set' method that does not correspond to a property
   * (declared using a {@link ImmutableBeans.Property} annotation on a
   * 'get' method;
   * used in {@link #testValidate()}. */
  interface BeanWhoseSetMethodDoesNotMatchProperty {
    @ImmutableBeans.Property int getBar();
    void setFoo(int x);
  }

  /** Bean that has a 'set' method whose argument type is not the same as the
   * type of the property (the return type of a 'get{PropertyName}' method);
   * used in {@link #testValidate()}. */
  interface BeanWhoseSetMethodHasArgOfWrongType {
    @ImmutableBeans.Property int getFoo();
    void setFoo(float x);
  }

  /** Bean that has a 'set' method that has too many arguments;
   * it should have just one;
   * used in {@link #testValidate()}. */
  interface BeanWhoseSetMethodHasTooManyArgs {
    @ImmutableBeans.Property int getFoo();
    void setFoo(int x, int y);
  }

  /** Bean that has a 'set' method that has too few arguments;
   * it should have just one;
   * used in {@link #testValidate()}. */
  interface BeanWhoseSetMethodHasTooFewArgs {
    @ImmutableBeans.Property int getFoo();
    void setFoo();
  }

  // ditto setXxx

    // TODO it is an error to declare an int property to be not required
    // TODO it is an error to declare an boolean property to be not required

  /** A simple bean with properties of various types, no defaults. */
  public interface MyBean {
    default <T> T as(Class<T> class_) {
      return ImmutableBeans.copy(class_, this);
    }

    @ImmutableBeans.Property
    int getFoo();
    MyBean withFoo(int x);

    @ImmutableBeans.Property
    boolean isBar();
    MyBean withBar(boolean x);

    @ImmutableBeans.Property
    String getBaz();
    MyBean withBaz(String s);
  }

  /** A bean class with just about every combination of default values
   * missing and present, and required or not. */
  interface Bean2 {
    @ImmutableBeans.Property
    @ImmutableBeans.IntDefault(1)
    int getIntWithDefault();
    Bean2 withIntWithDefault(int x);

    @ImmutableBeans.Property
    int getIntSansDefault();
    Bean2 withIntSansDefault(int x);

    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(true)
    boolean isBooleanWithDefault();
    Bean2 withBooleanWithDefault(boolean x);

    @ImmutableBeans.Property
    boolean isBooleanSansDefault();
    Bean2 withBooleanSansDefault(boolean x);

    @ImmutableBeans.Property(required = true)
    String getStringSansDefault();
    Bean2 withStringSansDefault(String x);

    @ImmutableBeans.Property
    String getOptionalString();
    Bean2 withOptionalString(String s);

    /** Property is required because it has 'Nonnull' annotation. */
    @ImmutableBeans.Property
    @Nonnull String getNonnullString();
    Bean2 withNonnullString(String s);

    @ImmutableBeans.Property
    @ImmutableBeans.StringDefault("abc")
    @Nonnull String getStringWithDefault();
    Bean2 withStringWithDefault(String s);

    @ImmutableBeans.Property
    @ImmutableBeans.NullDefault
    String getStringWithNullDefault();
    Bean2 withStringWithNullDefault(String s);

    @ImmutableBeans.Property
    @ImmutableBeans.EnumDefault("RED")
    @Nonnull Color getColorWithDefault();
    Bean2 withColorWithDefault(Color color);

    @ImmutableBeans.Property
    @ImmutableBeans.NullDefault
    Color getColorWithNullDefault();
    Bean2 withColorWithNullDefault(Color color);

    @ImmutableBeans.Property()
    Color getColorOptional();
    Bean2 withColorOptional(Color color);
  }

  /** Red, blue, green. */
  enum Color {
    RED,
    BLUE,
    GREEN
  }

  /** Bean interface that has a default method and one property. */
  public interface BeanWithDefault {
    default String nTimes(int x) {
      if (x <= 0) {
        return "";
      }
      final char c = getChar();
      return c + nTimes(x - 1);
    }

    @ImmutableBeans.Property
    char getChar();
    BeanWithDefault withChar(char c);
  }

  /** A bean that extends another bean.
   *
   * <p>Its {@code with} methods return either the base interface
   * or the derived interface.
   */
  public interface SubBean extends MyBean {
    @ImmutableBeans.Property
    int getBuzz();
    SubBean withBuzz(int i);
  }

  /** A bean that has collection-valued properties. */
  public interface CollectionBean {
    @ImmutableBeans.Property(makeImmutable = false)
    List<String> list();

    CollectionBean withList(List<String> list);

    @ImmutableBeans.Property(makeImmutable = true)
    List<String> immutableList();

    CollectionBean withImmutableList(List<String> list);

    @ImmutableBeans.Property(makeImmutable = false)
    Set<String> set();

    CollectionBean withSet(Set<String> set);

    @ImmutableBeans.Property(makeImmutable = true)
    Set<String> immutableSet();

    CollectionBean withImmutableSet(Set<String> set);

    @ImmutableBeans.Property(makeImmutable = false)
    Map<String, Integer> map();

    CollectionBean withMap(Map<String, Integer> map);

    @ImmutableBeans.Property(makeImmutable = true)
    Map<String, Integer> immutableMap();

    CollectionBean withImmutableMap(Map<String, Integer> map);
  }
}
