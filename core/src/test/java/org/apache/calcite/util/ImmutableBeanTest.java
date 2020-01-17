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

import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nonnull;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.jupiter.api.Assertions.fail;

/** Unit test for {@link ImmutableBeans}. */
public class ImmutableBeanTest {

  @Test public void testSimple() {
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

  @Test public void testDefault() {
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

  @Test public void testValidate() {
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

  @Test public void testDefaultMethod() {
    assertThat(ImmutableBeans.create(BeanWithDefault.class)
        .withChar('a').nTimes(2), is("aa"));
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
  interface MyBean {
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
}
