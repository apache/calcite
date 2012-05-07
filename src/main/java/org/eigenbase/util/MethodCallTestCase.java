/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package org.eigenbase.util;

import java.lang.reflect.*;

import junit.framework.*;


/**
 * A <code>MethodCallTestCase</code> is a {@link TestCase} which invokes a
 * method on an object. You can use this class to expose methods of a
 * non-TestCase class as unit tests; {@link #addTestMethods} does this for all
 * <code>public</code>, non-<code>static</code>, <code>void</code> methods whose
 * names start with "test", and have one .
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 19, 2003
 */
public class MethodCallTestCase
    extends TestCase
{
    //~ Instance fields --------------------------------------------------------

    private final Dispatcher dispatcher;
    private final Method method;
    private final Object o;
    private final Object [] args;

    //~ Constructors -----------------------------------------------------------

    MethodCallTestCase(
        String name,
        Object o,
        Method method,
        Dispatcher dispatcher)
    {
        super(name);
        this.o = o;
        this.args = new Object[] { this };
        this.method = method;
        this.dispatcher = dispatcher;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns whether a method can be called as a test case; it must:
     *
     * <ol>
     * <li>be <code>public</code></li>
     * <li>be non-<code>static</code></li>
     * <li>return <code>void</code></li>
     * <li>begin with <code>test</code></li>
     * <li>have precisely one parameter of type {@link TestCase} (or a class
     * derived from it)</li>
     * </ol>
     */
    public static boolean isSuitable(Method method)
    {
        final int modifiers = method.getModifiers();
        if (!Modifier.isPublic(modifiers)) {
            //return false;
        }
        if (Modifier.isStatic(modifiers)) {
            return false;
        }
        if (method.getReturnType() != Void.TYPE) {
            return false;
        }
        if (!method.getName().startsWith("test")) {
            return false;
        }
        final Class [] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length != 1) {
            return false;
        }
        if (!TestCase.class.isAssignableFrom(parameterTypes[0])) {
            return false;
        }
        return true;
    }

    public static void addTestMethods(
        TestSuite suite,
        Object o,
        Dispatcher dispatcher)
    {
        Class clazz = o.getClass();
        Method [] methods = clazz.getDeclaredMethods();
        for (int i = 0; i < methods.length; i++) {
            final Method method = methods[i];
            if (isSuitable(method)) {
                suite.addTest(
                    new MethodCallTestCase(
                        method.getName(),
                        o,
                        method,
                        dispatcher));
            }
        }
    }

    protected void runTest()
        throws Throwable
    {
        Util.discard(dispatcher.call(method, o, args));
    }

    //~ Inner Interfaces -------------------------------------------------------

    /**
     * A class implementing <code>Dispatcher</code> calls a method from within
     * its own security context. It exists to allow a {@link MethodCallTestCase}
     * to call non-public methods.
     */
    public interface Dispatcher
    {
        Object call(
            Method method,
            Object o,
            Object [] args)
            throws IllegalAccessException,
                IllegalArgumentException,
                InvocationTargetException;
    }
}

// End MethodCallTestCase.java
