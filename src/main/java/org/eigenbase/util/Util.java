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
package org.eigenbase.util;

import java.awt.Toolkit;
import java.io.*;
import java.lang.Iterable;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.math.*;
import java.net.*;
import java.nio.charset.*;
import java.sql.*;
import java.text.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.jar.*;
import java.util.logging.*;
import java.util.regex.*;

import org.eigenbase.sql.validate.*;

import net.hydromatic.linq4j.Ord;


/**
 * Miscellaneous utility functions.
 */
public class Util
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Name of the system property that controls whether the AWT work-around is
     * enabled. This workaround allows Farrago to load its native libraries
     * despite a conflict with AWT and allows applications that use AWT to
     * function normally.
     *
     * @see #loadLibrary(String)
     */
    public static final String awtWorkaroundProperty =
        "org.eigenbase.util.AWT_WORKAROUND";

    /**
     * System-dependent newline character.
     */
    public static final String lineSeparator =
        System.getProperty("line.separator");

    /**
     * System-dependent file separator, for example, "/" or "\."
     */
    public static final String fileSeparator =
        System.getProperty("file.separator");

    /**
     * Datetime format string for generating a timestamp string to be used as
     * part of a filename. Conforms to SimpleDateFormat conventions.
     */
    public static final String fileTimestampFormat = "yyyy-MM-dd_HH_mm_ss";

    public static final Object [] emptyObjectArray = new Object[0];
    public static final String [] emptyStringArray = new String[0];
    public static final SqlMoniker [] emptySqlMonikerArray = new SqlMoniker[0];

    private static boolean driversLoaded = false;

    /**
     * Regular expression for a valid java identifier which contains no
     * underscores and can therefore be returned intact by {@link #toJavaId}.
     */
    private static final Pattern javaIdPattern =
        Pattern.compile("[a-zA-Z_$][a-zA-Z0-9$]*");

    /**
     * @see #loadLibrary(String)
     */
    private static Toolkit awtToolkit;

    /**
     * Maps classes to the map of their enum values. Uses a weak map so that
     * classes are not prevented from being unloaded.
     */
    private static final Map<Class, Map<String, ? extends Enum>>
        mapClazzToMapNameToEnum =
            new WeakHashMap<Class, Map<String, ? extends Enum>>();
    public static final String [] spaces =
            {
                "",
                " ",
                "  ",
                "   ",
                "    ",
                "     ",
                "      ",
                "       ",
                "        ",
            };

    //~ Methods ----------------------------------------------------------------

    /**
     * Does nothing with its argument. Call this method when you have a value
     * you are not interested in, but you don't want the compiler to warn that
     * you are not using it.
     */
    public static final void discard(Object o)
    {
        if (false) {
            discard(o);
        }
    }

    /**
     * Does nothing with its argument. Call this method when you have a value
     * you are not interested in, but you don't want the compiler to warn that
     * you are not using it.
     */
    public static final void discard(int i)
    {
        if (false) {
            discard(i);
        }
    }

    /**
     * Does nothing with its argument. Call this method when you have a value
     * you are not interested in, but you don't want the compiler to warn that
     * you are not using it.
     */
    public static final void discard(boolean b)
    {
        if (false) {
            discard(b);
        }
    }

    /**
     * Does nothing with its argument. Call this method when you have a value
     * you are not interested in, but you don't want the compiler to warn that
     * you are not using it.
     */
    public static final void discard(double d)
    {
        if (false) {
            discard(d);
        }
    }

    /**
     * Records that an exception has been caught but will not be re-thrown. If
     * the tracer is not null, logs the exception to the tracer.
     *
     * @param e Exception
     * @param logger If not null, logs exception to this logger
     */
    public static final void swallow(
        Throwable e,
        Logger logger)
    {
        if (logger != null) {
            logger.log(Level.FINER, "Discarding exception", e);
        }
    }

    /**
     * Returns whether two objects are equal or are both null.
     */
    public static final boolean equal(
        Object s0,
        Object s1)
    {
        if (s0 == s1) {
            return true;
        } else if (s0 == null) {
            return false;
        } else {
            return s0.equals(s1);
        }
    }

    /**
     * Returns whether two lists are equal to each other using shallow
     * comparisons.
     *
     * @param list0 First list
     * @param list1 Second list
     * @return Whether lists are same length and all of their elements are
     *   equal using {@code ==} (may be null).
     */
    public static final <T> boolean equalShallow(
        List<? extends T> list0, List<? extends T> list1)
    {
        if (list0.size() != list1.size()) {
            return false;
        }
        for (int i = 0; i < list0.size(); i++) {
            if (list0.get(i) != list1.get(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Combines two integers into a hash code.
     */
    public static int hash(
        int i,
        int j)
    {
        return (i << 4) ^ j;
    }

    /**
     * Computes a hash code from an existing hash code and an object (which may
     * be null).
     */
    public static int hash(
        int h,
        Object o)
    {
        int k = (o == null) ? 0 : o.hashCode();
        return ((h << 4) | h) ^ k;
    }

    /**
     * Computes a hash code from an existing hash code and an array of objects
     * (which may be null).
     */
    public static int hashArray(
        int h,
        Object [] a)
    {
        // The hashcode for a null array and an empty array should be different
        // than h, so use magic numbers.
        if (a == null) {
            return hash(h, 19690429);
        }
        if (a.length == 0) {
            return hash(h, 19690721);
        }
        for (int i = 0; i < a.length; i++) {
            h = hash(h, a[i]);
        }
        return h;
    }

    /**
     * Computes a hash code over var args.
     */
    public static int hashV(Object... a)
    {
        int h = 19690721;
        for (Object o : a) {
            h = hash(h, o);
        }
        return h;
    }

    /**
     * Returns a set of the elements which are in <code>set1</code> but not in
     * <code>set2</code>, without modifying either.
     */
    public static <T> Set<T> minus(Set<T> set1, Set<T> set2)
    {
        if (set1.isEmpty()) {
            return set1;
        } else if (set2.isEmpty()) {
            return set1;
        } else {
            Set<T> set = new HashSet<T>(set1);
            set.removeAll(set2);
            return set;
        }
    }

    /**
     * Computes <code>nlogn(n)</code> using the natural logarithm (or <code>
     * n</code> if <code>n<{@link Math#E}</code>, so the result is never
     * negative.
     */
    public static double nLogN(double d)
    {
        return (d < Math.E) ? d : (d * Math.log(d));
    }

    /**
     * Prints an object using reflection. We can handle <code>null</code>;
     * arrays of objects and primitive values; for regular objects, we print all
     * public fields.
     */
    public static void print(
        PrintWriter pw,
        Object o)
    {
        print(pw, o, 0);
    }

    public static void print(
        PrintWriter pw,
        Object o,
        int indent)
    {
        if (o == null) {
            pw.print("null");
            return;
        }
        Class clazz = o.getClass();
        if (o instanceof String) {
            printJavaString(pw, (String) o, true);
        } else if (
            (clazz == Integer.class)
            || (clazz == Boolean.class)
            || (clazz == Character.class)
            || (clazz == Byte.class)
            || (clazz == Short.class)
            || (clazz == Long.class)
            || (clazz == Float.class)
            || (clazz == Double.class)
            || (clazz == Void.class))
        {
            pw.print(o.toString());
        } else if (clazz.isArray()) {
            // o is an array, but we can't cast to Object[] because it may be
            // an array of primitives.
            Object [] a; // for debug
            if (o instanceof Object []) {
                a = (Object []) o;
                discard(a);
            }
            int n = Array.getLength(o);
            pw.print("{");
            for (int i = 0; i < n; i++) {
                if (i > 0) {
                    pw.println(",");
                } else {
                    pw.println();
                }
                for (int j = 0; j < indent; j++) {
                    pw.print("\t");
                }
                print(
                    pw,
                    Array.get(o, i),
                    indent + 1);
            }
            pw.print("}");
        } else if (o instanceof Iterator) {
            pw.print(clazz.getName());
            Iterator iter = (Iterator) o;
            pw.print(" {");
            int i = 0;
            while (iter.hasNext()) {
                if (i++ > 0) {
                    pw.println(",");
                }
                print(
                    pw,
                    iter.next(),
                    indent + 1);
            }
            pw.print("}");
        } else if (o instanceof Enumeration) {
            pw.print(clazz.getName());
            Enumeration e = (Enumeration) o;
            pw.print(" {");
            int i = 0;
            while (e.hasMoreElements()) {
                if (i++ > 0) {
                    pw.println(",");
                }
                print(
                    pw,
                    e.nextElement(),
                    indent + 1);
            }
            pw.print("}");
        } else {
            pw.print(clazz.getName());
            pw.print(" {");
            Field [] fields = clazz.getFields();
            int printed = 0;
            for (int i = 0; i < fields.length; i++) {
                if (isStatic(fields[i])) {
                    continue;
                }
                if (printed++ > 0) {
                    pw.println(",");
                } else {
                    pw.println();
                }
                for (int j = 0; j < indent; j++) {
                    pw.print("\t");
                }
                pw.print(fields[i].getName());
                pw.print("=");
                Object val = null;
                try {
                    val = fields[i].get(o);
                } catch (IllegalAccessException e) {
                    throw newInternal(e);
                }
                print(pw, val, indent + 1);
            }
            pw.print("}");
        }
    }

    /**
     * Prints a string, enclosing in double quotes (") and escaping if
     * necessary. For examples, <code>printDoubleQuoted(w,"x\"y",false)</code>
     * prints <code>"x\"y"</code>.
     */
    public static final void printJavaString(
        PrintWriter pw,
        String s,
        boolean nullMeansNull)
    {
        if (s == null) {
            if (nullMeansNull) {
                pw.print("null");
            } else {
                //pw.print("");
            }
        } else {
            String s1 = replace(s, "\\", "\\\\");
            String s2 = replace(s1, "\"", "\\\"");
            String s3 = replace(s2, "\n\r", "\\n");
            String s4 = replace(s3, "\n", "\\n");
            String s5 = replace(s4, "\r", "\\r");
            pw.print("\"");
            pw.print(s5);
            pw.print("\"");
        }
    }

    public static void println(
        PrintWriter pw,
        Object o)
    {
        print(pw, o, 0);
        pw.println();
    }

    /**
     * Formats a {@link BigDecimal} value to a string in scientific notation For
     * example<br>
     *
     * <ul>
     * <li>A value of 0.00001234 would be formated as <code>1.234E-5</code></li>
     * <li>A value of 100000.00 would be formated as <code>1.00E5</code></li>
     * <li>A value of 100 (scale zero) would be formated as <code>
     * 1E2</code></li><br>
     * If <code>bd</code> has a precision higher than 20, this method will
     * truncate the output string to have a precision of 20 (no rounding will be
     * done, just a truncate).
     */
    public static String toScientificNotation(BigDecimal bd)
    {
        final int truncateAt = 20;
        String unscaled = bd.unscaledValue().toString();
        if (bd.signum() < 0) {
            unscaled = unscaled.substring(1);
        }
        int len = unscaled.length();
        int scale = bd.scale();
        int e = len - scale - 1;

        StringBuilder ret = new StringBuilder();
        if (bd.signum() < 0) {
            ret.append('-');
        }

        // do truncation
        unscaled =
            unscaled.substring(
                0,
                Math.min(truncateAt, len));
        ret.append(unscaled.charAt(0));
        if (scale == 0) {
            // trim trailing zeroes since they aren't significant
            int i = unscaled.length();
            while (i > 1) {
                if (unscaled.charAt(i - 1) != '0') {
                    break;
                }
                --i;
            }
            unscaled = unscaled.substring(0, i);
        }
        if (unscaled.length() > 1) {
            ret.append(".");
            ret.append(unscaled.substring(1));
        }

        ret.append("E");
        ret.append(e);
        return ret.toString();
    }

    /**
     * Replaces every occurrence of <code>find</code> in <code>s</code> with
     * <code>replace</code>.
     */
    public static final String replace(
        String s,
        String find,
        String replace)
    {
        // let's be optimistic
        int found = s.indexOf(find);
        if (found == -1) {
            return s;
        }
        StringBuilder sb = new StringBuilder(s.length());
        int start = 0;
        for (;;) {
            for (; start < found; start++) {
                sb.append(s.charAt(start));
            }
            if (found == s.length()) {
                break;
            }
            sb.append(replace);
            start += find.length();
            found = s.indexOf(find, start);
            if (found == -1) {
                found = s.length();
            }
        }
        return sb.toString();
    }

    /**
     * Creates a file-protocol URL for the given file.
     */
    public static URL toURL(File file)
        throws MalformedURLException
    {
        String path = file.getAbsolutePath();

        // This is a bunch of weird code that is required to
        // make a valid URL on the Windows platform, due
        // to inconsistencies in what getAbsolutePath returns.
        String fs = System.getProperty("file.separator");
        if (fs.length() == 1) {
            char sep = fs.charAt(0);
            if (sep != '/') {
                path = path.replace(sep, '/');
            }
            if (path.charAt(0) != '/') {
                path = '/' + path;
            }
        }
        path = "file://" + path;
        return new URL(path);
    }

    /**
     * Gets a timestamp string for use in file names. The generated timestamp
     * string reflects the current time.
     */
    public static String getFileTimestamp()
    {
        SimpleDateFormat sdf = new SimpleDateFormat(fileTimestampFormat);
        return sdf.format(new java.util.Date());
    }

    /**
     * Converts double-quoted Java strings to their contents. For example,
     * <code>"foo\"bar"</code> becomes <code>foo"bar</code>.
     */
    public static String stripDoubleQuotes(String value)
    {
        assert (value.charAt(0) == '"');
        assert (value.charAt(value.length() - 1) == '"');
        String s5 = value.substring(1, value.length() - 1);
        String s4 = Util.replace(s5, "\\r", "\r");
        String s3 = Util.replace(s4, "\\n", "\n");
        String s2 = Util.replace(s3, "\\\"", "\"");
        String s1 = Util.replace(s2, "\\\\", "\\");
        return s1;
    }

    /**
     * Converts an arbitrary string into a string suitable for use as a Java
     * identifier.
     *
     * <p>The mapping is one-to-one (that is, distinct strings will produce
     * distinct java identifiers). The mapping is also reversible, but the
     * inverse mapping is not implemented.</p>
     *
     * <p>A valid Java identifier must start with a Unicode letter, underscore,
     * or dollar sign ($). The other characters, if any, can be a Unicode
     * letter, underscore, dollar sign, or digit.</p>
     *
     * <p>This method uses an algorithm similar to URL encoding. Valid
     * characters are unchanged; invalid characters are converted to an
     * underscore followed by the hex code of the character; and underscores are
     * doubled.</p>
     *
     * Examples:
     *
     * <ul>
     * <li><code>toJavaId("foo")</code> returns <code>"foo"</code>
     * <li><code>toJavaId("foo bar")</code> returns <code>"foo_20_bar"</code>
     * <li><code>toJavaId("foo_bar")</code> returns <code>"foo__bar"</code>
     * <li><code>toJavaId("0bar")</code> returns <code>"_40_bar"</code> (digits
     * are illegal as a prefix)
     * <li><code>toJavaId("foo0bar")</code> returns <code>"foo0bar"</code>
     * </ul>
     *
     * @testcase
     */
    public static String toJavaId(
        String s,
        int ordinal)
    {
        // If it's already a valid Java id (and doesn't contain any
        // underscores), return it unchanged.
        if (javaIdPattern.matcher(s).matches()) {
            // prepend "ID$" to string so it doesn't clash with java keywords
            return "ID$" + ordinal + "$" + s;
        }

        // Escape underscores and other undesirables.
        StringBuilder buf = new StringBuilder(s.length() + 10);
        buf.append("ID$");
        buf.append(ordinal);
        buf.append("$");
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '_') {
                buf.append("__");
            } else if (
                (c < 0x7F) /* Normal ascii character */
                && !Character.isISOControl(c)
                && ((i == 0) ? Character.isJavaIdentifierStart(c)
                    : Character.isJavaIdentifierPart(c)))
            {
                buf.append(c);
            } else {
                buf.append("_");
                buf.append(Integer.toString(c, 16));
                buf.append("_");
            }
        }
        return buf.toString();
    }

    /**
     * Materializes the results of a {@link java.util.Iterator} as a {@link
     * java.util.List}.
     *
     * @param iter iterator to materialize
     *
     * @return materialized list
     */
    public static <T> List<T> toList(Iterator<T> iter)
    {
        List<T> list = new ArrayList<T>();
        while (iter.hasNext()) {
            list.add(iter.next());
        }
        return list;
    }

    static boolean isStatic(java.lang.reflect.Member member)
    {
        int modifiers = member.getModifiers();
        return java.lang.reflect.Modifier.isStatic(modifiers);
    }

    /**
     * @return true if s==null or if s.length()==0
     */
    public static boolean isNullOrEmpty(String s)
    {
        return (null == s) || (s.length() == 0);
    }

    /**
     * Returns the connect string with which to connect to the 'Sales' test
     * database. In the process, it loads the necessary drivers.
     */
    public static String getSalesConnectString()
    {
        loadDrivers();
        return SaffronProperties.instance().testJdbcUrl.get();
    }

    private static synchronized void loadDrivers()
    {
        if (driversLoaded) {
            return;
        }
        String jdbcDrivers = SaffronProperties.instance().testJdbcDrivers.get();
        StringTokenizer tok = new StringTokenizer(jdbcDrivers, ",");
        while (tok.hasMoreTokens()) {
            String jdbcDriver = tok.nextToken();
            try {
                Class.forName(jdbcDriver);
            } catch (ClassNotFoundException e) {
                System.out.println(
                    "Warning: could not find driver "
                    + jdbcDriver);
            }
        }
        driversLoaded = true;
    }

    /**
     * Returns the {@link Charset} object representing the value of {@link
     * SaffronProperties#defaultCharset}
     *
     * @throws java.nio.charset.IllegalCharsetNameException If the given charset
     * name is illegal
     * @throws java.nio.charset.UnsupportedCharsetException If no support for
     * the named charset is available in this instance of the Java virtual
     * machine
     */
    public static Charset getDefaultCharset()
    {
        return Charset.forName(
            SaffronProperties.instance().defaultCharset.get());
    }

    public static Error newInternal()
    {
        return newInternal("(unknown cause)");
    }

    public static Error newInternal(String s)
    {
        return new AssertionError("Internal error: " + s);
    }

    public static Error newInternal(Throwable e)
    {
        return newInternal(e, "(unknown cause)");
    }

    public static Error newInternal(Throwable e, String s)
    {
        String message = "Internal error: " + s;
        if (false) {
            // TODO re-enable this code when we're no longer throwing spurious
            //   internal errors (which should be parse errors, for example)
            System.err.println(message);
            e.printStackTrace(System.err);
        }
        AssertionError ae = new AssertionError(message);
        ae.initCause(e);
        return ae;
    }

    /**
     * Retrieves messages in a exception and writes them to a string. In the
     * string returned, each message will appear on a different line.
     *
     * @return a non-null string containing all messages of the exception
     */
    public static String getMessages(Throwable t)
    {
        StringBuilder sb = new StringBuilder();
        for (Throwable curr = t; curr != null; curr = curr.getCause()) {
            String msg =
                ((curr instanceof EigenbaseException)
                    || (curr instanceof SQLException)) ? curr.getMessage()
                : curr.toString();
            if (sb.length() > 0) {
                sb.append("\n");
            }
            sb.append(msg);
        }
        return sb.toString();
    }

    /**
     * Returns the stack trace of a throwable. Called from native code.
     *
     * @param t Throwable
     *
     * @return Stack trace
     */
    public static String getStackTrace(Throwable t)
    {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        pw.flush();
        return sw.toString();
    }

    /**
     * Checks a pre-condition.
     *
     * <p>For example,
     *
     * <pre>
     * /**
     *   * @ pre x != 0
     *   * /
     * void foo(int x) {
     *     Util.pre(x != 0, "x != 0");
     * }</pre>
     *
     * @param b Result of evaluating the pre-condition.
     * @param description Description of the pre-condition.
     */
    public static void pre(boolean b, String description)
    {
        if (!b) {
            throw newInternal("pre-condition failed: " + description);
        }
    }

    /**
     * Checks a post-condition.
     *
     * <p>For example,
     *
     * <pre>
     * /**
     *   * @ post return != 0
     *   * /
     * void foo(int x) {
     *     int res = bar(x);
     *     Util.post(res != 0, "return != 0");
     * }</pre>
     *
     * @param b Result of evaluating the pre-condition.
     * @param description Description of the pre-condition.
     */
    public static void post(boolean b, String description)
    {
        if (!b) {
            throw newInternal("post-condition failed: " + description);
        }
    }

    /**
     * Checks an invariant.
     *
     * <p>This is similar to <code>assert</code> keyword, except that the
     * condition is always evaluated even if asserts are disabled.
     */
    public static void permAssert(boolean b, String description)
    {
        if (!b) {
            throw newInternal("invariant violated: " + description);
        }
    }

    /**
     * Returns a {@link java.lang.RuntimeException} indicating that a particular
     * feature has not been implemented, but should be.
     *
     * <p>If every 'hole' in our functionality uses this method, it will be
     * easier for us to identity the holes. Throwing a {@link
     * java.lang.UnsupportedOperationException} isn't as good, because sometimes
     * we actually want to partially implement an API.
     *
     * <p>Example usage:
     *
     * <blockquote>
     * <pre><code>class MyVisitor extends BaseVisitor {
     *     void accept(Foo foo) {
     *         // Exception will identify which subclass forgot to override
     *         // this method
     *         throw Util.needToImplement(this);
     *     }
     * }</pre>
     * </blockquote>
     *
     * @param o The object which was the target of the call, or null. Passing
     * the object gives crucial information if a method needs to be overridden
     * and a subclass forgot to do so.
     *
     * @return an {@link UnsupportedOperationException}.
     */
    public static RuntimeException needToImplement(Object o)
    {
        String description = null;
        if (o != null) {
            description = o.getClass().toString() + ": " + o.toString();
        }
        throw new UnsupportedOperationException(description);
    }

    /**
     * Flags a piece of code as needing to be cleaned up before you check in.
     *
     * <p>Introduce a call to this method to indicate that a piece of code, or a
     * javadoc comment, needs work before you check in. If you have an IDE which
     * can easily trace references, this is an easy way to maintain a to-do
     * list.
     *
     * <p><strong>Checked-in code must never call this method</strong>: you must
     * remove all calls/references to this method before you check in.
     *
     * <p>The <code>argument</code> has generic type and determines the type of
     * the result. This allows you to use the method inside an expression, for
     * example
     *
     * <blockquote>
     * <pre><code>int x = Util.deprecated(0, false);</code></pre>
     * </blockquote>
     *
     * but the usual usage is to pass in a descriptive string.
     *
     * <h3>Examples</h3>
     *
     * <h4>Example #1: Using <code>deprecated</code> to fail if a piece of
     * supposedly dead code is reached</h4>
     *
     * <blockquote>
     * <pre><code>void foo(int x) {
     *     if (x &lt; 0) {
     *         // If this code is executed, an error will be thrown.
     *         Util.deprecated(
     *             "no longer need to handle negative numbers", true);
     *         bar(x);
     *     } else {
     *         baz(x);
     *     }
     * }</code></pre>
     * </blockquote>
     *
     * <h4>Example #2: Using <code>deprecated</code> to comment out dead
     * code</h4>
     *
     * <blockquote>
     * <pre>if (Util.deprecated(false, false)) {
     *     // This code will not be executed, but an error will not be thrown.
     *     baz();
     * }</pre>
     * </blockquote>
     *
     * @param argument Arbitrary argument to the method.
     * @param fail Whether to throw an exception if this method is called
     *
     * @return The value of the <code>argument</code>.
     *
     * @deprecated If a piece of code calls this method, it indicates that the
     * code needs to be cleaned up.
     */
    public static <T> T deprecated(T argument, boolean fail)
    {
        if (fail) {
            throw new UnsupportedOperationException();
        }
        return argument;
    }

    /**
     * Uses {@link System#loadLibrary(String)} to load a native library
     * correctly under mingw (Windows/Cygwin) and Linux environments.
     *
     * <p>This method also implements a work-around for applications that wish
     * to load AWT. AWT conflicts with some native libraries in a way that
     * requires AWT to be loaded first. This method checks the system property
     * named {@link #awtWorkaroundProperty} and if it is set to "on" (default;
     * case-insensitive) it pre-loads AWT to avoid the conflict.
     *
     * @param libName the name of the library to load, as in {@link
     * System#loadLibrary(String)}.
     */
    public static void loadLibrary(String libName)
    {
        String awtSetting = System.getProperty(awtWorkaroundProperty, "on");
        if ((awtToolkit == null) && awtSetting.equalsIgnoreCase("on")) {
            // REVIEW jvs 8-Sept-2006:  workaround upon workaround.  This
            // is required because in native code, we sometimes (see Farrago)
            // have to use dlopen("libfoo.so", RTLD_GLOBAL) in order for native
            // plugins to load correctly.  But the RTLD_GLOBAL causes trouble
            // later if someone tries to use AWT from within the same JVM.
            // So... preload AWT here unless someone configured explicitly
            // not to do so.
            try {
                awtToolkit = Toolkit.getDefaultToolkit();
            } catch (Throwable ex) {
                // Suppress problems so that a headless server doesn't fail on
                // startup.  If AWT is actually needed, the same exception will
                // show up later, which is fine.

                // NOTE jvs 27-Mar-2007: If this exception occurs, we'll
                // retry the AWT load on each loadLibrary call.  That's okay,
                // since there are only a few libraries and they're loaded
                // via static initializers.
            }
        }

        System.loadLibrary(libName);
    }

    /**
     * Returns whether an array of strings contains a given string among the
     * first <code>length</code> entries.
     *
     * @param a Array of strings
     * @param length Number of entries to search
     * @param s String to seek
     *
     * @return Whether array contains the name
     */
    public static boolean contains(
        String [] a,
        int length,
        String s)
    {
        for (int i = 0; i < length; i++) {
            if (a[i].equals(s)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Reads all remaining contents from a {@link java.io.Reader} and returns
     * them as a string.
     *
     * @param reader reader to read from
     *
     * @return reader contents as string
     */
    public static String readAllAsString(Reader reader)
        throws IOException
    {
        StringBuilder sb = new StringBuilder();
        char [] buf = new char[4096];
        for (;;) {
            int n = reader.read(buf);
            if (n == -1) {
                break;
            }
            sb.append(buf, 0, n);
        }
        return sb.toString();
    }

    /**
     * Closes a Jar, ignoring any I/O exception. This should only be
     * used in finally blocks when it's necessary to avoid throwing an exception
     * which might mask a real exception.
     *
     * @param jar jar to close
     */
    public static void squelchJar(JarFile jar)
    {
        try {
            if (jar != null) {
                jar.close();
            }
        } catch (IOException ex) {
            // intentionally suppressed
        }
    }

    /**
     * Closes an InputStream, ignoring any I/O exception. This should only be
     * used in finally blocks when it's necessary to avoid throwing an exception
     * which might mask a real exception.
     *
     * @param stream stream to close
     */
    public static void squelchStream(InputStream stream)
    {
        try {
            if (stream != null) {
                stream.close();
            }
        } catch (IOException ex) {
            // intentionally suppressed
        }
    }

    /**
     * Closes an OutputStream, ignoring any I/O exception. This should only be
     * used in finally blocks when it's necessary to avoid throwing an exception
     * which might mask a real exception. If you want to make sure that data has
     * been successfully flushed, do NOT use this anywhere else; use
     * stream.close() instead.
     *
     * @param stream stream to close
     */
    public static void squelchStream(OutputStream stream)
    {
        try {
            if (stream != null) {
                stream.close();
            }
        } catch (IOException ex) {
            // intentionally suppressed
        }
    }

    /**
     * Closes a Reader, ignoring any I/O exception. This should only be used in
     * finally blocks when it's necessary to avoid throwing an exception which
     * might mask a real exception.
     *
     * @param reader reader to close
     */
    public static void squelchReader(Reader reader)
    {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException ex) {
            // intentionally suppressed
        }
    }

    /**
     * Closes a Writer, ignoring any I/O exception. This should only be used in
     * finally blocks when it's necessary to avoid throwing an exception which
     * might mask a real exception. If you want to make sure that data has been
     * successfully flushed, do NOT use this anywhere else; use writer.close()
     * instead.
     *
     * @param writer writer to close
     */
    public static void squelchWriter(Writer writer)
    {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException ex) {
            // intentionally suppressed
        }
    }

    /**
     * Closes a Statement, ignoring any SQL exception. This should only be used
     * in finally blocks when it's necessary to avoid throwing an exception
     * which might mask a real exception.
     *
     * @param stmt stmt to close
     */
    public static void squelchStmt(Statement stmt)
    {
        try {
            if (stmt != null) {
                stmt.close();
            }
        } catch (SQLException ex) {
            // intentionally suppressed
        }
    }

    /**
     * Closes a Connection, ignoring any SQL exception. This should only be used
     * in finally blocks when it's necessary to avoid throwing an exception
     * which might mask a real exception.
     *
     * @param connection connection to close
     */
    public static void squelchConnection(Connection connection)
    {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException ex) {
            // intentionally suppressed
        }
    }

    /**
     * Trims trailing spaces from a string.
     *
     * @param s string to be trimmed
     *
     * @return trimmed string
     */
    public static String rtrim(String s)
    {
        int n = s.length() - 1;
        if (n >= 0) {
            if (s.charAt(n) != ' ') {
                return s;
            }
            while ((--n) >= 0) {
                if (s.charAt(n) != ' ') {
                    return s.substring(0, n + 1);
                }
            }
        }
        return "";
    }

    /**
     * Pads a string with spaces up to a given length.
     *
     * @param s string to be padded
     * @param len desired length
     *
     * @return padded string
     */
    public static String rpad(String s, int len)
    {
        if (s.length() >= len) {
            return s;
        }
        StringBuilder sb = new StringBuilder(s);
        while (sb.length() < len) {
            sb.append(' ');
        }
        return sb.toString();
    }

    /** Converts an iterable to a string. */
    public static <T> String toString(
        Iterable<T> iterable, String start, String sep, String end)
    {
        final StringBuilder buf = new StringBuilder();
        buf.append(start);
        for (Ord<T> ord : Ord.zip(iterable)) {
            if (ord.i > 0) {
                buf.append(sep);
            }
            buf.append(ord.e);
        }
        buf.append(end);
        return buf.toString();
    }

    /**
     * Converts a Java timezone to POSIX format, so that the boost C++ library
     * can instantiate timezone objects.
     *
     * <p><a
     * href="http://www.opengroup.org/onlinepubs/000095399/basedefs/xbd_chap08.html">POSIX
     * IEEE 1003.1</a> defines a format for timezone specifications.
     *
     * <p>The boost C++ library can read these specifications and instantiate <a
     * href="http://www.boost.org/doc/html/date_time/local_time.html#date_time.local_time.posix_time_zone">
     * posix_time_zone</a> objects from them. The purpose of this method,
     * therefore, is to allow the C++ code such as the fennel calculator to use
     * the same notion of timezone as Java code.
     *
     * <p>The format is as follows:
     *
     * <blockquote>"std offset dst [offset],start[/time],end[/time]"
     * </blockquote>
     *
     * where:
     *
     * <ul>
     * <li>'std' specifies the abbrev of the time zone.
     * <li>'offset' is the offset from UTC, and takes the form <code>
     * [+|-]hh[:mm[:ss]] {h=0-23, m/s=0-59}</code>
     * <li>'dst' specifies the abbrev of the time zone during daylight savings
     * time
     * <li>The second offset is how many hours changed during DST. Default=1
     * <li>'start' & 'end' are the dates when DST goes into (and out of) effect.
     * They can each be one of three forms:
     *
     * <ol>
     * <li>Mm.w.d {month=1-12, week=1-5 (5 is always last), day=0-6}
     * <li>Jn {n=1-365 Feb29 is never counted}
     * <li>n {n=0-365 Feb29 is counted in leap years}
     * </ol>
     * <li>'time' has the same format as 'offset', and defaults to 02:00:00
     *
     * <p>For example:
     *
     * <ul>
     * <li>"PST-8PDT01:00:00,M4.1.0/02:00:00,M10.1.0/02:00:00"; or more tersely
     * <li>"PST-8PDT,M4.1.0,M10.1.0"
     * </ul>
     *
     * <p>(Real format strings do not contain spaces; they are in the above
     * template only for readability.)
     *
     * <p>Boost apparently diverges from the POSIX standard in how it treats the
     * sign of timezone offsets. The POSIX standard states '<i>If preceded by a
     * '-', the timezone shall be east of the Prime Meridian; otherwise, it
     * shall be west</i>', yet boost requires the opposite. For instance, PST
     * has offset '-8' above. This method generates timezone strings consistent
     * with boost's expectations.
     *
     * @param tz Timezone
     * @param verbose Whether to include fields which can be omitted because
     * they have their default values
     *
     * @return Timezone in POSIX format (offset sign reversed, per boost's
     * idiosyncracies)
     */
    public static String toPosix(TimeZone tz, boolean verbose)
    {
        StringBuilder buf = new StringBuilder();
        buf.append(tz.getDisplayName(false, TimeZone.SHORT));
        appendPosixTime(buf, tz.getRawOffset());
        final int dstSavings = tz.getDSTSavings();
        if (dstSavings == 0) {
            return buf.toString();
        }
        buf.append(tz.getDisplayName(true, TimeZone.SHORT));
        if (verbose || (dstSavings != 3600000)) {
            // POSIX allows us to omit DST offset if it is 1:00:00
            appendPosixTime(buf, dstSavings);
        }
        String patternString =
            ".*,"
            + "startMode=([0-9]*),"
            + "startMonth=([0-9]*),"
            + "startDay=([-0-9]*),"
            + "startDayOfWeek=([0-9]*),"
            + "startTime=([0-9]*),"
            + "startTimeMode=([0-9]*),"
            + "endMode=([0-9]*),"
            + "endMonth=([0-9]*),"
            + "endDay=([-0-9]*),"
            + "endDayOfWeek=([0-9]*),"
            + "endTime=([0-9]*),"
            + "endTimeMode=([0-9]*).*";
        Pattern pattern = Pattern.compile(patternString);
        String tzString = tz.toString();
        Matcher matcher = pattern.matcher(tzString);
        if (!matcher.matches()) {
            throw new AssertionError(
                "tz.toString not of expected format: "
                + tzString);
        }
        int j = 0;
        int startMode = Integer.valueOf(matcher.group(++j));
        int startMonth = Integer.valueOf(matcher.group(++j));
        int startDay = Integer.valueOf(matcher.group(++j));
        int startDayOfWeek = Integer.valueOf(matcher.group(++j));
        int startTime = Integer.valueOf(matcher.group(++j));
        int startTimeMode = Integer.valueOf(matcher.group(++j));
        int endMode = Integer.valueOf(matcher.group(++j));
        int endMonth = Integer.valueOf(matcher.group(++j));
        int endDay = Integer.valueOf(matcher.group(++j));
        int endDayOfWeek = Integer.valueOf(matcher.group(++j));
        int endTime = Integer.valueOf(matcher.group(++j));
        int endTimeMode = Integer.valueOf(matcher.group(++j));
        appendPosixDaylightTransition(
            tz,
            buf,
            startMode,
            startDay,
            startMonth,
            startDayOfWeek,
            startTime,
            startTimeMode,
            verbose,
            false);
        appendPosixDaylightTransition(
            tz,
            buf,
            endMode,
            endDay,
            endMonth,
            endDayOfWeek,
            endTime,
            endTimeMode,
            verbose,
            true);
        return buf.toString();
    }

    /**
     * Writes a daylight savings time transition to a POSIX timezone
     * description.
     *
     * @param tz Timezone
     * @param buf Buffer to append to
     * @param mode Transition mode
     * @param day Day of transition
     * @param month Month of transition
     * @param dayOfWeek Day of week of transition
     * @param time Time of transition in millis
     * @param timeMode Mode of time transition
     * @param verbose Verbose
     * @param isEnd Whether this transition is leaving DST
     */
    private static void appendPosixDaylightTransition(
        TimeZone tz,
        StringBuilder buf,
        int mode,
        int day,
        int month,
        int dayOfWeek,
        int time,
        int timeMode,
        boolean verbose,
        boolean isEnd)
    {
        buf.append(',');
        int week = day;
        switch (mode) {
        case 1: // SimpleTimeZone.DOM_MODE
            throw Util.needToImplement(0);

        case 3: // SimpleTimeZone.DOW_GE_DOM_MODE

            // If the day is 1, 8, 15, 22, we can translate this to case 2.
            switch (day) {
            case 1:
                week = 1; // 1st week of month
                break;
            case 8:
                week = 2; // 2nd week of month
                break;
            case 15:
                week = 3; // 3rd week of month
                break;
            case 22:
                week = 4; // 4th week of month
                break;
            default:
                throw new AssertionError(
                    "POSIX timezone format cannot represent " + tz);
            }
            // fall through

        case 2: // SimpleTimeZone.DOW_IN_MONTH_MODE
            buf.append('M');
            buf.append(month + 1); // 1 <= m <= 12
            buf.append('.');
            if (week == -1) {
                // java represents 'last week' differently from POSIX
                week = 5;
            }
            buf.append(week); // 1 <= n <= 5, 5 means 'last'
            buf.append('.');
            buf.append(dayOfWeek - 1); // 0 <= d <= 6, 0=Sunday
            break;

        case 4: // SimpleTimeZone.DOW_LE_DOM_MODE
            throw Util.needToImplement(0);
        default:
            throw new AssertionError("unexpected value: " + mode);
        }
        switch (timeMode) {
        case 0: // SimpleTimeZone.WALL_TIME
            break;
        case 1: // SimpleTimeZone.STANDARD_TIME, e.g. Australia/Sydney
            if (isEnd) {
                time += tz.getDSTSavings();
            }
            break;
        case 2: // SimpleTimeZone.UTC_TIME, e.g. Europe/Paris
            time += tz.getRawOffset();
            if (isEnd) {
                time += tz.getDSTSavings();
            }
            break;
        }
        if (verbose || (time != 7200000)) {
            // POSIX allows us to omit the time if it is 2am (the default)
            buf.append('/');
            appendPosixTime(buf, time);
        }
    }

    /**
     * Given a time expressed in milliseconds, append the time formatted as
     * "hh[:mm[:ss]]".
     *
     * @param buf Buffer to append to
     * @param millis Milliseconds
     */
    private static void appendPosixTime(StringBuilder buf, int millis)
    {
        if (millis < 0) {
            buf.append('-');
            millis = -millis;
        }
        int hours = millis / 3600000;
        buf.append(hours);
        millis -= (hours * 3600000);
        if (millis == 0) {
            return;
        }
        buf.append(':');
        int minutes = millis / 60000;
        if (minutes < 10) {
            buf.append('0');
        }
        buf.append(minutes);
        millis -= (minutes * 60000);
        if (millis == 0) {
            return;
        }
        buf.append(':');
        int seconds = millis / 1000;
        if (seconds < 10) {
            buf.append('0');
        }
        buf.append(seconds);
    }

    /**
     * Parses a locale string.
     *
     * <p>The inverse operation of {@link java.util.Locale#toString()}.
     *
     * @param localeString Locale string, e.g. "en" or "en_US"
     * @return Java locale object
     */
    public static Locale parseLocale(String localeString) {
        String[] strings = localeString.split("_");
        switch (strings.length) {
        case 1:
            return new Locale(strings[0]);
        case 2:
            return new Locale(strings[0], strings[1]);
        case 3:
            return new Locale(strings[0], strings[1], strings[2]);
        default:
            throw newInternal(
                "bad locale string '" + localeString + "'");
        }
    }

    /**
     * Runs an external application.
     *
     * @param cmdarray command and arguments, see {@link ProcessBuilder}
     * @param logger if not null, command and exit status will be logged
     * @param appInput if not null, data will be copied to application's stdin
     * @param appOutput if not null, data will be captured from application's
     * stdout and stderr
     *
     * @return application process exit value
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public static int runApplication(
        String [] cmdarray,
        Logger logger,
        Reader appInput,
        Writer appOutput)
        throws IOException, InterruptedException
    {
        return runAppProcess(
            newAppProcess(cmdarray),
            logger,
            appInput,
            appOutput);
    }

    /**
     * Constructs a {@link ProcessBuilder} to run an external application.
     *
     * @param cmdarray command and arguments.
     * @return a ProcessBuilder.
     */
    public static ProcessBuilder newAppProcess(String [] cmdarray)
    {
        // Concatenate quoted words from cmdarray.
        // REVIEW mb 2/24/09 Why is this needed?
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < cmdarray.length; ++i) {
            if (i > 0) {
                buf.append(" ");
            }
            buf.append('"');
            buf.append(cmdarray[i]);
            buf.append('"');
        }
        String fullcmd = buf.toString();
        buf.setLength(0);
        return new ProcessBuilder(cmdarray);
    }


    /**
     * Runs an external application process.
     *
     * @param pb {@link ProcessBuilder} for the application; might be returned by {@link #newAppProcess}.
     * @param logger if not null, command and exit status will be logged here
     * @param appInput if not null, data will be copied to application's stdin
     * @param appOutput if not null, data will be captured from application's
     * stdout and stderr
     *
     * @return application process exit value
     * @throws IOException
     * @throws InterruptedException
     */
    public static int runAppProcess(
        ProcessBuilder pb,
        Logger logger,
        Reader appInput,
        Writer appOutput)
        throws IOException, InterruptedException
    {
        pb.redirectErrorStream(true);
        if (logger != null) {
            logger.info("start process: " + pb.command());
        }
        Process p = pb.start();

        // Setup the input/output streams to the subprocess.
        // The buffering here is arbitrary. Javadocs strongly encourage
        // buffering, but the size needed is very dependent on the
        // specific application being run, the size of the input
        // provided by the caller, and the amount of output expected.
        // Since this method is currently used only by unit tests,
        // large-ish fixed buffer sizes have been chosen. If this
        // method becomes used for something in production, it might
        // be better to have the caller provide them as arguments.
        if (appInput != null) {
            OutputStream out =
                new BufferedOutputStream(
                    p.getOutputStream(),
                    100 * 1024);
            int c;
            while ((c = appInput.read()) != -1) {
                out.write(c);
            }
            out.flush();
        }
        if (appOutput != null) {
            InputStream in =
                new BufferedInputStream(
                    p.getInputStream(),
                    100 * 1024);
            int c;
            while ((c = in.read()) != -1) {
                appOutput.write(c);
            }
            appOutput.flush();
            in.close();
        }
        p.waitFor();

        int status = p.exitValue();
        if (logger != null) {
            logger.info("exit status=" + status + " from " + pb.command());
        }
        return status;
    }

    /**
     * Converts a list whose members are automatically down-cast to a given
     * type.
     *
     * <p>If a member of the backing list is not an instanceof <code>E</code>,
     * the accessing method (such as {@link List#get}) will throw a {@link
     * ClassCastException}.
     *
     * <p>All modifications are automatically written to the backing list. Not
     * synchronized.
     *
     * @param list Backing list.
     * @param clazz Class to cast to.
     *
     * @return A list whose members are of the desired type.
     */
    public static <E> List<E> cast(List<? super E> list, Class<E> clazz)
    {
        return new CastingList<E>(list, clazz);
    }

    /**
     * Converts a iterator whose members are automatically down-cast to a given
     * type.
     *
     * <p>If a member of the backing iterator is not an instanceof <code>
     * E</code>, {@link Iterator#next()}) will throw a {@link
     * ClassCastException}.
     *
     * <p>All modifications are automatically written to the backing iterator.
     * Not synchronized.
     *
     * @param iter Backing iterator.
     * @param clazz Class to cast to.
     *
     * @return An iterator whose members are of the desired type.
     */
    public static <E> Iterator<E> cast(
        final Iterator<?> iter,
        final Class<E> clazz)
    {
        return new Iterator<E>() {
            public boolean hasNext()
            {
                return iter.hasNext();
            }

            public E next()
            {
                return clazz.cast(iter.next());
            }

            public void remove()
            {
                iter.remove();
            }
        };
    }

    /**
     * Converts an {@link Iterable} whose members are automatically down-cast to
     * a given type.
     *
     * <p>All modifications are automatically written to the backing iterator.
     * Not synchronized.
     *
     * @param iterable Backing iterable
     * @param clazz Class to cast to
     *
     * @return An iterable whose members are of the desired type.
     */
    public static <E> Iterable<E> cast(
        final Iterable<? super E> iterable,
        final Class<E> clazz)
    {
        return new Iterable<E>() {
            public Iterator<E> iterator()
            {
                return cast(iterable.iterator(), clazz);
            }
        };
    }

    /**
     * Makes a collection of untyped elements appear as a list of strictly typed
     * elements, by filtering out those which are not of the correct type.
     *
     * <p>The returned object is an {@link Iterable},
     * which makes it ideal for use with the 'foreach' construct. For example,
     *
     * <blockquote><code>List&lt;Number&gt; numbers = Arrays.asList(1, 2, 3.14,
     * 4, null, 6E23);<br/>
     * for (int myInt : filter(numbers, Integer.class)) {<br/>
     * &nbsp;&nbsp;&nbsp;&nbsp;print(i);<br/>
     * }</code></blockquote>
     *
     * will print 1, 2, 4.
     *
     * @param iterable Iterable
     * @param includeFilter Class whose instances to include
     */
    public static <E> Iterable<E> filter(
        final Iterable<? extends Object> iterable,
        final Class<E> includeFilter)
    {
        return new Iterable<E>() {
            public Iterator<E> iterator()
            {
                return new Filterator<E>(iterable.iterator(), includeFilter);
            }
        };
    }

    public static <E> Collection<E> filter(
        final Collection<?> collection,
        final Class<E> includeFilter)
    {
        return new AbstractCollection<E>() {
            private int size = -1;

            public Iterator<E> iterator()
            {
                return new Filterator<E>(collection.iterator(), includeFilter);
            }

            public int size()
            {
                if (size == -1) {
                    // Compute size.  This is expensive, but the value
                    // collection.size() is not correct since we're
                    // filtering values.  (Some java.util algorithms
                    // call next() on the result of iterator() size() times.)
                    int s = 0;
                    Iterator<E> iter = iterator();
                    while (iter.hasNext()) {
                        iter.next();
                        s++;
                    }
                    size = s;
                }

                return size;
            }
        };
    }

    /**
     * Returns a subset of a list containing only elements of a given type.
     *
     * <p>Modifications to the list are NOT written back to the source list.
     *
     * @param list List of objects
     * @param includeFilter Class to filter for
     *
     * @return List of objects of given class (or a subtype)
     */
    public static <E> List<E> filter(
        final List<?> list,
        final Class<E> includeFilter)
    {
        List<E> result = new ArrayList<E>();
        for (Object o : list) {
            if (includeFilter.isInstance(o)) {
                result.add(includeFilter.cast(o));
            }
        }
        return result;
    }

    /**
     * Converts a {@link Properties} object to a <code>{@link Map}&lt;String,
     * String&gt;</code>.
     *
     * <p>This is necessary because {@link Properties} is a dinosaur class. It
     * ought to extend <code>Map&lt;String,String&gt;</code>, but instead
     * extends <code>{@link Hashtable}&lt;Object,Object&gt;</code>.
     *
     * <p>Typical usage, to iterate over a {@link Properties}:
     *
     * <blockquote>
     * <code>
     * Properties properties;<br/>
     * for (Map.Entry&lt;String, String&gt; entry =
     * Util.toMap(properties).entrySet()) {<br/>
     *   println("key=" + entry.getKey() + ", value=" + entry.getValue());<br/>
     * }
     * </code>
     * </blockquote>
     */
    public static Map<String, String> toMap(
        final Properties properties)
    {
        return (Map) properties;
    }

    /**
     * Returns a hashmap with given contents.
     *
     * <p>Use this method in initializers. Type parameters are inferred from
     * context, and the contents are initialized declaratively. For example,
     *
     * <blockquote><code>Map&lt;String, Integer&gt; population =<br/>
     * &nbsp;&nbsp;Olap4jUtil.mapOf(<br/>
     * &nbsp;&nbsp;&nbsp;&nbsp;"UK", 65000000,<br/>
     * &nbsp;&nbsp;&nbsp;&nbsp;"USA", 300000000);</code></blockquote>
     *
     * @param key First key
     * @param value First value
     * @param keyValues Second and sequent key/value pairs
     * @param <K> Key type
     * @param <V> Value type
     * @return Map with given contents
     */
    public static <K, V> Map<K, V> mapOf(K key, V value, Object... keyValues)
    {
        final Map<K, V> map = new LinkedHashMap<K, V>(1 + keyValues.length);
        map.put(key, value);
        for (int i = 0; i < keyValues.length;) {
            //noinspection unchecked
            map.put((K) keyValues[i++], (V) keyValues[i++]);
        }
        return map;
    }

    /**
     * Returns an exception indicating that we didn't expect to find this
     * enumeration here.
     *
     * @param value Enumeration value which was not expected
     *
     * @return an error, to be thrown
     */
    public static <E extends Enum<E>> Error unexpected(E value)
    {
        return new AssertionError(
            "Was not expecting value '" + value
            + "' for enumeration '" + value.getDeclaringClass().getName()
            + "' in this context");
    }

    /**
     * Creates a map of the values of an enumeration by name.
     *
     * @param clazz Enumeration class
     *
     * @return map of values
     */
    public static <T extends Enum<T>> Map<String, T> enumConstants(
        Class<T> clazz)
    {
        final T [] ts = clazz.getEnumConstants();
        if (ts == null) {
            // not an enum type
            return null;
        }
        Map<String, T> map = new HashMap<String, T>(ts.length * 2);
        for (T t : ts) {
            map.put(t.name(), t);
        }
        return Collections.unmodifiableMap(map);
    }

    /**
     * Returns the value of an enumeration with a particular name.
     *
     * <p>Similar to {@link Enum#valueOf(Class, String)}, but returns {@code
     * null} rather than throwing {@link IllegalArgumentException}.
     *
     * @param clazz Enum class
     * @param name Name of enum constant
     * @param <T> Enum class type
     *
     * @return Enum constant or null
     */
    @SuppressWarnings({ "unchecked" })
    public static synchronized <T extends Enum<T>> T enumVal(
        Class<T> clazz,
        String name)
    {
        Map<String, T> mapNameToEnum =
            (Map<String, T>) mapClazzToMapNameToEnum.get(clazz);
        if (mapNameToEnum == null) {
            mapNameToEnum = enumConstants(clazz);
            mapClazzToMapNameToEnum.put(clazz, mapNameToEnum);
        }
        return mapNameToEnum.get(name);
    }

    /**
     * Returns an iterable over the bits in a bitmap that are set to '1'.
     *
     * <p>This allows you to iterate over a bit set using a 'foreach' construct.
     * For instance:
     *
     * <blockquote><code>
     * BitSet bitSet;<br/>
     * for (int i : Util.toIter(bitSet)) {<br/>
     * &nbsp;&nbsp;print(i);<br/>
     * }<br/></code></blockquote>
     *
     * @param bitSet Bit set
     * @return Iterable
     */
    public static Iterable<Integer> toIter(final BitSet bitSet)
    {
        return new Iterable<Integer>()
        {
            public Iterator<Integer> iterator()
            {
                return new Iterator<Integer>()
                {
                    int i = bitSet.nextSetBit(0);

                    public boolean hasNext()
                    {
                        return i >= 0;
                    }

                    public Integer next()
                    {
                        int prev = i;
                        i = bitSet.nextSetBit(i + 1);
                        return prev;
                    }

                    public void remove()
                    {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    /**
     * Converts a bitset to a list.
     *
     * <p>The list is mutable, and future changes to the list do not affect the
     * contents of the bit set.
     *
     * @param bitSet Bit set
     * @return List of set bits
     */
    public static IntList toList(final BitSet bitSet)
    {
        final IntList list = new IntList();
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1))
        {
            list.add(i);
        }
        return list;
    }

    /**
     * Converts a bitset to an array.
     *
     * @param bitSet Bit set
     * @return List of set bits
     */
    public static Integer[] toArray(final BitSet bitSet)
    {
        final List<Integer> list = toList(bitSet);
        return list.toArray(new Integer[list.size()]);
    }

    /**
     * Creates a bitset with given bits set.
     *
     * <p>For example, {@code bitSetOf(0, 3)} returns a bit set with bits {0, 3}
     * set.
     *
     * @param bits Array of bits to set
     * @return Bit set
     */
    public static BitSet bitSetOf(int... bits)
    {
        final BitSet bitSet = new BitSet();
        for (int bit : bits) {
            bitSet.set(bit);
        }
        return bitSet;
    }

    /**
     * Creates a bitset with given bits set.
     *
     * <p>For example, {@code bitSetOf(new Integer[] {0, 3})} returns a bit set
     * with bits {0, 3} set.
     *
     * @param bits Array of bits to set
     * @return Bit set
     */
    public static BitSet bitSetOf(Integer[] bits)
    {
        final BitSet bitSet = new BitSet();
        for (int bit : bits) {
            bitSet.set(bit);
        }
        return bitSet;
    }

    /**
     * Creates a bitset with given bits set.
     *
     * <p>For example, {@code bitSetOf(Arrays.asList(0, 3)) } returns a bit set
     * with bits {0, 3} set.
     *
     * @param bits Collection of bits to set
     * @return Bit set
     */
    public static BitSet bitSetOf(Collection<Integer> bits)
    {
        final BitSet bitSet = new BitSet();
        for (int bit : bits) {
            bitSet.set(bit);
        }
        return bitSet;
    }

    /**
     * Creates a bitset with bits from {@code fromIndex} (inclusive) to
     * specified {@code toIndex} (exclusive) set to {@code true}.
     *
     * <p>For example, {@code bitSetBetween(0, 3)} returns a bit set with bits
     * {0, 1, 2} set.
     *
     * @param fromIndex Index of the first bit to be set.
     * @param toIndex   Index after the last bit to be set.
     * @return Bit set
     */
    public static BitSet bitSetBetween(int fromIndex, int toIndex)
    {
        final BitSet bitSet = new BitSet();
        if (toIndex > fromIndex) {
            // Avoid http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6222207
            // "BitSet internal invariants may be violated"
            bitSet.set(fromIndex, toIndex);
        }
        return bitSet;
    }

    /**
     * Returns a string of N spaces.
     */
    public static String spaces(int i)
    {
        return SpaceList.INSTANCE.get(i);
    }

    /** Creates a list that returns every {@code n}th element of a list,
     * starting at element {@code k}.
     *
     * <p>It is OK if the list is empty or its size is not a multiple of
     * {@code n}.</p>
     *
     * <p>For instance, {@code quotientList(list, 2, 0)} returns the even
     * elements of a list, and {@code quotientList(list, 2, 1)} returns the odd
     * elements. Those lists are the same length only if list has even size.</p>
     */
    public static <E> List<E> quotientList(
        final List<E> list, final int n, final int k)
    {
        if (n <= 0 || k < 0 || k >= n) {
            throw new IllegalArgumentException(
                "n must be positive; k must be between 0 and n - 1");
        }
        final int size = (list.size() + n - k - 1) / n;
        return new AbstractList<E>() {
            public E get(int index) {
                return list.get(index * n + k);
            }

            public int size() {
                return size;
            }
        };
    }

    /** Returns whether a bit set contains every bit of another bit set. */
    public static boolean isSupersetOf(BitSet set0, BitSet set1) {
        if (set1.isEmpty()) {
            return true;
        }
        if (!set0.intersects(set1)) {
            return false;
        }
        for (Integer integer : toIter(set1)) {
            if (!set0.get(integer)) {
                return false;
            }
        }
        return true;
    }

    /** Returns the last element of a list. Throws if the list is empty or
     * null. */
    public static <T> T last(List<T> list) {
        return list.get(list.size() - 1);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Exception used to interrupt a tree walk of any kind.
     */
    public static class FoundOne
        extends RuntimeException
    {
        private final Object node;

        public FoundOne(Object node)
        {
            this.node = node;
        }

        public Object getNode()
        {
            return node;
        }
    }

    // Experimental support for functional programming follows...

    /**
     * Function of arity 0.
     *
     * @param <R> Result type.
     */
    interface Function0<R> {
        /**
         * Applies the function.
         *
         * @return Result value.
         */
        R apply();
    }

    /**
     * Function of arity 1.
     *
     * @param <R> Result type.
     * @param <T0> Type of parameter 0.
     */
    public interface Function1<R, T0> {
        /**
         * Applies the function.
         *
         * @param p0 Parameter 0.
         * @return Result value.
         */
        R apply(T0 p0);
    }

    /**
     * Function of arity 2.
     *
     * @param <R> Result type.
     * @param <T0> Type of parameter 0.
     * @param <T1> Type of parameter 1.
     */
    public interface Function2<R, T0, T1> {
        /**
         * Applies the function.
         *
         * @param p0 Parameter 0.
         * @param p1 Parameter 1.
         * @return Result value.
         */
        R apply(T0 p0, T1 p1);
    }

    @SuppressWarnings({"unchecked"})
    public static class Functions
    {
        /**
         * Returns a function of arity 0 that does nothing.
         *
         * @param <R> Return type
         * @return Function that does nothing.
         */
        public static <R> Function0<R> ignore0() {
            return Ignore.INSTANCE;
        }

        /**
         * Returns a function of arity 1 that does nothing.
         *
         * @param <R> Return type
         * @param <T0> Type of parameter 0
         * @return Function that does nothing.
         */
        public static <R, T0> Function1<R, T0> ignore1() {
            return Ignore.INSTANCE;
        }

        /**
         * Returns a function of arity 2 that does nothing.
         *
         * @param <R> Return type
         * @param <T0> Type of parameter 0
         * @param <T1> Type of parameter 1
         * @return Function that does nothing.
         */
        public static <R, T0, T1> Function2<R, T0, T1> ignore2() {
            return Ignore.INSTANCE;
        }
    }

    private static final class Ignore<R, T0, T1>
        implements
        Function0<R>,
        Function1<R, T0>,
        Function2<R, T0, T1>
    {
        public R apply()
        {
            return null;
        }

        public R apply(T0 p0)
        {
            return null;
        }

        public R apply(T0 p0, T1 p1)
        {
            return null;
        }

        static final Ignore INSTANCE = new Ignore();
    }

    private static class SpaceList extends CopyOnWriteArrayList<String> {
        private static final List<String> INSTANCE = new SpaceList();

        public SpaceList() {
            populate("               ");
        }

        @Override
        public String get(int index) {
            for (;;) {
                try {
                    return super.get(index);
                } catch (IndexOutOfBoundsException e) {
                    if (index < 0) {
                        throw e;
                    }
                    String s = get(size() - 1);
                    populate(s + s);
                }
            }
        }

        /** Populates this list with all prefix strings of a given string. All
         * of the prefix strings share the same backing array of chars. */
        private void populate(final String s) {
            // If another thread sees list as momentarily empty, it will throw
            // and retry.
            clear();
            addAll(
                new AbstractList<String>() {
                    @Override
                    public String get(int index) {
                        return s.substring(0, index);
                    }

                    @Override
                    public int size() {
                        return s.length();
                    }
                });
        }
    }
}

// End Util.java
