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
package org.eigenbase.oj.util;

import java.util.*;

import openjava.mop.*;

import openjava.ptree.*;
import openjava.ptree.util.*;

import org.eigenbase.oj.*;
import org.eigenbase.reltype.*;
import org.eigenbase.runtime.RestartableIterator;
import org.eigenbase.util.*;


/**
 * Static utilities for manipulating OpenJava expressions.
 */
public abstract class OJUtil
{
    //~ Static fields/initializers ---------------------------------------------

    static {
        OJSystem.initConstants();
    }

    public static final OJClass clazzVoid =
        OJClass.forClass(
            void.class);

    public static final OJClass clazzObject =
        OJClass.forClass(
            java.lang.Object.class);

    public static final OJClass clazzObjectArray =
        OJClass.arrayOf(
            clazzObject);

    public static final OJClass clazzCollection =
        OJClass.forClass(
            java.util.Collection.class);

    public static final OJClass clazzMap =
        OJClass.forClass(
            java.util.Map.class);

    public static final OJClass clazzMapEntry =
        OJClass.forClass(
            java.util.Map.Entry.class);

    public static final OJClass clazzHashtable =
        OJClass.forClass(
            java.util.Hashtable.class);

    public static final OJClass clazzEnumeration =
        OJClass.forClass(
            java.util.Enumeration.class);

    public static final OJClass clazzIterator =
        OJClass.forClass(
            java.util.Iterator.class);

    public static final OJClass clazzIterable =
        OJClass.forClass(
            org.eigenbase.runtime.Iterable.class);

    public static final OJClass clazzTupleIter =
        OJClass.forClass(
            org.eigenbase.runtime.TupleIter.class);

    public static final OJClass clazzVector =
        OJClass.forClass(
            java.util.Vector.class);

    public static final OJClass clazzComparable =
        OJClass.forClass(
            java.lang.Comparable.class);

    public static final OJClass clazzComparator =
        OJClass.forClass(
            java.util.Comparator.class);

    public static final OJClass clazzResultSet =
        OJClass.forClass(
            java.sql.ResultSet.class);

    public static final OJClass clazzClass =
        OJClass.forClass(
            java.lang.Class.class);

    public static final OJClass clazzString =
        OJClass.forClass(
            java.lang.String.class);

    public static final OJClass clazzSet =
        OJClass.forClass(
            java.util.Set.class);

    public static final OJClass clazzSQLException =
        OJClass.forClass(
            java.sql.SQLException.class);

    public static final OJClass clazzEntry =
        OJClass.forClass(
            java.util.Map.Entry.class);

    public static final OJClass [] emptyArrayOfOJClass = new OJClass[] {};

    public static final ModifierList modFinal =
        new ModifierList(
            ModifierList.FINAL);

    public static final OJClass clazzInteger =
        OJClass.forClass(
            java.lang.Integer.class);

    public static final TypeName tnInt =
        TypeName.forOJClass(
            OJSystem.INT);

    public static final OJClass clazzList =
        OJClass.forClass(
            java.util.List.class);

    public static final TypeName tnList = TypeName.forOJClass(clazzList);

    public static final OJClass clazzArrays =
        OJClass.forClass(
            java.util.Arrays.class);

    public static final TypeName tnArrays = TypeName.forOJClass(clazzArrays);

    public static final TypeName tnObject =
        TypeName.forOJClass(
            OJSystem.OBJECT);

    public static final OJClass clazzRestartableIterator =
        OJClass.forClass(
            RestartableIterator.class);

    public static TypeName tnRestartableIterator =
        TypeName.forOJClass(
            clazzRestartableIterator);

    /**
     * Each thread's enclosing {@link OJClass}. Synthetic classes are declared
     * as inner classes of this.
     */
    public static final ThreadLocal<OJClass> threadDeclarers =
        new ThreadLocal<OJClass>();

    private static final ThreadLocal<OJTypeFactory> threadTypeFactories =
        new ThreadLocal<OJTypeFactory>();

    //~ Methods ----------------------------------------------------------------

    public static void setThreadTypeFactory(OJTypeFactory typeFactory)
    {
        threadTypeFactories.set(typeFactory);
    }

    public static OJTypeFactory threadTypeFactory()
    {
        return threadTypeFactories.get();
    }

    public static RelDataType ojToType(
        RelDataTypeFactory typeFactory,
        OJClass ojClass)
    {
        if (ojClass == null) {
            return null;
        }
        return ((OJTypeFactory) typeFactory).toType(ojClass);
    }

    /**
     * Converts a {@link RelDataType} to a {@link TypeName}.
     *
     * @pre threadDeclarers.get() != null
     */
    public static TypeName toTypeName(
        RelDataType rowType,
        RelDataTypeFactory typeFactory)
    {
        return TypeName.forOJClass(typeToOJClass(rowType, typeFactory));
    }

    public static OJClass typeToOJClass(
        OJClass declarer,
        RelDataType rowType,
        RelDataTypeFactory typeFactory)
    {
        OJTypeFactory ojTypeFactory = (OJTypeFactory) typeFactory;
        return ojTypeFactory.toOJClass(declarer, rowType);
    }

    /**
     * Converts a {@link RelDataType} to a {@link OJClass}.
     *
     * @pre threadDeclarers.get() != null
     */
    public static OJClass typeToOJClass(
        RelDataType rowType,
        RelDataTypeFactory typeFactory)
    {
        OJClass declarer = threadDeclarers.get();
        if (declarer == null) {
            assert (false) : "threadDeclarers.get() != null";
        }
        return typeToOJClass(declarer, rowType, typeFactory);
    }

    public static Object literalValue(Literal literal)
    {
        String value = literal.toString();
        switch (literal.getLiteralType()) {
        case Literal.BOOLEAN:
            return value.equals("true") ? Boolean.TRUE : Boolean.FALSE;
        case Literal.INTEGER:
            return new Integer(Integer.parseInt(value));
        case Literal.LONG:
            value = value.substring(0, value.length() - 1); // remove 'l'
            return new Integer(Integer.parseInt(value));
        case Literal.FLOAT:
            value = value.substring(0, value.length() - 1); // remove 'f'
            return new Double(Double.parseDouble(value));
        case Literal.DOUBLE:
            value = value.substring(0, value.length() - 1); // remove 'd'
            return new Double(Double.parseDouble(value));
        case Literal.CHARACTER:
            return value.substring(1, 2); // 'x' --> x
        case Literal.STRING:
            return Util.stripDoubleQuotes(value); // "foo" --> foo
        case Literal.NULL:
            return null;
        default:
            throw Util.newInternal(
                "unknown literal type "
                + literal.getLiteralType());
        }
    }

    public static TypeName typeNameForClass(Class clazz)
    {
        return TypeName.forOJClass(OJClass.forClass(clazz));
    }

    public static String replaceDotWithDollar(String base, int i)
    {
        return base.substring(0, i) + '$' + base.substring(i + 1);
    }

    /**
     * Guesses the row-type of an expression which has type <code>clazz</code>.
     * For example, {@link String}[] --> {@link String}; {@link
     * java.util.Iterator} --> {@link Object}.
     */
    public static final OJClass guessRowType(OJClass clazz)
    {
        if (clazz.isArray()) {
            return clazz.getComponentType();
        } else if (
            clazzIterator.isAssignableFrom(clazz)
            || clazzEnumeration.isAssignableFrom(clazz)
            || clazzVector.isAssignableFrom(clazz)
            || clazzCollection.isAssignableFrom(clazz)
            || clazzResultSet.isAssignableFrom(clazz))
        {
            return clazzObject;
        } else if (
            clazzHashtable.isAssignableFrom(clazz)
            || clazzMap.isAssignableFrom(clazz))
        {
            return clazzEntry;
        } else {
            return null;
        }
    }

    /**
     * Sets a {@link ParseTreeVisitor} going on a parse tree, and returns the
     * result.
     */
    public static ParseTree go(ParseTreeVisitor visitor, ParseTree p)
    {
        ObjectList holder = new ObjectList(p);
        try {
            p.accept(visitor);
        } catch (StopIterationException e) {
            // ignore the exception -- it was just a way to abort the traversal
        } catch (ParseTreeException e) {
            throw Util.newInternal(
                e,
                "while visiting expression " + p);
        }
        return (ParseTree) holder.get(0);
    }

    /**
     * Sets a {@link ParseTreeVisitor} going on a given non-relational
     * expression, and returns the result.
     */
    public static Expression go(ParseTreeVisitor visitor, Expression p)
    {
        return (Expression) go(visitor, (ParseTree) p);
    }

    /**
     * Ensures that an expression is an object. Primitive expressions are
     * wrapped in a constructor (for example, the <code>int</code> expression
     * <code>2 + 3</code> becomes <code>new Integer(2 + 3)</code>);
     * non-primitive expressions are unchanged.
     *
     * @param exp an expression
     * @param clazz <code>exp</code>'s type
     *
     * @return a call to the constructor of a wrapper class if <code>exp</code>
     * is primitive, <code>exp</code> otherwise
     */
    public static Expression box(OJClass clazz, Expression exp)
    {
        if (clazz.isPrimitive()) {
            return new AllocationExpression(
                clazz.primitiveWrapper(),
                new ExpressionList(exp));
        } else {
            return exp;
        }
    }

    /**
     * Gets the root environment, which is always a {@link GlobalEnvironment}.
     *
     * @param env environment to start search from
     */
    public static GlobalEnvironment getGlobalEnvironment(Environment env)
    {
        for (;;) {
            Environment parent = env.getParent();
            if (parent == null) {
                return (GlobalEnvironment) env;
            } else {
                env = parent;
            }
        }
    }

    /**
     * If env is a {@link ClassEnvironment} for declarerName, records new inner
     * class innerName; otherwise, delegates up the environment hierarchy.
     *
     * @param env environment to start search from
     * @param declarerName fully-qualified name of enclosing class
     * @param innerName simple name of inner class
     */
    public static void recordMemberClass(
        Environment env,
        String declarerName,
        String innerName)
    {
        do {
            if (env instanceof ClassEnvironment) {
                if (declarerName.equals(env.currentClassName())) {
                    ((ClassEnvironment) env).recordMemberClass(innerName);
                    return;
                }
            } else {
                env = env.getParent();
            }
        } while (env != null);
    }

    public static OJClass getType(Environment env, Expression exp)
    {
        try {
            OJClass clazz = exp.getType(env);
            assert (clazz != null);
            return clazz;
        } catch (Exception e) {
            throw Util.newInternal(e, "while deriving type for '" + exp + "'");
        }
    }

    /**
     * Counts the number of nodes in a parse tree.
     *
     * @param parseTree tree to walk
     *
     * @return count of nodes
     */
    public static int countParseTreeNodes(ParseTree parseTree)
    {
        int n = 1;
        if (parseTree instanceof NonLeaf) {
            Object [] contents = ((NonLeaf) parseTree).getContents();
            for (Object obj : contents) {
                if (obj instanceof ParseTree) {
                    n += countParseTreeNodes((ParseTree) obj);
                } else {
                    n += 1;
                }
            }
        } else if (parseTree instanceof openjava.ptree.List) {
            Enumeration e = ((openjava.ptree.List) parseTree).elements();
            while (e.hasMoreElements()) {
                Object obj = (Object) e.nextElement();
                if (obj instanceof ParseTree) {
                    n += countParseTreeNodes((ParseTree) obj);
                } else {
                    n += 1;
                }
            }
        }
        return n;
    }

    public static ExpressionList expressionList(
        Expression... expressions)
    {
        ExpressionList list = new ExpressionList();
        for (Expression expression : expressions) {
            list.add(expression);
        }
        return list;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * A <code>StopIterationException</code> is a way to tell a {@link
     * openjava.ptree.util.ParseTreeVisitor} to halt traversal of the tree, but
     * is not regarded as an error.
     */
    public static class StopIterationException
        extends ParseTreeException
    {
        public StopIterationException()
        {
        }
    }
}

// End OJUtil.java
