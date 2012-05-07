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
package org.eigenbase.oj.util;

import java.util.Hashtable;
import java.util.Vector;
import java.util.logging.*;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.runtime.*;
import org.eigenbase.trace.*;
import org.eigenbase.util.*;


/**
 * An <code>OJClassMap</code> is ...
 *
 * @author jhyde
 * @version $Id$
 * @since Jun 5, 2002
 */
public class OJClassMap
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer = EigenbaseTrace.getClassMapTracer();

    //~ Instance fields --------------------------------------------------------

    // REVIEW jvs 21-Jun-2003:  change to HashMap?  This shouldn't be accessed
    // from unsynchronized code.  Same for Vector later on.
    /**
     * Map a {@link HashableArray} (which is just a wrapper around an array of
     * classes and names to the {@link OJSyntheticClass} which implements that
     * array of types.
     */
    private Hashtable<String, OJSyntheticClass> mapKey2SyntheticClass =
        new Hashtable<String, OJSyntheticClass>();

    /**
     * Class from which synthetic classes should be subclassed.
     */
    private Class syntheticSuperClass;

    // NOTE jvs 29-Sept-2004:  I made the id generator non-static because
    // for inner classes there's no need to worry about conflicts between
    // multiple threads.  Previously, this variable was static, but without
    // proper synchronization.

    /**
     * Sequence generator for generated class names.
     */
    private int id;

    /**
     * Do generated classes need value constructors? This can blow the Java
     * virtual machine's limit on number of parameters per method, and in
     * Farrago we don't actually use anything but the default constructor.
     */
    private final boolean defineValueConstructors;

    //~ Constructors -----------------------------------------------------------

    public OJClassMap(Class<SyntheticObject> syntheticSuperClass)
    {
        this(syntheticSuperClass, true);
    }

    public OJClassMap(
        Class syntheticSuperClass,
        boolean defineValueConstructors)
    {
        this.syntheticSuperClass = syntheticSuperClass;
        this.defineValueConstructors = defineValueConstructors;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Creates a <code>OJSyntheticClass</code> with named fields. We don't check
     * whether there is an equivalent class -- all classes with named fields are
     * different.
     */
    public OJClass createProject(
        OJClass declarer,
        OJClass [] classes,
        String [] fieldNames)
    {
        boolean isJoin = false;
        return create(declarer, classes, fieldNames, isJoin);
    }

    private OJClass create(
        OJClass declarer,
        OJClass [] classes,
        String [] fieldNames,
        boolean isJoin)
    {
        if (fieldNames == null) {
            fieldNames = new String[classes.length];
        }
        assert classes.length == fieldNames.length
            : "OJSyntheticClass.create: "
            + "mismatch between classes and field names";
        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldNames[i] == null) {
                fieldNames[i] = OJSyntheticClass.makeField(i);
            }
        }

        // make description
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (int i = 0; i < classes.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(fieldNames[i]);
            sb.append(": ");
            sb.append(classes[i].toString().replace('$', '.'));

            if (isJoin) {
                assert !OJSyntheticClass.isJoinClass(classes[i])
                    : "join classes cannot contain join classes";
            }
        }
        sb.append("}");
        String description = sb.toString();

        // is there already an equivalent OJSyntheticClass?
        OJSyntheticClass clazz = mapKey2SyntheticClass.get(description);
        if (clazz == null) {
            Environment env = declarer.getEnvironment();
            String className =
                (isJoin ? OJSyntheticClass.JOIN_CLASS_PREFIX
                    : OJSyntheticClass.PROJECT_CLASS_PREFIX)
                + Integer.toHexString(id++);
            ClassDeclaration decl =
                makeDeclaration(
                    className,
                    classes,
                    fieldNames);
            clazz =
                new OJSyntheticClass(
                    env,
                    declarer,
                    classes,
                    fieldNames,
                    decl,
                    description,
                    defineValueConstructors);

            // register ourself
            try {
                declarer.addClass(clazz);
            } catch (openjava.mop.CannotAlterException e) {
                throw Util.newInternal(
                    e,
                    "holder class must be OJClassSourceCode");
            }
            OJUtil.recordMemberClass(
                env,
                declarer.getName(),
                decl.getName());
            OJUtil.getGlobalEnvironment(env).record(
                clazz.getName(),
                clazz);

            tracer.fine(
                "created OJSyntheticClass: name=" + clazz.getName()
                + ", description=" + description);
            mapKey2SyntheticClass.put(description, clazz);
        }
        return clazz;
    }

    private ClassDeclaration makeDeclaration(
        String className,
        OJClass [] classes,
        String [] fieldNames)
    {
        MemberDeclarationList fieldList = new MemberDeclarationList();
        for (int i = 0; i < classes.length; i++) {
            FieldDeclaration field =
                new FieldDeclaration(
                    new ModifierList(ModifierList.PUBLIC),
                    TypeName.forOJClass(classes[i]),
                    fieldNames[i],
                    null);
            fieldList.add(field);
        }
        ModifierList modifierList =
            new ModifierList(
                ModifierList.PUBLIC | ModifierList.STATIC);
        ClassDeclaration classDecl =
            new ClassDeclaration(
                modifierList,
                className,
                new TypeName[] { OJUtil.typeNameForClass(syntheticSuperClass) },
                null,
                fieldList);
        return classDecl;
    }

    /**
     * Creates a <code>OJSyntheticClass</code>, or if there is already one with
     * the same number and type of fields, returns that.
     */
    public OJClass createJoin(OJClass declarer, OJClass [] classes)
    {
        if (classes.length == 1) {
            // don't make a singleton OJSyntheticClass, just return the atomic
            // class
            return classes[0];
        }
        boolean isJoin = true;
        return create(declarer, classes, null, isJoin);
    }

    /**
     * <p>Makes the type of a join. There are two kinds of classes. A <dfn>real
     * class</dfn> exists in the developer's environment. A <dfn>synthetic
     * class</dfn> is constructed by the system to describe the intermediate and
     * final results of a query. We are at liberty to modify synthetic
     * classes.</p>
     *
     * <p>If we join class C1 to class C2, the result is a synthetic class:
     *
     * <pre>
     * class SC1 {
     *     C1 $f0;
     *     C2 $f1;
     * }
     * </pre>
     *
     * Suppose that we now join class C3 to this; you would expect the result
     * type to be a new synthetic class:
     *
     * <pre>
     * class SC2 {
     *     class SC1 {
     *         C1 $f0;
     *         C2 $f1;
     *     } $f0;
     *     class C3 $f1;
     * }
     * </pre>
     *
     * Now imagine the type resulting from a 6-way join. It will be very
     * difficult to unpick the nesting in order to reference fields or to
     * permute the join order. Therefore when one or both of the inputs to a
     * join are synthetic, we break them apart and re-construct them. Type of
     * synthetic class SC1 joined to class C3 above is
     *
     * <pre>
     * class SC3 {
     *     C1 $f0;
     *     C2 $f1;
     *     C3 $f2;
     * }
     * </pre>
     *
     * <p>There are also <dfn>row classes</dfn>, which are synthetic classes
     * arising from projections. The type of
     *
     * <pre>select from (select deptno from dept)
     *   join emp
     *   join (select loc.nation, loc.zipcode from loc)</pre>
     *
     * is
     *
     * <pre>
     * class SC {
     *     int $f0;
     *     Emp $f1;
     *     class RC {
     *         String nation;
     *         int zipcode;
     *     } $f2;
     * }
     * </pre>
     *
     * <p>This deals with nesting; we still need to deal with the field
     * permutations which occur when we re-order joins. A permutation operator
     * moves fields back to their original positions, so that join transforms
     * preserve type.</p>
     */
    public OJClass makeJoinType(
        OJClass declarer,
        OJClass left,
        OJClass right)
    {
        Vector<OJClass> classesVector = new Vector<OJClass>();
        addAtomicClasses(classesVector, left);
        addAtomicClasses(classesVector, right);
        OJClass [] classes = new OJClass[classesVector.size()];
        classesVector.copyInto(classes);
        return createJoin(declarer, classes);
    }

    private static void addAtomicClasses(
        Vector<OJClass> classesVector,
        OJClass clazz)
    {
        if (OJSyntheticClass.isJoinClass(clazz)) {
            OJClass [] classes = ((OJSyntheticClass) clazz).classes;
            for (int i = 0; i < classes.length; i++) {
                addAtomicClasses(classesVector, classes[i]);
            }
        } else {
            classesVector.addElement(clazz);
        }
    }
}

// End OJClassMap.java
