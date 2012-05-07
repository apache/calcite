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
package org.eigenbase.rel.metadata;

import java.lang.reflect.*;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.util.*;


/**
 * ReflectiveRelMetadataProvider provides an abstract base for reflective
 * implementations of the {@link RelMetadataProvider} interface. For an example,
 * see {@link DefaultRelMetadataProvider}.
 *
 * <p>TODO jvs 28-Mar-2006: most of this should probably be refactored into
 * ReflectUtil.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class ReflectiveRelMetadataProvider
    implements RelMetadataProvider,
        ReflectiveVisitor
{
    //~ Instance fields --------------------------------------------------------

    private final Map<String, List<Class>> parameterTypeMap;

    private final ReflectiveVisitDispatcher<ReflectiveRelMetadataProvider,
        RelNode> visitDispatcher;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a ReflectiveRelMetadataProvider.
     */
    protected ReflectiveRelMetadataProvider()
    {
        parameterTypeMap = new HashMap<String, List<Class>>();
        visitDispatcher =
            ReflectUtil.createDispatcher(
                ReflectiveRelMetadataProvider.class,
                RelNode.class);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Maps the parameter type signature to look up for a given metadata query.
     *
     * @param metadataQueryName name of metadata query to map
     * @param parameterTypes argument types (beyond the overloaded rel type) to
     * map
     */
    protected void mapParameterTypes(
        String metadataQueryName,
        List<Class> parameterTypes)
    {
        parameterTypeMap.put(metadataQueryName, parameterTypes);
    }

    // implement RelMetadataProvider
    public Object getRelMetadata(
        RelNode rel,
        String metadataQueryName,
        Object [] args)
    {
        List<Class> parameterTypes = parameterTypeMap.get(metadataQueryName);
        if (parameterTypes == null) {
            parameterTypes = Collections.emptyList();
        }
        Method method =
            visitDispatcher.lookupVisitMethod(
                getClass(),
                rel.getClass(),
                metadataQueryName,
                parameterTypes);

        if (method == null) {
            return null;
        }

        Object [] allArgs;
        if (args != null) {
            allArgs = new Object[args.length + 1];
            allArgs[0] = rel;
            System.arraycopy(args, 0, allArgs, 1, args.length);
        } else {
            allArgs = new Object[] { rel };
        }

        try {
            return method.invoke(this, allArgs);
        } catch (Throwable ex) {
            // TODO jvs 28-Mar-2006:  share code with ReflectUtil
            if (ex instanceof RuntimeException) {
                throw (RuntimeException) ex;
            } else if (ex instanceof Error) {
                throw (Error) ex;
            } else {
                throw Util.newInternal(ex);
            }
        }
    }
}

// End ReflectiveRelMetadataProvider.java
