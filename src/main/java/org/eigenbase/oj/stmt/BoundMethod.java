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
package org.eigenbase.oj.stmt;

import java.lang.reflect.*;


/**
 * BoundMethod is a "thunk": a method which has already been bound to a
 * particular object on which it should be invoked, together with the arguments
 * which should be passed on invocation.
 */
public class BoundMethod
{
    //~ Instance fields --------------------------------------------------------

    Method method;
    Object o;
    String [] parameterNames;
    Object [] args;

    //~ Constructors -----------------------------------------------------------

    BoundMethod(
        Object o,
        Method method,
        String [] parameterNames)
    {
        this.o = o;
        this.method = method;
        this.parameterNames = parameterNames;
    }

    //~ Methods ----------------------------------------------------------------

    public Object call()
        throws IllegalAccessException, InvocationTargetException
    {
        return method.invoke(o, args);
    }
}

// End BoundMethod.java
