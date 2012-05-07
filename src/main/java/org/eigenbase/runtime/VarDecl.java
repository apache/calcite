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
package org.eigenbase.runtime;

/**
 * An array of <code>VarDecl</code>s is returned from the <code>dummy()</code>
 * method which is generated to implement a variable declaration, or a list of
 * statements which contain variable declarations.
 */
public class VarDecl
{
    //~ Instance fields --------------------------------------------------------

    public Class clazz;
    public Object value;
    public String name;

    //~ Constructors -----------------------------------------------------------

    public VarDecl(
        String name,
        Class clazz,
        Object value)
    {
        this.name = name;
        this.clazz = clazz;
        this.value = value;
    }
}

// End VarDecl.java
