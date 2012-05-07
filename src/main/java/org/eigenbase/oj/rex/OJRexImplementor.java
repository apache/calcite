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
package org.eigenbase.oj.rex;

import openjava.ptree.*;

import org.eigenbase.rex.*;


/**
 * OJRexImplementor translates a call to a particular operator to OpenJava code.
 *
 * <p>Implementors are held in a {@link OJRexImplementorTable}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface OJRexImplementor
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Implements a single {@link RexCall}.
     *
     * @param translator provides translation context
     * @param call the call to be translated
     * @param operands call's operands, which have already been translated
     * independently
     */
    public Expression implement(
        RexToOJTranslator translator,
        RexCall call,
        Expression [] operands);

    /**
     * Tests whether it is possible to implement a call.
     *
     * @param call the call for which translation is being considered
     *
     * @return whether the call can be implemented
     */
    public boolean canImplement(RexCall call);
}

// End OJRexImplementor.java
