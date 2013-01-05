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
package org.eigenbase.oj.rel;

import openjava.ptree.*;

import org.eigenbase.rel.*;


/**
 * A relational expression of one of the the Java-based calling conventions.
 *
 * <p>Objects which implement this interface must:
 *
 * <ul>
 * <li>extend {@link org.eigenbase.rel.AbstractRelNode}, and</li>
 * <li>return one of the following calling-conventions from their {@link
 * #getConvention} method:
 *
 * <ul>
 * <li>{@link org.eigenbase.relopt.CallingConvention#ARRAY ARRAY},
 * <li>{@link org.eigenbase.relopt.CallingConvention#ITERABLE ITERABLE},
 * <li>{@link org.eigenbase.relopt.CallingConvention#ITERATOR ITERATOR},
 * <li>{@link org.eigenbase.relopt.CallingConvention#COLLECTION COLLECTION},
 * <li>{@link org.eigenbase.relopt.CallingConvention#MAP MAP},
 * <li>{@link org.eigenbase.relopt.CallingConvention#VECTOR VECTOR},
 * <li>{@link org.eigenbase.relopt.CallingConvention#HASHTABLE HASHTABLE},
 * <li>{@link org.eigenbase.relopt.CallingConvention#JAVA JAVA}.
 * </ul>
 * </li>
 * </ul>
 *
 * <p>For {@link org.eigenbase.relopt.CallingConvention#JAVA JAVA
 * calling-convention}, see the sub-interface {@link JavaLoopRel}, and the
 * auxiliary interface {@link JavaSelfRel}.
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 22, 2003
 */
public interface JavaRel
    extends RelNode
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Creates a plan for this expression according to a calling convention.
     *
     * @param implementor implementor
     */
    ParseTree implement(JavaRelImplementor implementor);
}

// End JavaRel.java
