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

/**
 * A relational expression which implements itself by generating Java
 * flow-control statements. This interface corresponds to the {@link
 * org.eigenbase.relopt.CallingConvention#JAVA Java calling-convention}.
 *
 * <p>For example, <code>JavaFilterRe</code> implements filtering logic by
 * generating an <code>if (...) { ... }</code> construct.
 *
 * @author jhyde
 * @version $Id$
 * @since May 27, 2004
 */
public interface JavaLoopRel
    extends JavaRel
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Calls a parent back.
     *
     * @param implementor implementor
     * @param ordinal of the child which is making the call
     */
    void implementJavaParent(
        JavaRelImplementor implementor,
        int ordinal);
}

// End JavaLoopRel.java
