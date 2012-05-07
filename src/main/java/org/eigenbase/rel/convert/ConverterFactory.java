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
package org.eigenbase.rel.convert;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * A <code>ConverterFactory</code> is ...
 *
 * @author jhyde
 * @version $Id$
 * @since Jun 18, 2003
 */
public interface ConverterFactory
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Returns the calling convention this converter converts to.
     */
    CallingConvention getConvention();

    /**
     * Returns the calling convention this converter converts from.
     */
    CallingConvention getInConvention();

    /**
     * Converts a relational expression, which must have {@link
     * #getInConvention} calling convention, to a relational expression of
     * {@link #getConvention} calling convention.
     */
    ConverterRel convert(RelNode rel);
}

// End ConverterFactory.java
