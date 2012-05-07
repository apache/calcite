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


/**
 * Generic implementation of {@link ConverterRule} which lets a {@link
 * ConverterFactory} do the work.
 *
 * @author jhyde
 * @version $Id$
 * @since Jun 18, 2003
 */
public class FactoryConverterRule
    extends ConverterRule
{
    //~ Instance fields --------------------------------------------------------

    private final ConverterFactory factory;

    //~ Constructors -----------------------------------------------------------

    public FactoryConverterRule(ConverterFactory factory)
    {
        super(
            RelNode.class,
            factory.getInConvention(),
            factory.getConvention(),
            null);
        this.factory = factory;
    }

    //~ Methods ----------------------------------------------------------------

    public boolean isGuaranteed()
    {
        return true;
    }

    public RelNode convert(RelNode rel)
    {
        return factory.convert(rel);
    }
}

// End FactoryConverterRule.java
