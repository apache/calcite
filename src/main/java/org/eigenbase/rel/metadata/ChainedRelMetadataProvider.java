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

import java.util.*;

import org.eigenbase.rel.*;


/**
 * ChainedRelMetadataProvider implements the {@link RelMetadataProvider}
 * interface via the {@link
 * org.eigenbase.util.Glossary#ChainOfResponsibilityPattern}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class ChainedRelMetadataProvider
    implements RelMetadataProvider
{
    //~ Instance fields --------------------------------------------------------

    private List<RelMetadataProvider> providers;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new empty chain.
     */
    public ChainedRelMetadataProvider()
    {
        providers = new ArrayList<RelMetadataProvider>();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Adds a provider, giving it higher priority than all those already in
     * chain. Chain order matters, since the first provider which answers a
     * query is used.
     *
     * @param provider provider to add
     */
    public void addProvider(
        RelMetadataProvider provider)
    {
        providers.add(0, provider);
    }

    // implement RelMetadataProvider
    public Object getRelMetadata(
        RelNode rel,
        String metadataQueryName,
        Object [] args)
    {
        for (RelMetadataProvider provider : providers) {
            Object result =
                provider.getRelMetadata(
                    rel,
                    metadataQueryName,
                    args);
            if (result != null) {
                return result;
            }
        }
        return null;
    }
}

// End ChainedRelMetadataProvider.java
