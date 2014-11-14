/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.eigenbase.util;

import org.eigenbase.resource.EigenbaseNewResource;
import org.eigenbase.resource.Resources;

/**
 * Definitions of objects to be statically imported.
 *
 * <p>Developers: Give careful consideration before including an object in this
 * class.
 * Pros: Code that uses these objects will be terser.
 * Cons: Namespace pollution,
 * code that is difficult to understand (a general problem with static imports),
 * potential cyclic initialization.</p>
 */
public abstract class Static {
  private Static() {}

  /** Resources. */
  public static final EigenbaseNewResource RESOURCE =
      Resources.create("org.eigenbase.resource.EigenbaseResource",
          EigenbaseNewResource.class);
}

// End Static.java
