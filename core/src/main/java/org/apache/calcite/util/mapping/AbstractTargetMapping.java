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
package org.apache.calcite.util.mapping;

import java.util.Iterator;

/**
 * Simple implementation of
 * {@link org.apache.calcite.util.mapping.Mappings.TargetMapping} where the
 * number of sources and targets are specified as constructor parameters and you
 * just need to implement one method,
 */
public abstract class AbstractTargetMapping
    extends Mappings.AbstractMapping
    implements Mapping {
  private final int sourceCount;
  private final int targetCount;

  public AbstractTargetMapping(int sourceCount, int targetCount) {
    this.sourceCount = sourceCount;
    this.targetCount = targetCount;
  }

  @Override public int getSourceCount() {
    return sourceCount;
  }

  @Override public int getTargetCount() {
    return targetCount;
  }

  public Mapping inverse() {
    return Mappings.invert(this);
  }

  public int size() {
    return sourceCount;
  }

  public void clear() {
    throw new UnsupportedOperationException();
  }

  public MappingType getMappingType() {
    return MappingType.PARTIAL_FUNCTION;
  }

  public Iterator<IntPair> iterator() {
    return new Iterator<IntPair>() {
      int source = -1;
      int target;

      {
        moveToNext();
      }

      private void moveToNext() {
        while (++source < sourceCount) {
          target = getTargetOpt(source);
          if (target >= 0) {
            break;
          }
        }
      }

      public boolean hasNext() {
        return source < sourceCount;
      }

      public IntPair next() {
        IntPair p = new IntPair(source, target);
        moveToNext();
        return p;
      }

      public void remove() {
        throw new UnsupportedOperationException("remove");
      }
    };
  }

  public abstract int getTargetOpt(int source);
}

// End AbstractTargetMapping.java
