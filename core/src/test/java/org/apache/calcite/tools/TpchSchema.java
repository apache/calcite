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
package org.apache.calcite.tools;

/**
 * TPC-H table schema.
 */
public class TpchSchema {
  public final Part[] part = { p(1), p(2) };
  public final PartSupp[] partsupp = { ps(1, 250), ps(2, 100) };

  /**
   * Part in TPC-H.
   */
  public static class Part {
    public int pPartkey;


    public Part(int pPartkey) {
      super();
      this.pPartkey = pPartkey;
    }

    @Override public String toString() {
      return "Part [pPartkey=" + pPartkey + "]";
    }
  }

  /**
   * Part supplier in TPC-H.
   */
  public static class PartSupp {
    public int psPartkey;
    public int psSupplyCost;

    public PartSupp(int psPartkey, int psSupplyCost) {
      super();
      this.psPartkey = psPartkey;
      this.psSupplyCost = psSupplyCost;
    }

    @Override public String toString() {
      return "PartSupp [pSupplyCost=" + psPartkey + ", pSupplyCost="
        + psSupplyCost + "]";
    }
  }

  public static PartSupp ps(int pPartkey, int pSupplyCost) {
    return new PartSupp(pPartkey, pSupplyCost);
  }

  public static Part p(int pPartkey) {
    return new Part(pPartkey);
  }
}

// End TpchSchema.java
