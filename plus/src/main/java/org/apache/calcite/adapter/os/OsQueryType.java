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
package org.apache.calcite.adapter.os;

import java.util.List;

import static org.apache.calcite.adapter.utils.OsQueryTableUtil.getCpuInfo;
import static org.apache.calcite.adapter.utils.OsQueryTableUtil.getCpuTimeInfo;
import static org.apache.calcite.adapter.utils.OsQueryTableUtil.getInterfaceAddressesInfo;
import static org.apache.calcite.adapter.utils.OsQueryTableUtil.getInterfaceDetailsInfo;
import static org.apache.calcite.adapter.utils.OsQueryTableUtil.getJavaInfo;
import static org.apache.calcite.adapter.utils.OsQueryTableUtil.getMemoryInfo;
import static org.apache.calcite.adapter.utils.OsQueryTableUtil.getMountsInfo;
import static org.apache.calcite.adapter.utils.OsQueryTableUtil.getOsVersionInfo;
import static org.apache.calcite.adapter.utils.OsQueryTableUtil.getSystemInfo;

/**
 * Get system enumeration information.
 */
public enum OsQueryType {
  SYSTEM_INFO {
    @Override public List<Object[]> getInfo() {
      return getSystemInfo();
    }
  },
  JAVA_INFO {
    @Override public List<Object[]> getInfo() {
      return getJavaInfo();
    }
  },
  OS_VERSION {
    @Override public List<Object[]> getInfo() {
      return getOsVersionInfo();
    }
  },
  MEMORY_INFO {
    @Override public List<Object[]> getInfo() {
      return getMemoryInfo();
    }
  },
  CPU_INFO {
    @Override public List<Object[]> getInfo() {
      return getCpuInfo();
    }
  },
  CPU_TIME {
    @Override public List<Object[]> getInfo() {
      return getCpuTimeInfo();
    }
  },
  INTERFACE_ADDRESSES {
    @Override public List<Object[]> getInfo() {
      return getInterfaceAddressesInfo();
    }
  },
  INTERFACE_DETAILS {
    @Override public List<Object[]> getInfo() {
      return getInterfaceDetailsInfo();
    }
  },
  MOUNTS {
    @Override public List<Object[]> getInfo() {
      return getMountsInfo();
    }
  };

  public abstract List<Object[]> getInfo();
}
