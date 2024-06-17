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
package org.apache.calcite.adapter.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import cn.hutool.system.oshi.CpuTicks;
import cn.hutool.system.oshi.OshiUtil;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.NetworkIF;
import oshi.hardware.VirtualMemory;
import oshi.software.os.FileSystem;
import oshi.software.os.OSFileStore;
import oshi.software.os.OperatingSystem;

/**
 * Used to put OS query related func.
 */
public class OsQueryTableUtil {
  private static final String IP_ADDRESS_SEPARATOR = "; ";

  private OsQueryTableUtil() {
  }

  public static List<Object[]> getSystemInfo() {
    final List<Object[]> list = new ArrayList<>();
    Object[] objects = {
        OshiUtil.getOs().getNetworkParams().getHostName(),
        OshiUtil.getSystem().getSerialNumber(),
        OshiUtil.getProcessor().getProcessorIdentifier().getMicroarchitecture(),
        OshiUtil.getProcessor().getProcessorIdentifier().getVendor(),
        OshiUtil.getProcessor().getProcessorIdentifier().getModel(),
        OshiUtil.getProcessor().getPhysicalProcessorCount(),
        OshiUtil.getProcessor().getLogicalProcessorCount(),
        OshiUtil.getProcessor().getPhysicalPackageCount(),
        OshiUtil.getMemory().getTotal(),
        OshiUtil.getHardware().getComputerSystem().getFirmware().getManufacturer(),
        OshiUtil.getHardware().getComputerSystem().getModel(),
        OshiUtil.getHardware().getComputerSystem().getFirmware().getVersion(),
        OshiUtil.getHardware().getComputerSystem().getSerialNumber(),
        OshiUtil.getHardware().getComputerSystem().getBaseboard().getManufacturer(),
        OshiUtil.getHardware().getComputerSystem().getBaseboard().getModel(),
        OshiUtil.getHardware().getComputerSystem().getBaseboard().getVersion(),
        OshiUtil.getHardware().getComputerSystem().getBaseboard().getSerialNumber(),
        OshiUtil.getOs().getNetworkParams().getDomainName()
    };
    list.add(objects);
    return list;
  }

  public static List<Object[]> getJavaInfo() {
    final List<Object[]> list = new ArrayList<>();
    final Properties props = System.getProperties();
    final Object[] objects = {
        props.getProperty("java.version"),
        props.getProperty("java.vendor"),
        props.getProperty("java.vendor.url"),
        props.getProperty("java.home"),
        props.getProperty("java.vm.specification.version"),
        props.getProperty("java.vm.specification.vendor"),
        props.getProperty("java.vm.specification.name"),
        props.getProperty("java.vm.version"),
        props.getProperty("java.vm.vendor"),
        props.getProperty("java.vm.name"),
        props.getProperty("java.specification.version"),
        props.getProperty("java.specification.vender"),
        props.getProperty("java.specification.name"),
        props.getProperty("java.class.version"),
        props.getProperty("java.class.path"),
        props.getProperty("java.io.tmpdir"),
        props.getProperty("java.ext.dirs"),
        props.getProperty("java.library.path")
    };
    list.add(objects);
    return list;
  }

  public static List<Object[]> getOsVersionInfo() {
    final List<Object[]> list = new ArrayList<>();
    OperatingSystem os = OshiUtil.getOs();
    final Object[] objects = {
        os.getVersionInfo().toString(),
        os.getVersionInfo().getCodeName(),
        os.getVersionInfo().getBuildNumber(),
        os.getVersionInfo().getCodeName(),
        os.getSystemBootTime()
    };
    list.add(objects);
    return list;
  }

  public static List<Object[]> getMemoryInfo() {
    GlobalMemory memory = OshiUtil.getMemory();
    VirtualMemory virtualMemory = memory.getVirtualMemory();

    final List<Object[]> list = new ArrayList<>();
    final Object[] objects = {
        getNetFileSizeDescription(memory.getTotal()),
        getNetFileSizeDescription(memory.getAvailable()),
        getNetFileSizeDescription(memory.getTotal() - memory.getAvailable()),
        getNetFileSizeDescription(virtualMemory.getSwapTotal()),
        getNetFileSizeDescription(virtualMemory.getSwapUsed()),
        getNetFileSizeDescription(virtualMemory.getSwapTotal() - virtualMemory.getSwapUsed()),
        getNetFileSizeDescription(virtualMemory.getSwapPagesIn()),
        getNetFileSizeDescription(virtualMemory.getSwapPagesOut())
    };
    list.add(objects);
    return list;
  }

  public static List<Object[]> getCpuInfo() {
    final List<Object[]> list = new ArrayList<>();
    CentralProcessor processor = OshiUtil.getProcessor();
    Object[] objects = {
        processor.getProcessorIdentifier().getProcessorID(),
        processor.getProcessorIdentifier().getModel(),
        processor.getProcessorIdentifier().getMicroarchitecture(),
        processor.getPhysicalProcessorCount(),
        processor.getLogicalProcessorCount(),
        processor.getProcessorIdentifier().isCpu64bit() ? 64 : 32,
        processor.getMaxFreq(),
        processor.getPhysicalPackageCount(),
        processor.getSystemCpuLoad(1000L)
    };
    list.add(objects);
    return list;
  }

  public static List<Object[]> getCpuTimeInfo() {
    final List<Object[]> list = new ArrayList<>();
    CpuTicks cpuTicks = OshiUtil.getCpuInfo().getTicks();
    Object[] objects = {
        cpuTicks.getIdle(),
        cpuTicks.getNice(),
        cpuTicks.getIrq(),
        cpuTicks.getSoftIrq(),
        cpuTicks.getSteal(),
        cpuTicks.getcSys(),
        cpuTicks.getUser(),
        cpuTicks.getIoWait()
    };
    list.add(objects);
    return list;
  }

  public static List<Object[]> getInterfaceAddressesInfo() {
    final List<Object[]> list = new ArrayList<>();
    List<NetworkIF> networkIFList = OshiUtil.getNetworkIFs();
    for (NetworkIF intf : networkIFList) {
      Object[] objects = {
          intf.getName(),
          getIPAddressesString(intf.getIPv4addr()),
          getIPAddressesString(intf.getIPv6addr()),
          intf.getMacaddr(),
          intf.getIfOperStatus().toString().contains("UNKNOWN") ? "" : intf.getIfOperStatus()
      };
      list.add(objects);
    }
    return list;
  }

  public static List<Object[]> getInterfaceDetailsInfo() {
    final List<Object[]> list = new ArrayList<>();
    List<NetworkIF> networkIFList = OshiUtil.getNetworkIFs();
    for (NetworkIF intf : networkIFList) {
      Object[] objects = {
          intf.getName(),
          intf.getMacaddr(),
          intf.queryNetworkInterface().isVirtual(),
          intf.getMTU(),
          intf.getSpeed(),
          intf.getPacketsRecv(),
          intf.getPacketsSent(),
          intf.getBytesRecv(),
          intf.getBytesSent(),
          intf.getInErrors(),
          intf.getOutErrors(),
          intf.getInDrops(),
          intf.getCollisions()
      };
      list.add(objects);
    }
    return list;
  }

  public static List<Object[]> getMountsInfo() {
    final List<Object[]> list = new ArrayList<>();
    FileSystem fileSystem = OshiUtil.getOs().getFileSystem();
    List<OSFileStore> fileStores = fileSystem.getFileStores();
    for (OSFileStore fileStore : fileStores) {
      Object[] objects = {
          fileStore.getName(),
          fileStore.getName(),
          getNetFileSizeDescription(fileStore.getTotalSpace()),
          getNetFileSizeDescription(fileStore.getUsableSpace()),
          getNetFileSizeDescription(fileStore.getFreeSpace()),
          getNetFileSizeDescription(fileStore.getTotalInodes()),
          getNetFileSizeDescription(fileStore.getFreeInodes()),
          fileStore.getMount()
      };
      list.add(objects);
    }
    return list;
  }

  public static String getIPAddressesString(String[] ipAddressArr) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;

    for (String ipAddress : ipAddressArr) {
      if (first) {
        first = false;
      } else {
        sb.append(IP_ADDRESS_SEPARATOR);
      }
      sb.append(ipAddress);
    }

    return sb.toString();
  }

  public static String getNetFileSizeDescription(long size) {
    String[] units = {"B", "KB", "MB", "GB"};
    int index = 0;
    double fileSize = size;
    while (fileSize >= 1024 && index < units.length - 1) {
      fileSize /= 1024;
      index++;
    }
    return String.format(Locale.ROOT, "%.2f%s", fileSize, units[index]);
  }
}
