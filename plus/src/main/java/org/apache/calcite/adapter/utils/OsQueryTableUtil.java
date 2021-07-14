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
    Object[] objects = new Object[18];
    objects[0] = OshiUtil.getOs().getNetworkParams().getHostName();
    objects[1] = OshiUtil.getSystem().getSerialNumber();
    objects[2] = OshiUtil.getProcessor().getProcessorIdentifier().getMicroarchitecture();
    objects[3] = OshiUtil.getProcessor().getProcessorIdentifier().getVendor();
    objects[4] = OshiUtil.getProcessor().getProcessorIdentifier().getModel();
    objects[5] = OshiUtil.getProcessor().getPhysicalProcessorCount();
    objects[6] = OshiUtil.getProcessor().getLogicalProcessorCount();
    objects[7] = OshiUtil.getProcessor().getPhysicalPackageCount();
    objects[8] = OshiUtil.getMemory().getTotal();
    objects[9] = OshiUtil.getHardware().getComputerSystem().getFirmware().getManufacturer();
    objects[10] = OshiUtil.getHardware().getComputerSystem().getModel();
    objects[11] = OshiUtil.getHardware().getComputerSystem().getFirmware().getVersion();
    objects[12] = OshiUtil.getHardware().getComputerSystem().getSerialNumber();
    objects[13] = OshiUtil.getHardware().getComputerSystem().getBaseboard().getManufacturer();
    objects[14] = OshiUtil.getHardware().getComputerSystem().getBaseboard().getModel();
    objects[15] = OshiUtil.getHardware().getComputerSystem().getBaseboard().getVersion();
    objects[16] = OshiUtil.getHardware().getComputerSystem().getBaseboard().getSerialNumber();
    objects[17] = OshiUtil.getOs().getNetworkParams().getDomainName();
    list.add(objects);
    return list;
  }

  public static List<Object[]> getJavaInfo() {
    final List<Object[]> list = new ArrayList<>();
    final Object[] objects = new Object[18];
    final Properties props = System.getProperties();
    objects[0] = props.getProperty("java.version");
    objects[1] = props.getProperty("java.vendor");
    objects[2] = props.getProperty("java.vendor.url");
    objects[3] = props.getProperty("java.home");
    objects[4] = props.getProperty("java.vm.specification.version");
    objects[5] = props.getProperty("java.vm.specification.vendor");
    objects[6] = props.getProperty("java.vm.specification.name");
    objects[7] = props.getProperty("java.vm.version");
    objects[8] = props.getProperty("java.vm.vendor");
    objects[9] = props.getProperty("java.vm.name");
    objects[10] = props.getProperty("java.specification.version");
    objects[11] = props.getProperty("java.specification.vender");
    objects[12] = props.getProperty("java.specification.name");
    objects[13] = props.getProperty("java.class.version");
    objects[14] = props.getProperty("java.class.path");
    objects[15] = props.getProperty("java.io.tmpdir");
    objects[16] = props.getProperty("java.ext.dirs");
    objects[17] = props.getProperty("java.library.path");
    list.add(objects);
    return list;
  }

  public static List<Object[]> getOsVersionInfo() {
    final List<Object[]> list = new ArrayList<>();
    final Object[] objects = new Object[5];
    OperatingSystem os = OshiUtil.getOs();
    objects[0] = os.getVersionInfo().toString();
    objects[1] = os.getVersionInfo().getCodeName();
    objects[2] = os.getVersionInfo().getBuildNumber();
    objects[3] = os.getVersionInfo().getCodeName();
    objects[4] = os.getSystemBootTime();
    list.add(objects);
    return list;
  }

  public static List<Object[]> getMemoryInfo() {
    GlobalMemory memory = OshiUtil.getMemory();
    VirtualMemory virtualMemory = memory.getVirtualMemory();

    final List<Object[]> list = new ArrayList<>();
    final Object[] objects = new Object[8];
    objects[0] = getNetFileSizeDescription(memory.getTotal());
    objects[1] = getNetFileSizeDescription(memory.getAvailable());
    objects[2] = getNetFileSizeDescription(memory.getTotal() - memory.getAvailable());
    objects[3] = getNetFileSizeDescription(virtualMemory.getSwapTotal());
    objects[4] = getNetFileSizeDescription(virtualMemory.getSwapUsed());
    objects[5] =
        getNetFileSizeDescription(virtualMemory.getSwapTotal() - virtualMemory.getSwapUsed());
    objects[6] = getNetFileSizeDescription(virtualMemory.getSwapPagesIn());
    objects[7] = getNetFileSizeDescription(virtualMemory.getSwapPagesOut());

    list.add(objects);
    return list;
  }

  public static List<Object[]> getCpuInfo() {
    final List<Object[]> list = new ArrayList<>();
    CentralProcessor processor = OshiUtil.getProcessor();
    Object[] objects = new Object[9];
    objects[0] = processor.getProcessorIdentifier().getProcessorID();
    objects[1] = processor.getProcessorIdentifier().getModel();
    objects[2] = processor.getProcessorIdentifier().getMicroarchitecture();
    objects[3] = processor.getPhysicalProcessorCount();
    objects[4] = processor.getLogicalProcessorCount();
    objects[5] = processor.getProcessorIdentifier().isCpu64bit() ? 64 : 32;
    objects[6] = processor.getMaxFreq();
    objects[7] = processor.getPhysicalPackageCount();
    objects[8] = processor.getSystemCpuLoad(1000L);
    list.add(objects);
    return list;
  }

  public static List<Object[]> getCpuTimeInfo() {
    final List<Object[]> list = new ArrayList<>();
    CpuTicks cpuTicks = OshiUtil.getCpuInfo().getTicks();
    Object[] objects = new Object[8];
    objects[0] = cpuTicks.getIdle();
    objects[1] = cpuTicks.getNice();
    objects[2] = cpuTicks.getIrq();
    objects[3] = cpuTicks.getSoftIrq();
    objects[4] = cpuTicks.getSteal();
    objects[5] = cpuTicks.getcSys();
    objects[6] = cpuTicks.getUser();
    objects[7] = cpuTicks.getIoWait();
    list.add(objects);
    return list;
  }

  public static List<Object[]> getInterfaceAddressesInfo() {
    final List<Object[]> list = new ArrayList<>();
    List<NetworkIF> networkIFList = OshiUtil.getNetworkIFs();
    for (NetworkIF intf : networkIFList) {
      Object[] objects = new Object[5];
      objects[0] = intf.getName();
      objects[1] = getIPAddressesString(intf.getIPv4addr());
      objects[2] = getIPAddressesString(intf.getIPv6addr());
      objects[3] = intf.getMacaddr();
      objects[4] =
          intf.getIfOperStatus().toString().contains("UNKNOWN") ? "" : intf.getIfOperStatus();
      list.add(objects);
    }
    return list;
  }

  public static List<Object[]> getInterfaceDetailsInfo() {
    final List<Object[]> list = new ArrayList<>();
    List<NetworkIF> networkIFList = OshiUtil.getNetworkIFs();
    for (NetworkIF intf : networkIFList) {
      Object[] objects = new Object[13];
      objects[0] = intf.getName();
      objects[1] = intf.getMacaddr();
      objects[2] = intf.queryNetworkInterface().isVirtual();
      objects[3] = intf.getMTU();
      objects[4] = intf.getSpeed();
      objects[5] = intf.getPacketsRecv();
      objects[6] = intf.getPacketsSent();
      objects[7] = intf.getBytesRecv();
      objects[8] = intf.getBytesSent();
      objects[9] = intf.getInErrors();
      objects[10] = intf.getOutErrors();
      objects[11] = intf.getInDrops();
      objects[12] = intf.getCollisions();
      list.add(objects);
    }
    return list;
  }

  public static List<Object[]> getMountsInfo() {
    final List<Object[]> list = new ArrayList<>();
    FileSystem fileSystem = OshiUtil.getOs().getFileSystem();
    List<OSFileStore> fileStores = fileSystem.getFileStores();
    for (OSFileStore fileStore : fileStores) {
      Object[] objects = new Object[8];
      objects[0] = fileStore.getName();
      objects[1] = fileStore.getName();
      objects[2] = getNetFileSizeDescription(fileStore.getTotalSpace());
      objects[3] = getNetFileSizeDescription(fileStore.getUsableSpace());
      objects[4] = getNetFileSizeDescription(fileStore.getFreeSpace());
      objects[5] = getNetFileSizeDescription(fileStore.getTotalInodes());
      objects[6] = getNetFileSizeDescription(fileStore.getFreeInodes());
      objects[7] = fileStore.getMount();
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
