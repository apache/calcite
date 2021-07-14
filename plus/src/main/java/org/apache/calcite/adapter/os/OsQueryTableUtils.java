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

import org.apache.calcite.util.Sources;
import org.apache.calcite.util.trace.CalciteTrace;

import org.apache.commons.lang3.StringUtils;

import org.hyperic.sigar.CpuInfo;
import org.hyperic.sigar.CpuPerc;
import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.FileSystemUsage;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.NetFlags;
import org.hyperic.sigar.NetInterfaceConfig;
import org.hyperic.sigar.NetInterfaceStat;
import org.hyperic.sigar.OperatingSystem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.hyperic.sigar.Swap;
import org.hyperic.sigar.Who;
import org.slf4j.Logger;

import java.net.InetAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 * The Util of OsQuery.
 */
public class OsQueryTableUtils {
  private static final Logger LOGGER = CalciteTrace.getParserTracer();

  private static final String SIGAR_PATH = "org.hyperic.sigar.path";
  private static final String SIGAR_LIB_PATH = "/hyperic-sigar-1.6.4/sigar-bin/lib/";

  private final OsQueryEnum type;
  private Sigar sigar;

  public OsQueryTableUtils(String type) {
    this.type = OsQueryEnum.valueOf(type.toUpperCase(Locale.ROOT));
    loadLibs();
  }

  private List<Object[]> systemInfo() throws Exception {
    final List<Object[]> list = new ArrayList<Object[]>();
    final CpuInfo[] infos = sigar.getCpuInfoList();
    for (CpuInfo info : infos) {
      Object[] objects = new Object[20];
      Runtime r = Runtime.getRuntime();
      Properties props = System.getProperties();
      InetAddress addr;
      addr = InetAddress.getLocalHost();
      String ip = addr.getHostAddress();
      Map<String, String> map = System.getenv();
      String computerName = map.get("COMPUTERNAME");
      String userDomain = map.get("USERDOMAIN");
      objects[0] = props.getProperty("user.name");
      objects[1] = computerName;
      objects[2] = userDomain;
      objects[3] = ip;
      objects[4] = addr.getHostName();
      objects[5] = r.totalMemory();
      objects[6] = r.freeMemory();
      objects[7] = r.availableProcessors();
      objects[8] = info.getTotalCores();
      objects[9] = info.getMhz();
      objects[10] = info.getVendor();
      objects[11] = info.getCacheSize();
      objects[12] = props.getProperty("os.name");
      objects[13] = props.getProperty("os.arch");
      objects[14] = props.getProperty("os.version");
      objects[15] = props.getProperty("file.separator");
      objects[16] = props.getProperty("path.separator");
      objects[17] = props.getProperty("line.separator");
      objects[18] = props.getProperty("user.home");
      objects[19] = props.getProperty("user.dir");
      list.add(objects);
    }
    return list;
  }

  private List<Object[]> javaInfo() {
    final List<Object[]> list = new ArrayList<Object[]>();
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

  private List<Object[]> osInfo() {
    final List<Object[]> list = new ArrayList<Object[]>();
    final Object[] objects = new Object[11];
    OperatingSystem os = OperatingSystem.getInstance();
    objects[0] = os.getArch();
    objects[1] = os.getCpuEndian();
    objects[2] = os.getDataModel();
    objects[3] = os.getDescription();
    objects[4] = os.getName();
    objects[5] = os.getPatchLevel();
    objects[6] = os.getVendor();
    objects[7] = os.getVendorCodeName();
    objects[8] = os.getVendorName();
    objects[9] = os.getVendorVersion();
    objects[10] = os.getVersion();
    list.add(objects);
    return list;
  }

  private List<Object[]> memoryInfo() throws SigarException {
    final List<Object[]> list = new ArrayList<Object[]>();
    final Object[] objects = new Object[6];
    Mem mem = sigar.getMem();
    Swap swap = sigar.getSwap();
    objects[0] = mem.getTotal() / 1024L;
    objects[1] = mem.getUsed() / 1024L;
    objects[2] = mem.getFree() / 1024L;
    objects[3] = swap.getTotal() / 1024L;
    objects[4] = swap.getUsed() / 1024L;
    objects[5] = swap.getFree() / 1024L;
    list.add(objects);
    return list;
  }

  private List<Object[]> cpuInfo() throws SigarException {
    final List<Object[]> list = new ArrayList<Object[]>();
    final CpuInfo[] infos = sigar.getCpuInfoList();
    final CpuPerc[] cpuList = sigar.getCpuPercList();
    for (int i = 0; i < infos.length; i++) {
      Object[] objects = new Object[12];
      CpuInfo info = infos[i];
      objects[0] = i + 1;
      objects[1] = info.getTotalCores();
      objects[2] = info.getMhz();
      objects[3] = info.getVendor();
      objects[4] = info.getCacheSize();
      objects[5] = info.getModel();
      objects[6] = CpuPerc.format(cpuList[i].getUser());
      objects[7] = CpuPerc.format(cpuList[i].getSys());
      objects[8] = CpuPerc.format(cpuList[i].getWait());
      objects[9] = CpuPerc.format(cpuList[i].getNice());
      objects[10] = CpuPerc.format(cpuList[i].getIdle());
      objects[11] = CpuPerc.format(cpuList[i].getCombined());
      list.add(objects);
    }
    return list;
  }

  private List<Object[]> interfaceDetails() throws Exception {
    final List<Object[]> list = new ArrayList<Object[]>();
    final String[] ifNames = sigar.getNetInterfaceList();
    for (String name : ifNames) {
      Object[] objects = new Object[11];
      NetInterfaceConfig ifconfig = sigar.getNetInterfaceConfig(name);
      if ((ifconfig.getFlags() & 1L) <= 0L) {
        LOGGER.warn("!IFF_UP...skipping getNetInterfaceStat");
        continue;
      }
      NetInterfaceStat ifstat = sigar.getNetInterfaceStat(name);
      objects[0] = name;
      objects[1] = ifconfig.getAddress();
      objects[2] = ifconfig.getNetmask();
      objects[3] = ifstat.getRxPackets();
      objects[4] = ifstat.getTxPackets();
      objects[5] = ifstat.getRxBytes();
      objects[6] = ifstat.getTxBytes();
      objects[7] = ifstat.getRxErrors();
      objects[8] = ifstat.getTxErrors();
      objects[9] = ifstat.getRxDropped();
      objects[10] = ifstat.getTxDropped();
      list.add(objects);
    }
    return list;
  }

  private List<Object[]> interfaceAddresses() throws SigarException {
    final List<Object[]> list = new ArrayList<Object[]>();
    final String[] ifaces = sigar.getNetInterfaceList();
    for (String iface : ifaces) {
      Object[] objects = new Object[6];
      NetInterfaceConfig cfg = sigar.getNetInterfaceConfig(iface);
      if (NetFlags.LOOPBACK_ADDRESS.equals(cfg.getAddress())
          || (cfg.getFlags() & NetFlags.IFF_LOOPBACK) != 0
          || NetFlags.NULL_HWADDR.equals(cfg.getHwaddr())) {
        continue;
      }
      objects[0] = cfg.getAddress();
      objects[1] = cfg.getBroadcast();
      objects[2] = cfg.getHwaddr();
      objects[3] = cfg.getNetmask();
      objects[4] = cfg.getDescription();
      objects[5] = cfg.getType();
      list.add(objects);
    }
    return list;
  }

  private List<Object[]> mounts() throws Exception {
    final List<Object[]> list = new ArrayList<Object[]>();
    final FileSystem[] fslist = sigar.getFileSystemList();
    for (int i = 0; i < fslist.length; i++) {
      Object[] objects = new Object[14];
      FileSystem fs = fslist[i];
      FileSystemUsage usage = null;
      usage = sigar.getFileSystemUsage(fs.getDirName());
      objects[0] = i;
      objects[1] = fs.getDevName();
      objects[2] = fs.getDirName();
      objects[3] = fs.getFlags();
      objects[4] = fs.getSysTypeName();
      objects[5] = fs.getTypeName();
      objects[6] = fs.getType();
      objects[7] = usage.getDiskReads();
      objects[8] = usage.getDiskWrites();
      switch (fs.getType()) {
      // TYPE_UNKNOWN ：unknown
      case 0:
        break;
      // TYPE_NONE
      case 1:
      case 2:
        double usePercent = usage.getUsePercent() * 100D;
        objects[9] = HexTransformationUtils.kbToGb(usage.getTotal()) + "GB";
        objects[10] = HexTransformationUtils.kbToGb(usage.getFree()) + "GB";
        objects[11] = HexTransformationUtils.kbToGb(usage.getAvail()) + "GB";
        objects[12] = HexTransformationUtils.kbToGb(usage.getUsed()) + "GB";
        objects[13] = usePercent + "%";
        break;
      // TYPE_NETWORK ：internet
      case 3:
        break;
      // TYPE_RAM_DISK ：Flash memory
      case 4:
        break;
      // TYPE_CDROM ：Optical drive
      case 5:
        break;
      // TYPE_SWAP ：Page exchang
      case 6:
        break;
      default:
        break;
      }
      list.add(objects);
    }
    return list;
  }

  private List<Object[]> whoUse() throws SigarException {
    final List<Object[]> list = new ArrayList<Object[]>();
    final Who[] whos = sigar.getWhoList();
    if (whos != null && whos.length > 0) {
      for (Who who : whos) {
        Object[] objects = new Object[4];
        objects[0] = who.getDevice();
        objects[1] = who.getHost();
        objects[2] = who.getUser();
        objects[3] = who.getTime();
        list.add(objects);
      }
    }
    return list;
  }

  private void loadLibs() {
    if (StringUtils.isEmpty(System.getProperty(SIGAR_PATH))) {
      URL sigarPathUrl = OsQueryTableUtils.class.getResource(SIGAR_LIB_PATH);
      System.setProperty(SIGAR_PATH, Sources.of(sigarPathUrl).file().getPath());
    }
  }

  public List<Object[]> getResultInfo() {
    this.sigar = new Sigar();
    List<Object[]> objs = new ArrayList<Object[]>();
    try {
      switch (type) {
      case SYSTEM_INFO:
        objs = systemInfo();
        break;
      case JAVA_INFO:
        objs = javaInfo();
        break;
      case OS_VERSION:
        objs = osInfo();
        break;
      case MEMORY_INFO:
        objs = memoryInfo();
        break;
      case CPU_INFO:
        objs = cpuInfo();
        break;
      case INTERFACE_DETAILS:
        objs = interfaceDetails();
        break;
      case INTERFACE_ADDRESSES:
        objs = interfaceAddresses();
        break;
      case MOUNTS:
        objs = mounts();
        break;
      case WHO_USE:
        objs = whoUse();
        break;
      default:
        break;
      }
    } catch (Exception e) {
      LOGGER.error("Failed to get result's info", e);
    }
    return objs;
  }

  /**
   * Storage unit conversion tool.
   */
  public static class HexTransformationUtils {
    public static Integer byteToTb(Long number) {
      Long standard = 1024 * 1024 * 1024 * 1024L;
      Long calculation = (long) (number / standard);
      Integer result = calculation.intValue();
      return result;
    }

    public static Integer gbToTb(Integer number) {
      Integer standard = 1024;
      Integer result = (int) (number / standard);
      return result;
    }

    public static Integer kbToGb(Long number) {
      Integer standard = 1024 * 1024;
      Integer result = (int) (number / standard);
      return result;
    }
  }
}
