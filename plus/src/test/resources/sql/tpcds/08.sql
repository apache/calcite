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

  select  s_store_name
      ,sum(ss_net_profit)
 from store_sales
     ,date_dim
     ,store,
     (select ca_zip
     from (
     (SELECT substring(ca_zip,1,5) ca_zip
      FROM customer_address
      WHERE substring(ca_zip,1,5) IN (
                          '[ZIP.1]','[ZIP.2]','[ZIP.3]','[ZIP.4]','[ZIP.5]','[ZIP.6]',
                          '[ZIP.7]','[ZIP.8]','[ZIP.9]','[ZIP.10]','[ZIP.11]',
                          '[ZIP.12]','[ZIP.13]','[ZIP.14]','[ZIP.15]','[ZIP.16]',
                          '[ZIP.17]','[ZIP.18]','[ZIP.19]','[ZIP.20]','[ZIP.21]',
                          '[ZIP.22]','[ZIP.23]','[ZIP.24]','[ZIP.25]','[ZIP.26]',
                          '[ZIP.27]','[ZIP.28]','[ZIP.29]','[ZIP.30]','[ZIP.31]',
                          '[ZIP.32]','[ZIP.33]','[ZIP.34]','[ZIP.35]','[ZIP.36]',
                          '[ZIP.37]','[ZIP.38]','[ZIP.39]','[ZIP.40]','[ZIP.41]',
                          '[ZIP.42]','[ZIP.43]','[ZIP.44]','[ZIP.45]','[ZIP.46]',
                          '[ZIP.47]','[ZIP.48]','[ZIP.49]','[ZIP.50]','[ZIP.51]',
                          '[ZIP.52]','[ZIP.53]','[ZIP.54]','[ZIP.55]','[ZIP.56]',
                          '[ZIP.57]','[ZIP.58]','[ZIP.59]','[ZIP.60]','[ZIP.61]',
                          '[ZIP.62]','[ZIP.63]','[ZIP.64]','[ZIP.65]','[ZIP.66]',
                          '[ZIP.67]','[ZIP.68]','[ZIP.69]','[ZIP.70]','[ZIP.71]',
                          '[ZIP.72]','[ZIP.73]','[ZIP.74]','[ZIP.75]','[ZIP.76]',
                          '[ZIP.77]','[ZIP.78]','[ZIP.79]','[ZIP.80]','[ZIP.81]',
                          '[ZIP.82]','[ZIP.83]','[ZIP.84]','[ZIP.85]','[ZIP.86]',
                          '[ZIP.87]','[ZIP.88]','[ZIP.89]','[ZIP.90]','[ZIP.91]',
                          '[ZIP.92]','[ZIP.93]','[ZIP.94]','[ZIP.95]','[ZIP.96]',
                          '[ZIP.97]','[ZIP.98]','[ZIP.99]','[ZIP.100]','[ZIP.101]',
                          '[ZIP.102]','[ZIP.103]','[ZIP.104]','[ZIP.105]','[ZIP.106]',
                          '[ZIP.107]','[ZIP.108]','[ZIP.109]','[ZIP.110]','[ZIP.111]',
                          '[ZIP.112]','[ZIP.113]','[ZIP.114]','[ZIP.115]','[ZIP.116]',
                          '[ZIP.117]','[ZIP.118]','[ZIP.119]','[ZIP.120]','[ZIP.121]',
                          '[ZIP.122]','[ZIP.123]','[ZIP.124]','[ZIP.125]','[ZIP.126]',
                          '[ZIP.127]','[ZIP.128]','[ZIP.129]','[ZIP.130]','[ZIP.131]',
                          '[ZIP.132]','[ZIP.133]','[ZIP.134]','[ZIP.135]','[ZIP.136]',
                          '[ZIP.137]','[ZIP.138]','[ZIP.139]','[ZIP.140]','[ZIP.141]',
                          '[ZIP.142]','[ZIP.143]','[ZIP.144]','[ZIP.145]','[ZIP.146]',
                          '[ZIP.147]','[ZIP.148]','[ZIP.149]','[ZIP.150]','[ZIP.151]',
                          '[ZIP.152]','[ZIP.153]','[ZIP.154]','[ZIP.155]','[ZIP.156]',
                          '[ZIP.157]','[ZIP.158]','[ZIP.159]','[ZIP.160]','[ZIP.161]',
                          '[ZIP.162]','[ZIP.163]','[ZIP.164]','[ZIP.165]','[ZIP.166]',
                          '[ZIP.167]','[ZIP.168]','[ZIP.169]','[ZIP.170]','[ZIP.171]',
                          '[ZIP.172]','[ZIP.173]','[ZIP.174]','[ZIP.175]','[ZIP.176]',
                          '[ZIP.177]','[ZIP.178]','[ZIP.179]','[ZIP.180]','[ZIP.181]',
                          '[ZIP.182]','[ZIP.183]','[ZIP.184]','[ZIP.185]','[ZIP.186]',
                          '[ZIP.187]','[ZIP.188]','[ZIP.189]','[ZIP.190]','[ZIP.191]',
                          '[ZIP.192]','[ZIP.193]','[ZIP.194]','[ZIP.195]','[ZIP.196]',
                          '[ZIP.197]','[ZIP.198]','[ZIP.199]','[ZIP.200]','[ZIP.201]',
                          '[ZIP.202]','[ZIP.203]','[ZIP.204]','[ZIP.205]','[ZIP.206]',
                          '[ZIP.207]','[ZIP.208]','[ZIP.209]','[ZIP.210]','[ZIP.211]',
                          '[ZIP.212]','[ZIP.213]','[ZIP.214]','[ZIP.215]','[ZIP.216]',
                          '[ZIP.217]','[ZIP.218]','[ZIP.219]','[ZIP.220]','[ZIP.221]',
                          '[ZIP.222]','[ZIP.223]','[ZIP.224]','[ZIP.225]','[ZIP.226]',
                          '[ZIP.227]','[ZIP.228]','[ZIP.229]','[ZIP.230]','[ZIP.231]',
                          '[ZIP.232]','[ZIP.233]','[ZIP.234]','[ZIP.235]','[ZIP.236]',
                          '[ZIP.237]','[ZIP.238]','[ZIP.239]','[ZIP.240]','[ZIP.241]',
                          '[ZIP.242]','[ZIP.243]','[ZIP.244]','[ZIP.245]','[ZIP.246]',
                          '[ZIP.247]','[ZIP.248]','[ZIP.249]','[ZIP.250]','[ZIP.251]',
                          '[ZIP.252]','[ZIP.253]','[ZIP.254]','[ZIP.255]','[ZIP.256]',
                          '[ZIP.257]','[ZIP.258]','[ZIP.259]','[ZIP.260]','[ZIP.261]',
                          '[ZIP.262]','[ZIP.263]','[ZIP.264]','[ZIP.265]','[ZIP.266]',
                          '[ZIP.267]','[ZIP.268]','[ZIP.269]','[ZIP.270]','[ZIP.271]',
                          '[ZIP.272]','[ZIP.273]','[ZIP.274]','[ZIP.275]','[ZIP.276]',
                          '[ZIP.277]','[ZIP.278]','[ZIP.279]','[ZIP.280]','[ZIP.281]',
                          '[ZIP.282]','[ZIP.283]','[ZIP.284]','[ZIP.285]','[ZIP.286]',
                          '[ZIP.287]','[ZIP.288]','[ZIP.289]','[ZIP.290]','[ZIP.291]',
                          '[ZIP.292]','[ZIP.293]','[ZIP.294]','[ZIP.295]','[ZIP.296]',
                          '[ZIP.297]','[ZIP.298]','[ZIP.299]','[ZIP.300]','[ZIP.301]',
                          '[ZIP.302]','[ZIP.303]','[ZIP.304]','[ZIP.305]','[ZIP.306]',
                          '[ZIP.307]','[ZIP.308]','[ZIP.309]','[ZIP.310]','[ZIP.311]',
                          '[ZIP.312]','[ZIP.313]','[ZIP.314]','[ZIP.315]','[ZIP.316]',
                          '[ZIP.317]','[ZIP.318]','[ZIP.319]','[ZIP.320]','[ZIP.321]',
                          '[ZIP.322]','[ZIP.323]','[ZIP.324]','[ZIP.325]','[ZIP.326]',
                          '[ZIP.327]','[ZIP.328]','[ZIP.329]','[ZIP.330]','[ZIP.331]',
                          '[ZIP.332]','[ZIP.333]','[ZIP.334]','[ZIP.335]','[ZIP.336]',
                          '[ZIP.337]','[ZIP.338]','[ZIP.339]','[ZIP.340]','[ZIP.341]',
                          '[ZIP.342]','[ZIP.343]','[ZIP.344]','[ZIP.345]','[ZIP.346]',
                          '[ZIP.347]','[ZIP.348]','[ZIP.349]','[ZIP.350]','[ZIP.351]',
                          '[ZIP.352]','[ZIP.353]','[ZIP.354]','[ZIP.355]','[ZIP.356]',
                          '[ZIP.357]','[ZIP.358]','[ZIP.359]','[ZIP.360]','[ZIP.361]',
                          '[ZIP.362]','[ZIP.363]','[ZIP.364]','[ZIP.365]','[ZIP.366]',
                          '[ZIP.367]','[ZIP.368]','[ZIP.369]','[ZIP.370]','[ZIP.371]',
                          '[ZIP.372]','[ZIP.373]','[ZIP.374]','[ZIP.375]','[ZIP.376]',
                          '[ZIP.377]','[ZIP.378]','[ZIP.379]','[ZIP.380]','[ZIP.381]',
                          '[ZIP.382]','[ZIP.383]','[ZIP.384]','[ZIP.385]','[ZIP.386]',
                          '[ZIP.387]','[ZIP.388]','[ZIP.389]','[ZIP.390]','[ZIP.391]',
                          '[ZIP.392]','[ZIP.393]','[ZIP.394]','[ZIP.395]','[ZIP.396]',
                          '[ZIP.397]','[ZIP.398]','[ZIP.399]','[ZIP.400]'))
     intersect
     (select ca_zip
      from (SELECT substring(ca_zip,1,5) ca_zip,count(*) cnt
            FROM customer_address, customer
            WHERE ca_address_sk = c_current_addr_sk and
                  c_preferred_cust_flag='Y'
            group by ca_zip
            having count(*) > 10)A1))A2) V1
 where ss_store_sk = s_store_sk
  and ss_sold_date_sk = d_date_sk
  and d_qoy = 2 and d_year = 1998
  and (substring(s_zip,1,2) = substring(V1.ca_zip,1,2))
 group by s_store_name
 order by s_store_name
 LIMIT 100
