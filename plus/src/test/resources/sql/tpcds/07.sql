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

SELECT I_ITEM_ID,
        AVG(SS_QUANTITY) AGG1,
        AVG(SS_LIST_PRICE) AGG2,
        AVG(SS_COUPON_AMT) AGG3,
        AVG(SS_SALES_PRICE) AGG4
 FROM STORE_SALES, CUSTOMER_DEMOGRAPHICS, DATE_DIM, ITEM, PROMOTION
 WHERE SS_SOLD_DATE_SK = D_DATE_SK AND
       SS_ITEM_SK = I_ITEM_SK AND
       SS_CDEMO_SK = CD_DEMO_SK AND
       SS_PROMO_SK = P_PROMO_SK AND
       CD_GENDER = 'F' AND
       CD_MARITAL_STATUS = 'W' AND
       CD_EDUCATION_STATUS = 'Primary' AND
       (P_CHANNEL_EMAIL = 'N' OR P_CHANNEL_EVENT = 'N') AND
       D_YEAR = 1998
 GROUP BY I_ITEM_ID
 ORDER BY I_ITEM_ID
 LIMIT 100
