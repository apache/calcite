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
package org.apache.calcite.test;

import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * A collection of queries about a foodmart.
 */
public class FoodmartQueries {

  private static final String[] QUERIES = {
    "select count(*) from (select 1 as \"c0\" from \"salary\" as \"salary\") as \"init\"",
    "EXPR$0=21252\n",
    "select count(*) from (select 1 as \"c0\" from \"salary\" as \"salary2\") as \"init\"",
    "EXPR$0=21252\n",
    "select count(*) from (select 1 as \"c0\" from \"department\" as \"department\") as \"init\"",
    "EXPR$0=12\n",
    "select count(*) from (select 1 as \"c0\" from \"employee\" as \"employee\") as \"init\"",
    "EXPR$0=1155\n",
    "select count(*) from (select 1 as \"c0\" from \"employee_closure\" as \"employee_closure\") as \"init\"",
    "EXPR$0=7179\n",
    "select count(*) from (select 1 as \"c0\" from \"position\" as \"position\") as \"init\"",
    "EXPR$0=18\n",
    "select count(*) from (select 1 as \"c0\" from \"promotion\" as \"promotion\") as \"init\"",
    "EXPR$0=1864\n",
    "select count(*) from (select 1 as \"c0\" from \"store\" as \"store\") as \"init\"",
    "EXPR$0=25\n",
    "select count(*) from (select 1 as \"c0\" from \"product\" as \"product\") as \"init\"",
    "EXPR$0=1560\n",
    "select count(*) from (select 1 as \"c0\" from \"product_class\" as \"product_class\") as \"init\"",
    "EXPR$0=110\n",
    "select count(*) from (select 1 as \"c0\" from \"time_by_day\" as \"time_by_day\") as \"init\"",
    "EXPR$0=730\n",
    "select count(*) from (select 1 as \"c0\" from \"customer\" as \"customer\") as \"init\"",
    "EXPR$0=10281\n",
    "select count(*) from (select 1 as \"c0\" from \"sales_fact_1997\" as \"sales_fact_1997\") as \"init\"",
    "EXPR$0=86837\n",
    "select count(*) from (select 1 as \"c0\" from \"inventory_fact_1997\" as \"inventory_fact_1997\") as \"init\"",
    "EXPR$0=4070\n",
    "select count(*) from (select 1 as \"c0\" from \"warehouse\" as \"warehouse\") as \"init\"",
    "EXPR$0=24\n",
    "select count(*) from (select 1 as \"c0\" from \"agg_c_special_sales_fact_1997\" as \"agg_c_special_sales_fact_1997\") as \"init\"",
    "EXPR$0=86805\n",
    "select count(*) from (select 1 as \"c0\" from \"agg_pl_01_sales_fact_1997\" as \"agg_pl_01_sales_fact_1997\") as \"init\"",
    "EXPR$0=86829\n",
    "select count(*) from (select 1 as \"c0\" from \"agg_l_05_sales_fact_1997\" as \"agg_l_05_sales_fact_1997\") as \"init\"",
    "EXPR$0=86154\n",
    "select count(*) from (select 1 as \"c0\" from \"agg_g_ms_pcat_sales_fact_1997\" as \"agg_g_ms_pcat_sales_fact_1997\") as \"init\"",
    "EXPR$0=2637\n",
    "select count(*) from (select 1 as \"c0\" from \"agg_c_14_sales_fact_1997\" as \"agg_c_14_sales_fact_1997\") as \"init\"",
    "EXPR$0=86805\n",
    "select \"time_by_day\".\"the_year\" as \"c0\" from \"time_by_day\" as \"time_by_day\" group by \"time_by_day\".\"the_year\" order by \"time_by_day\".\"the_year\" ASC",
    "c0=1997\n"
        + "c0=1998\n",
    "select \"store\".\"store_country\" as \"c0\" from \"store\" as \"store\" where UPPER(\"store\".\"store_country\") = UPPER('USA') group by \"store\".\"store_country\" order by \"store\".\"store_country\" ASC",
    "c0=USA\n",
    "select \"store\".\"store_state\" as \"c0\" from \"store\" as \"store\" where (\"store\".\"store_country\" = 'USA') and UPPER(\"store\".\"store_state\") = UPPER('CA') group by \"store\".\"store_state\" order by \"store\".\"store_state\" ASC",
    "c0=CA\n",
    "select \"store\".\"store_city\" as \"c0\", \"store\".\"store_state\" as \"c1\" from \"store\" as \"store\" where (\"store\".\"store_state\" = 'CA' and \"store\".\"store_country\" = 'USA') and UPPER(\"store\".\"store_city\") = UPPER('Los Angeles') group by \"store\".\"store_city\", \"store\".\"store_state\" order by \"store\".\"store_city\" ASC",
    "c0=Los Angeles; c1=CA\n",
    "select \"customer\".\"country\" as \"c0\" from \"customer\" as \"customer\" where UPPER(\"customer\".\"country\") = UPPER('USA') group by \"customer\".\"country\" order by \"customer\".\"country\" ASC",
    "c0=USA\n",
    "select \"customer\".\"state_province\" as \"c0\", \"customer\".\"country\" as \"c1\" from \"customer\" as \"customer\" where (\"customer\".\"country\" = 'USA') and UPPER(\"customer\".\"state_province\") = UPPER('CA') group by \"customer\".\"state_province\", \"customer\".\"country\" order by \"customer\".\"state_province\" ASC",
    "c0=CA; c1=USA\n",
    "select \"customer\".\"city\" as \"c0\", \"customer\".\"country\" as \"c1\", \"customer\".\"state_province\" as \"c2\" from \"customer\" as \"customer\" where (\"customer\".\"country\" = 'USA' and \"customer\".\"state_province\" = 'CA' and \"customer\".\"country\" = 'USA' and \"customer\".\"state_province\" = 'CA' and \"customer\".\"country\" = 'USA') and UPPER(\"customer\".\"city\") = UPPER('Los Angeles') group by \"customer\".\"city\", \"customer\".\"country\", \"customer\".\"state_province\" order by \"customer\".\"city\" ASC",
    "c0=Los Angeles; c1=USA; c2=CA\n",
    "select \"store\".\"store_country\" as \"c0\" from \"store\" as \"store\" where UPPER(\"store\".\"store_country\") = UPPER('Gender') group by \"store\".\"store_country\" order by \"store\".\"store_country\" ASC",
    "",
    "select \"store\".\"store_type\" as \"c0\" from \"store\" as \"store\" where UPPER(\"store\".\"store_type\") = UPPER('Gender') group by \"store\".\"store_type\" order by \"store\".\"store_type\" ASC",
    "",
    "select \"product_class\".\"product_family\" as \"c0\" from \"product\" as \"product\", \"product_class\" as \"product_class\" where \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" and UPPER(\"product_class\".\"product_family\") = UPPER('Gender') group by \"product_class\".\"product_family\" order by \"product_class\".\"product_family\" ASC",
    "",
    "select \"promotion\".\"media_type\" as \"c0\" from \"promotion\" as \"promotion\" where UPPER(\"promotion\".\"media_type\") = UPPER('Gender') group by \"promotion\".\"media_type\" order by \"promotion\".\"media_type\" ASC",
    "",
    "select \"promotion\".\"promotion_name\" as \"c0\" from \"promotion\" as \"promotion\" where UPPER(\"promotion\".\"promotion_name\") = UPPER('Gender') group by \"promotion\".\"promotion_name\" order by \"promotion\".\"promotion_name\" ASC",
    "",
    "select \"promotion\".\"media_type\" as \"c0\" from \"promotion\" as \"promotion\" where UPPER(\"promotion\".\"media_type\") = UPPER('No Media') group by \"promotion\".\"media_type\" order by \"promotion\".\"media_type\" ASC",
    "c0=No Media\n",
    "select \"promotion\".\"media_type\" as \"c0\" from \"promotion\" as \"promotion\" group by \"promotion\".\"media_type\" order by \"promotion\".\"media_type\" ASC",
    "c0=Bulk Mail\n"
        + "c0=Cash Register Handout\n"
        + "c0=Daily Paper\n"
        + "c0=Daily Paper, Radio\n"
        + "c0=Daily Paper, Radio, TV\n"
        + "c0=In-Store Coupon\n"
        + "c0=No Media\n"
        + "c0=Product Attachment\n"
        + "c0=Radio\n"
        + "c0=Street Handout\n"
        + "c0=Sunday Paper\n"
        + "c0=Sunday Paper, Radio\n"
        + "c0=Sunday Paper, Radio, TV\n"
        + "c0=TV\n",
    "select count(distinct \"the_year\") from \"time_by_day\"",
    "EXPR$0=2\n",
    "select \"time_by_day\".\"the_year\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\" where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 group by \"time_by_day\".\"the_year\"",
    "c0=1997; m0=266773.0000\n",
    "select \"time_by_day\".\"the_year\" as \"c0\", \"promotion\".\"media_type\" as \"c1\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", \"promotion\" as \"promotion\" where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 and \"sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\" group by \"time_by_day\".\"the_year\", \"promotion\".\"media_type\"",
    "c0=1997; c1=Bulk Mail; m0=4320.0000\n"
        + "c0=1997; c1=Radio; m0=2454.0000\n"
        + "c0=1997; c1=Street Handout; m0=5753.0000\n"
        + "c0=1997; c1=TV; m0=3607.0000\n"
        + "c0=1997; c1=No Media; m0=195448.0000\n"
        + "c0=1997; c1=In-Store Coupon; m0=3798.0000\n"
        + "c0=1997; c1=Sunday Paper, Radio, TV; m0=2726.0000\n"
        + "c0=1997; c1=Product Attachment; m0=7544.0000\n"
        + "c0=1997; c1=Daily Paper; m0=7738.0000\n"
        + "c0=1997; c1=Cash Register Handout; m0=6697.0000\n"
        + "c0=1997; c1=Daily Paper, Radio; m0=6891.0000\n"
        + "c0=1997; c1=Daily Paper, Radio, TV; m0=9513.0000\n"
        + "c0=1997; c1=Sunday Paper, Radio; m0=5945.0000\n"
        + "c0=1997; c1=Sunday Paper; m0=4339.0000\n",
    "select \"store\".\"store_country\" as \"c0\", sum(\"inventory_fact_1997\".\"supply_time\") as \"m0\" from \"store\" as \"store\", \"inventory_fact_1997\" as \"inventory_fact_1997\" where \"inventory_fact_1997\".\"store_id\" = \"store\".\"store_id\" group by \"store\".\"store_country\"",
    "c0=USA; m0=10425\n",
    "select \"sn\".\"desc\" as \"c0\" from (SELECT * FROM (VALUES (1, 'SameName')) AS \"t\" (\"id\", \"desc\")) as \"sn\" group by \"sn\".\"desc\" order by \"sn\".\"desc\" ASC NULLS LAST",
    "c0=SameName\n",
    "select \"the_year\", count(*) as c, min(\"the_month\") as m\n"
        + "from \"foodmart2\".\"time_by_day\"\n"
        + "group by \"the_year\"\n"
        + "order by 1, 2",
    "the_year=1997; C=365; M=April\n"
        + "the_year=1998; C=365; M=April\n",
    "select\n"
        + " \"store\".\"store_state\" as \"c0\",\n"
        + " \"time_by_day\".\"the_year\" as \"c1\",\n"
        + " sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\",\n"
        + " sum(\"sales_fact_1997\".\"store_sales\") as \"m1\"\n"
        + "from \"store\" as \"store\",\n"
        + " \"sales_fact_1997\" as \"sales_fact_1997\",\n"
        + " \"time_by_day\" as \"time_by_day\"\n"
        + "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\"\n"
        + "and \"store\".\"store_state\" in ('DF', 'WA')\n"
        + "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
        + "and \"time_by_day\".\"the_year\" = 1997\n"
        + "group by \"store\".\"store_state\", \"time_by_day\".\"the_year\"",
    "c0=WA; c1=1997; m0=124366.0000; m1=263793.2200\n",
    "select count(distinct \"product_id\") from \"product\"",
    "EXPR$0=1560\n",
    "select \"store\".\"store_name\" as \"c0\",\n"
        + " \"time_by_day\".\"the_year\" as \"c1\",\n"
        + " sum(\"sales_fact_1997\".\"store_sales\") as \"m0\"\n"
        + "from \"store\" as \"store\",\n"
        + " \"sales_fact_1997\" as \"sales_fact_1997\",\n"
        + " \"time_by_day\" as \"time_by_day\"\n"
        + "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\"\n"
        + "and \"store\".\"store_name\" in ('Store 1', 'Store 10', 'Store 11', 'Store 15', 'Store 16', 'Store 24', 'Store 3', 'Store 7')\n"
        + "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
        + "and \"time_by_day\".\"the_year\" = 1997\n"
        + "group by \"store\".\"store_name\",\n"
        + " \"time_by_day\".\"the_year\"\n",
    "c0=Store 7; c1=1997; m0=54545.2800\n"
        + "c0=Store 24; c1=1997; m0=54431.1400\n"
        + "c0=Store 16; c1=1997; m0=49634.4600\n"
        + "c0=Store 3; c1=1997; m0=52896.3000\n"
        + "c0=Store 15; c1=1997; m0=52644.0700\n"
        + "c0=Store 11; c1=1997; m0=55058.7900\n",
    "select \"customer\".\"yearly_income\" as \"c0\","
        + " \"customer\".\"education\" as \"c1\" \n"
        + "from \"customer\" as \"customer\",\n"
        + " \"sales_fact_1997\" as \"sales_fact_1997\"\n"
        + "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\"\n"
        + " and ((not (\"customer\".\"yearly_income\" in ('$10K - $30K', '$50K - $70K'))\n"
        + " or (\"customer\".\"yearly_income\" is null)))\n"
        + "group by \"customer\".\"yearly_income\",\n"
        + " \"customer\".\"education\"\n"
        + "order by \"customer\".\"yearly_income\" ASC NULLS LAST,\n"
        + " \"customer\".\"education\" ASC NULLS LAST",
    "c0=$110K - $130K; c1=Bachelors Degree\n"
        + "c0=$110K - $130K; c1=Graduate Degree\n"
        + "c0=$110K - $130K; c1=High School Degree\n"
        + "c0=$110K - $130K; c1=Partial College\n"
        + "c0=$110K - $130K; c1=Partial High School\n"
        + "c0=$130K - $150K; c1=Bachelors Degree\n"
        + "c0=$130K - $150K; c1=Graduate Degree\n"
        + "c0=$130K - $150K; c1=High School Degree\n"
        + "c0=$130K - $150K; c1=Partial College\n"
        + "c0=$130K - $150K; c1=Partial High School\n"
        + "c0=$150K +; c1=Bachelors Degree\n"
        + "c0=$150K +; c1=Graduate Degree\n"
        + "c0=$150K +; c1=High School Degree\n"
        + "c0=$150K +; c1=Partial College\n"
        + "c0=$150K +; c1=Partial High School\n"
        + "c0=$30K - $50K; c1=Bachelors Degree\n"
        + "c0=$30K - $50K; c1=Graduate Degree\n"
        + "c0=$30K - $50K; c1=High School Degree\n"
        + "c0=$30K - $50K; c1=Partial College\n"
        + "c0=$30K - $50K; c1=Partial High School\n"
        + "c0=$70K - $90K; c1=Bachelors Degree\n"
        + "c0=$70K - $90K; c1=Graduate Degree\n"
        + "c0=$70K - $90K; c1=High School Degree\n"
        + "c0=$70K - $90K; c1=Partial College\n"
        + "c0=$70K - $90K; c1=Partial High School\n"
        + "c0=$90K - $110K; c1=Bachelors Degree\n"
        + "c0=$90K - $110K; c1=Graduate Degree\n"
        + "c0=$90K - $110K; c1=High School Degree\n"
        + "c0=$90K - $110K; c1=Partial College\n"
        + "c0=$90K - $110K; c1=Partial High School\n",
    "ignore:select \"time_by_day\".\"the_year\" as \"c0\", \"product_class\".\"product_family\" as \"c1\", \"customer\".\"state_province\" as \"c2\", \"customer\".\"city\" as \"c3\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", \"product_class\" as \"product_class\", \"product\" as \"product\", \"customer\" as \"customer\" where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\" and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" and \"product_class\".\"product_family\" = 'Drink' and \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" and \"customer\".\"state_province\" = 'WA' and \"customer\".\"city\" in ('Anacortes', 'Ballard', 'Bellingham', 'Bremerton', 'Burien', 'Edmonds', 'Everett', 'Issaquah', 'Kirkland', 'Lynnwood', 'Marysville', 'Olympia', 'Port Orchard', 'Puyallup', 'Redmond', 'Renton', 'Seattle', 'Sedro Woolley', 'Spokane', 'Tacoma', 'Walla Walla', 'Yakima') group by \"time_by_day\".\"the_year\", \"product_class\".\"product_family\", \"customer\".\"state_province\", \"customer\".\"city\"",
    "c0=1997; c1=Drink; c2=WA; c3=Sedro Woolley; m0=58.0000\n",
    "select \"store\".\"store_country\" as \"c0\",\n"
        + " \"time_by_day\".\"the_year\" as \"c1\",\n"
        + " sum(\"sales_fact_1997\".\"store_cost\") as \"m0\",\n"
        + " count(\"sales_fact_1997\".\"product_id\") as \"m1\",\n"
        + " count(distinct \"sales_fact_1997\".\"customer_id\") as \"m2\",\n"
        + " sum((case when \"sales_fact_1997\".\"promotion_id\" = 0 then 0\n"
        + "     else \"sales_fact_1997\".\"store_sales\" end)) as \"m3\"\n"
        + "from \"store\" as \"store\",\n"
        + " \"sales_fact_1997\" as \"sales_fact_1997\",\n"
        + " \"time_by_day\" as \"time_by_day\"\n"
        + "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\"\n"
        + "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
        + "and \"time_by_day\".\"the_year\" = 1997\n"
        + "group by \"store\".\"store_country\", \"time_by_day\".\"the_year\"",
    "c0=USA; c1=1997; m0=225627.2336; m1=86837; m2=5581; m3=151211.2100\n",
      // query 6077
      // disabled (runs out of memory)
    "ignore:select \"time_by_day\".\"the_year\" as \"c0\",\n"
        + " count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\"\n"
        + "from \"time_by_day\" as \"time_by_day\",\n"
        + " \"sales_fact_1997\" as \"sales_fact_1997\",\n"
        + " \"product_class\" as \"product_class\",\n"
        + " \"product\" as \"product\"\n"
        + "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
        + "and \"time_by_day\".\"the_year\" = 1997\n"
        + "and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
        + "and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
        + "and (((\"product\".\"brand_name\" = 'Cormorant'\n"
        + "   and \"product_class\".\"product_subcategory\" = 'Pot Scrubbers'\n"
        + "   and \"product_class\".\"product_category\" = 'Kitchen Products'\n"
        + "   and \"product_class\".\"product_department\" = 'Household'\n"
        + "   and \"product_class\".\"product_family\" = 'Non-Consumable')\n"
        + " or (\"product\".\"brand_name\" = 'Denny'\n"
        + "   and \"product_class\".\"product_subcategory\" = 'Pot Scrubbers'\n"
        + "   and \"product_class\".\"product_category\" = 'Kitchen Products'\n"
        + "   and \"product_class\".\"product_department\" = 'Household'\n"
        + "   and \"product_class\".\"product_family\" = 'Non-Consumable')\n"
        + " or (\"product\".\"brand_name\" = 'High Quality'\n"
        + "   and \"product_class\".\"product_subcategory\" = 'Pot Scrubbers'\n"
        + "   and \"product_class\".\"product_category\" = 'Kitchen Products'\n"
        + "   and \"product_class\".\"product_department\" = 'Household'\n"
        + "   and \"product_class\".\"product_family\" = 'Non-Consumable')\n"
        + " or (\"product\".\"brand_name\" = 'Red Wing'\n"
        + "   and \"product_class\".\"product_subcategory\" = 'Pot Scrubbers'\n"
        + "   and \"product_class\".\"product_category\" = 'Kitchen Products'\n"
        + "   and \"product_class\".\"product_department\" = 'Household'\n"
        + "   and \"product_class\".\"product_family\" = 'Non-Consumable'))\n"
        + " or (\"product_class\".\"product_subcategory\" = 'Pots and Pans'\n"
        + "   and \"product_class\".\"product_category\" = 'Kitchen Products'\n"
        + "   and \"product_class\".\"product_department\" = 'Household'\n"
        + "   and \"product_class\".\"product_family\" = 'Non-Consumable'))\n"
        + "group by \"time_by_day\".\"the_year\"\n",
    "xxtodo",
      // query 6077, simplified
      // disabled (slow)
    "ignore:select count(\"sales_fact_1997\".\"customer_id\") as \"m0\"\n"
        + "from \"sales_fact_1997\" as \"sales_fact_1997\",\n"
        + " \"product_class\" as \"product_class\",\n"
        + " \"product\" as \"product\"\n"
        + "where \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
        + "and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
        + "and ((\"product\".\"brand_name\" = 'Cormorant'\n"
        + "   and \"product_class\".\"product_subcategory\" = 'Pot Scrubbers')\n"
        + " or (\"product_class\".\"product_subcategory\" = 'Pots and Pans'))\n",
    "xxxx",
      // query 6077, simplified further
    "select count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\"\n"
        + "from \"sales_fact_1997\" as \"sales_fact_1997\",\n"
        + " \"product_class\" as \"product_class\",\n"
        + " \"product\" as \"product\"\n"
        + "where \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
        + "and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
        + "and \"product\".\"brand_name\" = 'Cormorant'\n",
    "m0=1298",
      // query 193
    "select \"store\".\"store_country\" as \"c0\",\n"
        + " \"time_by_day\".\"the_year\" as \"c1\",\n"
        + " \"time_by_day\".\"quarter\" as \"c2\",\n"
        + " \"product_class\".\"product_family\" as \"c3\",\n"
        + " count(\"sales_fact_1997\".\"product_id\") as \"m0\",\n"
        + " count(distinct \"sales_fact_1997\".\"customer_id\") as \"m1\"\n"
        + "from \"store\" as \"store\",\n"
        + " \"sales_fact_1997\" as \"sales_fact_1997\",\n"
        + " \"time_by_day\" as \"time_by_day\",\n"
        + " \"product_class\" as \"product_class\",\n"
        + " \"product\" as \"product\"\n"
        + "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\"\n"
        + "and \"store\".\"store_country\" = 'USA'\n"
        + "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
        + "and \"time_by_day\".\"the_year\" = 1997\n"
        + "and \"time_by_day\".\"quarter\" = 'Q3'\n"
        + "and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
        + "and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
        + "and \"product_class\".\"product_family\" = 'Food'\n"
        + "group by \"store\".\"store_country\",\n"
        + " \"time_by_day\".\"the_year\",\n"
        + " \"time_by_day\".\"quarter\",\n"
        + " \"product_class\".\"product_family\"",
    "c0=USA; c1=1997; c2=Q3; c3=Food; m0=15449; m1=2939",
  };

  public static final List<Pair<String, String>> FOODMART_QUERIES =
      querify(QUERIES);

  private FoodmartQueries() {}

  public static List<Pair<String, String>> getFoodmartQueries() {
    return FOODMART_QUERIES;
  }

  /** Returns a list of (query, expected) pairs. The expected result is
   * sometimes null. */
  private static List<Pair<String, String>> querify(String[] queries1) {
    final List<Pair<String, String>> list = new ArrayList<>();
    for (int i = 0; i < queries1.length; i++) {
      String query = queries1[i];
      String expected = null;
      if (i + 1 < queries1.length
          && queries1[i + 1] != null
          && !queries1[i + 1].startsWith("select")) {
        expected = queries1[++i];
      }
      list.add(Pair.of(query, expected));
    }
    return list;
  }
}
