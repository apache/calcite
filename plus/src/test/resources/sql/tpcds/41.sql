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

  select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 811 and 811+40
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and
        (i_color = '[COLOR.1]' or i_color = '[COLOR.2]') and
        (i_units = '[UNIT.1]' or i_units = '[UNIT.2]') and
        (i_size = '[SIZE.1]' or i_size = '[SIZE.2]')
        ) or
        (i_category = 'Women' and
        (i_color = '[COLOR.3]' or i_color = '[COLOR.4]') and
        (i_units = '[UNIT.3]' or i_units = '[UNIT.4]') and
        (i_size = '[SIZE.3]' or i_size = '[SIZE.4]')
        ) or
        (i_category = 'Men' and
        (i_color = '[COLOR.5]' or i_color = '[COLOR.6]') and
        (i_units = '[UNIT.5]' or i_units = '[UNIT.6]') and
        (i_size = '[SIZE.5]' or i_size = '[SIZE.6]')
        ) or
        (i_category = 'Men' and
        (i_color = '[COLOR.7]' or i_color = '[COLOR.8]') and
        (i_units = '[UNIT.7]' or i_units = '[UNIT.8]') and
        (i_size = '[SIZE.1]' or i_size = '[SIZE.2]')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and
        (i_color = '[COLOR.9]' or i_color = '[COLOR.10]') and
        (i_units = '[UNIT.9]' or i_units = '[UNIT.10]') and
        (i_size = '[SIZE.1]' or i_size = '[SIZE.2]')
        ) or
        (i_category = 'Women' and
        (i_color = '[COLOR.11]' or i_color = '[COLOR.12]') and
        (i_units = '[UNIT.11]' or i_units = '[UNIT.12]') and
        (i_size = '[SIZE.3]' or i_size = '[SIZE.4]')
        ) or
        (i_category = 'Men' and
        (i_color = '[COLOR.13]' or i_color = '[COLOR.14]') and
        (i_units = '[UNIT.13]' or i_units = '[UNIT.14]') and
        (i_size = '[SIZE.5]' or i_size = '[SIZE.6]')
        ) or
        (i_category = 'Men' and
        (i_color = '[COLOR.15]' or i_color = '[COLOR.16]') and
        (i_units = '[UNIT.15]' or i_units = '[UNIT.16]') and
        (i_size = '[SIZE.1]' or i_size = '[SIZE.2]')
        )))) > 0
 order by i_product_name
 LIMIT 100
