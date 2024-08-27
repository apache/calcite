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
package org.apache.calcite.adapter.innodb;

import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.util.Sources;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.innodb.java.reader.util.Utils;
import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneRules;

import static java.util.Objects.requireNonNull;

/**
 * Tests for the {@code org.apache.calcite.adapter.innodb} package related to data types.
 *
 * <p>Will read InnoDB data file {@code test_types.ibd}.
 */
public class InnodbAdapterDataTypesTest {

  private static final ImmutableMap<String, String> INNODB_MODEL =
      ImmutableMap.of("model",
          Sources.of(
                  requireNonNull(
                      InnodbAdapterTest.class.getResource("/model.json"),
                      "url"))
              .file().getAbsolutePath());

  @Test void testTypesRowType() {
    CalciteAssert.that()
        .with(INNODB_MODEL)
        .query("select * from \"test_types\"")
        .typeIs("[id INTEGER NOT NULL, "
            + "f_tinyint TINYINT NOT NULL, "
            + "f_smallint SMALLINT NOT NULL, "
            + "f_mediumint INTEGER NOT NULL, "
            + "f_int INTEGER NOT NULL, "
            + "f_bigint BIGINT NOT NULL, "
            + "f_datetime TIMESTAMP NOT NULL, "
            + "f_timestamp TIMESTAMP_WITH_LOCAL_TIME_ZONE NOT NULL, "
            + "f_time TIME NOT NULL, "
            + "f_year SMALLINT NOT NULL, "
            + "f_date DATE NOT NULL, "
            + "f_float REAL NOT NULL, "
            + "f_double DOUBLE NOT NULL, "
            + "f_decimal1 DECIMAL NOT NULL, "
            + "f_decimal2 DECIMAL NOT NULL, "
            + "f_decimal3 DECIMAL NOT NULL, "
            + "f_decimal4 DECIMAL NOT NULL, "
            + "f_decimal5 DECIMAL NOT NULL, "
            + "f_decimal6 DECIMAL, "
            + "f_varchar VARCHAR NOT NULL, "
            + "f_varchar_overflow VARCHAR NOT NULL, "
            + "f_varchar_null VARCHAR, "
            + "f_char_32 CHAR NOT NULL, "
            + "f_char_255 CHAR NOT NULL, "
            + "f_char_null CHAR, "
            + "f_boolean BOOLEAN NOT NULL, "
            + "f_bool BOOLEAN NOT NULL, "
            + "f_tinytext VARCHAR NOT NULL, "
            + "f_text VARCHAR NOT NULL, "
            + "f_mediumtext VARCHAR NOT NULL, "
            + "f_longtext VARCHAR NOT NULL, "
            + "f_tinyblob VARBINARY NOT NULL, "
            + "f_blob VARBINARY NOT NULL, "
            + "f_mediumblob VARBINARY NOT NULL, "
            + "f_longblob VARBINARY NOT NULL, "
            + "f_varbinary VARBINARY NOT NULL, "
            + "f_varbinary_overflow VARBINARY NOT NULL, "
            + "f_enum VARCHAR NOT NULL, "
            + "f_set VARCHAR NOT NULL]");
  }

  @Test void testTypesValues() {
    CalciteAssert.that()
        .with(INNODB_MODEL)
        .query("select * from \"test_types\"")
        .returnsOrdered(
            "id=1; "
                + "f_tinyint=100; "
                + "f_smallint=10000; "
                + "f_mediumint=1000000; "
                + "f_int=10000000; "
                + "f_bigint=100000000000; "
                + "f_datetime=2019-10-02 10:59:59; "
                + "f_timestamp=" + expectedLocalTime("1988-11-23 22:10:08") + "; "
                + "f_time=00:36:52; "
                + "f_year=2012; "
                + "f_date=2020-01-29; "
                + "f_float=0.9876543; "
                + "f_double=1.23456789012345E9; "
                + "f_decimal1=123456; "
                + "f_decimal2=12345.67890; "
                + "f_decimal3=12345678901; "
                + "f_decimal4=123.100; "
                + "f_decimal5=12346; "
                + "f_decimal6=12345.1234567890123456789012345; "
                + "f_varchar=c" + StringUtils.repeat('x', 31) + "; "
                + "f_varchar_overflow=c" + StringUtils.repeat("データ", 300) + "; "
                + "f_varchar_null=null; "
                + "f_char_32=c" + StringUtils.repeat("данные", 2) + "; "
                + "f_char_255=c" + StringUtils.repeat("数据", 100) + "; "
                + "f_char_null=null; "
                + "f_boolean=false; "
                + "f_bool=true; "
                + "f_tinytext=c" + StringUtils.repeat("Data", 50) + "; "
                + "f_text=c" + StringUtils.repeat("Daten", 200) + "; "
                + "f_mediumtext=c" + StringUtils.repeat("Datos", 200) + "; "
                + "f_longtext=c" + StringUtils.repeat("Les données", 800) + "; "
                + "f_tinyblob="
                + genByteArrayString("63", (byte) 0x0a, 100) + "; "
                + "f_blob="
                + genByteArrayString("63", (byte) 0x0b, 400) + "; "
                + "f_mediumblob="
                + genByteArrayString("63", (byte) 0x0c, 800) + "; "
                + "f_longblob="
                + genByteArrayString("63", (byte) 0x0d, 1000) + "; "
                + "f_varbinary="
                + genByteArrayString("63", (byte) 0x0e, 8) + "; "
                + "f_varbinary_overflow="
                + genByteArrayString("63", (byte) 0xff, 100) + "; "
                + "f_enum=MYSQL; "
                + "f_set=z",
            "id=2; "
                + "f_tinyint=-100; "
                + "f_smallint=-10000; "
                + "f_mediumint=-1000000; "
                + "f_int=-10000000; "
                + "f_bigint=-9223372036854775807; "
                + "f_datetime=2255-01-01 12:12:12; "
                + "f_timestamp=" + expectedLocalTime("2020-01-01 00:00:00") + "; "
                + "f_time=23:11:00; "
                + "f_year=0; "
                + "f_date=1970-01-01; "
                + "f_float=-1.2345678E7; "
                + "f_double=-1.234567890123456E9; "
                + "f_decimal1=9; "
                + "f_decimal2=-567.89100; "
                + "f_decimal3=987654321; "
                + "f_decimal4=456.000; "
                + "f_decimal5=0; "
                + "f_decimal6=-0.0123456789012345678912345; "
                + "f_varchar=d" + StringUtils.repeat('y', 31) + "; "
                + "f_varchar_overflow=d" + StringUtils.repeat("データ", 300) + "; "
                + "f_varchar_null=null; "
                + "f_char_32=d" + StringUtils.repeat("данные", 2) + "; "
                + "f_char_255=d" + StringUtils.repeat("数据", 100) + "; "
                + "f_char_null=null; "
                + "f_boolean=false; "
                + "f_bool=true; "
                + "f_tinytext=d" + StringUtils.repeat("Data", 50) + "; "
                + "f_text=d" + StringUtils.repeat("Daten", 200) + "; "
                + "f_mediumtext=d" + StringUtils.repeat("Datos", 200) + "; "
                + "f_longtext=d" + StringUtils.repeat("Les données", 800) + "; "
                + "f_tinyblob="
                + genByteArrayString("64", (byte) 0x0a, 100) + "; "
                + "f_blob="
                + genByteArrayString("64", (byte) 0x0b, 400) + "; "
                + "f_mediumblob="
                + genByteArrayString("64", (byte) 0x0c, 800) + "; "
                + "f_longblob="
                + genByteArrayString("64", (byte) 0x0d, 1000) + "; "
                + "f_varbinary="
                + genByteArrayString("64", (byte) 0x0e, 8) + "; "
                + "f_varbinary_overflow="
                + genByteArrayString("64", (byte) 0xff, 100) + "; "
                + "f_enum=Hello; "
                + "f_set=a,e,i,o,u");
  }

  private String genByteArrayString(String prefix, byte b, int repeat) {
    StringBuilder str = new StringBuilder();
    str.append(prefix);
    for (int i = 0; i < repeat; i++) {
      String hexString = Integer.toHexString(b & 0xFF);
      if (hexString.length() < 2) {
        str.append("0");
      }
      str.append(hexString);
    }
    return str.toString();
  }

  private static String expectedLocalTime(String dateTime) {
    ZoneRules rules = ZoneId.systemDefault().getRules();
    LocalDateTime ldt = Utils.parseDateTimeText(dateTime);
    Instant instant = ldt.toInstant(ZoneOffset.of("+00:00"));
    ZoneOffset standardOffset = rules.getOffset(instant);
    OffsetDateTime odt = instant.atOffset(standardOffset);
    return odt.toLocalDateTime().format(Utils.TIME_FORMAT_TIMESTAMP[0]);
  }
}
