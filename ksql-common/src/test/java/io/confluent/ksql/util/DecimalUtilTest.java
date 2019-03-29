/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

import java.math.BigDecimal;

public class DecimalUtilTest {
  @Test
  public void shouldReturnOptionalDecimalSchema() {
    final int anyValue = 1;

    assertThat(DecimalUtil.schema(anyValue, anyValue).isOptional(), is(true));
  }

  @Test
  public void shouldReturnTrueOnConnectDecimalSchema() {
    final int anyValue = 1;

    assertThat(DecimalUtil.isDecimalSchema(DecimalUtil.schema(anyValue, anyValue)), is(true));
  }

  @Test
  public void shouldReturnFalseOnUnknownConnectDecimalSchema() {
    assertThat(DecimalUtil.isDecimalSchema(null), is(false));
    assertThat(DecimalUtil.isDecimalSchema(SchemaBuilder.bytes().build()), is(false));
    assertThat(DecimalUtil.isDecimalSchema(SchemaBuilder.bytes().name("NoConnectDecimal")), is(false));
  }

  @Test
  public void shouldReturnPrecisionAndScaleSchemaParameters() {
    final int precision = 6;
    final int scale = 2;

    assertThat(DecimalUtil.getPrecision(DecimalUtil.schema(precision, scale)), is(precision));
    assertThat(DecimalUtil.getScale(DecimalUtil.schema(precision, scale)), is(scale));
  }

  @Test
  public void shouldReturnOriginalValuesIfScaleFitsIntoMaxPrecisionScale() {
    final int maxPrecision = 6;
    final int maxScale = 2;

    assertThat(DecimalUtil.enforcePrecisionScale(
        new BigDecimal("10.01"), maxPrecision, maxScale), is(new BigDecimal("10.01")));

    assertThat(DecimalUtil.enforcePrecisionScale(
        new BigDecimal("10.1"), maxPrecision, maxScale), is(new BigDecimal("10.1")));

    assertThat(DecimalUtil.enforcePrecisionScale(
        new BigDecimal("100.01"), maxPrecision, maxScale), is(new BigDecimal("100.01")));

    assertThat(DecimalUtil.enforcePrecisionScale(
        new BigDecimal("1000.01"), maxPrecision, maxScale), is(new BigDecimal("1000.01")));

    assertThat(DecimalUtil.enforcePrecisionScale(
        new BigDecimal(".01"), maxPrecision, maxScale), is(new BigDecimal(".01")));

    assertThat(DecimalUtil.enforcePrecisionScale(
        new BigDecimal("10"), maxPrecision, maxScale), is(new BigDecimal("10")));

    assertThat(DecimalUtil.enforcePrecisionScale(
        null, maxPrecision, maxScale), is(nullValue()));
  }

  @Test
  public void shouldReturnRoundedValueIfScaleDoesNotFitIntoMaxPrecisionScale() {
    final int maxPrecision = 6;
    final int maxScale = 2;

    assertThat(DecimalUtil.enforcePrecisionScale(
        new BigDecimal("10.012"), maxPrecision, maxScale), is(new BigDecimal("10.01")));

    assertThat(DecimalUtil.enforcePrecisionScale(
        new BigDecimal("10.0129"), maxPrecision, maxScale), is(new BigDecimal("10.01")));

    assertThat(DecimalUtil.enforcePrecisionScale(
        new BigDecimal("10.019"), maxPrecision, maxScale), is(new BigDecimal("10.02")));
  }

  @Test
  public void shouldThrowErrorIfPrecisionScaleDoesNotFitIntoMaxPrecisionScale() {
    final int maxPrecision = 6;
    final int maxScale = 2;

    try {
      DecimalUtil.enforcePrecisionScale(new BigDecimal("12345"), maxPrecision, maxScale);
      fail("KsqlException is expected if decimal precision/scale is larger than max. allowed");
    } catch (KsqlException e) {
      // pass
    }
  }

  @Test
  public void shouldThrowErrorIfMaximumPrecisionScaleAreOutOfRange() {
    try {
      DecimalUtil.enforcePrecisionScale(new BigDecimal("1"), 0, 0);
      fail("IllegalArgumentException is expected if max. precision/scale are out of range");
    } catch (IllegalArgumentException e) {
      // pass
    }

    try {
      DecimalUtil.enforcePrecisionScale(new BigDecimal("1"), -1, 0);
      fail("IllegalArgumentException is expected if max. precision/scale are out of range");
    } catch (IllegalArgumentException e) {
      // pass
    }

    try {
      DecimalUtil.enforcePrecisionScale(new BigDecimal("1"), 1, -1);
      fail("IllegalArgumentException is expected if max. precision/scale are out of range");
    } catch (IllegalArgumentException e) {
      // pass
    }
  }
}
