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

package io.confluent.ksql.serde.util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.util.KsqlException;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;

public class SerdeUtilsTest {

  @Test
  public void shouldConvertToBooleanCorrectly() {
    final Boolean b = SerdeUtils.toBoolean(true);
    assertThat(b, equalTo(true));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingNonBooleanToBoolean() {
    SerdeUtils.toBoolean(1);
  }

  @Test
  public void shouldConvertToIntCorrectly() {
    final Integer i = SerdeUtils.toInteger(1);
    assertThat(i, equalTo(1));
  }

  @Test
  public void shouldConvertLongToIntCorrectly() {
    final Integer i = SerdeUtils.toInteger(1L);
    assertThat(i, equalTo(1));
  }

  @Test
  public void shouldConvertDoubleToIntCorrectly() {
    final Integer i = SerdeUtils.toInteger(1.0);
    assertThat(i, equalTo(1));
  }

  @Test
  public void shouldConvertStringToIntCorrectly() {
    final Integer i = SerdeUtils.toInteger("1");
    assertThat(i, equalTo(1));
  }

  @Test(expected = KsqlException.class)
  public void shouldNotConvertIncorrectStringToInt() {
    SerdeUtils.toInteger("1!:)");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingNonIntegerToIntegr() {
    SerdeUtils.toInteger(true);
  }

  @Test
  public void shouldConvertToLongCorrectly() {
    final Long l = SerdeUtils.toLong(1L);
    assertThat(l, equalTo(1L));
  }

  @Test
  public void shouldConvertIntToLongCorrectly() {
    final Long l = SerdeUtils.toLong(1);
    assertThat(l, equalTo(1L));
  }

  @Test
  public void shouldConvertDoubleToLongCorrectly() {
    final Long l = SerdeUtils.toLong(1.0);
    assertThat(l, equalTo(1L));
  }

  @Test
  public void shouldConvertStringToLongCorrectly() {
    final Long l = SerdeUtils.toLong("1");
    assertThat(l, equalTo(1L));
  }

  @Test(expected = KsqlException.class)
  public void shouldNotConvertIncorrectStringToLong() {
    SerdeUtils.toLong("1!:)");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingIncompatibleLong() {
    SerdeUtils.toInteger(true);
  }

  @Test
  public void shouldConvertToDoubleCorrectly() {
    final Double d = SerdeUtils.toDouble(1.0);
    assertThat(d, equalTo(1.0));
  }

  @Test
  public void shouldConvertIntToDoubleCorrectly() {
    final Double d = SerdeUtils.toDouble(1);
    assertThat(d, equalTo(1.0));
  }

  @Test
  public void shouldConvertLongToDoubleCorrectly() {
    final Double d = SerdeUtils.toDouble(1L);
    assertThat(d, equalTo(1.0));
  }

  @Test
  public void shouldConvertStringToDoubleCorrectly() {
    final Double d = SerdeUtils.toDouble("1.0");
    assertThat(d, equalTo(1.0));
  }

  @Test
  public void shouldConvertToDecimalCorrectly() {
    final BigDecimal d = SerdeUtils.toDecimal(BigDecimal.valueOf(1.0));
    assertThat(d, equalTo(BigDecimal.valueOf(1.0)));
  }

  @Test
  public void shouldConvertBooleanToDecimalCorrectly() {
    BigDecimal d = SerdeUtils.toDecimal(true);
    assertThat(d, equalTo(BigDecimal.valueOf(1)));

    d = SerdeUtils.toDecimal(false);
    assertThat(d, equalTo(BigDecimal.valueOf(0)));
  }

  @Test
  public void shouldConvertIntToDecimalCorrectly() {
    final BigDecimal d = SerdeUtils.toDecimal(2);
    assertThat(d, equalTo(BigDecimal.valueOf(2)));
  }

  @Test
  public void shouldConvertLongToDecimalCorrectly() {
    final BigDecimal d = SerdeUtils.toDecimal(2L);
    assertThat(d, equalTo(BigDecimal.valueOf(2)));
  }

  @Test
  public void shouldConvertDoubleToDecimalCorrectly() {
    final BigDecimal d = SerdeUtils.toDecimal(2.0);
    assertThat(d, equalTo(BigDecimal.valueOf(2.0)));
  }

  @Test
  public void shouldConvertStringToDecimalCorrectly() {
    final BigDecimal d = SerdeUtils.toDecimal("2.0");
    assertThat(d, equalTo(BigDecimal.valueOf(2.0)));
  }

  @Test(expected = KsqlException.class)
  public void shouldNotConvertIncorrectStringToDecimal() {
    SerdeUtils.toDouble("1!:");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingIncompatibleDecimal() {
    SerdeUtils.toDecimal(new ArrayList<>());
  }

  @Test(expected = KsqlException.class)
  public void shouldNotConvertIncorrectStringToDouble() {
    SerdeUtils.toDouble("1!:)");
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenConvertingIncompatibleDouble() {
    SerdeUtils.toDouble(true);
  }
}
