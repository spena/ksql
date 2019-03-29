/*
 * Copyright 2019 Confluent Inc.
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

import java.math.BigDecimal;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;


public final class DecimalUtil {
  public static final String PRECISION_FIELD = "precision";
  public static final String SCALE_FIELD = Decimal.SCALE_FIELD;

  private DecimalUtil() {

  }

  public static Schema schema(final int precision, final int scale) {
    return builder(precision, scale).build();
  }

  private static SchemaBuilder builder(final int precision, final int scale) {
    // Do not use Decimal.schema(scale). The Precision parameter must be set
    // first (before scale) so that EntityUtil can obtain the list of parameters
    // in the right order, i.e. DECIMAL(precision, scale)
    return SchemaBuilder.bytes()
        .name(Decimal.LOGICAL_NAME)
        .parameter(PRECISION_FIELD, Integer.toString(precision))
        .parameter(SCALE_FIELD, Integer.toString(scale))
        .optional(); // KSQL only uses optional types
  }

  public static Integer getPrecision(final Schema schema) {
    return Integer.parseInt(schema.parameters().get(PRECISION_FIELD));
  }

  public static Integer getScale(final Schema schema) {
    return Integer.parseInt(schema.parameters().get(SCALE_FIELD));
  }

  public static boolean isDecimalSchema(final Schema schema) {
    if (schema == null) {
      return false;
    }

    // Use the Connect Decimal name because Schema does not have a type for decimals
    return (schema.name() != null) ? schema.name().equalsIgnoreCase(Decimal.LOGICAL_NAME) : false;
  }

  /**
   * Forces a BigDecimal value to fit into the specified precision and scale.
   * </p>
   * If the scale is bigger than the value scale, then the decimal is rounded
   * (round up if discarded decimal fraction is >= 0.5).
   * </p>
   * If the left digits length of the decimal point is bigger than the left digits length
   * of the value, then a NULL is returned.
   *
   * @param maxPrecision The maximum precision
   * @param maxScale The maximum scale
   * @param value The value to be enforced to the destination precision and scale
   * @return The original decimal if no adjustment is necessary. Otherwise, null if the
   *         decimal does not fit within left maxPrecision or rounding if the decimal does
   *         not fit withing maxScale
   */
  public static BigDecimal enforcePrecisionScale(
      final BigDecimal value,
      final int maxPrecision,
      final int maxScale
  ) {
    if (maxPrecision < 1) {
      throw new IllegalArgumentException("Decimal precision must be greater than or equal to 1");
    }

    if (maxScale < 0) {
      throw new IllegalArgumentException("Decimal scale must be greater than or equal to 0");
    }

    if (maxPrecision < maxScale) {
      throw new IllegalArgumentException("Decimal scale must be less than or equal to precision");
    }

    if (value == null) {
      return null;
    }

    final int maxLeftDigits = maxPrecision - maxScale;
    if ((value.precision() - value.scale()) > maxLeftDigits) {
      throw new KsqlException(String.format("Decimal precision/scale of value '%s' does not "
              + "fit into destination precision/scale of %d,%d",
          value.toPlainString(), maxPrecision, maxScale));
    }

    if (value.scale() <= maxScale) {
      return value;
    }

    return value.setScale(maxScale, BigDecimal.ROUND_HALF_UP);
  }
}
