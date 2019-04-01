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

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Utility functions to handle Decimal types.
 */
public final class DecimalUtil {
  public static final String PRECISION_FIELD = "precision";
  public static final String SCALE_FIELD = Decimal.SCALE_FIELD;

  private DecimalUtil() {

  }

  /**
   * Returns true if the specified {@link Schema} is a valid Decimal schema.
   * </p>
   * A valid Decimal schema is based on the Connect {@link Decimal} logical name to keep a standard
   * across Connect and KSQL decimals.
   * @param schema The schema to check for Decimal name
   * @return True if the schema is a valid Decimal type; False otherwise
   */
  public static boolean isDecimalSchema(final Schema schema) {
    if (schema == null) {
      return false;
    }

    // Use the Connect Decimal name because Schema does not have a type for decimals
    return (schema.name() != null) ? schema.name().equalsIgnoreCase(Decimal.LOGICAL_NAME) : false;
  }

  /**
   * Returns the precision of a Decimal schema.
   * </p>
   * The precision is obtained from the 'precision' key found on the {@link Schema}
   * parameters list.
   *
   * @param schema The schema to get the precision from
   * @return A numeric precision value
   */
  public static Integer getPrecision(final Schema schema) {
    Preconditions.checkArgument(isDecimalSchema(schema),
        "Schema is not a valid Decimal schema: " + schema);

    return Integer.parseInt(schema.parameters().get(PRECISION_FIELD));
  }

  /**
   * Returns the scale of a Decimal schema.
   * </p>
   * The scale is obtained from the 'scale' key found on the {@link Schema}
   * parameters list.
   *
   * @param schema The schema to get the scale from
   * @return A numeric scale value
   */
  public static Integer getScale(final Schema schema) {
    Preconditions.checkArgument(isDecimalSchema(schema),
        "Schema is not a valid Decimal schema: " + schema);

    return Integer.parseInt(schema.parameters().get(SCALE_FIELD));
  }

  /**
   * Builds a Decimal {@link Schema} type using the precision and scale values specified.
   * </p>
   * The schema is created by setting the {@link Decimal} logical name, and adding the
   * precision and scale values as a {@link Schema} parameters.
   * </p>
   * Precision and scale must be valid Decimal values.
   * <ul>
   *   <li>Precision must be >= 1</li>
   *   <li>Precision must be >= scale</li>
   *   <li>Scale must be >= 0</li>
   * </ul>
   *
   * @param precision The precision for the Decimal schema
   * @param scale The scale for the Decimal schema
   * @return A valid Decimal schema
   * @throws KsqlException if precision and scale parameters are not valid Decimal values
   */
  public static Schema schema(final int precision, final int scale) {
    validateParameters(precision, scale);
    return builder(precision, scale).build();
  }

  private static SchemaBuilder builder(final int precision, final int scale) {
    // Do not use Decimal.schema(scale). Decimal does not set the precision, the precision
    // parameter must be set first (before scale) so that EntityUtil can obtain the list
    // of parameters in the right order, i.e. DECIMAL(precision, scale)
    return SchemaBuilder.bytes()
        .name(Decimal.LOGICAL_NAME)
        .parameter(PRECISION_FIELD, Integer.toString(precision))
        .parameter(SCALE_FIELD, Integer.toString(scale))
        .optional(); // KSQL only uses optional types
  }

  public static void validateParameters(final int precision, final int scale) {
    /*
     * A DECIMAL schema is defined by two parameters: precision and scale.
     *
     * RULES
     * - The precision value must be higher than 1 and defines the number of digits a decimal value
     *   can hold.
     * - The scale value must be higher than 0 and defines the number of digits at the right of
     *   the decimal point.
     * - The precision value must be higher or equals to the scale.
     */

    if (precision < 1) {
      throw new KsqlException(
          String.format("DECIMAL precision must be >= 1: DECIMAL(%d,%d)", precision, scale));
    }

    if (scale < 0) {
      throw new KsqlException(
          String.format("DECIMAL scale must be >= 0: DECIMAL(%d,%d)", precision, scale));
    }

    if (precision < scale) {
      throw new KsqlException(
          String.format("DECIMAL precision must be >= scale: DECIMAL(%d,%d)", precision, scale));
    }
  }

  /**
   * Forces a BigDecimal value to fit into the specified precision and scale.
   * </p>
   * If the scale is bigger than the value scale, then the decimal is rounded
   * (round up if discarded decimal fraction is >= 0.5).
   * </p>
   * If the left digits length of the decimal point is bigger than the left digits length
   * of the value, then a {@link KsqlException} exception is thrown.
   *
   * @param maxPrecision The maximum precision
   * @param maxScale The maximum scale
   * @param value The value to be enforced to the destination precision and scale
   * @return The original decimal if no adjustment is necessary. Otherwise, the adjusted decimal is
   *         returned
   * @throws KsqlException if the adjustment cannot be done
   */
  public static BigDecimal enforcePrecisionScale(
      final BigDecimal value,
      final int maxPrecision,
      final int maxScale
  ) {
    if (value == null) {
      return null;
    }

    validateParameters(maxPrecision, maxScale);

    final int maxLeftDigits = maxPrecision - maxScale;
    if ((value.precision() - value.scale()) > maxLeftDigits) {
      throw new KsqlException(String.format("Decimal precision/scale for value '%s' does not "
              + "fit into destination precision/scale: %d,%d",
          value.toPlainString(), maxPrecision, maxScale));
    }

    if (value.scale() <= maxScale) {
      return value;
    }

    return value.setScale(maxScale, BigDecimal.ROUND_HALF_UP);
  }
}
