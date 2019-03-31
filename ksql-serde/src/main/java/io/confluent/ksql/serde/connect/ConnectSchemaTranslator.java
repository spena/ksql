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

package io.confluent.ksql.serde.connect;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectSchemaTranslator {
  private static final Logger log = LoggerFactory.getLogger(ConnectSchemaTranslator.class);

  private static final Map<Type, Function<Schema, Schema>> CONNECT_TO_KSQL =
      ImmutableMap.<Type, Function<Schema, Schema>>builder()
      .put(Type.INT8, s -> Schema.OPTIONAL_INT32_SCHEMA)
      .put(Type.INT16, s -> Schema.OPTIONAL_INT32_SCHEMA)
      .put(Type.INT32, s -> Schema.OPTIONAL_INT32_SCHEMA)
      .put(Type.INT64, s -> Schema.OPTIONAL_INT64_SCHEMA)
      .put(Type.FLOAT32, s -> Schema.OPTIONAL_FLOAT64_SCHEMA)
      .put(Type.FLOAT64, s -> Schema.OPTIONAL_FLOAT64_SCHEMA)
      .put(Type.STRING, s -> Schema.OPTIONAL_STRING_SCHEMA)
      .put(Type.BOOLEAN, s -> Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .put(Type.BYTES, ConnectSchemaTranslator::toKsqlLogicalSchema)
      .put(Type.ARRAY, ConnectSchemaTranslator::toKsqlArraySchema)
      .put(Type.MAP, ConnectSchemaTranslator::toKsqlMapSchema)
      .put(Type.STRUCT, ConnectSchemaTranslator::toKsqlStructSchema)
      .build();

  // Schema Registry does not set the precision parameter if the value on the AVRO schema
  // matches the default precision value that SR uses.
  // SR sets the default to 64 (see Connect AvroData.java for more info).
  private static final String CONNECT_DECIMAL_PRECISION_FIELD = "connect.decimal.precision";
  private static final String CONNECT_DECIMAL_PRECISION_DEFAULT = "64";

  protected static class UnsupportedTypeException extends RuntimeException {
    UnsupportedTypeException(final String error) {
      super(error);
    }
  }

  @SuppressWarnings("MethodMayBeStatic") // Part of injectable API.
  public Schema toKsqlSchema(final Schema schema) {
    try {
      final Schema rowSchema = toKsqlFieldSchema(schema);
      if (rowSchema.type() != Schema.Type.STRUCT) {
        throw new KsqlException("KSQL stream/table schema must be structured");
      }
      return rowSchema;
    } catch (final UnsupportedTypeException e) {
      throw new KsqlException("Unsupported type at root of schema: " + e.getMessage(), e);
    }
  }

  private static Schema toKsqlFieldSchema(final Schema schema) {
    final Function<Schema, Schema> handler = CONNECT_TO_KSQL.get(schema.type());
    if (handler == null) {
      throw new UnsupportedTypeException(
          String.format("Unsupported type: %s", schema.type().getName()));
    }

    return handler.apply(schema);
  }

  private static Schema toKsqlLogicalSchema(final Schema schema) {
    // Converts to optional DECIMAL schema
    if (DecimalUtil.isDecimalSchema(schema)) {
      return toKsqlDecimalSchema(schema);
    }

    throw new UnsupportedTypeException(
        String.format("Unsupported type: %s", schema.type().getName()));
  }

  private static Schema toKsqlDecimalSchema(final Schema schema) {
    /*
     * A Decimal Schema must have 2 parameters, precision and scale.
     *
     * Scale is always set by Schema Registry as a parameter named 'scale'.
     *
     * Precision is optional for Schema Registry, and it is set only
     * if the precision value read from AVRO does not match the default SR precision
     * which is 64. The parameter is named 'connect.decimal.precision'
     *
     */

    if (schema.parameters() == null) {
      throw new UnsupportedTypeException(
          String.format("Unknown decimal parameters: %s", schema.name()));
    }

    final Map<String, String> parameters = schema.parameters();

    try {
      // Throw an exception if scale is not set instead of setting a default value.
      // SR/Connect API has changed the precision parameter name twice in the past,
      // and if it changes the scale name as well then we won't find a bug if
      // a default value is used.
      // Note: It would be good for SR/Connect API to expose the precision and scale
      // parameter names as well as default values.
      final int scale =
          Integer.parseInt(
              Optional.ofNullable(parameters.get(Decimal.SCALE_FIELD))
                  .orElseThrow(() -> new UnsupportedTypeException(
                      String.format("Unknown decimal scale: %s", schema.name())))
          );

      final int precision =
          Integer.parseInt(
              Optional.ofNullable(parameters.get(CONNECT_DECIMAL_PRECISION_FIELD))
                  .orElse(CONNECT_DECIMAL_PRECISION_DEFAULT)
          );

      // Set precision as a new name 'precision' parameter to match AVRO parameters
      return DecimalUtil.schema(precision, scale);
    } catch (NumberFormatException e) {
      throw new UnsupportedTypeException(
          String.format("Invalid decimal parameters: %s", e.getMessage()));
    } catch (KsqlException e) {
      throw new UnsupportedTypeException(
          String.format("Invalid decimal parameters: %s", e.getMessage()));
    }
  }

  private static void checkMapKeyType(final Schema schema) {
    switch (schema.type()) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case BOOLEAN:
      case STRING:
        return;
      case BYTES:
        if (DecimalUtil.isDecimalSchema(schema)) {
          return;
        }

        throw new UnsupportedTypeException("Unsupported type for map key: "
            + schema.type().getName());
      default:
        throw new UnsupportedTypeException("Unsupported type for map key: "
            + schema.type().getName());
    }
  }

  private static Schema toKsqlMapSchema(final Schema schema) {
    final Schema keySchema = toKsqlFieldSchema(schema.keySchema());
    checkMapKeyType(keySchema);
    return SchemaBuilder.map(
        Schema.OPTIONAL_STRING_SCHEMA,
        toKsqlFieldSchema(schema.valueSchema())
    ).optional().build();
  }

  private static Schema toKsqlArraySchema(final Schema schema) {
    return SchemaBuilder.array(
        toKsqlFieldSchema(schema.valueSchema())
    ).optional().build();
  }

  private static Schema toKsqlStructSchema(final Schema schema) {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    for (final Field field : schema.fields()) {
      try {
        final Schema fieldSchema = toKsqlFieldSchema(field.schema());
        schemaBuilder.field(field.name().toUpperCase(), fieldSchema);
      } catch (final UnsupportedTypeException e) {
        log.error("Error inferring schema at field {}: {}", field.name(), e.getMessage());
      }
    }
    return schemaBuilder.optional().build();
  }
}
