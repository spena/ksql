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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.util.DecimalUtil;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;


public class ConnectSchemaTranslatorTest {
  private final ConnectSchemaTranslator schemaTranslator = new ConnectSchemaTranslator();

  @Test
  public void shouldTranslatePrimitives() {
    final Schema connectSchema = SchemaBuilder
        .struct()
        .field("intField", Schema.INT32_SCHEMA)
        .field("longField", Schema.INT64_SCHEMA)
        .field("doubleField", Schema.FLOAT64_SCHEMA)
        .field("stringField", Schema.STRING_SCHEMA)
        .field("booleanField", Schema.BOOLEAN_SCHEMA)
        .field("decimalField", DecimalUtil.schema(6, 2))
        .build();

    final Schema ksqlSchema = schemaTranslator.toKsqlSchema(connectSchema);
    assertThat(ksqlSchema.schema().type(), equalTo(Schema.Type.STRUCT));
    assertThat(ksqlSchema.fields().size(), equalTo(connectSchema.fields().size()));
    for (int i = 0; i < ksqlSchema.fields().size(); i++) {
        assertThat(
            ksqlSchema.fields().get(i).name(),
            equalTo(connectSchema.fields().get(i).name().toUpperCase()));
        assertThat(
            ksqlSchema.fields().get(i).schema().type(),
            equalTo(connectSchema.fields().get(i).schema().type()));
        assertThat(ksqlSchema.fields().get(i).schema().isOptional(), is(true));
    }
  }

  @Test
  public void shouldTranslateMaps() {
    final Schema connectSchema = SchemaBuilder
        .struct()
        .field("mapField", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA))
        .build();

    final Schema ksqlSchema = schemaTranslator.toKsqlSchema(connectSchema);
    assertThat(ksqlSchema.field("MAPFIELD"), notNullValue());
    final Schema mapSchema = ksqlSchema.field("MAPFIELD").schema();
    assertThat(mapSchema.type(), equalTo(Schema.Type.MAP));
    assertThat(mapSchema.keySchema(), equalTo(Schema.OPTIONAL_STRING_SCHEMA));
    assertThat(mapSchema.valueSchema(), equalTo(Schema.OPTIONAL_INT32_SCHEMA));
  }

  @Test
  public void shouldTranslateStructInsideMap() {
    final Schema connectSchema = SchemaBuilder
        .struct()
        .field(
            "mapField",
            SchemaBuilder.map(
                Schema.STRING_SCHEMA,
                SchemaBuilder.struct()
                    .field("innerIntField", Schema.INT32_SCHEMA)
                    .build()))
        .build();

    final Schema ksqlSchema = schemaTranslator.toKsqlSchema(connectSchema);
    assertThat(ksqlSchema.field("MAPFIELD"), notNullValue());
    final Schema mapSchema = ksqlSchema.field("MAPFIELD").schema();
    assertThat(mapSchema.type(), equalTo(Schema.Type.MAP));
    assertThat(mapSchema.isOptional(), is(true));
    assertThat(mapSchema.keySchema(), equalTo(Schema.OPTIONAL_STRING_SCHEMA));
    assertThat(mapSchema.valueSchema().type(), equalTo(Schema.Type.STRUCT));
    assertThat(mapSchema.valueSchema().fields().size(), equalTo(1));
    assertThat(mapSchema.valueSchema().fields().get(0).name(), equalTo("INNERINTFIELD"));
    assertThat(mapSchema.valueSchema().fields().get(0).schema(), equalTo(Schema.OPTIONAL_INT32_SCHEMA));
  }

  @Test
  public void shouldTranslateArray() {
    final Schema connectSchema = SchemaBuilder
        .struct()
        .field("arrayField", SchemaBuilder.array(Schema.INT32_SCHEMA))
        .build();

    final Schema ksqlSchema = schemaTranslator.toKsqlSchema(connectSchema);
    assertThat(ksqlSchema.field("ARRAYFIELD"), notNullValue());
    final Schema arraySchema = ksqlSchema.field("ARRAYFIELD").schema();
    assertThat(arraySchema.type(), equalTo(Schema.Type.ARRAY));
    assertThat(arraySchema.isOptional(), is(true));
    assertThat(arraySchema.valueSchema(), equalTo(Schema.OPTIONAL_INT32_SCHEMA));
  }

  @Test
  public void shouldTranslateStructInsideArray() {
    final Schema connectSchema = SchemaBuilder
        .struct()
        .field(
            "arrayField",
            SchemaBuilder.array(
                SchemaBuilder.struct()
                    .field("innerIntField", Schema.OPTIONAL_INT32_SCHEMA)
                    .build()))
        .build();

    final Schema ksqlSchema = schemaTranslator.toKsqlSchema(connectSchema);
    assertThat(ksqlSchema.field("ARRAYFIELD"), notNullValue());
    final Schema arraySchema = ksqlSchema.field("ARRAYFIELD").schema();
    assertThat(arraySchema.type(), equalTo(Schema.Type.ARRAY));
    assertThat(arraySchema.valueSchema().type(), equalTo(Schema.Type.STRUCT));
    assertThat(arraySchema.valueSchema().fields().size(), equalTo(1));
    assertThat(arraySchema.valueSchema().fields().get(0).name(), equalTo("INNERINTFIELD"));
    assertThat(arraySchema.valueSchema().fields().get(0).schema(), equalTo(Schema.OPTIONAL_INT32_SCHEMA));
  }

  @Test
  public void shouldTranslateNested() {
    final Schema connectInnerSchema = SchemaBuilder
        .struct()
        .field("intField", Schema.INT32_SCHEMA)
        .build();
    final Schema connectSchema = SchemaBuilder
        .struct()
        .field("structField", connectInnerSchema)
        .build();

    final Schema ksqlSchema = schemaTranslator.toKsqlSchema(connectSchema);
    assertThat(ksqlSchema.field("STRUCTFIELD"), notNullValue());
    final Schema innerSchema = ksqlSchema.field("STRUCTFIELD").schema();
    assertThat(innerSchema.fields().size(), equalTo(connectInnerSchema.fields().size()));
    for (int i = 0; i < connectInnerSchema.fields().size(); i++) {
      assertThat(
          innerSchema.fields().get(i).name().toUpperCase(),
          equalTo(connectInnerSchema.fields().get(i).name().toUpperCase()));
      assertThat(
          innerSchema.fields().get(i).schema().type(),
          equalTo(connectInnerSchema.fields().get(i).schema().type()));
      assertThat(innerSchema.fields().get(i).schema().isOptional(), is(true));
    }
  }

  @Test
  public void shouldTranslateMapWithNonStringKey() {
    final Schema connectSchema = SchemaBuilder
        .struct()
        .field("mapfield", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA))
        .build();

    final Schema ksqlSchema = schemaTranslator.toKsqlSchema(connectSchema);

    assertThat(ksqlSchema.field("MAPFIELD"), notNullValue());
    final Schema mapSchema = ksqlSchema.field("MAPFIELD").schema();
    assertThat(mapSchema.type(), equalTo(Schema.Type.MAP));
    assertThat(mapSchema.keySchema(), equalTo(Schema.OPTIONAL_STRING_SCHEMA));
    assertThat(mapSchema.valueSchema(), equalTo(Schema.OPTIONAL_INT32_SCHEMA));
  }

  @Test
  public void shouldIgnoreUnsupportedType() {
    final Schema connectSchema = SchemaBuilder
        .struct()
        .field("bytesField", Schema.BYTES_SCHEMA)
        .build();

    final Schema ksqlSchema = schemaTranslator.toKsqlSchema(connectSchema);
    assertThat(ksqlSchema.fields().size(), equalTo(0));
  }

  @Test
  public void shouldIgnoreDecimalsWithUnknownScale() {
    final String TEST_SCALE = "2";

    final Schema connectSchema = SchemaBuilder
        .struct()
        .field("decimalField", SchemaBuilder.bytes().name(Decimal.LOGICAL_NAME)
            .parameter("invalid.scale", TEST_SCALE).build())
        .build();

    final Schema ksqlSchema = schemaTranslator.toKsqlSchema(connectSchema);
    assertThat(ksqlSchema.fields().size(), equalTo(0));
  }

  @Test
  public void shouldIgnoreDecimalsWithUnsetParameters() {

    final Schema connectSchema = SchemaBuilder
        .struct()
        .field("decimalField", SchemaBuilder.bytes().name(Decimal.LOGICAL_NAME).build())
        .build();

    final Schema ksqlSchema = schemaTranslator.toKsqlSchema(connectSchema);
    assertThat(ksqlSchema.fields().size(), equalTo(0));
  }

  @Test
  public void shouldIgnoreDecimalsWithInvalidScale() {
    final String TEST_SCALE = "-1";

    final Schema connectSchema = SchemaBuilder
        .struct()
        .field("decimalField", Decimal.schema(Integer.parseInt(TEST_SCALE)))
        .build();

    final Schema ksqlSchema = schemaTranslator.toKsqlSchema(connectSchema);
    assertThat(ksqlSchema.fields().size(), equalTo(0));
  }

  @Test
  public void shouldIgnoreDecimalsWithInvalidPrecision() {
    final String TEST_PRECISION = "0";
    final String TEST_SCALE = "0";

    final Schema connectSchema = SchemaBuilder
        .struct()
        .field("decimalField", Decimal.builder(Integer.parseInt(TEST_SCALE))
            .parameter("connect.decimal.precision", TEST_PRECISION).build())
        .build();

    final Schema ksqlSchema = schemaTranslator.toKsqlSchema(connectSchema);
    assertThat(ksqlSchema.fields().size(), equalTo(0));
  }

  @Test
  public void shouldIgnoreDecimalsWithPrecisionLowerThanScale() {
    final String TEST_PRECISION = "1";
    final String TEST_SCALE = "2";

    final Schema connectSchema = SchemaBuilder
        .struct()
        .field("decimalField", Decimal.builder(Integer.parseInt(TEST_SCALE))
            .parameter("connect.decimal.precision", TEST_PRECISION).build())
        .build();

    final Schema ksqlSchema = schemaTranslator.toKsqlSchema(connectSchema);
    assertThat(ksqlSchema.fields().size(), equalTo(0));
  }

  @Test
  public void shouldTranslateDecimalsWithUnknownPrecision() {
    final String DEFAULT_CONNECT_PRECISION = "64";
    final String TEST_SCALE = "2";

    final Schema connectSchema = SchemaBuilder
        .struct()
        .field("decimalField", Decimal.schema(Integer.parseInt(TEST_SCALE)))
        .build();

    final Schema ksqlSchema = schemaTranslator.toKsqlSchema(connectSchema);
    assertThat(ksqlSchema.schema().type(), equalTo(Schema.Type.STRUCT));
    assertThat(ksqlSchema.fields().size(), equalTo(connectSchema.fields().size()));

    for (int i = 0; i < ksqlSchema.fields().size(); i++) {
      assertThat(
          ksqlSchema.fields().get(i).name(),
          equalTo(connectSchema.fields().get(i).name().toUpperCase()));
      assertThat(
          ksqlSchema.fields().get(i).schema().type(),
          equalTo(Schema.BYTES_SCHEMA.type())
      );
      assertThat(ksqlSchema.fields().get(i).schema().isOptional(), is(true));
      assertThat(ksqlSchema.fields().get(i).schema().name(), equalTo(Decimal.LOGICAL_NAME));
      assertThat(
          ksqlSchema.fields().get(i).schema().parameters().get("scale"),
          equalTo(TEST_SCALE)
      );
      assertThat(
          ksqlSchema.fields().get(i).schema().parameters().get("precision"),
          equalTo(DEFAULT_CONNECT_PRECISION)
      );
    }
  }

  @Test
  public void shouldTranslateDecimalsWithKnownPrecision() {
    final String TEST_PRECISION = "6";
    final String TEST_SCALE = "2";

    // SR/Connect will set the precision in the connect.decimal.precision parameter
    // KSQL should convert it to 'precision' so it matches AVRO parameters
    final Schema connectSchema = SchemaBuilder
        .struct()
        .field("decimalField", Decimal.builder(Integer.parseInt(TEST_SCALE))
            .parameter("connect.decimal.precision", TEST_PRECISION).build())
        .build();

    final Schema ksqlSchema = schemaTranslator.toKsqlSchema(connectSchema);
    assertThat(ksqlSchema.schema().type(), equalTo(Schema.Type.STRUCT));
    assertThat(ksqlSchema.fields().size(), equalTo(connectSchema.fields().size()));

    for (int i = 0; i < ksqlSchema.fields().size(); i++) {
      assertThat(
          ksqlSchema.fields().get(i).name(),
          equalTo(connectSchema.fields().get(i).name().toUpperCase()));
      assertThat(
          ksqlSchema.fields().get(i).schema().type(),
          equalTo(Schema.BYTES_SCHEMA.type())
      );
      assertThat(ksqlSchema.fields().get(i).schema().isOptional(), is(true));
      assertThat(ksqlSchema.fields().get(i).schema().name(), equalTo(Decimal.LOGICAL_NAME));
      assertThat(
          ksqlSchema.fields().get(i).schema().parameters().get("scale"),
          equalTo(TEST_SCALE)
      );
      assertThat(
          ksqlSchema.fields().get(i).schema().parameters().get("precision"),
          equalTo(TEST_PRECISION)
      );
    }
  }
}
