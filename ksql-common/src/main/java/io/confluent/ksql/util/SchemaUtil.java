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

import static org.apache.avro.Schema.create;
import static org.apache.avro.Schema.createArray;
import static org.apache.avro.Schema.createMap;
import static org.apache.avro.Schema.createUnion;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.codehaus.jackson.node.IntNode;

public final class SchemaUtil {

  private static final String DEFAULT_NAMESPACE = "ksql";

  public static final String DECIMAL = "DECIMAL";
  public static final String ARRAY = "ARRAY";
  public static final String MAP = "MAP";
  public static final String STRUCT = "STRUCT";

  public static final String ROWKEY_NAME = "ROWKEY";
  public static final String ROWTIME_NAME = "ROWTIME";
  public static final int ROWKEY_NAME_INDEX = 1;

  private static final String AVRO_LOGICAL_TYPE_PARAM = "logicalType";
  private static final String AVRO_LOGICAL_DECIMAL_TYPE = "decimal";

  private static final Map<Type, Supplier<SchemaBuilder>> typeToSchema
      = ImmutableMap.<Type, Supplier<SchemaBuilder>>builder()
      .put(String.class, () -> SchemaBuilder.string().optional())
      .put(boolean.class, SchemaBuilder::bool)
      .put(Boolean.class, () -> SchemaBuilder.bool().optional())
      .put(Integer.class, () -> SchemaBuilder.int32().optional())
      .put(int.class, SchemaBuilder::int32)
      .put(Long.class, () -> SchemaBuilder.int64().optional())
      .put(long.class, SchemaBuilder::int64)
      .put(Double.class, () -> SchemaBuilder.float64().optional())
      .put(double.class, SchemaBuilder::float64)
      .build();

  private static final Ordering<Schema.Type> ARITHMETIC_TYPE_ORDERING = Ordering.explicit(
      ImmutableList.of(
          Schema.Type.INT8,
          Schema.Type.INT16,
          Schema.Type.INT32,
          Schema.Type.INT64,
          Schema.Type.FLOAT32,
          Schema.Type.FLOAT64
      )
  );

  private static final NavigableMap<Schema.Type, Schema> TYPE_TO_SCHEMA =
      ImmutableSortedMap.<Schema.Type, Schema>orderedBy(ARITHMETIC_TYPE_ORDERING)
          .put(Schema.Type.INT32, Schema.OPTIONAL_INT32_SCHEMA)
          .put(Schema.Type.INT64, Schema.OPTIONAL_INT64_SCHEMA)
          .put(Schema.Type.FLOAT32, Schema.OPTIONAL_FLOAT64_SCHEMA)
          .put(Schema.Type.FLOAT64, Schema.OPTIONAL_FLOAT64_SCHEMA)
          .build();

  private static final Map<Schema.Type, Function<Schema, Type>> TYPE_TO_JAVA_TYPE =
      ImmutableMap.<Schema.Type, Function<Schema, Type>>builder()
          .put(Schema.Type.STRING, s -> String.class)
          .put(Schema.Type.BOOLEAN, s -> Boolean.class)
          .put(Schema.Type.INT32, s -> Integer.class)
          .put(Schema.Type.INT64, s -> Long.class)
          .put(Schema.Type.FLOAT64, s -> Double.class)
          .put(Schema.Type.BYTES, SchemaUtil::toLogicalJavaType)
          .put(Schema.Type.ARRAY, s -> List.class)
          .put(Schema.Type.MAP, s -> Map.class)
          .put(Schema.Type.STRUCT, s -> Struct.class)
          .build();

  private static Map<Schema.Type, Function<Schema, String>> TYPE_TO_SQL_STRING =
      ImmutableMap.<Schema.Type, Function<Schema, String>>builder()
          .put(Schema.Type.INT32, s -> "INT")
          .put(Schema.Type.INT64, s -> "BIGINT")
          .put(Schema.Type.FLOAT32, s -> "DOUBLE")
          .put(Schema.Type.FLOAT64, s -> "DOUBLE")
          .put(Schema.Type.BOOLEAN, s -> "BOOLEAN")
          .put(Schema.Type.STRING, s -> "VARCHAR")
          .put(Schema.Type.BYTES, SchemaUtil::toLogicalTypeName)
          .put(Schema.Type.ARRAY, SchemaUtil::toArrayTypeName)
          .put(Schema.Type.MAP, SchemaUtil::toMapTypeName)
          .put(Schema.Type.STRUCT, SchemaUtil::toStructTypeName)
          .build();

  private static Map<String, Function<String, Schema>> SQL_STRING_TO_SCHEMA =
      ImmutableMap.<String, Function<String, Schema>>builder()
          .put("VARCHAR", s -> Schema.OPTIONAL_STRING_SCHEMA)
          .put("STRING", s -> Schema.OPTIONAL_STRING_SCHEMA)
          .put("BOOLEAN", s -> Schema.OPTIONAL_BOOLEAN_SCHEMA)
          .put("BOOL", s -> Schema.OPTIONAL_BOOLEAN_SCHEMA)
          .put("INTEGER", s -> Schema.OPTIONAL_INT32_SCHEMA)
          .put("INT", s -> Schema.OPTIONAL_INT32_SCHEMA)
          .put("BIGINT", s -> Schema.OPTIONAL_INT64_SCHEMA)
          .put("LONG", s -> Schema.OPTIONAL_INT64_SCHEMA)
          .put("DOUBLE", s -> Schema.OPTIONAL_FLOAT64_SCHEMA)
          .put("DECIMAL", SchemaUtil::getDecimalSchema)
          .put("DEC", SchemaUtil::getDecimalSchema)
          .put("ARRAY", SchemaUtil::getArraySchema)
          .put("MAP", SchemaUtil::getMapSchema)
          .build();

  private SchemaUtil() {
  }

  public static Class getJavaType(final Schema schema) {
    final Function<Schema, Type> handler = TYPE_TO_JAVA_TYPE.get(schema.type());
    if (handler == null) {
      throw new KsqlException("Type is not supported: " + schema.type());
    }

    return (Class) handler.apply(schema);
  }

  private static Class toLogicalJavaType(final Schema schema) {
    if (DecimalUtil.isDecimalSchema(schema)) {
      return BigDecimal.class;
    }

    throw new KsqlException("Type is not supported: " + schema.type());
  }

  public static Schema getSchemaFromType(final Type type) {
    return getSchemaFromType(type, null, null);
  }

  public static Schema getSchemaFromType(final Type type, final String name, final String doc) {
    final SchemaBuilder schema =
        typeToSchema.getOrDefault(type, () -> handleParametrizedType(type)).get();

    schema.name(name);
    schema.doc(doc);
    return schema.build();
  }

  public static boolean matchFieldName(final Field field, final String fieldName) {
    return field.name().equals(fieldName)
        || field.name().equals(fieldName.substring(fieldName.indexOf(".") + 1));
  }

  public static Optional<Field> getFieldByName(final Schema schema, final String fieldName) {
    return schema.fields()
        .stream()
        .filter(f -> matchFieldName(f, fieldName))
        .findFirst();
  }

  public static Schema getTypeSchema(final String sqlType) {
    // Get the simple type form in case there are () or <> in the type
    final String simpleType = sqlType.split("<|\\(")[0].trim();

    final Function<String, Schema> handler = SQL_STRING_TO_SCHEMA.get(simpleType);
    if (handler == null) {
      throw new KsqlException("Unsupported type: " + sqlType);
    }

    return handler.apply(sqlType);
  }

  private static Schema getDecimalSchema(final String sqlType) {
    // DECIMAL must use explicit parameters
    if (sqlType.equals(DECIMAL)) {
      throw new KsqlException("DECIMAL is not defined correctly: " + sqlType);
    }

    final String[] decimalParams = sqlType
        .substring(DECIMAL.length() + 1, sqlType.length() - 1)
        .trim()
        .split(",");

    if (decimalParams.length != 2) {
      throw new KsqlException("DECIMAL is not defined correctly: " + sqlType);
    }

    try {
      final int precision = Integer.parseInt(decimalParams[0].trim());
      final int scale = Integer.parseInt(decimalParams[1].trim());

      return DecimalUtil.schema(precision, scale);
    } catch (NumberFormatException e) {
      throw new KsqlException("DECIMAL is not defined correctly: " + sqlType);
    }
  }

  private static Schema getArraySchema(final String sqlType) {
    return SchemaBuilder.array(
        getTypeSchema(
            sqlType.substring(
                ARRAY.length() + 1,
                sqlType.length() - 1
            )
        )
    ).optional().build();
  }

  private static Schema getMapSchema(final String sqlType) {
    //TODO: For now only primitive data types for map are supported. Will have to add nested
    // types.
    final String[] mapTypesStrs = sqlType
        .substring(MAP.length() + 1, sqlType.length() - 1)
        .trim()
        .split(",");
    if (mapTypesStrs.length != 2) {
      throw new KsqlException("Map type is not defined correctly: " + sqlType);
    }
    final String keyType = mapTypesStrs[0].trim();
    final String valueType = mapTypesStrs[1].trim();
    return SchemaBuilder.map(getTypeSchema(keyType), getTypeSchema(valueType))
        .optional().build();
  }

  public static int getFieldIndexByName(final Schema schema, final String fieldName) {
    if (schema.fields() == null) {
      return -1;
    }
    for (int i = 0; i < schema.fields().size(); i++) {
      final Field field = schema.fields().get(i);
      final int dotIndex = field.name().indexOf('.');
      if (dotIndex == -1) {
        if (field.name().equals(fieldName)) {
          return i;
        }
      } else {
        if (dotIndex < fieldName.length()) {
          final String
              fieldNameWithDot =
              fieldName.substring(0, dotIndex) + "." + fieldName.substring(dotIndex + 1);
          if (field.name().equals(fieldNameWithDot)) {
            return i;
          }
        }
      }

    }
    return -1;
  }

  public static Schema buildSchemaWithAlias(final Schema schema, final String alias) {
    final SchemaBuilder newSchema = SchemaBuilder.struct().name(schema.name());
    for (final Field field : schema.fields()) {
      newSchema.field((alias + "." + field.name()), field.schema());
    }
    return newSchema.build();
  }

  private static final ImmutableMap<String, String> TYPE_MAP =
      new ImmutableMap.Builder<String, String>()
          .put("STRING", "VARCHAR(STRING)")
          .put("INT64", "BIGINT")
          .put("INT32", "INTEGER")
          .put("FLOAT64", "DOUBLE")
          .put("BOOLEAN", "BOOLEAN")
          .put("ARRAY", "ARRAY")
          .put("MAP", "MAP")
          .put("STRUCT", "STRUCT")
          .build();

  public static String getSchemaTypeAsSqlType(final Schema.Type type) {
    final String sqlType = TYPE_MAP.get(type.name());
    if (sqlType == null) {
      throw new IllegalArgumentException("Unknown schema type: " + type);
    }

    return sqlType;
  }

  public static String getJavaCastString(final Schema schema) {
    switch (schema.type()) {
      case INT32:
        return "(Integer)";
      case INT64:
        return "(Long)";
      case FLOAT64:
        return "(Double)";
      case STRING:
        return "(String)";
      case BOOLEAN:
        return "(Boolean)";
      default:
        //TODO: Add complex types later!
        return "";
    }
  }

  public static Schema addImplicitRowTimeRowKeyToSchema(final Schema schema) {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.field(SchemaUtil.ROWTIME_NAME, Schema.OPTIONAL_INT64_SCHEMA);
    schemaBuilder.field(SchemaUtil.ROWKEY_NAME, Schema.OPTIONAL_STRING_SCHEMA);
    for (final Field field : schema.fields()) {
      if (!field.name().equals(SchemaUtil.ROWKEY_NAME)
          && !field.name().equals(SchemaUtil.ROWTIME_NAME)) {
        schemaBuilder.field(field.name(), field.schema());
      }
    }
    return schemaBuilder.build();
  }

  public static Schema removeImplicitRowTimeRowKeyFromSchema(final Schema schema) {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    for (final Field field : schema.fields()) {
      String fieldName = field.name();
      fieldName = fieldName.substring(fieldName.indexOf('.') + 1);
      if (!fieldName.equalsIgnoreCase(SchemaUtil.ROWTIME_NAME)
          && !fieldName.equalsIgnoreCase(SchemaUtil.ROWKEY_NAME)) {
        schemaBuilder.field(fieldName, field.schema());
      }
    }
    return schemaBuilder.build();
  }

  public static Set<Integer> getRowTimeRowKeyIndexes(final Schema schema) {
    final Set<Integer> indexSet = new HashSet<>();
    for (int i = 0; i < schema.fields().size(); i++) {
      final Field field = schema.fields().get(i);
      if (field.name().equalsIgnoreCase(SchemaUtil.ROWTIME_NAME)
          || field.name().equalsIgnoreCase(SchemaUtil.ROWKEY_NAME)) {
        indexSet.add(i);
      }
    }
    return indexSet;
  }

  public static String getSchemaDefinitionString(final Schema schema) {
    return schema.fields().stream()
        .map(field -> field.name() + " : " + getSqlTypeName(field.schema()))
        .collect(Collectors.joining(", ", "[", "]"));
  }

  private static String toLogicalTypeName(final Schema schema) {
    if (DecimalUtil.isDecimalSchema(schema)) {
      return getDecimalString(schema);
    }

    throw new KsqlException(String.format("Invalid type in schema: %s.", schema.toString()));
  }

  private static String getDecimalString(final Schema schema) {
    return "DECIMAL("
        + DecimalUtil.getPrecision(schema)
        + ","
        + DecimalUtil.getScale(schema)
        + ")";
  }

  private static String toArrayTypeName(final Schema schema) {
    return "ARRAY<" + getSqlTypeName(schema.valueSchema()) + ">";
  }

  private static String toMapTypeName(final Schema schema) {
    return "MAP<"
        + getSqlTypeName(schema.keySchema())
        + ","
        + getSqlTypeName(schema.valueSchema())
        + ">";
  }

  private static String toStructTypeName(final Schema schema) {
    return getStructString(schema);
  }

  public static String getSqlTypeName(final Schema schema) {
    final Function<Schema, String> handler = TYPE_TO_SQL_STRING.get(schema.type());
    if (handler == null) {
      throw new KsqlException(String.format("Invalid type in schema: %s.", schema.toString()));
    }

    return handler.apply(schema);
  }

  private static String getStructString(final Schema schema) {
    return schema.fields().stream()
        .map(field -> field.name() + " " + getSqlTypeName(field.schema()))
        .collect(Collectors.joining(", ", "STRUCT<", ">"));
  }

  public static org.apache.avro.Schema buildAvroSchema(final Schema schema, final String name) {
    return buildAvroSchema(DEFAULT_NAMESPACE, name, schema);
  }

  private static org.apache.avro.Schema buildAvroSchema(
      final String namespace,
      final String name,
      final Schema schema
  ) {
    final String avroName = avroify(name);
    final FieldAssembler<org.apache.avro.Schema> fieldAssembler = org.apache.avro.SchemaBuilder
        .record(avroName).namespace(namespace)
        .fields();

    for (final Field field : schema.fields()) {
      final String fieldName = avroify(field.name());
      final String fieldNamespace = namespace + "." + avroName;

      fieldAssembler
          .name(fieldName)
          .type(getAvroSchemaForField(fieldNamespace, fieldName, field.schema()))
          .withDefault(null);
    }

    return fieldAssembler.endRecord();
  }

  private static String avroify(final String name) {
    return name
        .replace(".", "_")
        .replace("-", "_");
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  private static org.apache.avro.Schema getAvroSchemaForField(
      final String namespace,
      final String fieldName,
      final Schema fieldSchema
  ) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    switch (fieldSchema.type()) {
      case STRING:
        return unionWithNull(create(org.apache.avro.Schema.Type.STRING));
      case BOOLEAN:
        return unionWithNull(create(org.apache.avro.Schema.Type.BOOLEAN));
      case INT32:
        return unionWithNull(create(org.apache.avro.Schema.Type.INT));
      case INT64:
        return unionWithNull(create(org.apache.avro.Schema.Type.LONG));
      case FLOAT64:
        return unionWithNull(create(org.apache.avro.Schema.Type.DOUBLE));
      case BYTES:
        if (DecimalUtil.isDecimalSchema(fieldSchema)) {
          return unionWithNull(createAvroDecimal(fieldSchema));
        }

        throw new KsqlException("Unsupported AVRO type: " + fieldSchema.type().name());
      case ARRAY:
        return unionWithNull(createArray(
            getAvroSchemaForField(namespace, fieldName, fieldSchema.valueSchema())));
      case MAP:
        return unionWithNull(createMap(
            getAvroSchemaForField(namespace, fieldName, fieldSchema.valueSchema())));
      case STRUCT:
        return unionWithNull(buildAvroSchema(namespace, fieldName, fieldSchema));
      default:
        throw new KsqlException("Unsupported AVRO type: " + fieldSchema.type().name());
    }
  }

  private static org.apache.avro.Schema createAvroDecimal(final Schema schema) {
    final org.apache.avro.Schema avroSchema =
        org.apache.avro.SchemaBuilder.builder().bytesType();

    final int precision = DecimalUtil.getPrecision(schema);
    final int scale = DecimalUtil.getScale(schema);

    avroSchema.addProp(AVRO_LOGICAL_TYPE_PARAM, AVRO_LOGICAL_DECIMAL_TYPE);
    avroSchema.addProp(DecimalUtil.PRECISION_FIELD, new IntNode(precision));
    avroSchema.addProp(DecimalUtil.SCALE_FIELD, new IntNode(scale));

    return avroSchema;
  }

  private static org.apache.avro.Schema unionWithNull(final org.apache.avro.Schema schema) {
    return createUnion(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL), schema);
  }

  /**
   * Rename field names to be consistent with the internal column names.
   */
  public static Schema getAvroSerdeKsqlSchema(final Schema schema) {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    for (final Field field : schema.fields()) {
      schemaBuilder.field(field.name().replace(".", "_"), field.schema());
    }

    return schemaBuilder.build();
  }

  public static String getFieldNameWithNoAlias(final Field field) {
    final String name = field.name();
    if (name.contains(".")) {
      return name.substring(name.indexOf(".") + 1);
    } else {
      return name;
    }
  }

  /**
   * Remove the alias when reading/writing from outside
   */
  public static Schema getSchemaWithNoAlias(final Schema schema) {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    for (final Field field : schema.fields()) {
      final String name = getFieldNameWithNoAlias(field);
      schemaBuilder.field(name, field.schema());
    }
    return schemaBuilder.build();
  }

  public static boolean areEqualSchemas(final Schema schema1, final Schema schema2) {
    if (schema1.fields().size() != schema2.fields().size()) {
      return false;
    }
    for (int i = 0; i < schema1.fields().size(); i++) {
      if (!schema1.fields().get(i).equals(schema2.fields().get(i))) {
        return false;
      }
    }
    return true;
  }

  public static int getIndexInSchema(final String fieldName, final Schema schema) {
    final List<Field> fields = schema.fields();
    for (int i = 0; i < fields.size(); i++) {
      final Field field = fields.get(i);
      if (field.name().equals(fieldName)) {
        return i;
      }
    }
    throw new KsqlException(
        "Couldn't find field with name="
            + fieldName
            + " in schema. fields="
            + fields
    );
  }

  public static Schema resolveBinaryOperatorResultType(final Schema.Type left,
                                                       final Schema.Type right) {
    if (left == Schema.Type.STRING && right == Schema.Type.STRING) {
      return Schema.OPTIONAL_STRING_SCHEMA;
    }

    if (!TYPE_TO_SCHEMA.containsKey(left) || !TYPE_TO_SCHEMA.containsKey(right)) {
      throw new KsqlException("Unsupported arithmetic types. " + left + " " + right);
    }

    return TYPE_TO_SCHEMA.ceilingEntry(ARITHMETIC_TYPE_ORDERING.max(left, right)).getValue();
  }

  static boolean isNumber(final Schema.Type type) {
    return type == Schema.Type.INT32
        || type == Schema.Type.INT64
        || type == Schema.Type.FLOAT64
        ;
  }

  private static SchemaBuilder handleParametrizedType(final Type type) {
    if (type instanceof ParameterizedType) {
      final ParameterizedType parameterizedType = (ParameterizedType) type;
      if (parameterizedType.getRawType() == Map.class) {
        return SchemaBuilder.map(getSchemaFromType(
            parameterizedType.getActualTypeArguments()[0]),
            getSchemaFromType(parameterizedType.getActualTypeArguments()[1]));
      } else if (parameterizedType.getRawType() == List.class) {
        return SchemaBuilder.array(getSchemaFromType(
            parameterizedType.getActualTypeArguments()[0]));
      }
    }
    throw new KsqlException("Type is not supported: " + type);
  }
}
