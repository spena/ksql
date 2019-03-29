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

package io.confluent.ksql.parser.tree;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.util.KsqlException;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

@Immutable
public final class PrimitiveType extends Type {

  private static final ImmutableMap<SqlType, Function<Optional<List<Integer>>, PrimitiveType>>
      TYPES = ImmutableMap.<SqlType, Function<Optional<List<Integer>>, PrimitiveType>>builder()
      .put(SqlType.BOOLEAN, p -> new PrimitiveType(SqlType.BOOLEAN))
      .put(SqlType.INTEGER, p -> new PrimitiveType(SqlType.INTEGER))
      .put(SqlType.BIGINT,  p -> new PrimitiveType(SqlType.BIGINT))
      .put(SqlType.DOUBLE,  p -> new PrimitiveType(SqlType.DOUBLE))
      .put(SqlType.STRING,  p -> new PrimitiveType(SqlType.STRING))
      .put(SqlType.DECIMAL, p -> {
        final int paramLen = p.orElse(Collections.emptyList()).size();
        if (paramLen != 2) {
          throw new KsqlException("Invalid number of DECIMAL type parameters: " + paramLen);
        }

        return new PrimitiveType(SqlType.DECIMAL, p);
      })
      .build();

  /* Types may have parameters, such as DECIMAL(p, s) and VARCHAR(n) */
  final Optional<List<Integer>> typeParameters;

  public static PrimitiveType of(
      final String typeName,
      final Optional<List<Integer>> typeParameters
  ) {
    switch (typeName) {
      case "BOOLEAN":
        return PrimitiveType.of(SqlType.BOOLEAN);
      case "INT":
        return PrimitiveType.of(SqlType.INTEGER);
      case "VARCHAR":
      case "STRING":
        return PrimitiveType.of(SqlType.STRING);
      case "DECIMAL":
        final int paramSize = typeParameters.orElse(Collections.emptyList()).size();
        if (paramSize != 2) {
          throw new KsqlException("Invalid decimal type parameters length: " + paramSize);
        }

        return new PrimitiveType(Type.SqlType.DECIMAL, typeParameters);
      default:
        try {
          final SqlType sqlType = SqlType.valueOf(typeName.toUpperCase());
          return PrimitiveType.of(sqlType);
        } catch (final IllegalArgumentException e) {
          throw new KsqlException("Unknown primitive type: " + typeName, e);
        }
    }
  }

  public static PrimitiveType of(final SqlType sqlType) {
    final Function<Optional<List<Integer>>, PrimitiveType> primitive =
        TYPES.get(Objects.requireNonNull(sqlType, "sqlType"));
    if (primitive == null) {
      throw new KsqlException("Invalid primitive type: " + sqlType);
    }
    return primitive.apply(Optional.empty());
  }

  public static PrimitiveType of(final SqlType sqlType, final List<Integer> sqlTypeParameters) {
    final Function<Optional<List<Integer>>, PrimitiveType> primitive =
        TYPES.get(Objects.requireNonNull(sqlType, "sqlType"));
    if (primitive == null) {
      throw new KsqlException("Invalid primitive type: " + sqlType);
    }
    return primitive.apply(Optional.ofNullable(sqlTypeParameters));
  }

  private PrimitiveType(final SqlType sqlType) {
    this(sqlType, Optional.empty());
  }

  private PrimitiveType(
      final SqlType ksqlType,
      final Optional<List<Integer>> typeParameters
  ) {
    super(Optional.empty(), ksqlType);
    this.typeParameters = typeParameters;
  }

  public List<Integer> getParameters() {
    if (typeParameters.isPresent()) {
      return typeParameters.get();
    }

    return Collections.emptyList();
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitPrimitiveType(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (o == null) {
      return false;
    }
    if (!(o instanceof PrimitiveType)) {
      return false;
    }

    final PrimitiveType that = (PrimitiveType) o;
    if (!Objects.equals(this.getSqlType(), that.getSqlType())) {
      return false;
    }

    if (that.typeParameters.isPresent() != typeParameters.isPresent()) {
      return false;
    }

    if (that.typeParameters.isPresent()) {
      final List<Integer> typeParameters1 = that.typeParameters.get();
      final List<Integer> typeParameters2 = typeParameters.get();

      if (typeParameters1.size() != typeParameters2.size()) {
        return false;
      }

      for (int i = 0; i < typeParameters1.size(); i++) {
        if (!typeParameters1.get(i).equals(typeParameters2.get(i))) {
          return false;
        }
      }
    }

    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getSqlType(), typeParameters.orElse(Collections.emptyList()));
  }
}
