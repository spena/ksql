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

package io.confluent.ksql.rest.server.security;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.util.function.Supplier;
import org.apache.kafka.streams.KafkaClientSupplier;

/**
 * Provides access to clients required to communicate with remote services using the context of
 * a specified user name and/or credentials.
 * <p/>
 * This context is used by KSQL to access Kafka and/or Schema Registry resources when a user
 * executes a KSQL command so it can access those resources using the same user's permissions.
 */
public interface KsqlUserClientContext {
  /**
   * Constructs a {@link org.apache.kafka.streams.KafkaClientSupplier} with the specified user's
   * credentials.
   *
   * {@link org.apache.kafka.streams.KafkaClientSupplier}.
   */
  KafkaClientSupplier getKafkaClientSupplier();

  /**
   * Constructs a {@link io.confluent.kafka.schemaregistry.client.SchemaRegistryClient} supplier
   * with the specified user's credentials.
   *
   * {@link io.confluent.kafka.schemaregistry.client.SchemaRegistryClient} supplier.
   */
  Supplier<SchemaRegistryClient> getSchemaRegistryClientSupplier();
}
