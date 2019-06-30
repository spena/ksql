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

import io.confluent.ksql.services.DefaultServiceContext;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.security.Principal;

/**
 * Creates a ServiceContext with user's credentials that is used by KSQL to access remote services
 * with the user permission context.
 * </p>
 * This factory relies on a {@link KsqlSecurityExtension} to provide the remote clients with
 * the user permission context.
 */
public class UserServiceContextFactory {
  private final KsqlConfig ksqlConfig;
  private final KsqlSecurityExtension securityExtension;

  public UserServiceContextFactory(
      final KsqlConfig ksqlConfig,
      final KsqlSecurityExtension securityExtension
  ) {
    this.ksqlConfig = ksqlConfig;
    this.securityExtension = securityExtension;
  }

  /**
   * Creates a ServiceContext with the {@code Principal} user context. If no client context is
   * provided by the security extension, then a default ServiceContext using KSQL default
   * permissions is returned.
   *
   * @param principal The user principal
   * @return A ServiceContext
   */
  public ServiceContext create(final Principal principal) {
    return securityExtension.getUserClientContext(principal)
        .map(clientContext ->
            create(clientContext))
        .orElse(
            createDefault());
  }

  private ServiceContext createDefault() {
    return DefaultServiceContext.create(ksqlConfig);
  }

  private ServiceContext create(final KsqlUserClientContext clientContext) {
    return DefaultServiceContext.create(
        ksqlConfig,
        clientContext.getKafkaClientSupplier(),
        clientContext.getSchemaRegistryClientSupplier()
    );
  }
}
