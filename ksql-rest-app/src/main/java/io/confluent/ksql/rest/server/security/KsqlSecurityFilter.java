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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.security.auth.client.rest.RestClient;
import io.confluent.security.auth.client.rest.exceptions.RestClientException;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizeResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

// TODO: Do I need to implement ContainerResponseFilter?
public class KsqlSecurityFilter implements ContainerRequestFilter {
  private static final Logger log = LoggerFactory.getLogger(KsqlSecurityFilter.class);

  @Context
  @SuppressWarnings("unused")
  private ResourceInfo resourceInfo;

  private final RestClient restClient;
  private final KsqlActions actions;

  public KsqlSecurityFilter(final String scope, final KsqlConfig ksqlConfig) {
    this.restClient = new RestClient(getAuthorizerConfigProps(ksqlConfig));
    this.actions = KsqlActions.build(scope);
  }

  // TODO: This method could be part of the KsqlConfig class.
  private Map<String, ?> getAuthorizerConfigProps(final KsqlConfig ksqlConfig) {
    return ImmutableMap.of(
        "bootstrap.metadata.server.urls",
        ksqlConfig.getString("bootstrap.metadata.server.urls")
    );
  }

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    final Method resourceMethod = resourceInfo.getResourceMethod();
    final List<Action> actions = this.actions.actions(resourceMethod);

    // Avoids accidentally create a new REST API without proper permissions
    if (actions.isEmpty()) {
      throw new KsqlException("Unauthorized operation");
    }

    final String userPrincipal = requestContext.getSecurityContext().getUserPrincipal().getName();
    log.debug("Authorizing request for principal {}. Actions: {}", userPrincipal, actions);

    try {
      List<String> authorizations = restClient.authorize(userPrincipal, null, actions);
      if (!authorizations.stream().allMatch(AuthorizeResult.ALLOWED.name()::equalsIgnoreCase)) {
        // TODO: Create a custom KsqlSecurityException?
        throw new KsqlException("Unauthorized operation");
      }
    } catch (final RestClientException e) {
      // TODO: Create a custom KsqlSecurityException?
      throw new KsqlException("Error while authorizing request", e);
    }
  }
}
