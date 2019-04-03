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

package io.confluent.ksql.rest.server.security;

import io.confluent.ksql.rest.entity.ClusterTerminateRequest;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.server.resources.KsqlResource;
import io.confluent.ksql.rest.server.resources.streaming.StreamedQueryResource;
import io.confluent.ksql.util.KsqlException;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.ResourceType;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KsqlActions {
  private static final List<Action> EMPTY_ACTIONS = Collections.emptyList();

  public static final ResourceType KSQL_RESOURCE = new ResourceType("KSQL");
  private final Map<Method, List<Action>> actions;

  public static KsqlActions build(final String scope) {
    Map<Method, List<Action>> actions = new HashMap<>();

    try {
      actions.put(
          StreamedQueryResource.class.getMethod(
              "streamQuery",
              KsqlRequest.class
          ),
          Collections.singletonList(new Action(
              scope,
              KSQL_RESOURCE,
              "",
              KsqlOperations.CONNECT
          ))
      );

      actions.put(
          KsqlResource.class.getMethod(
              "handleKsqlStatements",
              KsqlRequest.class
          ),
          Collections.singletonList(new Action(
              scope,
              KSQL_RESOURCE,
              "",
              KsqlOperations.CONNECT
          ))
      );

      actions.put(
          KsqlResource.class.getMethod(
              "terminateCluster",
              ClusterTerminateRequest.class
          ),
          Collections.singletonList(new Action(
              scope,
              KSQL_RESOURCE,
              "",
              KsqlOperations.TERMINATE
          ))
      );
    } catch (final NoSuchMethodException e) {
      throw new KsqlException("Failed to initialize KSQL Security Actions");
    }

    return new KsqlActions(actions);
  }

  private KsqlActions(final Map<Method, List<Action>> actions) {
    this.actions = actions;
  }

  public List<Action> actions(final Method resourceMethod) {
    return actions.getOrDefault(resourceMethod, EMPTY_ACTIONS);
  }
}
