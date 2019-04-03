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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.security.auth.client.rest.RestClient;

import java.util.Map;
import java.util.Objects;

public class KsqlAuthorizer {
  private final RestClient restClient;

  // Keep a copy of the Server ID in case KsqlConfig changes the ID accidentally
  private final String ksqlServerId;

  // Having one single authorizer instance
  public static KsqlAuthorizer INSTANCE;

  public static synchronized KsqlAuthorizer get(final KsqlConfig ksqlConfig) {
    if (INSTANCE == null) {
      INSTANCE = new KsqlAuthorizer(ksqlConfig);
    }

    return INSTANCE;
  }

  private KsqlAuthorizer(final KsqlConfig ksqlConfig) {
    // Configurations required:
    // - bootstrap.metadata.server.urls = <list of comma separated MDS urls>
    // - basic.auth.credentials.provider = <NONE (default if empty) or USER_INFO>
    // - ssl.truststore.location = <location to ssl trustore>
    // - enable.metadata.server.urls.refresh = <true or false>
    restClient = new RestClient(getAuthorizerConfigProps(ksqlConfig));
    ksqlServerId = Objects.requireNonNull(ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG));
  }

  private Map<String, ?> getAuthorizerConfigProps(final KsqlConfig ksqlConfig) {
    return ImmutableMap.of(
        "bootstrap.metadata.server.urls",
              ksqlConfig.getString("bootstrap.metadata.server.urls")
    );
  }

  public boolean canConnectToServer() {

  }
}
