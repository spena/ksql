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

import io.confluent.security.authorizer.Operation;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class KsqlOperations {
  private KsqlOperations() {

  }

  public static final Operation CONNECT = new Operation("CONNECT");
  public static final Operation TERMINATE = new Operation("TERMINATE");

  public static final Set<Operation> ALL = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      CONNECT, TERMINATE
  )));
}
