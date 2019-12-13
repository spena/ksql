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

package io.confluent.ksql.security;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.CreateAsSelect;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.PrintTopic;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.topic.SourceTopicsExtractor;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.acl.AclOperation;

/**
 * This authorization implementation checks if the user can perform Kafka and/or SR operations
 * on the topics or schemas found in the specified KSQL {@link Statement}.
 * </p>
 * This validator only works on Kakfa 2.3 or later.
 */
public class KsqlAuthorizationValidatorImpl implements KsqlAuthorizationValidator {
  private static final String UNKNOWN_USER = "";

  private static final boolean ACCESS_ALLOWED = true;
  private static final boolean ACCESS_DENIED = false;

  private final LoadingCache<CacheKey, Boolean> cache;

  private static class CacheKey {
    private final ServiceContext serviceContext;
    private final String topicName;
    private final AclOperation operation;

    CacheKey(ServiceContext serviceContext, String topicName, AclOperation operation) {
      this.serviceContext = serviceContext;
      this.topicName = topicName;
      this.operation = operation;
    }

    public ServiceContext getServiceContext() {
      return serviceContext;
    }

    public String getTopicName() {
      return topicName;
    }

    public AclOperation getOperation() {
      return operation;
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          serviceContext.getUserName().orElse(UNKNOWN_USER),
          topicName,
          operation.code()
      );
    }
  }


  public KsqlAuthorizationValidatorImpl(final KsqlConfig ksqlConfig) {
    long expirationTime = ksqlConfig.getInt(KsqlConfig.KSQL_AUTH_CACHE_EXPIRE_TIME_IN_SECS);
    long maxEntries = ksqlConfig.getInt(KsqlConfig.KSQL_AUTH_CACHE_MAX_ENTRIES);

    // initialize cache
    cache = CacheBuilder.newBuilder()
        .expireAfterWrite(expirationTime, TimeUnit.SECONDS)
        .maximumSize(maxEntries)
        .build(buildCacheLoader());
  }

  @Override
  public void checkAuthorization(
      final ServiceContext serviceContext,
      final MetaStore metaStore,
      final Statement statement
  ) {
    if (statement instanceof Query) {
      validateQuery(serviceContext, metaStore, (Query)statement);
    } else if (statement instanceof InsertInto) {
      validateInsertInto(serviceContext, metaStore, (InsertInto)statement);
    } else if (statement instanceof CreateAsSelect) {
      validateCreateAsSelect(serviceContext, metaStore, (CreateAsSelect)statement);
    } else if (statement instanceof PrintTopic) {
      validatePrintTopic(serviceContext, (PrintTopic)statement);
    } else if (statement instanceof CreateSource) {
      validateCreateSource(serviceContext, (CreateSource)statement);
    }
  }

  private void validateQuery(
      final ServiceContext serviceContext,
      final MetaStore metaStore,
      final Query query
  ) {
    final SourceTopicsExtractor extractor = new SourceTopicsExtractor(metaStore);
    extractor.process(query, null);
    for (String kafkaTopic : extractor.getSourceTopics()) {
      checkCacheAccess(serviceContext, kafkaTopic, AclOperation.READ);
    }
  }

  private void validateCreateAsSelect(
      final ServiceContext serviceContext,
      final MetaStore metaStore,
      final CreateAsSelect createAsSelect
  ) {
    /*
     * Check topic access for CREATE STREAM/TABLE AS SELECT statements.
     *
     * Validates Write on the target topic if exists, and Read on the query sources topics.
     *
     * The Create access is validated by the TopicCreateInjector which will attempt to create
     * the target topic using the same ServiceContext used for validation.
     */

    validateQuery(serviceContext, metaStore, createAsSelect.getQuery());

    // At this point, the topic should have been created by the TopicCreateInjector
    final String kafkaTopic = getCreateAsSelectSinkTopic(metaStore, createAsSelect);
    checkCacheAccess(serviceContext, kafkaTopic, AclOperation.WRITE);
  }

  private void validateInsertInto(
      final ServiceContext serviceContext,
      final MetaStore metaStore,
      final InsertInto insertInto
  ) {
    /*
     * Check topic access for INSERT INTO statements.
     *
     * Validates Write on the target topic, and Read on the query sources topics.
     */

    validateQuery(serviceContext, metaStore, insertInto.getQuery());

    final String kafkaTopic = getSourceTopicName(metaStore, insertInto.getTarget());
    checkCacheAccess(serviceContext, kafkaTopic, AclOperation.WRITE);
  }

  private void validatePrintTopic(
          final ServiceContext serviceContext,
          final PrintTopic printTopic
  ) {
    checkCacheAccess(serviceContext, printTopic.getTopic(), AclOperation.READ);
  }

  private void validateCreateSource(
      final ServiceContext serviceContext,
      final CreateSource createSource
  ) {
    final String sourceTopic = createSource.getProperties().getKafkaTopic();
    checkCacheAccess(serviceContext, sourceTopic, AclOperation.READ);
  }

  private String getSourceTopicName(final MetaStore metaStore, final SourceName streamOrTable) {
    final DataSource<?> dataSource = metaStore.getSource(streamOrTable);
    if (dataSource == null) {
      throw new KsqlException("Cannot validate for topic access from an unknown stream/table: "
          + streamOrTable);
    }

    return dataSource.getKafkaTopicName();
  }

  private CacheLoader<CacheKey, Boolean> buildCacheLoader() {
    return new CacheLoader<CacheKey, Boolean>() {
      @Override
      public Boolean load(CacheKey key) {
        final ServiceContext serviceContext = key.getServiceContext();
        final String topicName = key.getTopicName();
        final AclOperation operation = key.getOperation();

        try {
          final Set<AclOperation> authorizedOperations = serviceContext.getTopicClient()
              .describeTopic(topicName).authorizedOperations();

          // Kakfa 2.2 or lower do not support authorizedOperations(). In case of running on a
          // unsupported broker version, then the authorizeOperation will be null.
          if (authorizedOperations != null && !authorizedOperations.contains(operation)) {
            return ACCESS_DENIED;
          }
        } catch (Exception e) {
          return ACCESS_DENIED;
        }

        return ACCESS_ALLOWED;
      }
    };
  }

  private void checkCacheAccess(
      final ServiceContext serviceContext,
      final String topicName,
      final AclOperation operation
  ) {
    CacheKey cacheKey = new CacheKey(serviceContext, topicName, operation);
    if (cache.getUnchecked(cacheKey) == ACCESS_DENIED) {
      // This error message is similar to what Kafka throws when it cannot access the topic
      // due to an authorization error. I used this message to keep a consistent message.
      throw new KsqlTopicAuthorizationException(operation, Collections.singleton(topicName));
    }
  }

  private String getCreateAsSelectSinkTopic(
      final MetaStore metaStore,
      final CreateAsSelect createAsSelect
  ) {
    return createAsSelect.getProperties().getKafkaTopic()
        .orElseGet(() -> getSourceTopicName(metaStore, createAsSelect.getName()));
  }
}
