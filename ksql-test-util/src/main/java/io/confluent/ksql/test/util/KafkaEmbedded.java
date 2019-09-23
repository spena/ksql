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

package io.confluent.ksql.test.util;

import static io.confluent.ksql.test.util.AssertEventually.assertThatEventually;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.SystemTime;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs an in-memory, "embedded" instance of a Kafka broker, which listens at `127.0.0.1:9092` by
 * default.
 *
 * <p>Requires a running ZooKeeper instance to connect to.  By default, it expects a ZooKeeper
 * instance running at `127.0.0.1:2181`.  You can specify a different ZooKeeper instance by setting
 * the `zookeeper.connect` parameter in the broker's configuration.
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
class KafkaEmbedded {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

  private static final Logger log = LoggerFactory.getLogger(KafkaEmbedded.class);

  public static final Duration ZK_SESSION_TIMEOUT = Duration.ofSeconds(30);
  // Jenkins builds can take ages to create the ZK log, so the initial connect can be slow, hence:
  public static final Duration ZK_CONNECT_TIMEOUT = Duration.ofSeconds(60);

  private final Properties effectiveConfig;
  private final File logDir;
  private final TemporaryFolder tmpFolder;
  private final KafkaServer kafka;

  /**
   * Creates and starts an embedded Kafka broker.
   *
   * @param config Broker configuration settings.  Used to modify, for example, the listeners
   *               the broker should use.  Note that you cannot change some settings such as
   *               `log.dirs`.
   */
  KafkaEmbedded(final Properties config) throws IOException {
    this.tmpFolder = new TemporaryFolder();
    this.tmpFolder.create();
    this.logDir = tmpFolder.newFolder();
    this.effectiveConfig = effectiveConfigFrom(config, logDir);

    final KafkaConfig kafkaConfig = new KafkaConfig(effectiveConfig, true);
    log.debug("Starting embedded Kafka broker (with log.dirs={} and ZK ensemble at {}) ...",
        logDir, zookeeperConnect());

    kafka = TestUtils.createServer(kafkaConfig, new SystemTime());
    log.debug("Startup of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...",
        brokerList(), zookeeperConnect());
  }

  /**
   * This broker's `metadata.broker.list` value.  Example: `127.0.0.1:9092`.
   *
   * <p>You can use this to tell Kafka producers and consumers how to connect to this instance.
   *
   * <p>This version returns the port of the first listener.
   * @return the broker list
   */
  String brokerList() {
    final ListenerName listenerName = kafka.config().advertisedListeners().apply(0).listenerName();
    return kafka.config().hostName() + ":" + kafka.boundPort(listenerName);
  }

  /**
   * The broker's `metadata.broker.list` value.  Example: `127.0.0.1:9092`.
   *
   * <p>You can use this to tell Kafka producers and consumers how to connect to this instance.
   *
   * @param securityProtocol the security protocol the returned broker list should use.
   * @return the broker list
   */
  String brokerList(final SecurityProtocol securityProtocol) {
    return kafka.config().hostName() + ":"
           + kafka.boundPort(new ListenerName(securityProtocol.toString()));
  }

  /**
   * Stop the broker.
   */
  void stop() {
    log.debug("Shutting down embedded Kafka broker at {} (with ZK ensemble at {}) ...",
        brokerList(), zookeeperConnect());
    kafka.shutdown();
    kafka.awaitShutdown();
    log.debug("Removing temp folder {} with logs.dir at {} ...", tmpFolder, logDir);
    tmpFolder.delete();
    log.debug("Shutdown of embedded Kafka broker at {} completed (with ZK ensemble at {}) ...",
        brokerList(), zookeeperConnect());
  }

  /**
   * Create a Kafka topic with 1 partition and a replication factor of 1.
   *
   * @param topic The name of the topic.
   */
  void createTopic(final String topic) {
    createTopic(topic, 1, 1);
  }

  /**
   * Create a Kafka topic with the given parameters.
   *
   * @param topic       The name of the topic.
   * @param partitions  The number of partitions for this topic.
   * @param replication The replication factor for (the partitions of) this topic.
   */
  void createTopic(final String topic, final int partitions, final int replication) {
    createTopic(topic, partitions, replication, ImmutableMap.of());
  }

  /**
   * Create a Kafka topic with the given parameters.
   *
   * @param topic       The name of the topic.
   * @param partitions  The number of partitions for this topic.
   * @param replication The replication factor for (partitions of) this topic.
   * @param topicConfig Additional topic-level configuration settings.
   */
  void createTopic(final String topic,
      final int partitions,
      final int replication,
      final Map<String, String> topicConfig
  ) {
    log.debug("Creating topic { name: {}, partitions: {}, replication: {}, config: {} }",
        topic, partitions, replication, topicConfig);

    try (Admin adminClient = adminClient()) {

      final NewTopic newTopic = new NewTopic(topic, partitions, (short) replication);
      newTopic.configs(topicConfig);

      try {
        final CreateTopicsResult result = adminClient.createTopics(ImmutableList.of(newTopic));
        result.all().get();
      } catch (final Exception e) {
        throw new RuntimeException("Failed to create topic:" + topic, e);
      }
    }
  }

  Set<String> getTopics() {
    try (Admin adminClient = adminClient()) {
      return getTopicNames(adminClient);
    }
  }

  /**
   * Delete topics from the cluster.
   *
   * <p><b>IMPORTANT:</b> topic deletion is asynchronous. Attempts to recreate the same topic may
   * fail for a short period of time.  Avoid recreating topics with the same name if at all
   * possible. Where this is not possible, wrap the creation in a retry loop.
   *
   * @param topics the topics to delete.
   */
  void deleteTopics(final Collection<String> topics) {
    try (Admin adminClient = adminClient()) {

      try {
        adminClient.deleteTopics(topics);
      } catch (final Exception e) {
        throw new RuntimeException("Failed to delete topics: " + topics, e);
      }
    }
  }

  /**
   * Wait for topics with names {@code topicNames} to not exist in Kafka.
   *
   * @param topicNames the names of the topics to await absence for.
   */
  void waitForTopicsToBePresent(final String... topicNames) {
    try (Admin adminClient = adminClient()) {
      final Set<String> required = ImmutableSet.copyOf(topicNames);

      final Supplier<Collection<String>> remaining = () -> {
        final Set<String> names = getTopicNames(adminClient);
        names.retainAll(required);
        return names;
      };

      assertThatEventually(
          "topics not all prresent after timeout",
          remaining,
          is(required)
      );
    }
  }

  /**
   * Wait for topics with names {@code topicNames} to not exist in Kafka.
   *
   * @param topicNames the names of the topics to await absence for.
   */
  void waitForTopicsToBeAbsent(final String... topicNames) {
    try (Admin adminClient = adminClient()) {

      final Supplier<Collection<String>> remaining = () -> {
        final Set<String> names = getTopicNames(adminClient);
        names.retainAll(Arrays.asList(topicNames));
        return names;
      };

      assertThatEventually(
          "topics not all absent after timeout",
          remaining,
          is(empty())
      );
    }
  }

  private static Set<String> getTopicNames(final Admin adminClient) {
    try {
      return adminClient.listTopics().names().get();
    } catch (final Exception e) {
      throw new RuntimeException("Failed to get topic names", e);
    }
  }

  private static Properties effectiveConfigFrom(final Properties initialConfig, final File logDir) {
    final Properties effectiveConfig = new Properties();
    effectiveConfig.put(KafkaConfig.BrokerIdProp(), 0);
    effectiveConfig.put(KafkaConfig.HostNameProp(), "127.0.0.1");
    effectiveConfig.put(KafkaConfig.ListenersProp(), "PLAINTEXT://:0");
    effectiveConfig.put(KafkaConfig.NumPartitionsProp(), 1);
    effectiveConfig.put(KafkaConfig.AutoCreateTopicsEnableProp(), true);
    effectiveConfig.put(KafkaConfig.MessageMaxBytesProp(), 1_000_000);
    effectiveConfig.put(KafkaConfig.ControlledShutdownEnableProp(), true);
    effectiveConfig.put(KafkaConfig.ZkSessionTimeoutMsProp(), (int) ZK_SESSION_TIMEOUT.toMillis());
    effectiveConfig
        .put(KafkaConfig.ZkConnectionTimeoutMsProp(), (int) ZK_CONNECT_TIMEOUT.toMillis());

    effectiveConfig.putAll(initialConfig);
    effectiveConfig.put(KafkaConfig.LogDirProp(), logDir.getAbsolutePath());
    return effectiveConfig;
  }

  private String zookeeperConnect() {
    return effectiveConfig.getProperty(KafkaConfig.ZkConnectProp());
  }

  private AdminClient adminClient() {
    final ImmutableMap<String, Object> props = ImmutableMap.of(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList(),
        AdminClientConfig.RETRIES_CONFIG, 5);

    return AdminClient.create(props);
  }
}
