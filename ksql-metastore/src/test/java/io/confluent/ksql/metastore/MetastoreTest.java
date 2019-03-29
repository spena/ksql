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

package io.confluent.ksql.metastore;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.function.TestFunctionRegistry;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.metastore.model.StructuredDataSource;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.serde.json.KsqlJsonTopicSerDe;
import io.confluent.ksql.util.MetaStoreFixture;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MetastoreTest {

  private MutableMetaStore metaStore;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore(new TestFunctionRegistry());
  }

  @Test
  public void testTopicMap() {
    final KsqlTopic ksqlTopic1 = new KsqlTopic("testTopic", "testTopicKafka", new KsqlJsonTopicSerDe(), false);
    metaStore.putTopic(ksqlTopic1);
    final KsqlTopic ksqlTopic2 = metaStore.getTopic("testTopic");
    Assert.assertNotNull(ksqlTopic2);

    // Check non-existent topic
    final KsqlTopic ksqlTopic3 = metaStore.getTopic("TESTTOPIC_");
    Assert.assertNull(ksqlTopic3);
  }

  @Test
  public void testStreamMap() {
    final StructuredDataSource<?> structuredDataSource1 = metaStore.getSource("ORDERS");
    Assert.assertNotNull(structuredDataSource1);
    assertThat(structuredDataSource1.getDataSourceType(), is(DataSource.DataSourceType.KSTREAM));

    // Check non-existent stream
    final StructuredDataSource<?> structuredDataSource2 = metaStore.getSource("nonExistentStream");
    Assert.assertNull(structuredDataSource2);
  }

  @Test
  public void testDelete() {
    final StructuredDataSource<?> structuredDataSource1 = metaStore.getSource("ORDERS");
    final StructuredDataSource<?> structuredDataSource2 = new KsqlStream<>(
        "sqlexpression", "testStream",
        structuredDataSource1.getSchema(),
        structuredDataSource1.getKeyField(),
        structuredDataSource1.getTimestampExtractionPolicy(),
        structuredDataSource1.getKsqlTopic(),
        Serdes::String);

    metaStore.putSource(structuredDataSource2);
    final StructuredDataSource<?> structuredDataSource3 = metaStore.getSource("testStream");
    Assert.assertNotNull(structuredDataSource3);
    metaStore.deleteSource("testStream");
    final StructuredDataSource<?> structuredDataSource4 = metaStore.getSource("testStream");
    Assert.assertNull(structuredDataSource4);
  }

  @Test
  public void shouldGetTheCorrectSourceNameForTopic() {
    final StructuredDataSource<?> structuredDataSource = metaStore.getSourceForTopic("TEST2").get();
    assertThat(structuredDataSource, instanceOf(KsqlTable.class));
    assertThat(structuredDataSource.getDataSourceType(), equalTo(DataSourceType.KTABLE));
    assertThat(structuredDataSource.getName(), equalTo("TEST2"));
  }
}