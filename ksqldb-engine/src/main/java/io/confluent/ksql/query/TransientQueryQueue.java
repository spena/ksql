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

package io.confluent.ksql.query;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.GenericRow;

import java.util.Collection;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A queue of rows for transient queries.
 */
public class TransientQueryQueue implements BlockingRowQueue {
  private static final Logger log = LoggerFactory.getLogger(TransientQueryQueue.class);

  public static final int BLOCKING_QUEUE_CAPACITY = 500;

  private final BlockingQueue<GenericRow> rowQueue;
  private final int offerTimeoutMs;
  private LimitQueueCallback callback;
  private volatile boolean closed = false;

  private Optional<QueryId> queryId = Optional.empty();

  public TransientQueryQueue(final OptionalInt limit) {
    this(limit, BLOCKING_QUEUE_CAPACITY, 100);
  }

  public TransientQueryQueue(final OptionalInt limit, final QueryId queryId) {
    this(limit, BLOCKING_QUEUE_CAPACITY, 100);
    this.queryId = Optional.of(queryId);
  }

  @VisibleForTesting
  TransientQueryQueue(
      final OptionalInt limit,
      final int queueSizeLimit,
      final int offerTimeoutMs
  ) {
    this.callback = limit.isPresent()
        ? new LimitedQueueCallback(limit.getAsInt())
        : new UnlimitedQueueCallback();
    this.rowQueue = new LinkedBlockingQueue<>(queueSizeLimit);
    this.offerTimeoutMs = offerTimeoutMs;
  }

  @Override
  public void setLimitHandler(final LimitHandler limitHandler) {
    callback.setLimitHandler(limitHandler);
  }

  @Override
  public void setQueuedCallback(final Runnable queuedCallback) {
    final LimitQueueCallback parent = callback;

    callback = new LimitQueueCallback() {
      @Override
      public boolean shouldQueue() {
        return parent.shouldQueue();
      }

      @Override
      public void onQueued() {
        parent.onQueued();
        queuedCallback.run();
      }

      @Override
      public void setLimitHandler(final LimitHandler limitHandler) {
        parent.setLimitHandler(limitHandler);
      }
    };
  }

  @Override
  public GenericRow poll(final long timeout, final TimeUnit unit)
      throws InterruptedException {
    return rowQueue.poll(timeout, unit);
  }

  @Override
  public GenericRow poll() {
    return rowQueue.poll();
  }

  @Override
  public void drainTo(final Collection<? super GenericRow> collection) {
    rowQueue.drainTo(collection);
  }

  @Override
  public int size() {
    return rowQueue.size();
  }

  @Override
  public boolean isEmpty() {
    return rowQueue.isEmpty();
  }

  @Override
  public void close() {
    closed = true;
    log.info("ESCALATION-4417: Queue closed for queryID {}",
        queryId.map(QueryId::toString).orElse("<unknown>"));
  }

  public void acceptRow(final GenericRow row) {
    try {
      if (row == null) {
        log.info("ESCALATION-4417: Null row found. QueryID = {}",
            queryId.map(QueryId::toString).orElse("<unknown>"));
        return;
      }

      if (!callback.shouldQueue()) {
        log.info("ESCALATION-4417: Row cannot be queued. QueryID = {}",
            queryId.map(QueryId::toString).orElse("<unknown>"));
        return;
      }

      while (!closed) {
        if (rowQueue.offer(row, offerTimeoutMs, TimeUnit.MILLISECONDS)) {
          callback.onQueued();
          break;
        } else {
          log.info("ESCALATION-4417: Row cannot be offered. QueryID = {}",
              queryId.map(QueryId::toString).orElse("<unknown>"));
        }
      }

      if (closed) {
        log.info("ESCALATION-4417: Row not accepted because queue is already closed. QueryID = {}",
            queryId.map(QueryId::toString).orElse("<unknown>"));
      }
    } catch (final InterruptedException e) {
      log.info("ESCALATION-4417: acceptRow interrupted. QueryID = "
          + queryId.map(QueryId::toString).orElse("<unknown>"), e);
      // Forced shutdown?
      Thread.currentThread().interrupt();
    } catch (final Exception e) {
      log.info("ESCALATION-4417: acceptRow threw an exception. QueryID = "
          + queryId.map(QueryId::toString).orElse("<unknown>"), e);
      throw e;
    }
  }
}
