/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.strimzi.operator.common.BackOff;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@ExtendWith(VertxExtension.class)
public class KafkaConnectApiMockTest {
	public KafkaConnectApiImpl mockKafkaConnectApiImpl1(Vertx vertx, Queue<Future<Map<String, Object>>> statusResults) {
		Queue<Future<Map<String, Object>>> mockFieldVariableStatusResults;
		KafkaConnectApiImpl mockInstance = spy(new KafkaConnectApiImpl(vertx));
		mockFieldVariableStatusResults = statusResults;
		doAnswer((stubInvo) -> {
			return mockFieldVariableStatusResults.remove();
		}).when(mockInstance).status(any(String.class), anyInt(), any(String.class));
		return mockInstance;
	}

	private static Vertx vertx;
	private BackOff backOff = new BackOff(1L, 2, 3);

	@BeforeAll
	public static void before() {
		vertx = Vertx.vertx();
	}

	@AfterAll
	public static void after() {
		vertx.close();
	}

	@Test
	public void testStatusWithBackOffSucceedingImmediately(VertxTestContext context) {
		Queue<Future<Map<String, Object>>> statusResults = new ArrayBlockingQueue<>(1);
		statusResults.add(Future.succeededFuture(Collections.emptyMap()));

		KafkaConnectApiImpl api = mockKafkaConnectApiImpl1(vertx, statusResults);
		Checkpoint async = context.checkpoint();

		api.statusWithBackOff(backOff, "some-host", 8083, "some-connector")
				.onComplete(context.succeeding(res -> async.flag()));
	}

	@Test
	public void testStatusWithBackOffSuccedingEventually(VertxTestContext context) {
		Queue<Future<Map<String, Object>>> statusResults = new ArrayBlockingQueue<>(3);
		statusResults.add(Future.failedFuture(new ConnectRestException(null, null, 404, null, null)));
		statusResults.add(Future.failedFuture(new ConnectRestException(null, null, 404, null, null)));
		statusResults.add(Future.succeededFuture(Collections.emptyMap()));

		KafkaConnectApiImpl api = mockKafkaConnectApiImpl1(vertx, statusResults);
		Checkpoint async = context.checkpoint();

		api.statusWithBackOff(backOff, "some-host", 8083, "some-connector")
				.onComplete(context.succeeding(res -> async.flag()));
	}

	@Test
	public void testStatusWithBackOffFailingRepeatedly(VertxTestContext context) {
		Queue<Future<Map<String, Object>>> statusResults = new ArrayBlockingQueue<>(4);
		statusResults.add(Future.failedFuture(new ConnectRestException(null, null, 404, null, null)));
		statusResults.add(Future.failedFuture(new ConnectRestException(null, null, 404, null, null)));
		statusResults.add(Future.failedFuture(new ConnectRestException(null, null, 404, null, null)));
		statusResults.add(Future.failedFuture(new ConnectRestException(null, null, 404, null, null)));

		KafkaConnectApiImpl api = mockKafkaConnectApiImpl1(vertx, statusResults);
		Checkpoint async = context.checkpoint();

		api.statusWithBackOff(backOff, "some-host", 8083, "some-connector")
				.onComplete(context.failing(res -> async.flag()));
	}

	@Test
	public void testStatusWithBackOffOtherExceptionStillFails(VertxTestContext context) {
		Queue<Future<Map<String, Object>>> statusResults = new ArrayBlockingQueue<>(1);
		statusResults.add(Future.failedFuture(new ConnectRestException(null, null, 500, null, null)));

		KafkaConnectApiImpl api = mockKafkaConnectApiImpl1(vertx, statusResults);
		Checkpoint async = context.checkpoint();

		api.statusWithBackOff(backOff, "some-host", 8083, "some-connector")
				.onComplete(context.failing(res -> async.flag()));
	}
}
