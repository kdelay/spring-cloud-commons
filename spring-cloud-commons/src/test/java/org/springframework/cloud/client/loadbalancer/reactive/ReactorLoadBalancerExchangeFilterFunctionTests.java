/*
 * Copyright 2012-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.client.loadbalancer.reactive;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.CompletionContext;
import org.springframework.cloud.client.loadbalancer.LoadBalancerLifecycle;
import org.springframework.cloud.client.loadbalancer.LoadBalancerProperties;
import org.springframework.cloud.client.loadbalancer.Request;
import org.springframework.cloud.client.loadbalancer.Response;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ReactorLoadBalancerExchangeFilterFunction}.
 *
 * @author kdelay
 */
@SuppressWarnings("unchecked")
class ReactorLoadBalancerExchangeFilterFunctionTests {

	private final ReactiveLoadBalancer.Factory<ServiceInstance> factory = mock(ReactiveLoadBalancer.Factory.class);

	private final ReactorLoadBalancerExchangeFilterFunction filterFunction = new ReactorLoadBalancerExchangeFilterFunction(
			factory, Collections.emptyList());

	private final ClientRequest clientRequest = ClientRequest.create(HttpMethod.GET, URI.create("http://test/path"))
		.build();

	private final CompletionRecordingLifecycle lifecycle = new CompletionRecordingLifecycle();

	@BeforeEach
	void setUp() {
		when(factory.getInstance("test")).thenReturn(new TestReactiveLoadBalancer());
		when(factory.getProperties(any())).thenReturn(new LoadBalancerProperties());
		when(factory.getInstances("test", LoadBalancerLifecycle.class))
			.thenReturn(Collections.<String, LoadBalancerLifecycle>singletonMap("lifecycle", lifecycle));
	}

	@Test
	void shouldCompleteLifecycleWhenRequestIsCancelled() {
		ExchangeFunction next = request -> Mono.never();

		filterFunction.filter(clientRequest, next).subscribe().dispose();

		assertThat(lifecycle.completionContexts).singleElement().satisfies(completionContext -> {
			assertThat(completionContext.status()).isEqualTo(CompletionContext.Status.FAILED);
			assertThat(completionContext.getThrowable()).isInstanceOf(CancellationException.class);
		});
	}

	@Test
	void shouldCompleteLifecycleOnceWhenResponseHandlingFails() {
		ExchangeFunction next = request -> Mono.just(ClientResponse.create(HttpStatus.OK).build());

		filterFunction.filter(clientRequest, next).map(clientResponse -> {
			throw new IllegalStateException("Failed to handle the response");
		}).subscribe(clientResponse -> {
		}, throwable -> {
		});

		assertThat(lifecycle.completionContexts).singleElement()
			.satisfies(completionContext -> assertThat(completionContext.status())
				.isEqualTo(CompletionContext.Status.SUCCESS));
	}

	static class CompletionRecordingLifecycle implements LoadBalancerLifecycle<Object, Object, ServiceInstance> {

		final List<CompletionContext<Object, ServiceInstance, Object>> completionContexts = new CopyOnWriteArrayList<>();

		@Override
		public void onStart(Request<Object> request) {
		}

		@Override
		public void onStartRequest(Request<Object> request, Response<ServiceInstance> lbResponse) {
		}

		@Override
		public void onComplete(CompletionContext<Object, ServiceInstance, Object> completionContext) {
			completionContexts.add(completionContext);
		}

	}

}
