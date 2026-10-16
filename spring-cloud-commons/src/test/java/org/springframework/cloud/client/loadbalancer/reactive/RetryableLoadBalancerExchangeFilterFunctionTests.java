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

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import reactor.core.publisher.Mono;

import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.loadbalancer.CompletionContext;
import org.springframework.cloud.client.loadbalancer.LoadBalancerLifecycle;
import org.springframework.cloud.client.loadbalancer.LoadBalancerProperties;
import org.springframework.cloud.client.loadbalancer.Request;
import org.springframework.cloud.client.loadbalancer.Response;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link RetryableLoadBalancerExchangeFilterFunction}.
 *
 * @author Olga Maciaszek-Sharma
 * @since 3.0.0
 */
@SuppressWarnings("unchecked")
class RetryableLoadBalancerExchangeFilterFunctionTests {

	private final LoadBalancerProperties properties = new LoadBalancerProperties();

	private final ReactiveLoadBalancer.Factory<ServiceInstance> factory = mock(ReactiveLoadBalancer.Factory.class);

	private final RetryableLoadBalancerExchangeFilterFunction filterFunction = new RetryableLoadBalancerExchangeFilterFunction(
			new RetryableExchangeFilterFunctionLoadBalancerRetryPolicy.Factory(factory), factory,
			Collections.emptyList());

	private final ClientRequest clientRequest = mock(ClientRequest.class);

	private final ExchangeFunction next = mock(ExchangeFunction.class);

	private final ClientResponse clientResponse = mock(ClientResponse.class);

	private final InOrder inOrder = inOrder(next, factory);

	@BeforeEach
	void setUp() {
		properties.getRetry().setMaxRetriesOnSameServiceInstance(1);
		properties.getRetry().getRetryableStatusCodes().add(404);
		when(clientRequest.url()).thenReturn(URI.create("http://test"));
		when(factory.getInstance("test")).thenReturn(new TestReactiveLoadBalancer());
		when(factory.getProperties(any())).thenReturn(properties);
		when(clientRequest.headers()).thenReturn(new HttpHeaders());
		when(clientRequest.cookies()).thenReturn(new LinkedMultiValueMap<>());

	}

	@Test
	void shouldRetryOnSameAndNextServiceInstanceOnException() {
		when(clientRequest.method()).thenReturn(HttpMethod.GET);
		when(next.exchange(any())).thenThrow(new IllegalStateException(new IOException()));

		filterFunction.filter(clientRequest, next).subscribe();

		inOrder.verify(factory, times(1)).getInstance(any());
		inOrder.verify(next, times(2)).exchange(any());
		inOrder.verify(factory, times(1)).getInstance(any());
		inOrder.verify(next, times(2)).exchange(any());
	}

	@Test
	void shouldRetryOnSameAndNextServiceInstanceOnRetryableStatusCode() {
		when(clientRequest.method()).thenReturn(HttpMethod.GET);
		when(clientResponse.statusCode()).thenReturn(HttpStatus.NOT_FOUND);
		when(next.exchange(any())).thenReturn(Mono.just(clientResponse));

		filterFunction.filter(clientRequest, next).subscribe();

		inOrder.verify(factory, times(1)).getInstance(any());
		inOrder.verify(next, times(2)).exchange(any());
		inOrder.verify(factory, times(1)).getInstance(any());
		inOrder.verify(next, times(2)).exchange(any());
	}

	@Test
	void shouldNotRetryWhenNoRetryableExceptionOrStatusCode() {
		when(clientRequest.method()).thenReturn(HttpMethod.GET);
		when(clientResponse.statusCode()).thenReturn(HttpStatus.OK);
		when(next.exchange(any())).thenReturn(Mono.just(clientResponse));

		filterFunction.filter(clientRequest, next).subscribe();

		verify(next, times(1)).exchange(any());
		verify(factory, times(1)).getInstance(any());
	}

	@Test
	void shouldNotRetryOnMethodOtherThanGet() {
		when(clientRequest.method()).thenReturn(HttpMethod.POST);
		when(clientResponse.statusCode()).thenReturn(HttpStatus.NOT_FOUND);
		when(next.exchange(any())).thenReturn(Mono.just(clientResponse));

		filterFunction.filter(clientRequest, next).subscribe();

		verify(next, times(1)).exchange(any());
		verify(factory, times(1)).getInstance(any());
	}

	@Test
	void shouldRetryOnMethodOtherThanGetWhenEnabled() {
		LoadBalancerProperties properties = new LoadBalancerProperties();
		properties.getRetry().setRetryOnAllOperations(true);
		properties.getRetry().setMaxRetriesOnSameServiceInstance(1);
		properties.getRetry().getRetryableStatusCodes().add(404);
		LoadBalancerRetryPolicy policy = new RetryableExchangeFilterFunctionLoadBalancerRetryPolicy(properties);
		RetryableLoadBalancerExchangeFilterFunction filterFunction = new RetryableLoadBalancerExchangeFilterFunction(
				s -> policy, factory, Collections.emptyList());
		when(clientRequest.method()).thenReturn(HttpMethod.POST);
		when(clientResponse.statusCode()).thenReturn(HttpStatus.NOT_FOUND);
		when(next.exchange(any())).thenReturn(Mono.just(clientResponse));

		filterFunction.filter(clientRequest, next).subscribe();

		inOrder.verify(factory, times(1)).getInstance(any());
		inOrder.verify(next, times(2)).exchange(any());
		inOrder.verify(factory, times(1)).getInstance(any());
		inOrder.verify(next, times(2)).exchange(any());
	}

	@Test
	void shouldNotRetryNotConfiguredException() {
		when(clientRequest.method()).thenReturn(HttpMethod.GET);
		when(next.exchange(any())).thenThrow(new TestException());

		filterFunction.filter(clientRequest, next).subscribe();

		inOrder.verify(factory, times(1)).getInstance(any());
		inOrder.verify(next, times(1)).exchange(any());
	}

	@Test
	void shouldRetryOnNotConfiguredExceptionWhenRetryOnAllExceptions() {
		properties.getRetry().setRetryOnAllExceptions(true);
		when(clientRequest.method()).thenReturn(HttpMethod.GET);
		when(next.exchange(any())).thenThrow(new TestException());

		filterFunction.filter(clientRequest, next).subscribe();

		inOrder.verify(factory, times(1)).getInstance(any());
		inOrder.verify(next, times(2)).exchange(any());
		inOrder.verify(factory, times(1)).getInstance(any());
		inOrder.verify(next, times(2)).exchange(any());
	}

	@Test
	void shouldRetryOnConfiguredException() {
		properties.getRetry().setRetryableExceptions(new HashSet<>(Collections.singletonList(TestException.class)));
		when(clientRequest.method()).thenReturn(HttpMethod.GET);
		when(next.exchange(any())).thenThrow(new TestException());

		filterFunction.filter(clientRequest, next).subscribe();

		inOrder.verify(factory, times(1)).getInstance(any());
		inOrder.verify(next, times(2)).exchange(any());
		inOrder.verify(factory, times(1)).getInstance(any());
		inOrder.verify(next, times(2)).exchange(any());
	}

	@Test
	void shouldRetryOnRetryableStatusCodeExceptionEvenWhenCustomExceptionsConfigured() {
		properties.getRetry().setRetryableExceptions(new HashSet<>(Collections.singletonList(TestException.class)));
		when(clientRequest.method()).thenReturn(HttpMethod.GET);
		when(next.exchange(any())).thenThrow(new RetryableStatusCodeException());

		filterFunction.filter(clientRequest, next).subscribe();

		inOrder.verify(factory, times(1)).getInstance(any());
		inOrder.verify(next, times(2)).exchange(any());
		inOrder.verify(factory, times(1)).getInstance(any());
		inOrder.verify(next, times(2)).exchange(any());
	}

	@Test
	void shouldCompleteLifecycleWhenRequestIsCancelled() {
		CompletionRecordingLifecycle lifecycle = registerLifecycle();
		when(clientRequest.method()).thenReturn(HttpMethod.GET);
		when(next.exchange(any())).thenReturn(Mono.never());

		filterFunction.filter(clientRequest, next).subscribe().dispose();

		assertThat(lifecycle.completionContexts).singleElement().satisfies(completionContext -> {
			assertThat(completionContext.status()).isEqualTo(CompletionContext.Status.FAILED);
			assertThat(completionContext.getThrowable()).isInstanceOf(CancellationException.class);
		});
	}

	@Test
	void shouldCompleteLifecycleOnceForEachRetriedResponse() {
		CompletionRecordingLifecycle lifecycle = registerLifecycle();
		when(clientRequest.method()).thenReturn(HttpMethod.GET);
		when(next.exchange(any())).thenReturn(Mono.just(ClientResponse.create(HttpStatus.NOT_FOUND).build()));

		filterFunction.filter(clientRequest, next).subscribe();

		verify(next, times(4)).exchange(any());
		assertThat(lifecycle.completionContexts).hasSize(4)
			.allSatisfy(completionContext -> assertThat(completionContext.status())
				.isEqualTo(CompletionContext.Status.SUCCESS));
	}

	private CompletionRecordingLifecycle registerLifecycle() {
		CompletionRecordingLifecycle lifecycle = new CompletionRecordingLifecycle();
		when(factory.getInstances("test", LoadBalancerLifecycle.class))
			.thenReturn(Collections.<String, LoadBalancerLifecycle>singletonMap("lifecycle", lifecycle));
		return lifecycle;
	}

	protected static class TestException extends RuntimeException {

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
