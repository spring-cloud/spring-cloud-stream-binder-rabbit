/*
 * Copyright 2021-2021 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit.stream;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.springframework.cloud.stream.binder.rabbit.RabbitStreamMessageHandler;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.rabbit.stream.producer.RabbitStreamTemplate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @since 3.2
 *
 */
public class RabbitStreamMessageHandlerTests extends AbstractIntegrationTests {

	@Test
	void convertAndSend() throws InterruptedException {
		Environment env = Environment.builder()
				.lazyInitialization(true)
				.addressResolver(add -> new Address("localhost", streamPort()))
				.build();
		try {
			env.deleteStream("stream.stream");
		}
		catch (Exception e) {
		}
		env.streamCreator().stream("stream.stream").create();
		RabbitStreamTemplate streamTemplate = new RabbitStreamTemplate(env, "stream.stream");
		RabbitStreamMessageHandler handler = new RabbitStreamMessageHandler(streamTemplate);
		handler.setSync(true);
		handler.handleMessage(MessageBuilder.withPayload("foo")
				.setHeader("bar", "baz")
				.build());
		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<com.rabbitmq.stream.Message> received = new AtomicReference<>();
		Consumer consumer = env.consumerBuilder().stream("stream.stream")
				.offset(OffsetSpecification.first())
				.messageHandler((context, msg) -> {
					received.set(msg);
					latch.countDown();
				})
				.build();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(received.get()).isNotNull();
		assertThat(received.get().getBodyAsBinary()).isEqualTo("foo".getBytes());
		assertThat((String) received.get().getApplicationProperties().get("bar")).isEqualTo("baz");
		consumer.close();
		handler.stop();
	}

	@Test
	@Disabled
	void sendNative() throws InterruptedException {
		Environment env = Environment.builder()
				.lazyInitialization(true)
				.build();
		try {
			env.deleteStream("stream.stream");
		}
		catch (Exception e) {
		}
		env.streamCreator().stream("stream.stream").create();
		RabbitStreamTemplate streamTemplate = new RabbitStreamTemplate(env, "stream.stream");
		RabbitStreamMessageHandler handler = new RabbitStreamMessageHandler(streamTemplate);
		handler.setSync(true);
		handler.handleMessage(MessageBuilder.withPayload(streamTemplate.messageBuilder()
						.addData("foo".getBytes())
						.applicationProperties().entry("bar", "baz")
						.messageBuilder()
						.build())
				.build());
		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<com.rabbitmq.stream.Message> received = new AtomicReference<>();
		Consumer consumer = env.consumerBuilder().stream("stream.stream")
				.offset(OffsetSpecification.first())
				.messageHandler((context, msg) -> {
					received.set(msg);
					latch.countDown();
				})
				.build();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(received.get()).isNotNull();
		assertThat(received.get().getBodyAsBinary()).isEqualTo("foo".getBytes());
		assertThat((String) received.get().getApplicationProperties().get("bar")).isEqualTo("baz");
		consumer.close();
		handler.stop();
	}

}
