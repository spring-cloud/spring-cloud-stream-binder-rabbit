/*
 * Copyright 2015-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.binder.rabbit.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.actuate.health.CompositeHealthIndicator;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.Cloud;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.rabbit.RabbitMessageChannelBinder;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitProducerProperties;
import org.springframework.cloud.stream.binder.test.junit.rabbit.RabbitTestSupport;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

/**
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Artem Bilan
 */
public class RabbitBinderModuleTests {

	@ClassRule
	public static RabbitTestSupport rabbitTestSupport = new RabbitTestSupport();

	private ConfigurableApplicationContext context;

	public static final ConnectionFactory MOCK_CONNECTION_FACTORY = mock(ConnectionFactory.class,
			Mockito.RETURNS_MOCKS);

	@After
	public void tearDown() {
		if (context != null) {
			context.close();
			context = null;
		}
		RabbitAdmin admin = new RabbitAdmin(rabbitTestSupport.getResource());
		admin.deleteQueue("binder.input.default");
		admin.deleteQueue("binder.output.default");
		admin.deleteExchange("binder.input");
		admin.deleteExchange("binder.output");
	}

	@Test
	public void testParentConnectionFactoryInheritedByDefault() {
		context = new SpringApplicationBuilder(SimpleProcessor.class)
				.web(WebApplicationType.NONE)
				.run("--server.port=0");
		BinderFactory binderFactory = context.getBean(BinderFactory.class);
		Binder<?, ?, ?> binder = binderFactory.getBinder(null, MessageChannel.class);
		assertThat(binder).isInstanceOf(RabbitMessageChannelBinder.class);
		DirectFieldAccessor binderFieldAccessor = new DirectFieldAccessor(binder);
		ConnectionFactory binderConnectionFactory = (ConnectionFactory) binderFieldAccessor
				.getPropertyValue("connectionFactory");
		assertThat(binderConnectionFactory).isInstanceOf(CachingConnectionFactory.class);
		ConnectionFactory connectionFactory = context.getBean(ConnectionFactory.class);
		assertThat(binderConnectionFactory).isSameAs(connectionFactory);

		ConnectionFactory producerConnectionFactory = (ConnectionFactory) binderFieldAccessor
				.getPropertyValue("producerConnectionFactory");
		assertThat(producerConnectionFactory).isNotSameAs(connectionFactory);

		CompositeHealthIndicator bindersHealthIndicator = context.getBean("bindersHealthIndicator",
				CompositeHealthIndicator.class);
		DirectFieldAccessor directFieldAccessor = new DirectFieldAccessor(bindersHealthIndicator);
		assertThat(bindersHealthIndicator).isNotNull();
		@SuppressWarnings("unchecked")
		Map<String, HealthIndicator> healthIndicators = (Map<String, HealthIndicator>) directFieldAccessor
				.getPropertyValue("indicators");
		assertThat(healthIndicators).containsKey(("rabbit"));
		assertThat(healthIndicators.get("rabbit").health().getStatus()).isEqualTo((Status.UP));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testParentConnectionFactoryInheritedByDefaultAndRabbitSettingsPropagated() {
		context = new SpringApplicationBuilder(SimpleProcessor.class)
				.web(WebApplicationType.NONE)
				.run("--server.port=0",
						"--spring.cloud.stream.rabbit.bindings.input.consumer.transacted=true",
						"--spring.cloud.stream.rabbit.bindings.output.producer.transacted=true");
		BinderFactory binderFactory = context.getBean(BinderFactory.class);
		Binder<?, ?, ?> binder = binderFactory.getBinder(null, MessageChannel.class);
		assertThat(binder).isInstanceOf(RabbitMessageChannelBinder.class);
		BindingService bindingService = context.getBean(BindingService.class);
		DirectFieldAccessor channelBindingServiceAccessor = new DirectFieldAccessor(bindingService);
		Map<String, List<Binding<MessageChannel>>> consumerBindings = (Map<String, List<Binding<MessageChannel>>>) channelBindingServiceAccessor
				.getPropertyValue("consumerBindings");
		Binding<MessageChannel> inputBinding = consumerBindings.get("input").get(0);
		SimpleMessageListenerContainer container = TestUtils.getPropertyValue(inputBinding,
				"lifecycle.messageListenerContainer", SimpleMessageListenerContainer.class);
		assertThat(TestUtils.getPropertyValue(container, "transactional", Boolean.class)).isTrue();
		Map<String, Binding<MessageChannel>> producerBindings = (Map<String, Binding<MessageChannel>>) TestUtils
				.getPropertyValue(bindingService, "producerBindings");
		Binding<MessageChannel> outputBinding = producerBindings.get("output");
		assertThat(TestUtils.getPropertyValue(outputBinding, "lifecycle.amqpTemplate.transactional",
				Boolean.class)).isTrue();
		DirectFieldAccessor binderFieldAccessor = new DirectFieldAccessor(binder);
		ConnectionFactory binderConnectionFactory = (ConnectionFactory) binderFieldAccessor
				.getPropertyValue("connectionFactory");
		assertThat(binderConnectionFactory).isInstanceOf(CachingConnectionFactory.class);
		ConnectionFactory connectionFactory = context.getBean(ConnectionFactory.class);
		assertThat(binderConnectionFactory).isSameAs(connectionFactory);
		CompositeHealthIndicator bindersHealthIndicator = context.getBean("bindersHealthIndicator",
				CompositeHealthIndicator.class);
		DirectFieldAccessor directFieldAccessor = new DirectFieldAccessor(bindersHealthIndicator);
		assertThat(bindersHealthIndicator).isNotNull();
		Map<String, HealthIndicator> healthIndicators = (Map<String, HealthIndicator>) directFieldAccessor
				.getPropertyValue("indicators");
		assertThat(healthIndicators).containsKey("rabbit");
		assertThat(healthIndicators.get("rabbit").health().getStatus()).isEqualTo(Status.UP);
	}

	@Test
	public void testParentConnectionFactoryInheritedIfOverridden() {
		context = new SpringApplicationBuilder(SimpleProcessor.class, ConnectionFactoryConfiguration.class)
				.web(WebApplicationType.NONE)
				.run("--server.port=0");
		BinderFactory binderFactory = context.getBean(BinderFactory.class);
		Binder<?, ?, ?> binder = binderFactory.getBinder(null, MessageChannel.class);
		assertThat(binder).isInstanceOf(RabbitMessageChannelBinder.class);
		DirectFieldAccessor binderFieldAccessor = new DirectFieldAccessor(binder);
		ConnectionFactory binderConnectionFactory = (ConnectionFactory) binderFieldAccessor
				.getPropertyValue("connectionFactory");
		assertThat(binderConnectionFactory).isSameAs(MOCK_CONNECTION_FACTORY);
		ConnectionFactory connectionFactory = context.getBean(ConnectionFactory.class);
		assertThat(binderConnectionFactory).isSameAs(connectionFactory);
		CompositeHealthIndicator bindersHealthIndicator = context.getBean("bindersHealthIndicator",
				CompositeHealthIndicator.class);
		assertThat(bindersHealthIndicator).isNotNull();
		DirectFieldAccessor directFieldAccessor = new DirectFieldAccessor(bindersHealthIndicator);
		@SuppressWarnings("unchecked")
		Map<String, HealthIndicator> healthIndicators = (Map<String, HealthIndicator>) directFieldAccessor
				.getPropertyValue("indicators");
		assertThat(healthIndicators).containsKey("rabbit");
		// mock connection factory behaves as if down
		assertThat(healthIndicators.get("rabbit").health().getStatus()).isEqualTo(Status.DOWN);
	}

	@Test
	public void testParentConnectionFactoryNotInheritedByCustomizedBindersAndProducerRetryBootProperties() {
		List<String> params = new ArrayList<>();
		params.add("--spring.cloud.stream.input.binder=custom");
		params.add("--spring.cloud.stream.output.binder=custom");
		params.add("--spring.cloud.stream.binders.custom.type=rabbit");
		params.add("--spring.cloud.stream.binders.custom.environment.foo=bar");
		params.add("--server.port=0");
		params.add("--spring.rabbitmq.template.retry.enabled=true");
		params.add("--spring.rabbitmq.template.retry.maxAttempts=2");
		params.add("--spring.rabbitmq.template.retry.initial-interval=1000");
		params.add("--spring.rabbitmq.template.retry.multiplier=1.1");
		params.add("--spring.rabbitmq.template.retry.max-interval=3000");
		context = new SpringApplicationBuilder(SimpleProcessor.class)
				.web(WebApplicationType.NONE)
				.run(params.toArray(new String[params.size()]));
		BinderFactory binderFactory = context.getBean(BinderFactory.class);
		@SuppressWarnings("unchecked")
		Binder<MessageChannel, ExtendedConsumerProperties<RabbitConsumerProperties>, ExtendedProducerProperties<RabbitProducerProperties>> binder = (Binder<MessageChannel, ExtendedConsumerProperties<RabbitConsumerProperties>, ExtendedProducerProperties<RabbitProducerProperties>>) binderFactory
				.getBinder(null, MessageChannel.class);
		assertThat(binder).isInstanceOf(RabbitMessageChannelBinder.class);
		DirectFieldAccessor binderFieldAccessor = new DirectFieldAccessor(binder);
		ConnectionFactory binderConnectionFactory = (ConnectionFactory) binderFieldAccessor
				.getPropertyValue("connectionFactory");
		ConnectionFactory connectionFactory = context.getBean(ConnectionFactory.class);
		assertThat(binderConnectionFactory).isNotSameAs(connectionFactory);
		CompositeHealthIndicator bindersHealthIndicator = context.getBean("bindersHealthIndicator",
				CompositeHealthIndicator.class);
		assertThat(bindersHealthIndicator);
		DirectFieldAccessor directFieldAccessor = new DirectFieldAccessor(bindersHealthIndicator);
		@SuppressWarnings("unchecked")
		Map<String, HealthIndicator> healthIndicators = (Map<String, HealthIndicator>) directFieldAccessor
				.getPropertyValue("indicators");
		assertThat(healthIndicators).containsKey("custom");
		assertThat(healthIndicators.get("custom").health().getStatus()).isEqualTo(Status.UP);
		Binding<MessageChannel> binding = binder.bindProducer("foo", new DirectChannel(),
				new ExtendedProducerProperties<>(new RabbitProducerProperties()));
		RetryTemplate template = TestUtils.getPropertyValue(binding, "lifecycle.amqpTemplate.retryTemplate",
				RetryTemplate.class);
		assertThat(template).isNotNull();
		SimpleRetryPolicy retryPolicy = TestUtils.getPropertyValue(template, "retryPolicy", SimpleRetryPolicy.class);
		ExponentialBackOffPolicy backOff = TestUtils.getPropertyValue(template, "backOffPolicy",
				ExponentialBackOffPolicy.class);
		assertThat(retryPolicy.getMaxAttempts()).isEqualTo(2);
		assertThat(backOff.getInitialInterval()).isEqualTo(1000L);
		assertThat(backOff.getMultiplier()).isEqualTo(1.1);
		assertThat(backOff.getMaxInterval()).isEqualTo(3000L);
		binding.unbind();
		context.close();
	}

	@Test
	public void testCloudProfile() {
		this.context = new SpringApplicationBuilder(SimpleProcessor.class, MockCloudConfiguration.class)
				.web(WebApplicationType.NONE)
				.profiles("cloud")
				.run();
		BinderFactory binderFactory = this.context.getBean(BinderFactory.class);
		Binder<?, ?, ?> binder = binderFactory.getBinder(null, MessageChannel.class);
		assertThat(binder).isInstanceOf(RabbitMessageChannelBinder.class);
		DirectFieldAccessor binderFieldAccessor = new DirectFieldAccessor(binder);
		ConnectionFactory binderConnectionFactory = (ConnectionFactory) binderFieldAccessor
				.getPropertyValue("connectionFactory");
		ConnectionFactory connectionFactory = this.context.getBean(ConnectionFactory.class);

		assertThat(binderConnectionFactory).isNotSameAs(connectionFactory);

		ConnectionFactory producerConnectionFactory = (ConnectionFactory) binderFieldAccessor
				.getPropertyValue("producerConnectionFactory");

		assertThat(producerConnectionFactory).isNotSameAs(connectionFactory);

		assertThat(binderConnectionFactory).isNotSameAs(producerConnectionFactory);

		assertThat(TestUtils.getPropertyValue(connectionFactory, "addresses")).isNotNull();
		assertThat(TestUtils.getPropertyValue(binderConnectionFactory, "addresses")).isNull();
		assertThat(TestUtils.getPropertyValue(producerConnectionFactory, "addresses")).isNull();

		Cloud cloud = this.context.getBean(Cloud.class);

		verify(cloud).getSingletonServiceConnector(ConnectionFactory.class, null);
	}

	@EnableBinding(Processor.class)
	@SpringBootApplication
	public static class SimpleProcessor {

	}

	public static class ConnectionFactoryConfiguration {

		@Bean
		public ConnectionFactory connectionFactory() {
			return MOCK_CONNECTION_FACTORY;
		}

	}

	public static class MockCloudConfiguration {

		@Bean
		public Cloud cloud() {
			Cloud cloud = mock(Cloud.class);

			willReturn(new CachingConnectionFactory())
							.given(cloud)
							.getSingletonServiceConnector(ConnectionFactory.class, null);

			return cloud;
		}

	}

}
