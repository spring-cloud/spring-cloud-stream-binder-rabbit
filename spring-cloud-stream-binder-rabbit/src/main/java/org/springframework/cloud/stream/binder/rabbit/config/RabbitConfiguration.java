/*
 * Copyright 2015-2021 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit.config;

import java.time.Duration;

import com.rabbitmq.client.impl.CredentialsProvider;
import com.rabbitmq.client.impl.CredentialsRefreshService;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionNameStrategy;
import org.springframework.amqp.rabbit.connection.RabbitConnectionFactoryBean;
import org.springframework.amqp.rabbit.core.RabbitOperations;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.amqp.CachingConnectionFactoryConfigurer;
import org.springframework.boot.autoconfigure.amqp.ConnectionFactoryCustomizer;
import org.springframework.boot.autoconfigure.amqp.RabbitConnectionFactoryBeanConfigurer;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ResourceLoader;
import org.springframework.retry.support.RetryTemplate;

/**
 * Configuration for {@link RabbitTemplate} and {@link CachingConnectionFactory}.
 *
 * @author Chris Bono
 * @since 3.2
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(RabbitProperties.class)
public class RabbitConfiguration {

	@Bean
	@ConditionalOnMissingBean
	RabbitConnectionFactoryBeanConfigurer rabbitConnectionFactoryBeanConfigurer(RabbitProperties properties,
			ResourceLoader resourceLoader, ObjectProvider<CredentialsProvider> credentialsProvider,
			ObjectProvider<CredentialsRefreshService> credentialsRefreshService) {
		RabbitConnectionFactoryBeanConfigurer configurer = new RabbitConnectionFactoryBeanConfigurer();
		configurer.setRabbitProperties(properties);
		configurer.setResourceLoader(resourceLoader);
		configurer.setCredentialsProvider(credentialsProvider.getIfUnique());
		configurer.setCredentialsRefreshService(credentialsRefreshService.getIfUnique());
		return configurer;
	}

	@Bean
	@ConditionalOnMissingBean
	CachingConnectionFactoryConfigurer rabbitConnectionFactoryConfigurer(RabbitProperties rabbitProperties,
			ObjectProvider<ConnectionNameStrategy> connectionNameStrategy) {
		CachingConnectionFactoryConfigurer configurer = new CachingConnectionFactoryConfigurer();
		configurer.setRabbitProperties(rabbitProperties);
		configurer.setConnectionNameStrategy(connectionNameStrategy.getIfUnique());
		return configurer;
	}

	@Bean
	@ConditionalOnMissingBean(ConnectionFactory.class)
	CachingConnectionFactory rabbitConnectionFactory(
			RabbitConnectionFactoryBeanConfigurer rabbitConnectionFactoryBeanConfigurer,
			CachingConnectionFactoryConfigurer rabbitCachingConnectionFactoryConfigurer,
			ObjectProvider<ConnectionFactoryCustomizer> connectionFactoryCustomizers) throws Exception {

		RabbitConnectionFactoryBean connectionFactoryBean = new RabbitConnectionFactoryBean();
		rabbitConnectionFactoryBeanConfigurer.configure(connectionFactoryBean);
		connectionFactoryBean.afterPropertiesSet();
		com.rabbitmq.client.ConnectionFactory connectionFactory = connectionFactoryBean.getObject();
		connectionFactoryCustomizers.orderedStream()
				.forEach((customizer) -> customizer.customize(connectionFactory));

		CachingConnectionFactory cachingConnectionFactory = new CachingConnectionFactory(connectionFactory);
		rabbitCachingConnectionFactoryConfigurer.configure(cachingConnectionFactory);

		return cachingConnectionFactory;
	}

	@Bean
	@ConditionalOnSingleCandidate(ConnectionFactory.class)
	@ConditionalOnMissingBean(RabbitOperations.class)
	public RabbitTemplate rabbitTemplate(RabbitProperties rabbitProperties, ConnectionFactory connectionFactory, ObjectProvider<MessageConverter> messageConverter,
			ObjectProvider<RetryTemplate> retryTemplate) {
		RabbitTemplate template = new RabbitTemplate();
		template.setConnectionFactory(connectionFactory);
		template.setMandatory(determineMandatoryFlag(rabbitProperties));
		messageConverter.ifAvailable(template::setMessageConverter);
		retryTemplate.ifAvailable(template::setRetryTemplate);
		PropertyMapper map = PropertyMapper.get();
		RabbitProperties.Template templateProperties = rabbitProperties.getTemplate();
		map.from(templateProperties::getReceiveTimeout).whenNonNull().as(Duration::toMillis)
				.to(template::setReceiveTimeout);
		map.from(templateProperties::getReplyTimeout).whenNonNull().as(Duration::toMillis)
				.to(template::setReplyTimeout);
		map.from(templateProperties::getExchange).to(template::setExchange);
		map.from(templateProperties::getRoutingKey).to(template::setRoutingKey);
		map.from(templateProperties::getDefaultReceiveQueue).whenNonNull().to(template::setDefaultReceiveQueue);
		return template;
	}

	private boolean determineMandatoryFlag(RabbitProperties rabbitProperties) {
		Boolean mandatory = rabbitProperties.getTemplate().getMandatory();
		return (mandatory != null) ? mandatory : rabbitProperties.isPublisherReturns();
	}

}
