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

package org.springframework.cloud.stream.binder.rabbit.config;

import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.RabbitConnectionFactoryBean;
import org.springframework.amqp.support.postprocessor.DelegatingDecompressingPostProcessor;
import org.springframework.amqp.support.postprocessor.GZipPostProcessor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.binder.rabbit.RabbitMessageChannelBinder;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitBinderConfigurationProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitExtendedBindingProperties;
import org.springframework.cloud.stream.binder.rabbit.provisioning.RabbitExchangeQueueProvisioner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.cloud.stream.config.codec.kryo.KryoCodecAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.codec.Codec;

/**
 * Configuration class for RabbitMQ message channel binder.
 *
 * @author David Turanski
 * @author Artem Bilan
 */

@Configuration
@Import({PropertyPlaceholderAutoConfiguration.class, KryoCodecAutoConfiguration.class})
@EnableConfigurationProperties({RabbitBinderConfigurationProperties.class, RabbitExtendedBindingProperties.class})
public class RabbitMessageChannelBinderConfiguration {

	@Autowired
	private ConfigurableApplicationContext applicationContext;

	@Autowired
	private Codec codec;

	@Autowired
	private ConnectionFactory rabbitConnectionFactory;

	@Autowired
	private RabbitProperties rabbitProperties;

	@Autowired
	private RabbitBinderConfigurationProperties rabbitBinderConfigurationProperties;

	@Autowired
	private RabbitExtendedBindingProperties rabbitExtendedBindingProperties;

	@Bean
	RabbitMessageChannelBinder rabbitMessageChannelBinder(
			@Qualifier("producerConnectionFactory") ObjectProvider<ConnectionFactory> producerConnectionFactory)
			throws Exception {

		RabbitMessageChannelBinder binder = new RabbitMessageChannelBinder(rabbitConnectionFactory, rabbitProperties,
				provisioningProvider());
		binder.setProducerConnectionFactory(obtainProducerConnectionFactory(producerConnectionFactory));
		binder.setCodec(codec);
		binder.setAdminAddresses(rabbitBinderConfigurationProperties.getAdminAddresses());
		binder.setCompressingPostProcessor(gZipPostProcessor());
		binder.setDecompressingPostProcessor(deCompressingPostProcessor());
		binder.setNodes(this.rabbitBinderConfigurationProperties.getNodes());
		binder.setExtendedBindingProperties(this.rabbitExtendedBindingProperties);
		return binder;
	}

	private ConnectionFactory obtainProducerConnectionFactory(
			ObjectProvider<ConnectionFactory> connectionFactoryObjectProvider) throws Exception {

		ConnectionFactory connectionFactory = connectionFactoryObjectProvider.getIfAvailable();

		if (connectionFactory != null) {
			return connectionFactory;
		}
		else {
			return buildProducerConnectionFactory();
		}
	}

	/**
	 * @see org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration.RabbitConnectionFactoryCreator
	 */
	private CachingConnectionFactory buildProducerConnectionFactory() throws Exception {
		com.rabbitmq.client.ConnectionFactory rabbitConnectionFactory;
		if (this.rabbitConnectionFactory instanceof CachingConnectionFactory) {
			rabbitConnectionFactory =
					((CachingConnectionFactory) this.rabbitConnectionFactory).getRabbitConnectionFactory();
		}
		else {
			RabbitConnectionFactoryBean factory = new RabbitConnectionFactoryBean();
			String host = this.rabbitProperties.determineHost();
			if (host != null) {
				factory.setHost(host);
			}

			factory.setPort(this.rabbitProperties.determinePort());


			String username = this.rabbitProperties.determineUsername();
			if (username != null) {
				factory.setUsername(username);
			}

			String password = this.rabbitProperties.determinePassword();
			if (password != null) {
				factory.setPassword(password);
			}

			String virtualHost = this.rabbitProperties.determineVirtualHost();
			if (virtualHost != null) {
				factory.setVirtualHost(virtualHost);
			}

			Integer requestedHeartbeat = this.rabbitProperties.getRequestedHeartbeat();
			if (requestedHeartbeat != null) {
				factory.setRequestedHeartbeat(requestedHeartbeat);
			}
			RabbitProperties.Ssl ssl = this.rabbitProperties.getSsl();

			if (ssl.isEnabled()) {
				factory.setUseSSL(true);
				if (ssl.getAlgorithm() != null) {
					factory.setSslAlgorithm(ssl.getAlgorithm());
				}
				factory.setKeyStore(ssl.getKeyStore());
				factory.setKeyStorePassphrase(ssl.getKeyStorePassword());
				factory.setTrustStore(ssl.getTrustStore());
				factory.setTrustStorePassphrase(ssl.getTrustStorePassword());
			}

			Integer connectionTimeout = this.rabbitProperties.getConnectionTimeout();
			if (connectionTimeout != null) {
				factory.setConnectionTimeout(connectionTimeout);
			}

			factory.afterPropertiesSet();

			rabbitConnectionFactory = factory.getObject();
		}

		CachingConnectionFactory connectionFactory = new CachingConnectionFactory(rabbitConnectionFactory);
		connectionFactory.setAddresses(this.rabbitProperties.determineAddresses());
		connectionFactory.setPublisherConfirms(this.rabbitProperties.isPublisherConfirms());
		connectionFactory.setPublisherReturns(this.rabbitProperties.isPublisherReturns());

		RabbitProperties.Cache.Channel channel = this.rabbitProperties.getCache().getChannel();
		RabbitProperties.Cache.Connection connection = this.rabbitProperties.getCache().getConnection();

		Integer channelSize = channel.getSize();
		if (channelSize != null) {
			connectionFactory.setChannelCacheSize(channelSize);
		}

		Long channelCheckoutTimeout = channel.getCheckoutTimeout();
		if (channelCheckoutTimeout != null) {
			connectionFactory.setChannelCheckoutTimeout(
					channelCheckoutTimeout);
		}

		CachingConnectionFactory.CacheMode connectionMode = connection.getMode();
		if (connectionMode != null) {
			connectionFactory.setCacheMode(connectionMode);
		}

		Integer connectionSize = connection.getSize();
		if (connectionSize != null) {
			connectionFactory.setConnectionCacheSize(connectionSize);
		}

		connectionFactory.setApplicationContext(this.applicationContext);
		this.applicationContext.addApplicationListener(connectionFactory);
		connectionFactory.afterPropertiesSet();

		return connectionFactory;
	}

	@Bean
	MessagePostProcessor deCompressingPostProcessor() {
		return new DelegatingDecompressingPostProcessor();
	}

	@Bean
	MessagePostProcessor gZipPostProcessor() {
		GZipPostProcessor gZipPostProcessor = new GZipPostProcessor();
		gZipPostProcessor.setLevel(this.rabbitBinderConfigurationProperties.getCompressionLevel());
		return gZipPostProcessor;
	}

	@Bean
	RabbitExchangeQueueProvisioner provisioningProvider() {
		return new RabbitExchangeQueueProvisioner(this.rabbitConnectionFactory);
	}

}
