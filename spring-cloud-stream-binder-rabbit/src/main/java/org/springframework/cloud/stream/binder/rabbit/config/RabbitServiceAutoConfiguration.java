/*
 * Copyright 2015-2017 the original author or authors.
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

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.ObjectProvider;import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.RabbitHealthIndicator;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.Cloud;
import org.springframework.cloud.CloudFactory;
import org.springframework.cloud.service.messaging.RabbitConnectionFactoryConfig;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.util.StringUtils;

/**
 * Bind to services, either locally or in a cloud environment.
 *
 * @author Mark Fisher
 * @author Dave Syer
 * @author Glenn Renfro
 * @author David Turanski
 * @author Eric Bottard
 * @author Marius Bogoevici
 * @author Ilayaperumal Gopinathan
 * @author Artem Bilan
 */
@Configuration
@ConditionalOnMissingBean(Binder.class)
@Import({ RabbitMessageChannelBinderConfiguration.class,
		RabbitServiceAutoConfiguration.RabbitHealthIndicatorConfiguration.class })
public class RabbitServiceAutoConfiguration {

	/**
	 * Configuration to be used when the cloud profile is set.
	 */
	@Configuration
	@Profile("cloud")
	protected static class CloudProfile {

		/**
		 * Configuration to be used when the cloud profile is set, and Cloud Connectors are found
		 * on the classpath.
		 */
		@Configuration
		@ConditionalOnClass(Cloud.class)
		protected static class CloudConnectors {

			@Bean
			@ConditionalOnMissingBean
			public Cloud cloud() {
				return new CloudFactory().getCloud();
			}

			/**
			 * Active only if {@code spring.cloud.stream.overrideCloudConnectors} is not set to
			 * {@code true}.
			 */
			@Configuration
			@ConditionalOnProperty(value = "spring.cloud.stream.overrideCloudConnectors", havingValue = "false", matchIfMissing = true)
			// Required to parse Rabbit properties which are passed to the binder for
			// clustering. We need to enable it here explicitly as the default Rabbit
			// configuration is not triggered.
			@EnableConfigurationProperties(RabbitProperties.class)
			protected static class UseCloudConnectors {

				/**
				 * Creates a {@link ConnectionFactory} using the singleton service connector.
				 * @param cloud {@link Cloud} instance to be used for accessing services.
				 * @param connectorConfigObjectProvider the {@link ObjectProvider} for the
				 * {@link RabbitConnectionFactoryConfig}.
				 * @return the {@link ConnectionFactory} used by the binder.
				 */
				@Bean
				@Primary
				ConnectionFactory rabbitConnectionFactory(Cloud cloud,
						ObjectProvider<RabbitConnectionFactoryConfig> connectorConfigObjectProvider,
						ConfigurableApplicationContext applicationContext,
						RabbitProperties rabbitProperties) throws Exception {

					ConnectionFactory connectionFactory = cloud.getSingletonServiceConnector(ConnectionFactory.class,
							connectorConfigObjectProvider.getIfUnique());

					configureCachingConnectionFactory((CachingConnectionFactory) connectionFactory,
							applicationContext, rabbitProperties);

					return connectionFactory;
				}

				@Bean
				@ConditionalOnMissingBean(RabbitTemplate.class)
				RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
					return new RabbitTemplate(connectionFactory);
				}
			}

			/**
			 * Configuration to be used if {@code spring.cloud.stream.overrideCloudConnectors} is set
			 * to {@code true}. Defers to Spring Boot auto-configuration.
			 */
			@Configuration
			@ConditionalOnProperty("spring.cloud.stream.overrideCloudConnectors")
			@Import(RabbitAutoConfiguration.class)
			protected static class OverrideCloudConnectors {

			}

		}

		@Configuration
		@ConditionalOnMissingClass("org.springframework.cloud.Cloud")
		@Import(RabbitAutoConfiguration.class)
		protected static class NoCloudConnectors {

		}

	}

	/**
	 * Configuration to be used when the cloud profile is not set. Defer to Spring Boot
	 * auto-configuration.
	 */
	@Configuration
	@Profile("!cloud")
	@Import(RabbitAutoConfiguration.class)
	protected static class NoCloudProfile {
	}

	@Configuration
	@ConditionalOnClass(name = "org.springframework.boot.actuate.health.HealthIndicator")
	public static class RabbitHealthIndicatorConfiguration {

		@Bean
		public HealthIndicator binderHealthIndicator(RabbitTemplate rabbitTemplate) {
			return new RabbitHealthIndicator(rabbitTemplate);
		}

	}

	static void configureCachingConnectionFactory(CachingConnectionFactory connectionFactory,
			ConfigurableApplicationContext applicationContext, RabbitProperties rabbitProperties) throws Exception {

		if (StringUtils.hasText(rabbitProperties.getAddresses())) {
			connectionFactory.setAddresses(rabbitProperties.determineAddresses());
		}

		connectionFactory.setPublisherConfirms(rabbitProperties.isPublisherConfirms());
		connectionFactory.setPublisherReturns(rabbitProperties.isPublisherReturns());
		if (rabbitProperties.getCache().getChannel().getSize() != null) {
			connectionFactory.setChannelCacheSize(rabbitProperties.getCache().getChannel().getSize());
		}
		if (rabbitProperties.getCache().getConnection().getMode() != null) {
			connectionFactory.setCacheMode(rabbitProperties.getCache().getConnection().getMode());
		}
		if (rabbitProperties.getCache().getConnection().getSize() != null) {
			connectionFactory.setConnectionCacheSize(
					rabbitProperties.getCache().getConnection().getSize());
		}
		if (rabbitProperties.getCache().getChannel().getCheckoutTimeout() != null) {
			connectionFactory.setChannelCheckoutTimeout(
					rabbitProperties.getCache().getChannel().getCheckoutTimeout());
		}
		connectionFactory.setApplicationContext(applicationContext);
		applicationContext.addApplicationListener(connectionFactory);
		connectionFactory.afterPropertiesSet();
	}

}
