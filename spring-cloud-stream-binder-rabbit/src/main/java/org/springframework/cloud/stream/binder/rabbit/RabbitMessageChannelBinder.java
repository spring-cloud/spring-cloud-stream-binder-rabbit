/*
 * Copyright 2013-2019 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.ImmediateAcknowledgeAmqpException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.LocalizedQueueConnectionFactory;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.core.BatchingRabbitTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.core.support.BatchingStrategy;
import org.springframework.amqp.rabbit.core.support.SimpleBatchingStrategy;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.DirectMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;
import org.springframework.amqp.rabbit.retry.RejectAndDontRequeueRecoverer;
import org.springframework.amqp.rabbit.retry.RepublishMessageRecoverer;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.amqp.support.converter.AbstractMessageConverter;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.amqp.support.postprocessor.DelegatingDecompressingPostProcessor;
import org.springframework.amqp.support.postprocessor.GZipPostProcessor;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties.ContainerType;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties.Retry;
import org.springframework.cloud.stream.binder.AbstractMessageChannelBinder;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;
import org.springframework.cloud.stream.binder.DefaultPollableMessageSource;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedPropertiesBinder;
import org.springframework.cloud.stream.binder.HeaderMode;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitCommonProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitExtendedBindingProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitProducerProperties;
import org.springframework.cloud.stream.binder.rabbit.provisioning.RabbitExchangeQueueProvisioner;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.acks.AcknowledgmentCallback.Status;
import org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter;
import org.springframework.integration.amqp.inbound.AmqpMessageSource;
import org.springframework.integration.amqp.outbound.AmqpOutboundEndpoint;
import org.springframework.integration.amqp.support.AmqpMessageHeaderErrorMessageStrategy;
import org.springframework.integration.amqp.support.DefaultAmqpHeaderMapper;
import org.springframework.integration.channel.AbstractMessageChannel;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.support.DefaultErrorMessageStrategy;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

/**
 * A {@link org.springframework.cloud.stream.binder.Binder} implementation backed by
 * RabbitMQ.
 *
 * @author Mark Fisher
 * @author Gary Russell
 * @author Jennifer Hickey
 * @author Gunnar Hillert
 * @author Ilayaperumal Gopinathan
 * @author David Turanski
 * @author Marius Bogoevici
 * @author Artem Bilan
 * @author Soby Chacko
 * @author Oleg Zhurakousky
 */
public class RabbitMessageChannelBinder
		extends AbstractMessageChannelBinder<ExtendedConsumerProperties<RabbitConsumerProperties>,
		ExtendedProducerProperties<RabbitProducerProperties>, RabbitExchangeQueueProvisioner>
		implements ExtendedPropertiesBinder<MessageChannel, RabbitConsumerProperties, RabbitProducerProperties>,
			DisposableBean {

	private static final SimplePassthroughMessageConverter passThoughConverter =
			new SimplePassthroughMessageConverter();

	private static final AmqpMessageHeaderErrorMessageStrategy errorMessageStrategy =
			new AmqpMessageHeaderErrorMessageStrategy();

	private static final MessagePropertiesConverter inboundMessagePropertiesConverter =
			new DefaultMessagePropertiesConverter() {

				@Override
				public MessageProperties toMessageProperties(AMQP.BasicProperties source, Envelope envelope,
						String charset) {
					MessageProperties properties = super.toMessageProperties(source, envelope, charset);
					properties.setDeliveryMode(null);
					return properties;
				}

			};

	private final RabbitProperties rabbitProperties;

	private boolean destroyConnectionFactory;

	private ConnectionFactory connectionFactory;

	private MessagePostProcessor decompressingPostProcessor = new DelegatingDecompressingPostProcessor();

	private MessagePostProcessor compressingPostProcessor = new GZipPostProcessor();

	private volatile String[] adminAddresses;

	private volatile String[] nodes;

	private volatile boolean clustered;

	private RabbitExtendedBindingProperties extendedBindingProperties = new RabbitExtendedBindingProperties();

	public RabbitMessageChannelBinder(ConnectionFactory connectionFactory, RabbitProperties rabbitProperties,
			RabbitExchangeQueueProvisioner provisioningProvider) {
		this(connectionFactory, rabbitProperties, provisioningProvider, null);
	}

	public RabbitMessageChannelBinder(ConnectionFactory connectionFactory, RabbitProperties rabbitProperties,
			RabbitExchangeQueueProvisioner provisioningProvider,
			ListenerContainerCustomizer<AbstractMessageListenerContainer> containerCustomizer) {
		super(new String[0], provisioningProvider, containerCustomizer);
		Assert.notNull(connectionFactory, "connectionFactory must not be null");
		Assert.notNull(rabbitProperties, "rabbitProperties must not be null");
		this.connectionFactory = connectionFactory;
		this.rabbitProperties = rabbitProperties;
	}

	/**
	 * Set a {@link MessagePostProcessor} to decompress messages. Defaults to a
	 * {@link DelegatingDecompressingPostProcessor} with its default delegates.
	 * @param decompressingPostProcessor the post processor.
	 */
	public void setDecompressingPostProcessor(MessagePostProcessor decompressingPostProcessor) {
		this.decompressingPostProcessor = decompressingPostProcessor;
	}

	/**
	 * Set a {@link org.springframework.amqp.core.MessagePostProcessor} to compress messages.
	 * Defaults to a {@link org.springframework.amqp.support.postprocessor.GZipPostProcessor}.
	 * @param compressingPostProcessor the post processor.
	 */
	public void setCompressingPostProcessor(MessagePostProcessor compressingPostProcessor) {
		this.compressingPostProcessor = compressingPostProcessor;
	}

	public void setAdminAddresses(String[] adminAddresses) {
		this.adminAddresses = Arrays.copyOf(adminAddresses, adminAddresses.length);
	}

	public void setNodes(String[] nodes) {
		this.nodes = Arrays.copyOf(nodes, nodes.length);
		this.clustered = nodes.length > 1;
	}

	public void setExtendedBindingProperties(RabbitExtendedBindingProperties extendedBindingProperties) {
		this.extendedBindingProperties = extendedBindingProperties;
	}

	@Override
	public void onInit() throws Exception {
		super.onInit();
		if (this.clustered) {
			String[] addresses = StringUtils.commaDelimitedListToStringArray(this.rabbitProperties.getAddresses());

			Assert.state(addresses.length == this.adminAddresses.length
					&& addresses.length == this.nodes.length,
					"'addresses', 'adminAddresses', and 'nodes' properties must have equal length");

			this.connectionFactory = new LocalizedQueueConnectionFactory(this.connectionFactory, addresses,
					this.adminAddresses, this.nodes, this.rabbitProperties.getVirtualHost(),
					this.rabbitProperties.getUsername(), this.rabbitProperties.getPassword(),
					this.rabbitProperties.getSsl().isEnabled(), this.rabbitProperties.getSsl().getKeyStore(),
					this.rabbitProperties.getSsl().getTrustStore(),
					this.rabbitProperties.getSsl().getKeyStorePassword(),
					this.rabbitProperties.getSsl().getTrustStorePassword());
			this.destroyConnectionFactory = true;
		}
	}

	@Override
	public void destroy() throws Exception {
		if (this.connectionFactory instanceof DisposableBean) {
			if (this.destroyConnectionFactory) {
				((DisposableBean) this.connectionFactory).destroy();
			}
		}
	}

	@Override
	public RabbitConsumerProperties getExtendedConsumerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedConsumerProperties(channelName);
	}

	@Override
	public RabbitProducerProperties getExtendedProducerProperties(String channelName) {
		return this.extendedBindingProperties.getExtendedProducerProperties(channelName);
	}

	@Override
	public String getDefaultsPrefix() {
		return this.extendedBindingProperties.getDefaultsPrefix();
	}

	@Override
	public Class<? extends BinderSpecificPropertiesProvider> getExtendedPropertiesEntryClass() {
		return this.extendedBindingProperties.getExtendedPropertiesEntryClass();
	}

	@Override
	protected MessageHandler createProducerMessageHandler(final ProducerDestination producerDestination,
			ExtendedProducerProperties<RabbitProducerProperties> producerProperties, MessageChannel errorChannel) {
		Assert.state(!HeaderMode.embeddedHeaders.equals(producerProperties.getHeaderMode()),
				"the RabbitMQ binder does not support embedded headers since RabbitMQ supports headers natively");
		String prefix = producerProperties.getExtension().getPrefix();
		String exchangeName = producerDestination.getName();
		String destination = StringUtils.isEmpty(prefix) ? exchangeName : exchangeName.substring(prefix.length());
		final AmqpOutboundEndpoint endpoint = new AmqpOutboundEndpoint(
				buildRabbitTemplate(producerProperties.getExtension(), errorChannel != null));
		endpoint.setExchangeName(producerDestination.getName());
		RabbitProducerProperties extendedProperties = producerProperties.getExtension();
		boolean expressionInterceptorNeeded = expressionInterceptorNeeded(extendedProperties);
		Expression routingKeyExpression = extendedProperties.getRoutingKeyExpression();
		if (!producerProperties.isPartitioned()) {
			if (routingKeyExpression == null) {
				endpoint.setRoutingKey(destination);
			}
			else {
				if (expressionInterceptorNeeded) {
					endpoint.setRoutingKeyExpressionString("headers['"
							+ RabbitExpressionEvaluatingInterceptor.ROUTING_KEY_HEADER + "']");
				}
				else {
					endpoint.setRoutingKeyExpression(routingKeyExpression);
				}
			}
		}
		else {
			if (routingKeyExpression == null) {
				endpoint.setRoutingKeyExpression(buildPartitionRoutingExpression(destination, false));
			}
			else {
				if (expressionInterceptorNeeded) {
					endpoint.setRoutingKeyExpression(buildPartitionRoutingExpression("headers['"
							+ RabbitExpressionEvaluatingInterceptor.ROUTING_KEY_HEADER + "']", true));
				}
				else {
					endpoint.setRoutingKeyExpression(buildPartitionRoutingExpression(routingKeyExpression.getExpressionString(),
							true));
				}
			}
		}
		if (extendedProperties.getDelayExpression() != null) {
			if (expressionInterceptorNeeded) {
				endpoint.setDelayExpressionString("headers['"
						+ RabbitExpressionEvaluatingInterceptor.DELAY_HEADER + "']");
			}
			else {
				endpoint.setDelayExpression(extendedProperties.getDelayExpression());
			}
		}
		DefaultAmqpHeaderMapper mapper = DefaultAmqpHeaderMapper.outboundMapper();
		List<String> headerPatterns = new ArrayList<>(extendedProperties.getHeaderPatterns().length + 1);
		headerPatterns.add("!" + BinderHeaders.PARTITION_HEADER);
		headerPatterns.addAll(Arrays.asList(extendedProperties.getHeaderPatterns()));
		mapper.setRequestHeaderNames(headerPatterns.toArray(new String[headerPatterns.size()]));
		endpoint.setHeaderMapper(mapper);
		endpoint.setDefaultDeliveryMode(extendedProperties.getDeliveryMode());
		endpoint.setBeanFactory(this.getBeanFactory());
		if (errorChannel != null) {
			checkConnectionFactoryIsErrorCapable();
			endpoint.setReturnChannel(errorChannel);
			endpoint.setConfirmNackChannel(errorChannel);
			String ackChannelBeanName = StringUtils.hasText(extendedProperties.getConfirmAckChannel())
					? extendedProperties.getConfirmAckChannel()
					: IntegrationContextUtils.NULL_CHANNEL_BEAN_NAME;
			if (!ackChannelBeanName.equals(IntegrationContextUtils.NULL_CHANNEL_BEAN_NAME)
					&& !getApplicationContext().containsBean(ackChannelBeanName)) {
				GenericApplicationContext context = (GenericApplicationContext) getApplicationContext();
				context.registerBean(ackChannelBeanName, DirectChannel.class, () -> new DirectChannel());
			}
			endpoint.setConfirmAckChannelName(ackChannelBeanName);
			endpoint.setConfirmCorrelationExpressionString("#root");
			endpoint.setErrorMessageStrategy(new DefaultErrorMessageStrategy());
		}
		endpoint.setHeadersMappedLast(true);
		return endpoint;
	}


	@Override
	protected void postProcessOutputChannel(MessageChannel outputChannel,
			ExtendedProducerProperties<RabbitProducerProperties> producerProperties) {
		RabbitProducerProperties extendedProperties = producerProperties.getExtension();
		if (expressionInterceptorNeeded(extendedProperties)) {
			((AbstractMessageChannel) outputChannel).addInterceptor(0,
					new RabbitExpressionEvaluatingInterceptor(extendedProperties.getRoutingKeyExpression(),
							extendedProperties.getDelayExpression(), getEvaluationContext()));
		}
	}

	private boolean expressionInterceptorNeeded(RabbitProducerProperties extendedProperties) {
		return extendedProperties.getRoutingKeyExpression() != null
					&& extendedProperties.getRoutingKeyExpression().getExpressionString().contains("payload")
				|| (extendedProperties.getDelayExpression() != null
					&& extendedProperties.getDelayExpression().getExpressionString().contains("payload"));
	}

	private void checkConnectionFactoryIsErrorCapable() {
		if (!(this.connectionFactory instanceof CachingConnectionFactory)) {
			logger.warn("Unknown connection factory type, cannot determine error capabilities: "
					+ this.connectionFactory.getClass());
		}
		else {
			CachingConnectionFactory ccf = (CachingConnectionFactory) this.connectionFactory;
			if (!ccf.isPublisherConfirms() && !ccf.isPublisherReturns()) {
				logger.warn("Producer error channel is enabled, but the connection factory is not configured for "
						+ "returns or confirms; the error channel will receive no messages");
			}
			else if (!ccf.isPublisherConfirms()) {
				logger.info("Producer error channel is enabled, but the connection factory is only configured to "
						+ "handle returned messages; negative acks will not be reported");
			}
			else if (!ccf.isPublisherReturns()) {
				logger.info("Producer error channel is enabled, but the connection factory is only configured to "
						+ "handle negatively acked messages; returned messages will not be reported");
			}
		}
	}

	private Expression buildPartitionRoutingExpression(String expressionRoot, boolean rootIsExpression) {
		String partitionRoutingExpression = rootIsExpression
				? expressionRoot + " + '-' + headers['" + BinderHeaders.PARTITION_HEADER + "']"
				: "'" + expressionRoot + "-' + headers['" + BinderHeaders.PARTITION_HEADER + "']";
		return new SpelExpressionParser().parseExpression(partitionRoutingExpression);
	}

	@Override
	protected MessageProducer createConsumerEndpoint(ConsumerDestination consumerDestination, String group,
			ExtendedConsumerProperties<RabbitConsumerProperties> properties) {
		Assert.state(!HeaderMode.embeddedHeaders.equals(properties.getHeaderMode()),
				"the RabbitMQ binder does not support embedded headers since RabbitMQ supports headers natively");
		String destination = consumerDestination.getName();
		boolean directContainer = properties.getExtension().getContainerType().equals(ContainerType.DIRECT);
		AbstractMessageListenerContainer listenerContainer = directContainer
				? new DirectMessageListenerContainer(this.connectionFactory)
				: new SimpleMessageListenerContainer(this.connectionFactory);
		listenerContainer.setAcknowledgeMode(properties.getExtension().getAcknowledgeMode());
		listenerContainer.setChannelTransacted(properties.getExtension().isTransacted());
		listenerContainer.setDefaultRequeueRejected(properties.getExtension().isRequeueRejected());
		int concurrency = properties.getConcurrency();
		concurrency = concurrency > 0 ? concurrency : 1;
		if (directContainer) {
			setDMLCProperties(properties, (DirectMessageListenerContainer) listenerContainer, concurrency);
		}
		else {
			setSMLCProperties(properties, (SimpleMessageListenerContainer) listenerContainer, concurrency);
		}
		listenerContainer.setPrefetchCount(properties.getExtension().getPrefetch());
		listenerContainer.setRecoveryInterval(properties.getExtension().getRecoveryInterval());
		listenerContainer.setTaskExecutor(new SimpleAsyncTaskExecutor(consumerDestination.getName() + "-"));
		String[] queues = StringUtils.tokenizeToStringArray(destination, ",", true, true);
		listenerContainer.setQueueNames(queues);
		listenerContainer.setAfterReceivePostProcessors(this.decompressingPostProcessor);
		listenerContainer.setMessagePropertiesConverter(
				RabbitMessageChannelBinder.inboundMessagePropertiesConverter);
		listenerContainer.setExclusive(properties.getExtension().isExclusive());
		listenerContainer.setMissingQueuesFatal(properties.getExtension().getMissingQueuesFatal());
		if (properties.getExtension().getFailedDeclarationRetryInterval() != null) {
			listenerContainer.setFailedDeclarationRetryInterval(
					properties.getExtension().getFailedDeclarationRetryInterval());
		}
		if (getApplicationEventPublisher() != null) {
			listenerContainer.setApplicationEventPublisher(getApplicationEventPublisher());
		}
		else if (getApplicationContext() != null) {
			listenerContainer.setApplicationEventPublisher(getApplicationContext());
		}
		getContainerCustomizer().configure(listenerContainer, consumerDestination.getName(), group);
		if (StringUtils.hasText(properties.getExtension().getConsumerTagPrefix())) {
			final AtomicInteger index = new AtomicInteger();
			listenerContainer.setConsumerTagStrategy(q ->
				properties.getExtension().getConsumerTagPrefix() + "#" + index.getAndIncrement());
		}
		listenerContainer.afterPropertiesSet();

		AmqpInboundChannelAdapter adapter = new AmqpInboundChannelAdapter(listenerContainer);
		adapter.setBeanFactory(this.getBeanFactory());
		adapter.setBeanName("inbound." + destination);
		DefaultAmqpHeaderMapper mapper = DefaultAmqpHeaderMapper.inboundMapper();
		mapper.setRequestHeaderNames(properties.getExtension().getHeaderPatterns());
		adapter.setHeaderMapper(mapper);
		ErrorInfrastructure errorInfrastructure = registerErrorInfrastructure(consumerDestination, group, properties);
		if (properties.getMaxAttempts() > 1) {
			adapter.setRetryTemplate(buildRetryTemplate(properties));
			adapter.setRecoveryCallback(errorInfrastructure.getRecoverer());
		}
		else {
			adapter.setErrorMessageStrategy(errorMessageStrategy);
			adapter.setErrorChannel(errorInfrastructure.getErrorChannel());
		}
		adapter.setMessageConverter(passThoughConverter);
		return adapter;
	}

	private void setSMLCProperties(ExtendedConsumerProperties<RabbitConsumerProperties> properties,
			SimpleMessageListenerContainer listenerContainer, int concurrency) {

		listenerContainer.setConcurrentConsumers(concurrency);
		int maxConcurrency = properties.getExtension().getMaxConcurrency();
		if (maxConcurrency > concurrency) {
			listenerContainer.setMaxConcurrentConsumers(maxConcurrency);
		}
		listenerContainer.setTxSize(properties.getExtension().getTxSize());
		if (properties.getExtension().getQueueDeclarationRetries() != null) {
			listenerContainer.setDeclarationRetries(properties.getExtension().getQueueDeclarationRetries());
		}
	}

	private void setDMLCProperties(ExtendedConsumerProperties<RabbitConsumerProperties> properties,
			DirectMessageListenerContainer listenerContainer, int concurrency) {

		listenerContainer.setConsumersPerQueue(concurrency);
		if (properties.getExtension().getMaxConcurrency() > concurrency) {
			this.logger.warn("maxConcurrency is not supported with a direct container type");
		}
		if (properties.getExtension().getTxSize() > 1) {
			this.logger.warn("txSize is not supported with a direct container type");
		}
		if (properties.getExtension().getQueueDeclarationRetries() != null) {
			this.logger.warn("queueDeclarationRetries is not supported with a direct container type");
		}
	}

	@Override
	protected PolledConsumerResources createPolledConsumerResources(String name, String group, ConsumerDestination destination,
			ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties) {
		Assert.isTrue(!consumerProperties.isMultiplex(),
				"The Spring Integration polled MessageSource does not currently support muiltiple queues");
		AmqpMessageSource source = new AmqpMessageSource(this.connectionFactory, destination.getName());
		source.setRawMessageHeader(true);
		return new PolledConsumerResources(source,
				registerErrorInfrastructure(destination, group, consumerProperties, true));
	}

	@Override
	protected void postProcessPollableSource(DefaultPollableMessageSource bindingTarget) {
		bindingTarget.setAttributesProvider((accessor, message) -> {
			Object rawMessage = message.getHeaders().get(AmqpMessageHeaderErrorMessageStrategy.AMQP_RAW_MESSAGE);
			if (rawMessage != null) {
				accessor.setAttribute(AmqpMessageHeaderErrorMessageStrategy.AMQP_RAW_MESSAGE, rawMessage);
			}
		});
	}

	@Override
	protected ErrorMessageStrategy getErrorMessageStrategy() {
		return errorMessageStrategy;
	}

	@Override
	protected MessageHandler getErrorMessageHandler(ConsumerDestination destination, String group,
			final ExtendedConsumerProperties<RabbitConsumerProperties> properties) {
		if (properties.getExtension().isRepublishToDlq()) {
			return new MessageHandler() {

				private final RabbitTemplate template = new RabbitTemplate(
						RabbitMessageChannelBinder.this.connectionFactory);

				{
					this.template.setUsePublisherConnection(true);
				}

				private final String exchange = deadLetterExchangeName(properties.getExtension());

				private final String routingKey = properties.getExtension().getDeadLetterRoutingKey();

				private final int frameMaxHeadroom = properties.getExtension().getFrameMaxHeadroom();

				private int maxStackTraceLength = -1;

				@Override
				public void handleMessage(org.springframework.messaging.Message<?> message) throws MessagingException {
					Message amqpMessage = (Message) message.getHeaders()
							.get(AmqpMessageHeaderErrorMessageStrategy.AMQP_RAW_MESSAGE);
					if (!(message instanceof ErrorMessage)) {
						logger.error("Expected an ErrorMessage, not a " + message.getClass().toString() + " for: "
								+ message);
					}
					else if (amqpMessage == null) {
						logger.error("No raw message header in " + message);
					}
					else {
						Throwable cause = (Throwable) message.getPayload();
						if (!shouldRepublish(cause)) {
							if (logger.isDebugEnabled()) {
								logger.debug("Skipping republish of: " + message);
							}
							return;
						}
						MessageProperties messageProperties = amqpMessage.getMessageProperties();
						Map<String, Object> headers = messageProperties.getHeaders();
						String stackTraceAsString = getStackTraceAsString(cause);
						if (this.maxStackTraceLength < 0) {
							int rabbitMaxStackTraceLength = RabbitUtils
									.getMaxFrame(this.template.getConnectionFactory());
							if (rabbitMaxStackTraceLength > 0) {
								//maxStackTraceLength -= this.frameMaxHeadroom;
								this.maxStackTraceLength = rabbitMaxStackTraceLength - this.frameMaxHeadroom;
							}
						}
						if (this.maxStackTraceLength > 0 && stackTraceAsString.length() > this.maxStackTraceLength) {
							stackTraceAsString = stackTraceAsString.substring(0, this.maxStackTraceLength);
							logger.warn("Stack trace in republished message header truncated due to frame_max limitations; "
									+ "consider increasing frame_max on the broker or reduce the stack trace depth", cause);
						}
						headers.put(RepublishMessageRecoverer.X_EXCEPTION_STACKTRACE, stackTraceAsString);
						headers.put(RepublishMessageRecoverer.X_EXCEPTION_MESSAGE,
								cause.getCause() != null ? cause.getCause().getMessage() : cause.getMessage());
						headers.put(RepublishMessageRecoverer.X_ORIGINAL_EXCHANGE,
								messageProperties.getReceivedExchange());
						headers.put(RepublishMessageRecoverer.X_ORIGINAL_ROUTING_KEY,
								messageProperties.getReceivedRoutingKey());
						if (properties.getExtension().getRepublishDeliveyMode() != null) {
							messageProperties.setDeliveryMode(properties.getExtension().getRepublishDeliveyMode());
						}
						this.template.send(this.exchange,
								this.routingKey != null ? this.routingKey : messageProperties.getConsumerQueue(),
								amqpMessage);
					}
				}

				/**
				 * Traverse the cause tree, stopping at AmqpRejectAndDontRequeueException
				 * or ImmediateAcknowledgeAmqpException.
				 * @param throwable the throwable.
				 * @return true if neither found or AmqpRejectAndDontRequeueException is
				 * found first.
				 */
				private boolean shouldRepublish(Throwable throwable) {
					Throwable cause = throwable;
					while (cause != null && !(cause instanceof AmqpRejectAndDontRequeueException)
						&& !(cause instanceof ImmediateAcknowledgeAmqpException)) {
						cause = cause.getCause();
					}
					return !(cause instanceof ImmediateAcknowledgeAmqpException);
				}

			};
		}
		else if (properties.getMaxAttempts() > 1) {
			return new MessageHandler() {
				private final RejectAndDontRequeueRecoverer recoverer = new RejectAndDontRequeueRecoverer();

				@Override
				public void handleMessage(org.springframework.messaging.Message<?> message) throws MessagingException {
					Message amqpMessage = (Message) message.getHeaders()
							.get(AmqpMessageHeaderErrorMessageStrategy.AMQP_RAW_MESSAGE);
					/*
					 * NOTE: The following IF and subsequent ELSE IF should never happen under normal interaction and
					 * it should always go to the last ELSE
					 * However, given that this is a handler subscribing to the public channel and that we can't control what
					 * type of Message may be sent to that channel (user decides to send a Message manually) the 'IF/ELSE IF' provides
					 * a safety net to handle any message properly.
					 */
					if (!(message instanceof ErrorMessage)) {
						logger.error("Expected an ErrorMessage, not a " + message.getClass().toString() + " for: "
								+ message);
						throw new ListenerExecutionFailedException("Unexpected error message " + message,
								new AmqpRejectAndDontRequeueException(""), null);
					}
					else if (amqpMessage == null) {
						logger.error("No raw message header in " + message);
						throw new ListenerExecutionFailedException("Unexpected error message " + message,
								new AmqpRejectAndDontRequeueException(""), amqpMessage);
					}
					else {
						this.recoverer.recover(amqpMessage, (Throwable) message.getPayload());
					}
				}

			};
		}
		else {
			return super.getErrorMessageHandler(destination, group, properties);
		}
	}


	@Override
	protected MessageHandler getPolledConsumerErrorMessageHandler(ConsumerDestination destination, String group,
			ExtendedConsumerProperties<RabbitConsumerProperties> properties) {
		MessageHandler handler = getErrorMessageHandler(destination, group, properties);
		if (handler != null) {
			return handler;
		}
		final MessageHandler superHandler = super.getErrorMessageHandler(destination, group, properties);
		return message -> {
			Message amqpMessage = (Message) message.getHeaders()
					.get(AmqpMessageHeaderErrorMessageStrategy.AMQP_RAW_MESSAGE);
			if (!(message instanceof ErrorMessage)) {
				logger.error("Expected an ErrorMessage, not a " + message.getClass().toString() + " for: "
						+ message);
			}
			else if (amqpMessage == null) {
				if (superHandler != null) {
					superHandler.handleMessage(message);
				}
			}
			else {
				if (message.getPayload() instanceof MessagingException) {
					AcknowledgmentCallback ack = StaticMessageHeaderAccessor.getAcknowledgmentCallback(
							((MessagingException) message.getPayload()).getFailedMessage());
					if (ack != null) {
						if (properties.getExtension().isRequeueRejected()) {
							ack.acknowledge(Status.REQUEUE);
						}
						else {
							ack.acknowledge(Status.REJECT);
						}
					}
				}
			}
		};
	}

	@Override
	protected String errorsBaseName(ConsumerDestination destination, String group,
			ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties) {
		return destination.getName() + ".errors";
	}

	private String deadLetterExchangeName(RabbitCommonProperties properties) {
		if (properties.getDeadLetterExchange() == null) {
			return applyPrefix(properties.getPrefix(), RabbitCommonProperties.DEAD_LETTER_EXCHANGE);
		}
		else {
			return properties.getDeadLetterExchange();
		}
	}

	@Override
	protected void afterUnbindConsumer(ConsumerDestination consumerDestination, String group,
			ExtendedConsumerProperties<RabbitConsumerProperties> consumerProperties) {
		provisioningProvider.cleanAutoDeclareContext(consumerDestination, consumerProperties);
	}

	private RabbitTemplate buildRabbitTemplate(RabbitProducerProperties properties, boolean mandatory) {
		RabbitTemplate rabbitTemplate;
		if (properties.isBatchingEnabled()) {
			BatchingStrategy batchingStrategy = new SimpleBatchingStrategy(
					properties.getBatchSize(),
					properties.getBatchBufferLimit(),
					properties.getBatchTimeout());
			rabbitTemplate = new BatchingRabbitTemplate(batchingStrategy,
					getApplicationContext().getBean(IntegrationContextUtils.TASK_SCHEDULER_BEAN_NAME,
							TaskScheduler.class));
		}
		else {
			rabbitTemplate = new RabbitTemplate();
		}
		rabbitTemplate.setMessageConverter(passThoughConverter);
		rabbitTemplate.setChannelTransacted(properties.isTransacted());
		rabbitTemplate.setConnectionFactory(this.connectionFactory);
		rabbitTemplate.setUsePublisherConnection(true);
		if (properties.isCompress()) {
			rabbitTemplate.setBeforePublishPostProcessors(this.compressingPostProcessor);
		}
		rabbitTemplate.setMandatory(mandatory); // returned messages
		if (rabbitProperties != null && rabbitProperties.getTemplate().getRetry().isEnabled()) {
			Retry retry = rabbitProperties.getTemplate().getRetry();
			RetryPolicy retryPolicy = new SimpleRetryPolicy(retry.getMaxAttempts());
			ExponentialBackOffPolicy backOff = new ExponentialBackOffPolicy();
			backOff.setInitialInterval(retry.getInitialInterval().toMillis());
			backOff.setMultiplier(retry.getMultiplier());
			backOff.setMaxInterval(retry.getMaxInterval().toMillis());
			RetryTemplate retryTemplate = new RetryTemplate();
			retryTemplate.setRetryPolicy(retryPolicy);
			retryTemplate.setBackOffPolicy(backOff);
			rabbitTemplate.setRetryTemplate(retryTemplate);
		}
		rabbitTemplate.afterPropertiesSet();
		return rabbitTemplate;
	}

	private String getStackTraceAsString(Throwable cause) {
		StringWriter stringWriter = new StringWriter();
		PrintWriter printWriter = new PrintWriter(stringWriter, true);
		cause.printStackTrace(printWriter);
		return stringWriter.getBuffer().toString();
	}

	private static final class SimplePassthroughMessageConverter extends AbstractMessageConverter {

		private static final SimpleMessageConverter converter = new SimpleMessageConverter();

		SimplePassthroughMessageConverter() {
			super();
		}

		@Override
		protected Message createMessage(Object object, MessageProperties messageProperties) {
			if (object instanceof byte[]) {
				return new Message((byte[]) object, messageProperties);
			}
			else {
				// just for safety (backwards compatibility)
				return converter.toMessage(object, messageProperties);
			}
		}

		@Override
		public Object fromMessage(Message message) throws MessageConversionException {
			return message.getBody();
		}

	}

}
