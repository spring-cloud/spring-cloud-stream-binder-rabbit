package org.springframework.cloud.stream.binder.rabbit;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;


import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;
import org.springframework.amqp.rabbit.retry.RejectAndDontRequeueRecoverer;
import org.springframework.amqp.rabbit.retry.RepublishMessageRecoverer;
import org.springframework.cloud.stream.binder.AbstractMessageChannelErrorConfigurer;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ConsumerBinding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.MessageProducerBinding;
import org.springframework.cloud.stream.binder.ProducerBinding;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitCommonProperties;
import org.springframework.cloud.stream.binder.rabbit.properties.RabbitConsumerProperties;
import org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter;
import org.springframework.integration.amqp.support.AmqpMessageHeaderErrorMessageStrategy;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.ErrorMessage;

/**
 * @author Vinicius Carvalho
 * @author Gary Russel
 */
public class RabbitMessageChannelErrorConfigurer extends AbstractMessageChannelErrorConfigurer <ExtendedConsumerProperties<RabbitConsumerProperties>>{

	private static final AmqpMessageHeaderErrorMessageStrategy errorMessageStrategy =
			new AmqpMessageHeaderErrorMessageStrategy();

	public RabbitMessageChannelErrorConfigurer(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	private final ConnectionFactory connectionFactory;



	@Override
	public void configure(Binding<MessageChannel> binding) {
		MessageProducerBinding consumerBinding = (MessageProducerBinding) binding;
		ExtendedConsumerProperties<RabbitConsumerProperties> properties = (ExtendedConsumerProperties<RabbitConsumerProperties>)consumerBinding.getDestination().getProperties();
		AmqpInboundChannelAdapter adapter = (AmqpInboundChannelAdapter)consumerBinding.getMessageProducer();
		ErrorInfrastructure errorInfrastructure = getErrorInfrastructure(consumerBinding.getDestination().getName());
		if (properties.getMaxAttempts() > 1) {
			adapter.setRetryTemplate(buildRetryTemplate(properties));
			if (properties.getExtension().isRepublishToDlq()) {
				adapter.setRecoveryCallback(errorInfrastructure.getRecoverer());
			}
		}
		else {
			adapter.setErrorMessageStrategy(getErrorMessageStrategy());
			adapter.setErrorChannel(errorInfrastructure.getErrorChannel());
		}
	}

	@Override
	protected MessageHandler getErrorMessageHandler(String destination, String group, final ExtendedConsumerProperties<RabbitConsumerProperties> properties) {
		if (properties.getExtension().isRepublishToDlq()) {
			return new MessageHandler() {

				private final RabbitTemplate template = new RabbitTemplate(
						RabbitMessageChannelErrorConfigurer.this.connectionFactory);

				private final String exchange = deadLetterExchangeName(properties.getExtension());

				private final String routingKey = properties.getExtension().getDeadLetterRoutingKey();

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
						MessageProperties messageProperties = amqpMessage.getMessageProperties();
						Map<String, Object> headers = messageProperties.getHeaders();
						headers.put(RepublishMessageRecoverer.X_EXCEPTION_STACKTRACE, getStackTraceAsString(cause));
						headers.put(RepublishMessageRecoverer.X_EXCEPTION_MESSAGE,
								cause.getCause() != null ? cause.getCause().getMessage() : cause.getMessage());
						headers.put(RepublishMessageRecoverer.X_ORIGINAL_EXCHANGE,
								messageProperties.getReceivedExchange());
						headers.put(RepublishMessageRecoverer.X_ORIGINAL_ROUTING_KEY,
								messageProperties.getReceivedRoutingKey());
						if (properties.getExtension().getRepublishDeliveyMode() != null) {
							messageProperties.setDeliveryMode(properties.getExtension().getRepublishDeliveyMode());
						}
						template.send(this.exchange,
								this.routingKey != null ? this.routingKey : messageProperties.getConsumerQueue(),
								amqpMessage);
					}
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

	private String deadLetterExchangeName(RabbitCommonProperties properties) {
		if (properties.getDeadLetterExchange() == null) {
			return RabbitMessageChannelBinder.applyPrefix(properties.getPrefix(), RabbitCommonProperties.DEAD_LETTER_EXCHANGE);
		}
		else {
			return properties.getDeadLetterExchange();
		}
	}

	@Override
	protected ErrorMessageStrategy getErrorMessageStrategy() {
		return errorMessageStrategy;
	}

	private String getStackTraceAsString(Throwable cause) {
		StringWriter stringWriter = new StringWriter();
		PrintWriter printWriter = new PrintWriter(stringWriter, true);
		cause.printStackTrace(printWriter);
		return stringWriter.getBuffer().toString();
	}
}
