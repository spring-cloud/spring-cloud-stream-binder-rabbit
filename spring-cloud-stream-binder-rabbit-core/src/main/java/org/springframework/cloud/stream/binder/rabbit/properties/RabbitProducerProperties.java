/*
 * Copyright 2016-2018 the original author or authors.
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

package org.springframework.cloud.stream.binder.rabbit.properties;

import javax.validation.constraints.Min;

import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.expression.Expression;

/**
 * @author Marius Bogoevici
 * @author Gary Russell
 */
public class RabbitProducerProperties extends RabbitCommonProperties {

	/**
	 * true to compress messages.
	 */
	private boolean compress;

	/**
	 * true to batch multiple messages into one.
	 */
	private boolean batchingEnabled;

	/**
	 * the number of messages to batch, when enabled.
	 */
	private int batchSize = 100;

	/**
	 * the size limit for batched messages.
	 */
	private int batchBufferLimit = 10000;

	/**
	 * the time after which an incomplete batch will be sent.
	 */
	private int batchTimeout = 5000;

	/**
	 * the bean name of a custom batching strategy to use instead of the
	 * {@link org.springframework.amqp.rabbit.batch.SimpleBatchingStrategy}.
	 */
	private String batchingStrategyBeanName;

	/**
	 * true to use transacted channels.
	 */
	private boolean transacted;

	/**
	 * the delivery mode for published messages.
	 */
	private MessageDeliveryMode deliveryMode = MessageDeliveryMode.PERSISTENT;

	/**
	 * patterns to match which headers are mapped (inbound).
	 */
	private String[] headerPatterns = new String[] { "*" };

	/**
	 * when using a delayed message exchange, a SpEL expression to determine the delay to
	 * apply to messages.
	 */
	private Expression delayExpression;

	/**
	 * a custom routing key when publishing messages; default is the destination name;
	 * suffixed by "-partition" when partitioned.
	 */
	private Expression routingKeyExpression;

	/**
	 * the channel name to which to send publisher confirms (acks) if the connection
	 * factory is so configured; default 'nullChannel'; requires
	 * 'errorChannelEnabled=true'.
	 */
	private String confirmAckChannel;

	/**
	 * @deprecated - use {@link #setHeaderPatterns(String[])}.
	 * @param requestHeaderPatterns the patterns.
	 */
	@Deprecated
	public void setRequestHeaderPatterns(String[] requestHeaderPatterns) {
		this.headerPatterns = requestHeaderPatterns;
	}

	/**
	 * @deprecated - use {@link #getHeaderPatterns()}.
	 * @return the header patterns.
	 */
	@Deprecated
	public String[] getRequestHeaderPatterns() {
		return this.headerPatterns;
	}

	public void setCompress(boolean compress) {
		this.compress = compress;
	}

	public boolean isCompress() {
		return compress;
	}

	public void setDeliveryMode(MessageDeliveryMode deliveryMode) {
		this.deliveryMode = deliveryMode;
	}

	public MessageDeliveryMode getDeliveryMode() {
		return deliveryMode;
	}

	public String[] getHeaderPatterns() {
		return headerPatterns;
	}

	public void setHeaderPatterns(String[] replyHeaderPatterns) {
		this.headerPatterns = replyHeaderPatterns;
	}

	public boolean isBatchingEnabled() {
		return batchingEnabled;
	}

	public void setBatchingEnabled(boolean batchingEnabled) {
		this.batchingEnabled = batchingEnabled;
	}

	@Min(value = 1, message = "Batch Size should be greater than zero.")
	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	@Min(value = 1, message = "Batch Buffer Limit should be greater than zero.")
	public int getBatchBufferLimit() {
		return batchBufferLimit;
	}

	public void setBatchBufferLimit(int batchBufferLimit) {
		this.batchBufferLimit = batchBufferLimit;
	}

	@Min(value = 1, message = "Batch Timeout should be greater than zero.")
	public int getBatchTimeout() {
		return batchTimeout;
	}

	public void setBatchTimeout(int batchTimeout) {
		this.batchTimeout = batchTimeout;
	}

	public boolean isTransacted() {
		return this.transacted;
	}

	public void setTransacted(boolean transacted) {
		this.transacted = transacted;
	}

	public Expression getDelayExpression() {
		return this.delayExpression;
	}

	public void setDelayExpression(Expression delayExpression) {
		this.delayExpression = delayExpression;
	}

	public Expression getRoutingKeyExpression() {
		return this.routingKeyExpression;
	}

	public void setRoutingKeyExpression(Expression routingKeyExpression) {
		this.routingKeyExpression = routingKeyExpression;
	}

	public String getConfirmAckChannel() {
		return this.confirmAckChannel;
	}

	public void setConfirmAckChannel(String confirmAckChannel) {
		this.confirmAckChannel = confirmAckChannel;
	}

	public String getBatchingStrategyBeanName() {
		return batchingStrategyBeanName;
	}

	public void setBatchingStrategyBeanName(String batchingStrategyBeanName) {
		this.batchingStrategyBeanName = batchingStrategyBeanName;
	}

}
