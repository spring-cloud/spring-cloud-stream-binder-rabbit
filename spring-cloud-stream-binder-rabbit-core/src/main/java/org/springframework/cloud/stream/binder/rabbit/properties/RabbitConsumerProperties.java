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

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.util.Assert;

/**
 * @author Marius Bogoevici
 * @author Gary Russell
 */
public class RabbitConsumerProperties extends RabbitCommonProperties {

	/**
	 * true to use transacted channels
	 */
	private boolean transacted;

	/**
	 * container acknowledge mode
	 */
	private AcknowledgeMode acknowledgeMode = AcknowledgeMode.AUTO;

	/**
	 * maxumum concurrency of this consumer (threads)
	 */
	private int maxConcurrency = 1;

	/**
	 * number of prefetched messages pre consumer thread
	 */
	private int prefetch = 1;

	/**
	 * messages per acknowledgment (and commit when transacted)
	 */
	private int txSize = 1;

	/**
	 * true for a durable subscription
	 */
	private boolean durableSubscription = true;

	/**
	 * republish failures to the DLQ with diagnostic headers
	 */
	private boolean republishToDlq;

	/**
	 * when republishing to the DLQ, the delivery mode to use
	 */
	private MessageDeliveryMode republishDeliveyMode = MessageDeliveryMode.PERSISTENT;

	/**
	 * true to requeue rejected messages, false to discard (or route to DLQ)
	 */
	private boolean requeueRejected = false;

	/**
	 * patterns to match which headers are mapped (inbound)
	 */
	private String[] headerPatterns = new String[] {"*"};

	/**
	 * interval between reconnection attempts
	 */
	private long recoveryInterval = 5000;

	/**
	 * true if the consumer is exclusive
	 */
	private boolean exclusive;

	/**
	 * when true, stop the container instead of retrying queue declarations
	 */
	private boolean missingQueuesFatal = false;

	/**
	 * how many times to attempt passive queue declaration
	 */
	private Integer queueDeclarationRetries;

	/**
	 * interval between attempts to passively declare missing queues
	 */
	private Long  failedDeclarationRetryInterval;

	public boolean isTransacted() {
		return transacted;
	}

	public void setTransacted(boolean transacted) {
		this.transacted = transacted;
	}

	public AcknowledgeMode getAcknowledgeMode() {
		return acknowledgeMode;
	}

	public void setAcknowledgeMode(AcknowledgeMode acknowledgeMode) {
		Assert.notNull(acknowledgeMode, "Acknowledge mode cannot be null");
		this.acknowledgeMode = acknowledgeMode;
	}

	@Min(value = 1, message = "Max Concurrency should be greater than zero.")
	public int getMaxConcurrency() {
		return maxConcurrency;
	}

	public void setMaxConcurrency(int maxConcurrency) {
		this.maxConcurrency = maxConcurrency;
	}

	@Min(value = 1, message = "Prefetch should be greater than zero.")
	public int getPrefetch() {
		return prefetch;
	}

	public void setPrefetch(int prefetch) {
		this.prefetch = prefetch;
	}

	/**
	 * @deprecated - use {@link #getHeaderPatterns()}.
	 * @return the header patterns.
	 */
	@Deprecated
	public String[] getRequestHeaderPatterns() {
		return this.headerPatterns;
	}

	/**
	 * @deprecated - use {@link #setHeaderPatterns(String[])}.
	 * @param requestHeaderPatterns
	 */
	@Deprecated
	public void setRequestHeaderPatterns(String[] requestHeaderPatterns) {
		this.headerPatterns = requestHeaderPatterns;
	}

	@Min(value = 1, message = "Tx Size should be greater than zero.")
	public int getTxSize() {
		return txSize;
	}

	public void setTxSize(int txSize) {
		this.txSize = txSize;
	}

	public boolean isDurableSubscription() {
		return durableSubscription;
	}

	public void setDurableSubscription(boolean durableSubscription) {
		this.durableSubscription = durableSubscription;
	}

	public boolean isRepublishToDlq() {
		return republishToDlq;
	}

	public void setRepublishToDlq(boolean republishToDlq) {
		this.republishToDlq = republishToDlq;
	}

	public boolean isRequeueRejected() {
		return requeueRejected;
	}

	public MessageDeliveryMode getRepublishDeliveyMode() {
		return this.republishDeliveyMode;
	}

	public void setRepublishDeliveyMode(MessageDeliveryMode republishDeliveyMode) {
		this.republishDeliveyMode = republishDeliveyMode;
	}

	public void setRequeueRejected(boolean requeueRejected) {
		this.requeueRejected = requeueRejected;
	}

	public String[] getHeaderPatterns() {
		return headerPatterns;
	}

	public void setHeaderPatterns(String[] replyHeaderPatterns) {
		this.headerPatterns = replyHeaderPatterns;
	}

	public long getRecoveryInterval() {
		return recoveryInterval;
	}

	public void setRecoveryInterval(long recoveryInterval) {
		this.recoveryInterval = recoveryInterval;
	}

	public boolean isExclusive() {
		return this.exclusive;
	}

	public void setExclusive(boolean exclusive) {
		this.exclusive = exclusive;
	}

	public boolean getMissingQueuesFatal() {
		return this.missingQueuesFatal;
	}

	public void setMissingQueuesFatal(boolean missingQueuesFatal) {
		this.missingQueuesFatal = missingQueuesFatal;
	}

	public Integer getQueueDeclarationRetries() {
		return this.queueDeclarationRetries;
	}

	public void setQueueDeclarationRetries(Integer queueDeclarationRetries) {
		this.queueDeclarationRetries = queueDeclarationRetries;
	}

	public Long getFailedDeclarationRetryInterval() {
		return this.failedDeclarationRetryInterval;
	}

	public void setFailedDeclarationRetryInterval(Long failedDeclarationRetryInterval) {
		this.failedDeclarationRetryInterval = failedDeclarationRetryInterval;
	}

}
