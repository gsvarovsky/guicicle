/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import io.vertx.core.AsyncResult;
import io.vertx.mqtt.messages.MqttPublishMessage;
import org.m_ld.guicicle.VertxCloseable;
import org.m_ld.guicicle.channel.ChannelOptions;

public interface MqttConsumer extends VertxCloseable
{
    class Subscription
    {
        final MqttTopicAddress address;
        final ChannelOptions.Quality qos;
        /**
         * AsyncResults go null --> succeeded(Integer) --> succeeded(null)
         */
        AsyncResult<Integer> subscribed = null, unsubscribed = null;
        int qosIndex = -1;

        Subscription(MqttTopicAddress address, ChannelOptions.Quality qos)
        {
            this.address = address;
            this.qos = qos;
        }

        static boolean isComplete(AsyncResult<Integer> state)
        {
            return state != null && state.result() == null;
        }
    }

    /**
     * Returned array should be const over the consumer's lifetime
     */
    Subscription[] subscriptions();

    void onMessage(MqttPublishMessage message, Subscription subscription);

    void onSubscribe(AsyncResult<Void> subscribeResult);

    void onUnsubscribe(AsyncResult<Void> unsubscribeResult);
}
