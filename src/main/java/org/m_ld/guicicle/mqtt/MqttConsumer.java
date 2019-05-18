/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.AsyncResult;
import io.vertx.mqtt.messages.MqttPublishMessage;
import io.vertx.mqtt.messages.MqttSubAckMessage;
import org.m_ld.guicicle.VertxCloseable;

import java.util.List;

public interface MqttConsumer extends VertxCloseable
{
    interface Subscription
    {
        String topic();

        MqttQoS qos();
    }

    static Subscription subscription(String topic, MqttQoS qos)
    {
        return new Subscription()
        {
            @Override public String topic()
            {
                return topic;
            }

            @Override public MqttQoS qos()
            {
                return qos;
            }
        };
    }

    List<Subscription> subscriptions();

    void onMessage(MqttPublishMessage message);

    default void onSubscribeSent(AsyncResult<Integer> subscribeSendResult, int qosIndex) {}

    default void onSubscribeAck(MqttSubAckMessage subAckMessage) {}

    default void onUnsubscribeAck(int messageId) {}
}
