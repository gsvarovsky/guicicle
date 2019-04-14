/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.mqtt.messages.MqttPublishMessage;
import org.m_ld.guicicle.channel.Channel;

class MqttEventMessage<T> implements Message<T>
{
    private final MqttPublishMessage message;
    private final MultiMap headers;
    private final T payload;

    public MqttEventMessage(MqttPublishMessage message, MultiMap headers, MessageCodec<?, T> codec)
    {
        this.message = message;
        this.headers = MultiMap.caseInsensitiveMultiMap();
        this.headers.addAll(headers);
        this.headers.add("mqtt.isDup", Boolean.toString(message.isDup()));
        this.headers.add("mqtt.isRetain", Boolean.toString(message.isRetain()));
        this.headers.add("mqtt.qosLevel", message.qosLevel().name());
        this.payload = codec.decodeFromWire(0, message.payload());
    }

    @Override public String address()
    {
        return message.topicName();
    }

    @Override public MultiMap headers()
    {
        return headers;
    }

    @Override public T body()
    {
        return payload;
    }

    @Override public String replyAddress()
    {
        throw new UnsupportedOperationException("Cannot reply with MQTT, yet");
    }

    @Override public boolean isSend()
    {
        return false; // MQTT is pub-sub
    }

    /**
     * {@inheritDoc}
     * <p>
     * The channel will notify whether the message has been successfully sent via its {@link
     * Channel#producedHandler(Handler)}.
     */
    @Override public void reply(Object message)
    {
        throw new UnsupportedOperationException("Cannot reply with MQTT, yet");
    }

    @Override public <R> void reply(Object message, Handler<AsyncResult<Message<R>>> replyHandler)
    {
        throw new UnsupportedOperationException("Cannot reply with MQTT, yet");
    }

    @Override public void reply(Object message, DeliveryOptions options)
    {
        throw new UnsupportedOperationException("Cannot reply with MQTT, yet");
    }

    @Override public <R> void reply(Object message, DeliveryOptions options,
                                    Handler<AsyncResult<Message<R>>> replyHandler)
    {
        throw new UnsupportedOperationException("Cannot reply with MQTT, yet");
    }

    @Override public void fail(int failureCode, String message)
    {
        throw new UnsupportedOperationException("Cannot reply with MQTT, yet");
    }
}
