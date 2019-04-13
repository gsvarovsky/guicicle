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

class MqttMessageEvent<T> implements Message<T>
{
    private final MqttPublishMessage message;
    private final MultiMap headers;
    private final T payload;

    public MqttMessageEvent(MqttPublishMessage message, MultiMap headers, MessageCodec<?, T> codec)
    {
        this.message = message;
        this.headers = MultiMap.caseInsensitiveMultiMap();
        this.headers.addAll(headers);
        this.headers.add("isDup", Boolean.toString(message.isDup()));
        this.headers.add("isRetain", Boolean.toString(message.isRetain()));
        this.headers.add("qosLevel", message.qosLevel().name());
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
        return null; // MQTT is pub-sub
    }

    @Override public boolean isSend()
    {
        return false; // MQTT is pub-sub
    }

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
