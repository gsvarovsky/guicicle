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

    public MqttMessageEvent(MqttPublishMessage message, MessageCodec<?, T> codec)
    {
        this.message = message;
        this.headers = MultiMap.caseInsensitiveMultiMap();
        headers.add("isDup", Boolean.toString(message.isDup()));
        headers.add("isRetain", Boolean.toString(message.isRetain()));
        headers.add("qosLevel", message.qosLevel().name());
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
        // Do nothing, MQTT is pub-sub
    }

    @Override public <R> void reply(Object message, Handler<AsyncResult<Message<R>>> replyHandler)
    {
        // Do nothing, MQTT is pub-sub
    }

    @Override public void reply(Object message, DeliveryOptions options)
    {
        // Do nothing, MQTT is pub-sub
    }

    @Override public <R> void reply(Object message, DeliveryOptions options,
                                    Handler<AsyncResult<Message<R>>> replyHandler)
    {
        // Do nothing, MQTT is pub-sub
    }

    @Override public void fail(int failureCode, String message)
    {
        // Do nothing, MQTT is pub-sub
    }
}
