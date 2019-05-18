/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import org.m_ld.guicicle.channel.ChannelOptions;

import static java.lang.String.format;
import static org.m_ld.guicicle.mqtt.VertxMqttModule.generateRandomId;

class MqttEventInFlight<T>
{
    final T message;
    // Only send messages have a message Id
    final String messageId;
    // Only sent messages have a reply handler
    final Handler<AsyncResult<Message>> replyHandler;

    MqttEventInFlight(T message, Handler<? extends AsyncResult<? extends Message>> replyHandler)
    {
        this.message = message;
        this.messageId = generateRandomId();
        //noinspection unchecked
        this.replyHandler = (Handler<AsyncResult<Message>>)replyHandler;
    }

    MqttEventInFlight(T message, ChannelOptions.Delivery delivery)
    {
        this.message = message;
        this.messageId = delivery == ChannelOptions.Delivery.SEND ? generateRandomId() : null;
        this.replyHandler = null;
    }

    boolean isSend()
    {
        return messageId != null;
    }

    @Override public String toString()
    {
        return format("%s %s", isSend() ? "Send" : "Publish", message);
    }
}
