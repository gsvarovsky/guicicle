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
import io.vertx.core.eventbus.ReplyException;
import org.jetbrains.annotations.Nullable;
import org.m_ld.guicicle.channel.Channel;

import static io.vertx.core.eventbus.ReplyFailure.RECIPIENT_FAILURE;

class MqttEventMessage<T> implements Message<T>
{
    interface Replier
    {
        String address();

        <R> void reply(Object message,
                       @Nullable DeliveryOptions options,
                       @Nullable Handler<AsyncResult<Message<R>>> replyHandler);
    }

    private final String address;
    private final MultiMap headers;
    private final T payload;
    private final Replier replier;

    MqttEventMessage(String address, MultiMap headers, T payload)
    {
        this(address, headers, payload, null);
    }

    MqttEventMessage(String address, MultiMap headers, T payload, @Nullable Replier replier)
    {
        this.address = address;
        this.headers = headers;
        this.payload = payload;
        this.replier = replier;
    }

    @Override public String address()
    {
        return address;
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
        return replier != null ? replier.address() : null;
    }

    @Override public boolean isSend()
    {
        return replier != null;
    }

    /**
     * {@inheritDoc}
     * <p>
     * The channel will notify whether the message has been successfully sent via its {@link
     * Channel#producedHandler(Handler)}.
     */
    @Override public void reply(Object message)
    {
        if (replier != null)
            replier.reply(message, null, null);
    }

    @Override public <R> void reply(Object message, Handler<AsyncResult<Message<R>>> replyHandler)
    {
        if (replier != null)
            replier.reply(message, null, replyHandler);
    }

    @Override public void reply(Object message, DeliveryOptions options)
    {
        if (replier != null)
            replier.reply(message, options, null);
    }

    @Override public <R> void reply(Object message,
                                    DeliveryOptions options,
                                    Handler<AsyncResult<Message<R>>> replyHandler)
    {
        if (replier != null)
            replier.reply(message, options, replyHandler);
    }

    @Override public void fail(int failureCode, String message)
    {
        if (replier != null)
            reply(new ReplyException(RECIPIENT_FAILURE, failureCode, message));
    }
}
