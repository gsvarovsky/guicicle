/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.channel;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;

public class EventBusChannel<T> implements Channel<T>
{
    private final MessageConsumer<T> consumer;
    private final MessageProducer<T> producer;

    public EventBusChannel(Vertx vertx, String address, ChannelOptions options)
    {
        this.consumer = vertx.eventBus().consumer(address);
        switch (options.getDelivery())
        {
            case SEND:
                this.producer = vertx.eventBus().sender(address, options);
                break;
            case PUBLISH:
                this.producer = vertx.eventBus().publisher(address, options);
                break;
            default:
                throw new AssertionError();
        }
        if (options.getQuality() != ChannelOptions.Quality.AT_MOST_ONCE)
            throw new IllegalArgumentException("EventBus does not support quality of service");
    }

    @Override public MessageConsumer<T> consumer()
    {
        return consumer;
    }

    @Override public MessageProducer<T> producer()
    {
        return producer;
    }
}
