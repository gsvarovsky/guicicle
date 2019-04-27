/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.channel;

import com.google.common.reflect.Reflection;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import org.jetbrains.annotations.NotNull;
import org.m_ld.guicicle.Handlers;

import static io.vertx.core.Future.succeededFuture;

public class EventBusChannel<T> extends AbstractChannel<T>
{
    private final EventBus eventBus;
    private final String address;
    private final ChannelOptions options;
    private final Handlers<AsyncResult<Object>> producedHandlers = new Handlers<>();

    public EventBusChannel(EventBus eventBus, String address, ChannelOptions options)
    {
        this.eventBus = eventBus;
        this.address = address;
        this.options = options;

        if (options.getQuality() != ChannelOptions.Quality.AT_MOST_ONCE)
            throw new IllegalArgumentException("EventBus does not support quality of service");
    }

    @Override protected @NotNull MessageConsumer<T> createConsumer()
    {
        return eventBus.consumer(address);
    }

    @Override protected @NotNull MessageProducer<T> createProducer()
    {
        final MessageProducer<T> producer = options.getDelivery() == ChannelOptions.Delivery.SEND ?
            eventBus.sender(address, options) : eventBus.publisher(address, options);

        //noinspection UnstableApiUsage, unchecked
        return Reflection.newProxy(MessageProducer.class, (proxy, method, args) -> {
            final Object rtn = method.invoke(producer, args);
            // The event bus is AT_MOST_ONCE, so immediately notify the handler
            if ("send".equals(method.getName()) || "write".equals(method.getName()))
                producedHandlers.handle(succeededFuture(args[0]));
            // Maintain fluent chaining
            return rtn instanceof MessageProducer ? proxy : rtn;
        });
    }

    @Override public Channel<T> producedHandler(Handler<AsyncResult<Object>> producedHandler)
    {
        producedHandlers.add(producedHandler);
        return this;
    }

    @Override public ChannelOptions options()
    {
        return new ChannelOptions(options);
    }

    @Override public <E> Channel<E> channel(String address, ChannelOptions options)
    {
        return new EventBusChannel<>(eventBus, subAddress(address), options);
    }

    private String subAddress(String address)
    {
        return this.address + '.' + address;
    }
}
