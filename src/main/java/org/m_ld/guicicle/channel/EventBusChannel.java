/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.channel;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import org.jetbrains.annotations.NotNull;
import org.m_ld.guicicle.Handlers;

import static io.vertx.core.Future.succeededFuture;

public class EventBusChannel<T> extends AbstractChannel<T>
{
    private final Vertx vertx;
    private final String address;
    private final ChannelOptions options;
    private final Handlers<AsyncResult<Object>> producedHandlers = new Handlers<>();

    public EventBusChannel(Vertx vertx, String address, ChannelOptions options)
    {
        this.vertx = vertx;
        this.address = address;
        this.options = options;

        if (options.getQuality() != ChannelOptions.Quality.AT_MOST_ONCE)
            throw new IllegalArgumentException("EventBus does not support quality of service");
    }

    @Override protected @NotNull MessageConsumer<T> createConsumer()
    {
        return vertx.eventBus().consumer(address);
    }

    @Override protected @NotNull MessageProducer<T> createProducer()
    {
        final MessageProducer<T> producer;
        switch (options.getDelivery())
        {
            case SEND:
                producer = vertx.eventBus().sender(address, options);
                break;
            case PUBLISH:
                producer = vertx.eventBus().publisher(address, options);
                break;
            default:
                throw new AssertionError();
        }
        return new MessageProducer<T>()
        {
            @Override public MessageProducer<T> send(T message)
            {
                producer.send(message);
                return produced(message);
            }

            @Override public <R> MessageProducer<T> send(T message, Handler<AsyncResult<Message<R>>> replyHandler)
            {
                producer.send(message, replyHandler);
                return produced(message);
            }

            @Override public MessageProducer<T> exceptionHandler(Handler<Throwable> handler)
            {
                producer.exceptionHandler(handler);
                return this;
            }

            @Override public MessageProducer<T> write(T message)
            {
                producer.write(message);
                return produced(message);
            }

            @Override public MessageProducer<T> setWriteQueueMaxSize(int maxSize)
            {
                producer.setWriteQueueMaxSize(maxSize);
                return this;
            }

            @Override public MessageProducer<T> drainHandler(Handler<Void> handler)
            {
                producer.drainHandler(handler);
                return this;
            }

            @Override public MessageProducer<T> deliveryOptions(DeliveryOptions options)
            {
                producer.deliveryOptions(options);
                return this;
            }

            @Override public String address()
            {
                return producer.address();
            }

            @Override public void end()
            {
                close();
            }

            @Override public void close()
            {
                producer.close();
            }

            @Override public boolean writeQueueFull()
            {
                return producer.writeQueueFull();
            }

            @NotNull private MessageProducer<T> produced(T message)
            {
                producedHandlers.handle(succeededFuture(message));
                return this;
            }
        };
    }

    @Override public Channel<T> producedHandler(Handler<AsyncResult<Object>> producedHandler)
    {
        producedHandlers.add(producedHandler);
        return this;
    }
}
