/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.channel;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.*;
import io.vertx.core.eventbus.impl.BodyReadStream;
import io.vertx.core.streams.ReadStream;
import org.jetbrains.annotations.NotNull;
import org.m_ld.guicicle.Handlers;
import org.m_ld.guicicle.PartialFluentProxy;
import org.m_ld.guicicle.PartialFluentProxy.Delegate;

import static io.vertx.core.Future.succeededFuture;
import static java.lang.invoke.MethodHandles.lookup;

public class EventBusChannel<T> extends AbstractChannel<T>
{
    private final EventBus eventBus;
    private final String address;
    private final Handlers<AsyncResult<Object>> producedHandlers = new Handlers<>();

    public EventBusChannel(EventBus eventBus, String address, ChannelOptions options)
    {
        super(options);

        this.eventBus = eventBus;
        this.address = address;

        checkOptions(null, options);
    }

    @Override protected @NotNull MessageConsumer<T> createConsumer()
    {
        final MessageConsumer<T> consumer = eventBus.consumer(address);
        //noinspection unchecked,unused
        return PartialFluentProxy.create(MessageConsumer.class, consumer, new Delegate<MessageConsumer<T>>(consumer, lookup())
        {
            MessageConsumer<T> handler(Handler<Message<T>> handler)
            {
                return consumer.handler(msg -> filterEcho(msg, handler));
            }

            ReadStream<T> bodyStream()
            {
                return new BodyReadStream<>(proxy);
            }
        });
    }

    @Override protected @NotNull MessageProducer<T> createProducer()
    {
        final MessageProducer<T> producer = options().getDelivery() == ChannelOptions.Delivery.SEND ?
            eventBus.sender(address, options()) : eventBus.publisher(address, options());
        //noinspection unchecked,unused
        return PartialFluentProxy.create(MessageProducer.class, producer, new Delegate<MessageProducer>(producer, lookup())
        {
            MessageProducer<T> send(T message)
            {
                return produced(producer.send(message), message);
            }

            <R> MessageProducer<T> send(T message, Handler<AsyncResult<Message<R>>> replyHandler)
            {
                return produced(producer.send(message, replyHandler), message);
            }

            MessageProducer<T> write(T data)
            {
                return produced(producer.write(data), data);
            }

            MessageProducer<T> deliveryOptions(DeliveryOptions options)
            {
                checkOptions(options(), options);
                return producer.deliveryOptions(options);
            }

            private MessageProducer<T> produced(MessageProducer<T> producer, T message)
            {
                // The event bus is AT_MOST_ONCE, so immediately notify the handler
                producedHandlers.handle(succeededFuture(message));
                return producer;
            }
        });
    }

    @Override public Channel<T> producedHandler(Handler<AsyncResult<Object>> producedHandler)
    {
        producedHandlers.add(producedHandler);
        return this;
    }

    @Override public <E> Channel<E> channel(String address, ChannelOptions options)
    {
        return new EventBusChannel<>(eventBus, this.address + '.' + address, options);
    }

    private void checkOptions(ChannelOptions oldOptions, DeliveryOptions options)
    {
        if (options instanceof ChannelOptions)
        {
            final ChannelOptions newOptions = (ChannelOptions)options;
            if (newOptions.getQuality() != ChannelOptions.Quality.AT_MOST_ONCE)
                throw new IllegalArgumentException("EventBus does not support quality of service");
            if (oldOptions != null && oldOptions.getDelivery() != newOptions.getDelivery())
                throw new IllegalArgumentException("EventBus channel cannot change delivery option");
        }
    }
}
