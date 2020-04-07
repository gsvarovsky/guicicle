/*
 * Copyright (c) George Svarovsky 2020. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.channel;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
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
    private static final String RESEND = "__channel.resend", REJECTED_ECHO = "__channel.reject";
    private static final int RESEND_THRESHOLD = 3;

    private final EventBus eventBus;
    private final String address;
    private final Handlers<AsyncResult<Object>> producedHandlers = new Handlers<>();

    private class Resend
    {
        int attempts;

        Resend()
        {
            this.attempts = 0;
        }

        Resend(MultiMap headers)
        {
            this.attempts = headers.contains(RESEND) ?
                Integer.parseInt(headers.get(RESEND)) : 0;
        }

        DeliveryOptions nextOptions()
        {
            final DeliveryOptions options = new DeliveryOptions(options());
            options.getHeaders().set(RESEND, Integer.toString(++attempts));
            return options;
        }

        boolean canResend()
        {
            return attempts < RESEND_THRESHOLD;
        }
    }

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
                return consumer.handler(msg -> {
                    final Resend resend;
                    if (isEcho(msg))
                    {
                        // If the message is a publish, or has already been resent enough, just ignore it
                        if (msg.isSend() && (resend = new Resend(msg.headers())).canResend())
                            if (msg.replyAddress() == null)
                                eventBus.send(address, msg.body(), resend.nextOptions());
                            else
                                msg.reply(REJECTED_ECHO);
                    }
                    else
                    {
                        handler.handle(msg);
                    }
                });
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
                // Note that echoes are handled in the consumer
                return produced(producer.send(message), message);
            }

            <R> MessageProducer<T> send(T message, Handler<AsyncResult<Message<R>>> replyHandler)
            {
                final Resend resend = new Resend();
                return produced(producer.<R>send(
                    message, replyResult -> handleReply(message, replyResult, replyHandler, resend)), message);
            }

            private <R> void handleReply(T message, AsyncResult<Message<R>> replyResult,
                                         Handler<AsyncResult<Message<R>>> replyHandler, Resend resend)
            {
                // Handle a signal to resend an echoed message
                if (replyResult.succeeded() && REJECTED_ECHO.equals(replyResult.result().body()))
                    eventBus.<R>send(address, message, resend.nextOptions(),
                                     nextReplyResult -> handleReply(message, nextReplyResult, replyHandler, resend));
                else
                    replyHandler.handle(replyResult);
            }

            MessageProducer<T> write(T data)
            {
                // Note that echoes are handled in the consumer
                return produced(producer.write(data), data);
            }

            MessageProducer<T> deliveryOptions(DeliveryOptions options)
            {
                checkOptions(options(), options);
                return producer.deliveryOptions(options);
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
                throw new UnsupportedOperationException("EventBus does not support quality of service");
            if (newOptions.isRetain())
                throw new UnsupportedOperationException("EventBus does not support message retention");
            if (oldOptions != null && oldOptions.getDelivery() != newOptions.getDelivery())
                throw new UnsupportedOperationException("EventBus channel cannot change delivery option");
        }
    }

    private MessageProducer<T> produced(MessageProducer<T> producer, T message)
    {
        // The event bus is AT_MOST_ONCE, so immediately notify the handler
        producedHandlers.handle(succeededFuture(message));
        return producer;
    }
}
