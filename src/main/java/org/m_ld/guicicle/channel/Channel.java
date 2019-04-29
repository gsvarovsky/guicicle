/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.channel;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import org.jetbrains.annotations.Nullable;
import org.m_ld.guicicle.Vertice;

public interface Channel<T> extends ChannelProvider
{
    /**
     * @return the channel consumer, for receiving messages from the channel.
     */
    MessageConsumer<T> consumer();

    /**
     * @return the channel producer, for pushing messages to the channel.
     */
    MessageProducer<T> producer();

    /**
     * Create a sub-channel with the given address and options.
     * <p>
     * The created channel will be scoped to this one and will not conflict with channels created directly from this
     * channel's provider. The returned channel's {@link #consumer()} may report a more specific address than given.
     *
     * @param address the sub-channel address (will be scoped to this channel)
     * @param options the channel options
     * @param <E>     the message type
     * @return the sub-channel, ready to use
     */
    @Override <E> Channel<E> channel(String address, ChannelOptions options);

    /**
     * Sets a handler on the channel that is notified when a message is successfully produced to the channel, and is
     * now subject to the channel's {@link ChannelOptions.Quality quality of service}.
     * <p>
     * This handler also applies to replies made on messages delivered to the {@link #consumer()}, but in this case may
     * be subject to a different quality of service based on the reply's delivery options.
     *
     * @see Message#reply(Object, DeliveryOptions)
     */
    Channel<T> producedHandler(Handler<AsyncResult<Object>> producedHandler);

    /**
     * @return a copy of the current channel options. Note that these can change after channel provision.
     * @see ChannelProvider#channel(String, ChannelOptions)
     * @see MessageProducer#deliveryOptions(DeliveryOptions)
     */
    ChannelOptions options();

    /**
     * Utility to make a request to the channel producer, expecting a response via a {@code Future}.
     *
     * @param request the request to the channel
     * @param <R>     the response type
     * @return the future response, which may be failed
     */
    default <R> Future<Message<R>> request(T request)
    {
        return Vertice.when(producer()::send, request);
    }

    /**
     * Utility to reply to a requests using the given async response. If the response is failed, the request is failed
     * with an error code provided by the {@link #options()}.
     * <p>
     * If an {@code ack} is given, it will be provided with any response acknowledgement.
     *
     * @param request  the request
     * @param response the asynchronous response
     * @param ack      an optional future which will be completed when the response is acknowledged
     */
    default <R> void respond(Message<T> request, AsyncResult<?> response,
                             @Nullable Handler<AsyncResult<Message<R>>> ack)
    {
        if (response.succeeded())
        {
            if (ack == null)
                request.reply(response.result());
            else
                request.reply(response.result(), ack);
        }
        else
        {
            final HttpResponseStatus status = options().getStatusForError(response.cause());
            request.fail(status.code(), status.reasonPhrase());
        }
    }

    /**
     * Closes the channel. This method should be called when the channel is not used anymore. This will unregister the
     * channel consumer and end the producer. However it will <strong>not</strong> close any sub-channels created with
     * {@link #channel(String, ChannelOptions)}.
     */
    void close();
}
