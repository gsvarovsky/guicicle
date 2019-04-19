/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.channel;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import org.m_ld.guicicle.Vertice;

import java.util.function.Function;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;

public interface Channel<T>
{
    /**
     * @return a channel consumer, for receiving messages from the channel.
     */
    MessageConsumer<T> consumer();

    /**
     * @return a channel producer, for pushing messages to the channel.
     */
    MessageProducer<T> producer();

    /**
     * Sets a handler on the channel that is notified when a message is successfully produced to the channel, and is
     * now subject to the channel's {@link ChannelOptions.Quality quality of service}.
     * <p>
     * This handler also applies to replies made on messages delivered to the {@link #consumer()}, but in this case may
     * be subject to a different quality of service based on the requested {@link io.vertx.core.eventbus.DeliveryOptions}.
     */
    Channel<T> producedHandler(Handler<AsyncResult<Object>> producedHandler);

    /**
     * Utility to make a request to the channel producer, expecting a response via a {@code Future}.
     *
     * @param request the request to the channel
     * @param <R>     the response type
     * @return the response, which may be failed
     */
    default <R> Future<R> request(T request)
    {
        return Vertice.<Message<R>, T>when(producer()::send, request).map(Message::body);
    }

    /**
     * Utility to add a responder to the channel, which replies to requests using the given async function. If the
     * response is failed, the request is failed with a generic server error, and the given local {@code
     * exceptionHandler} is notified.
     *
     * @param responder        takes a request from the channel and produces a future response
     * @param exceptionHandler called in case of a failed response
     */
    default void respond(Function<T, Future<?>> responder, Handler<Throwable> exceptionHandler)
    {
        consumer().handler(msg -> responder.apply(msg.body()).setHandler(result -> {
            if (result.succeeded())
            {
                msg.reply(result.result());
            }
            else
            {
                msg.fail(INTERNAL_SERVER_ERROR.code(), INTERNAL_SERVER_ERROR.reasonPhrase());
                exceptionHandler.handle(result.cause());
            }
        }));
    }
}
