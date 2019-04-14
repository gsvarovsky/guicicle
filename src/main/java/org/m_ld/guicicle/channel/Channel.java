/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.channel;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;

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
}
