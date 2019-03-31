/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.channel;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageConsumer;

import java.util.function.Consumer;

public abstract class EventBusChannel<T> implements Channel<T>
{
    protected final String address;
    protected final DeliveryOptions deliveryOptions;
    protected final Vertx vertx;

    public EventBusChannel(Vertx vertx, String address)
    {
        this.vertx = vertx;
        this.address = address;
        this.deliveryOptions = new DeliveryOptions();
    }

    public Channel<T> withOptions(Consumer<DeliveryOptions> modify)
    {
        modify.accept(deliveryOptions);
        return this;
    }

    @Override
    public MessageConsumer<T> consumer()
    {
        return vertx.eventBus().consumer(address);
    }
}
