package org.m_ld.guicicle.channel;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageProducer;

public class PubSubChannel<T> extends EventBusChannel<T>
{
    public PubSubChannel(Vertx vertx, String address)
    {
        super(vertx, address);
    }

    @Override
    public MessageProducer<T> producer()
    {
        return vertx.eventBus().publisher(address, deliveryOptions);
    }
}
