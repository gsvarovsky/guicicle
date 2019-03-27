package org.m_ld.guicicle.channel;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageProducer;

public class P2PChannel<T> extends EventBusChannel<T>
{
    public P2PChannel(Vertx vertx, String address)
    {
        super(vertx, address);
    }

    @Override
    public MessageProducer<T> producer()
    {
        return vertx.eventBus().sender(address, deliveryOptions);
    }
}
