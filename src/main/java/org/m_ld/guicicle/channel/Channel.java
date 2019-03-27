package org.m_ld.guicicle.channel;

import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;

public interface Channel<T>
{
    MessageConsumer<T> consumer();
    MessageProducer<T> producer();
}
