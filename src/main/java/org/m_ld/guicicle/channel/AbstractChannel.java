/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.channel;

import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import org.jetbrains.annotations.NotNull;

public abstract class AbstractChannel<T> implements Channel<T>
{
    private MessageConsumer<T> consumer;
    private MessageProducer<T> producer;

    @Override public final MessageConsumer<T> consumer()
    {
        if (consumer == null)
            consumer = createConsumer();
        return consumer;
    }

    @Override public final MessageProducer<T> producer()
    {
        if (producer == null)
            producer = createProducer();
        return producer;
    }

    @Override public void close()
    {
        if (consumer != null)
            consumer.unregister();
        if (producer != null)
            producer.close();
    }

    protected abstract @NotNull MessageConsumer<T> createConsumer();

    protected abstract @NotNull MessageProducer<T> createProducer();
}
