/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.channel;

import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import org.jetbrains.annotations.NotNull;

import static org.m_ld.guicicle.mqtt.VertxMqttModule.generateRandomId;

public abstract class AbstractChannel<T> implements Channel<T>
{
    private static final String ID_HEADER = "__channel.id";
    private final String channelId = generateRandomId();
    private final ChannelOptions options = new ChannelOptions();
    private MessageConsumer<T> consumer;
    private MessageProducer<T> producer;

    public AbstractChannel(ChannelOptions options)
    {
        setOptions(options);
    }

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

    @Override public ChannelOptions options()
    {
        return new ChannelOptions(options);
    }

    @Override public void close()
    {
        if (consumer != null)
            consumer.unregister();
        if (producer != null)
            producer.close();
    }

    protected String channelId()
    {
        return channelId;
    }

    protected abstract @NotNull MessageConsumer<T> createConsumer();

    protected abstract @NotNull MessageProducer<T> createProducer();

    protected void setOptions(DeliveryOptions newOptions)
    {
        options.setOptions(newOptions);
        if (!options.isEcho())
            // Headers may not exist yet in DeliveryOptions
            if (options.getHeaders() == null)
                options.addHeader(ID_HEADER, channelId);
            else
                options.getHeaders().set(ID_HEADER, channelId);
        else if (options.getHeaders() != null)
            options.getHeaders().remove(ID_HEADER);
    }

    protected boolean isEcho(Message<T> msg)
    {
        return channelId.equals(msg.headers().get(ID_HEADER));
    }
}
