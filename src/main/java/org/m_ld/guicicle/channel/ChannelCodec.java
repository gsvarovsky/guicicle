/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.channel;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.reflect.Modifier.isFinal;

public abstract class ChannelCodec<T> implements MessageCodec<T, T>
{
    protected final Class<T> dataClass;
    private final String name;

    protected ChannelCodec(Class<T> dataClass, String name)
    {
        this.dataClass = dataClass;
        this.name = name;
    }

    @Override
    public final T decodeFromWire(int pos, Buffer buffer)
    {
        return decodeFromWire(new AtomicInteger(pos), buffer);
    }

    public abstract T decodeFromWire(AtomicInteger pos, Buffer buffer);

    @Override
    public T transform(T t)
    {
        return t;
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public byte systemCodecID()
    {
        return -1;
    }

    public Class<T> getDataClass()
    {
        return dataClass;
    }

    public boolean isDefault()
    {
        return isFinal(dataClass.getModifiers());
    }
}
