/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.codec;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class BidiCodec<T> implements MessageCodec<T, T>
{
    protected final Class<T> dataClass;

    protected BidiCodec(Class<T> dataClass)
    {
        this.dataClass = dataClass;
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
        return dataClass.getName();
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
}
