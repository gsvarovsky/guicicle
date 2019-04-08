/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.jackson;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.buffer.Buffer;
import org.m_ld.guicicle.channel.ChannelCodec;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class JacksonCodec<T> extends ChannelCodec<T>
{
    private final ObjectMapper objectMapper;

    public JacksonCodec(ObjectMapper objectMapper, Class<T> dataClass, String name)
    {
        super(dataClass, name);
        this.objectMapper = objectMapper;
    }

    @Override
    public void encodeToWire(Buffer buffer, T pattern)
    {
        try
        {
            final byte[] bytes = objectMapper.writeValueAsBytes(pattern);
            buffer.appendInt(bytes.length);
            buffer.appendBytes(bytes);
        }
        catch (JsonProcessingException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T decodeFromWire(AtomicInteger pos, Buffer buffer)
    {
        try
        {
            final int length = buffer.getInt(pos.getAndAdd(4));
            return objectMapper.readValue(buffer.getBytes(pos.get(), pos.addAndGet(length)), dataClass);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
