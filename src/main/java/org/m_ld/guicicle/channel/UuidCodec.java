/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.channel;

import io.vertx.core.buffer.Buffer;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class UuidCodec extends ChannelCodec<UUID>
{
    public UuidCodec()
    {
        super(UUID.class, UUID.class.getSimpleName());
    }

    @Override
    public void encodeToWire(Buffer buffer, UUID uuid)
    {
        buffer.appendLong(uuid.getMostSignificantBits());
        buffer.appendLong(uuid.getLeastSignificantBits());
    }

    @Override
    public UUID decodeFromWire(AtomicInteger pos, Buffer buffer)
    {
        return new UUID(buffer.getLong(pos.getAndAdd(8)),
                        buffer.getLong(pos.getAndAdd(8)));
    }
}
