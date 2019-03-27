package org.m_ld.guicicle.codec;

import io.vertx.core.buffer.Buffer;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class UuidCodec extends BidiCodec<UUID>
{
    public UuidCodec()
    {
        super(UUID.class);
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
