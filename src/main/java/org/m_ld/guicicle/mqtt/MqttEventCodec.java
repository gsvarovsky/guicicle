/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import com.google.inject.Inject;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.eventbus.impl.CodecManager;

import static io.netty.util.CharsetUtil.UTF_8;
import static io.vertx.core.buffer.Buffer.buffer;

/**
 * Not strictly a {@link MessageCodec}, but similarly used to encode and decode MQTT events as published by the {@link
 * MqttEventVertice}.
 * <p>
 * The full encoding includes the payload encoding codec identity for decoding at the consumer. This is the system codec
 * ID (-1 for a non-system codec) and the codec name. The available codecs at the consumer must tally with those at the
 * producer, or have a suitable translation.
 */
public class MqttEventCodec
{
    private final CodecManager codecManager;

    @Inject public MqttEventCodec(CodecManager codecManager)
    {
        this.codecManager = codecManager;
    }

    public Buffer encodeToWire(Object payload, DeliveryOptions options)
    {
        final Buffer buffer = buffer();
        final MessageCodec codec = codecManager.lookupCodec(payload, options.getCodecName());
        buffer.appendByte(codec.systemCodecID());
        buffer.appendInt(codec.name().length());
        buffer.appendBytes(codec.name().getBytes(UTF_8));
        //noinspection unchecked
        codec.encodeToWire(buffer, payload);
        return buffer;
    }

    public Object decodeFromWire(Buffer payload)
    {
        int pos = 0;
        byte systemCodecID = payload.getByte(pos++);
        int length = payload.getInt(pos);
        pos += 4;
        String codecName = new String(payload.getBytes(pos, pos + length), UTF_8);
        pos += length;
        final MessageCodec codec = systemCodecID > -1 ?
            codecManager.systemCodecs()[systemCodecID] : codecManager.getCodec(codecName);
        return codec.decodeFromWire(pos, payload);
    }
}
