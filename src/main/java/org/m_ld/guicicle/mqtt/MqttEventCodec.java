/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import com.google.inject.Inject;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.eventbus.impl.CodecManager;

import java.util.Map;

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

    public Buffer encodeToWire(Object body, DeliveryOptions options)
    {
        final MessageCodec codec = codecManager.lookupCodec(body, options.getCodecName());
        class Encoder
        {
            private Buffer encode()
            {
                final Buffer buffer = buffer();
                encodeHeaders(buffer);
                encodeCodec(buffer);
                encodeBody(buffer);
                return buffer;
            }

            private void encodeHeaders(Buffer buffer)
            {
                short count = 0;
                buffer.appendByte((byte)0); // This will get re-set after encoding the headers, if any
                final MultiMap headers = options.getHeaders();
                if (headers != null)
                {
                    for (Map.Entry<String, String> e : headers)
                    {
                        encodeString(e.getKey(), buffer);
                        encodeString(e.getValue(), buffer);
                        if (++count > Byte.MAX_VALUE)
                            throw new IllegalArgumentException("Too many headers for encoding");
                    }
                    buffer.setByte(0, (byte)count);
                }
            }

            private void encodeCodec(Buffer buffer)
            {
                buffer.appendByte(codec.systemCodecID());
                encodeString(codec.name(), buffer);
            }

            private void encodeBody(Buffer buffer)
            {
                //noinspection unchecked
                codec.encodeToWire(buffer, body);
            }

            private void encodeString(String s, Buffer buffer)
            {
                buffer.appendInt(s.length());
                buffer.appendBytes(s.getBytes(UTF_8));
            }
        }
        return new Encoder().encode();
    }

    public Object decodeFromWire(Buffer payload, MultiMap headers)
    {
        class Decoder
        {
            private int pos = 0;

            private Object decode()
            {
                decodeHeaders();
                return decodeCodec().decodeFromWire(pos, payload);
            }

            private MessageCodec decodeCodec()
            {
                byte systemCodecID = payload.getByte(pos++);
                String codecName = decodeString();
                return systemCodecID > -1 ?
                    codecManager.systemCodecs()[systemCodecID] : codecManager.getCodec(codecName);
            }

            private void decodeHeaders()
            {
                byte headerCount = payload.getByte(pos++);
                for (byte n = 0; n < headerCount; n++)
                    headers.add(decodeString(), decodeString());
            }

            private String decodeString()
            {
                int length = payload.getInt(pos);
                pos += 4;
                String codecName = new String(payload.getBytes(pos, pos + length), UTF_8);
                pos += length;
                return codecName;
            }
        }
        return new Decoder().decode();
    }
}
