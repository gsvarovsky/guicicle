/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.channel;

import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageCodec;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper for {@link MessageCodec}s, able to take arbitrary object input and decide which message codec to use for
 * encoding, along with sufficient information to decode messages on the other end.
 */
public interface MessageCodecs
{
    /**
     * Encode the given message body using the given delivery options. May encode the option headers.
     *
     * @param body    the object to encode into a message
     * @param options the options to use (codec and headers)
     * @return the encoded message buffer
     */
    Buffer encode(Object body, DeliveryOptions options);

    /**
     * Decode a message buffer, and populate the headers as required.
     *
     * @param payload   the message buffer from the wire
     * @param headers   [out] headers to be populated with message headers from the wire (if available)
     * @param codecHint a hint to the implementation of the expected codec name. May not be used if there is sufficient
     *                  information in the message payload to decide the required codec.
     * @return the decoded message body
     */
    Object decode(Buffer payload, MultiMap headers, @Nullable String codecHint);
}
