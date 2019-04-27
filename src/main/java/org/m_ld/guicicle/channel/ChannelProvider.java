/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.channel;

import com.google.inject.BindingAnnotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public interface ChannelProvider
{
    /**
     * Marks a channel as JVM-local
     */
    @BindingAnnotation @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
    @interface Local {}

    /**
     * Marks a channel provider as using a central service, such as MQTT or websockets
     */
    @BindingAnnotation @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
    @interface Central {}

    /**
     * Creates a channel for the given address, using the given channel options.
     *
     * @param address the channel address
     * @param options the channel options
     * @param <T> the message type
     * @return the channel, ready to use
     */
    <T> Channel<T> channel(String address, ChannelOptions options);
}
