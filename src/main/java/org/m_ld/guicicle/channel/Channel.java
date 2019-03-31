/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.channel;

import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;

public interface Channel<T>
{
    MessageConsumer<T> consumer();
    MessageProducer<T> producer();
}
