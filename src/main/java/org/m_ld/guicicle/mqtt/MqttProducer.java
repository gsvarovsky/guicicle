/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import org.m_ld.guicicle.VertxCloseable;

public interface MqttProducer extends VertxCloseable
{
    void onPublished(Integer id);

    default void onConnected() {}
}
