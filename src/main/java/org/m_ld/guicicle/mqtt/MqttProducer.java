/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

public interface MqttProducer
{
    void onProduced(Integer id);

    default void onConnected() {}

    default void close() {}
}
