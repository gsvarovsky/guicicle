/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.mqtt.messages.MqttPublishMessage;
import org.m_ld.guicicle.Handlers;
import org.m_ld.guicicle.channel.ChannelOptions;

import java.util.Optional;

public interface MqttEventClient
{
    boolean isConnected();

    void subscribe(MqttConsumer consumer);

    void unsubscribe(MqttConsumer consumer);

    void addConsumer(MqttConsumer consumer);

    void addProducer(MqttProducer producer);

    Future<Integer> publish(String topic, Object event, ChannelOptions options);

    Future<Integer> publish(String topic, Buffer body, ChannelOptions.Quality qos, boolean isDup, boolean isRetain);

    Handlers<Throwable> exceptionHandlers();

    Object decodeFromWire(MqttPublishMessage message, MultiMap headers, String codecHint);

    Optional<MqttPresence> presence();

    Handlers<Long> tickHandlers();

    String clientId();
}
