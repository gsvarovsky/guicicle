/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.mqtt.messages.MqttPublishMessage;
import org.m_ld.guicicle.Handlers;

import java.util.Optional;

public interface MqttEventClient
{
    boolean isConnected();

    void subscribe(MqttConsumer consumer);

    CompositeFuture unsubscribe(MqttConsumer consumer);

    void addConsumer(MqttConsumer consumer);

    void addProducer(MqttProducer producer);

    Future<Integer> publish(String topic, Object payload, MqttQoS qos, DeliveryOptions options);

    Handlers<Throwable> exceptionHandlers();

    Object decodeFromWire(MqttPublishMessage message, MultiMap headers);

    Optional<MqttPresence> presence();
}
