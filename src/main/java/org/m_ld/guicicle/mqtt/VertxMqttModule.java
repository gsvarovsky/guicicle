/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.multibindings.ProvidesIntoSet;
import com.google.inject.name.Named;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.MqttClientOptions;
import org.m_ld.guicicle.Vertice;
import org.m_ld.guicicle.channel.ChannelProvider;
import org.m_ld.guicicle.channel.ChannelProvider.Central;

public class VertxMqttModule extends AbstractModule
{
    @Override protected void configure()
    {
        bind(MqttEventVertice.class).asEagerSingleton();
    }

    @Provides MqttClientOptions mqttClientOptions(@Named("config.mqtt") JsonObject mqttOptions)
    {
        return new MqttClientOptions(mqttOptions);
    }

    @Provides MqttClient mqttClient(Vertx vertx, MqttClientOptions mqttClientOptions)
    {
        return MqttClient.create(vertx, mqttClientOptions);
    }

    @Provides @Central ChannelProvider mqttChannels(MqttEventVertice mqttEventVertice)
    {
        return mqttEventVertice;
    }

    @ProvidesIntoSet Vertice mqttEventVertice(MqttEventVertice mqttEventVertice)
    {
        return mqttEventVertice;
    }
}

