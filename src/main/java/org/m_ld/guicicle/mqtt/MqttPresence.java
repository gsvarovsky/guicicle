/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import io.vertx.core.buffer.Buffer;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.messages.MqttPublishMessage;
import org.jetbrains.annotations.NotNull;

import java.util.*;

import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.m_ld.guicicle.mqtt.MqttConsumer.subscription;
import static org.m_ld.guicicle.mqtt.MqttTopicAddress.pattern;

public class MqttPresence implements MqttConsumer
{
    private static class PresenceAddress extends MqttTopicAddress<PresenceAddress>
    {
        private final String clientId, channelId;

        PresenceAddress()
        {
            super("__presence/#");
            this.clientId = "";
            this.channelId = null;
        }

        PresenceAddress(String[] parts)
        {
            super(parts);
            final String[] tail = parts[0].split("/");
            this.clientId = tail[0];
            this.channelId = tail.length > 1 ? tail[1] : null;
        }

        String clientId()
        {
            return clientId;
        }

        Optional<String> channelId()
        {
            return Optional.ofNullable(channelId);
        }

        PresenceAddress withIds(String clientId, String channelId)
        {
            return substitute(clientId + "/" + channelId);
        }

        @Override protected PresenceAddress create(String[] parts)
        {
            return new PresenceAddress(parts);
        }
    }

    private static final PresenceAddress PRESENCE_ADDRESS = new PresenceAddress();
    private static final String DISCONNECTED = "-";
    private final MqttClient mqtt;
    private final Map<String, Map<String, MqttTopicAddress>> present = new HashMap<>();

    MqttPresence(MqttClient mqtt)
    {
        this.mqtt = mqtt;
    }

    void join(String consumerId, String address)
    {
        setStatus(consumerId, address);
    }

    void leave(String consumerId)
    {
        setStatus(consumerId, DISCONNECTED);
    }

    @NotNull Set<String> present(String address)
    {
        return present.entrySet().stream()
            .flatMap(e -> e.getValue().entrySet().stream())
            .filter(e -> e.getValue().match(address).isPresent())
            .map(Map.Entry::getKey)
            .collect(toSet());
    }

    @Override public List<Subscription> subscriptions()
    {
        return singletonList(subscription(PRESENCE_ADDRESS.toString(), AT_MOST_ONCE));
    }

    @Override public void onMessage(MqttPublishMessage msg)
    {
        PRESENCE_ADDRESS.match(msg.topicName()).ifPresent(presence -> {
            final String address = msg.payload().toString();
            if (DISCONNECTED.equals(address))
            {
                if (presence.channelId().isPresent() && present.containsKey(presence.clientId()))
                {
                    // One consumer has disconnected
                    final Set<String> channelIds = present.get(presence.clientId()).keySet();
                    if (channelIds.remove(presence.channelId().get()) && channelIds.isEmpty())
                        present.remove(presence.clientId());
                }
                else if (!presence.channelId().isPresent())
                {
                    // A whole client has disconnected
                    present.remove(presence.clientId());
                }
            }
            else if (presence.channelId().isPresent())
            {
                present.computeIfAbsent(
                    presence.clientId(), cid -> new HashMap<>()).put(presence.channelId().get(), pattern(address));
            }
        });
    }

    private void setStatus(String consumerId, String address)
    {
        mqtt.publish(PRESENCE_ADDRESS.withIds(mqtt.clientId(), consumerId).toString(),
                     Buffer.buffer(address), AT_MOST_ONCE, false, true); // Retain for new clients
    }
}
