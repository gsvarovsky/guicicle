/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.messages.MqttPublishMessage;
import org.jetbrains.annotations.NotNull;
import org.m_ld.guicicle.Handlers;

import java.util.*;
import java.util.logging.Logger;

import static io.netty.handler.codec.mqtt.MqttQoS.AT_LEAST_ONCE;
import static io.netty.handler.codec.mqtt.MqttQoS.AT_MOST_ONCE;
import static io.vertx.core.Future.succeededFuture;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.m_ld.guicicle.Handlers.Flag.SINGLE_USE;
import static org.m_ld.guicicle.mqtt.MqttConsumer.subscription;
import static org.m_ld.guicicle.mqtt.MqttTopicAddress.pattern;

/**
 * Per https://github.com/mqtt/mqtt.github.io/wiki/presence
 */
public class MqttPresence implements MqttConsumer, MqttProducer
{
    private static final Logger LOG = Logger.getLogger(MqttPresence.class.getName());

    private static class PresenceAddress extends MqttTopicAddress<PresenceAddress>
    {
        private final String clientId, consumerId;

        PresenceAddress()
        {
            super("__presence/+/#");
            this.clientId = "";
            this.consumerId = null;
        }

        PresenceAddress(String[] parts)
        {
            super(parts);
            this.clientId = parts[2];
            this.consumerId = parts.length > 3 ? parts[3] : null;
        }

        PresenceAddress domain(String domain)
        {
            return substitute(1, domain);
        }

        String clientId()
        {
            return clientId;
        }

        Optional<String> consumerId()
        {
            return Optional.ofNullable(consumerId);
        }

        PresenceAddress withIds(String clientId, String channelId)
        {
            return substitute(2, clientId + "/" + channelId);
        }

        @Override protected PresenceAddress create(String[] parts)
        {
            return new PresenceAddress(parts);
        }
    }

    private static final String DISCONNECTED = "-";
    private static final PresenceAddress PRESENCE_ADDRESS = new PresenceAddress();
    private final PresenceAddress baseAddress;
    private final MqttClient mqtt;
    private final Map<String, Map<String, MqttTopicAddress>> present = new HashMap<>();
    private final Map<Integer, Handler<AsyncResult<Void>>> joinHandlers = new HashMap<>();
    private final Handlers<AsyncResult<Void>> closeHandlers = new Handlers<>(SINGLE_USE);

    MqttPresence(MqttClient mqtt, String domain)
    {
        this.mqtt = mqtt;
        this.baseAddress = PRESENCE_ADDRESS.domain(domain);
    }

    void join(String consumerId, String address, Handler<AsyncResult<Void>> handleResult)
    {
        setStatus(consumerId, address, AT_LEAST_ONCE, msgIdResult -> {
            if (msgIdResult.succeeded())
                joinHandlers.put(msgIdResult.result(), handleResult);
            else
                handleResult.handle(msgIdResult.mapEmpty());
        });
    }

    void leave(String consumerId, Handler<AsyncResult<Void>> handleResult)
    {
        setStatus(consumerId, DISCONNECTED, AT_MOST_ONCE, i -> handleResult.handle(i.mapEmpty()));
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
        return singletonList(subscription(baseAddress.toString(), AT_LEAST_ONCE));
    }

    @Override public void onMessage(MqttPublishMessage msg)
    {
        baseAddress.match(msg.topicName()).ifPresent(presence -> {
            final String address = msg.payload().toString();
            if (DISCONNECTED.equals(address))
            {
                if (presence.consumerId().isPresent() && present.containsKey(presence.clientId()))
                {
                    LOG.fine(format("%s: Client %s consumer %s disconnected",
                                    mqtt.clientId(), presence.clientId(), presence.consumerId().get()));
                    final Set<String> channelIds = present.get(presence.clientId()).keySet();
                    if (channelIds.remove(presence.consumerId().get()) && channelIds.isEmpty())
                        present.remove(presence.clientId());
                }
                else if (!presence.consumerId().isPresent())
                {
                    // A whole client has disconnected
                    LOG.fine(format("%s: Client %s disconnected", mqtt.clientId(), presence.clientId()));
                    present.remove(presence.clientId());
                }
            }
            else if (presence.consumerId().isPresent())
            {
                LOG.fine(format("%s: Client %s consumer %s present on %s",
                                mqtt.clientId(), presence.clientId(), presence.consumerId().get(), address));
                present.computeIfAbsent(
                    presence.clientId(), cid -> new HashMap<>()).put(presence.consumerId().get(), pattern(address));
            }
        });
    }

    @Override public void onProduced(Integer id)
    {
        final Handler<AsyncResult<Void>> joinHandler = joinHandlers.remove(id);
        if (joinHandler != null)
            joinHandler.handle(succeededFuture());
    }

    @Override public void addCloseHandler(Handler<AsyncResult<Void>> closeHandler)
    {
        closeHandlers.add(closeHandler);
    }

    @Override public void close()
    {
        mqtt.unsubscribe(baseAddress.toString());
        closeHandlers.handle(succeededFuture());
    }

    private void setStatus(String consumerId, String address, MqttQoS qos, Handler<AsyncResult<Integer>> handleResult)
    {
        LOG.info(format("%s: Local consumer %s setting presence %s", mqtt.clientId(), consumerId, address));

        // Retain presence (but not absence) for new clients
        final boolean isRetain = !DISCONNECTED.equals(address);
        mqtt.publish(baseAddress.withIds(mqtt.clientId(), consumerId).toString(),
                     Buffer.buffer(address),
                     qos,
                     false,
                     isRetain,
                     handleResult);
    }
}
