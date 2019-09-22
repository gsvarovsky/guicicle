/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.mqtt.messages.MqttPublishMessage;
import org.jetbrains.annotations.NotNull;
import org.m_ld.guicicle.Handlers;
import org.m_ld.guicicle.channel.ChannelOptions;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;

import static io.vertx.core.Future.succeededFuture;
import static io.vertx.core.Promise.promise;
import static io.vertx.core.buffer.Buffer.buffer;
import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;
import static org.m_ld.guicicle.Handlers.Flag.SINGLE_USE;
import static org.m_ld.guicicle.channel.ChannelOptions.Quality.AT_LEAST_ONCE;
import static org.m_ld.guicicle.channel.ChannelOptions.Quality.AT_MOST_ONCE;
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
            this("__presence/+/#");
        }

        PresenceAddress(String pattern)
        {
            this(pattern.split("/"));
        }

        PresenceAddress(String[] parts)
        {
            super(parts);
            if (parts.length < 3)
                throw new IllegalArgumentException("Presence address requires three or more parts");

            this.clientId = "+".equals(parts[2]) ? "" : parts[2];
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
    private final Subscription[] subscriptions;
    private final PresenceAddress baseAddress;
    private final MqttEventClient mqtt;
    private final Map<String, Map<String, MqttTopicAddress>> present = new HashMap<>();
    private final Map<Integer, Handler<AsyncResult<Void>>> joinHandlers = new HashMap<>();
    private final Handlers<AsyncResult<Void>> closeHandlers = new Handlers<>(SINGLE_USE);
    private final Promise<Void> subscribed = promise(), unsubscribed = promise();

    MqttPresence(MqttEventClient mqtt, String domain)
    {
        this.mqtt = mqtt;
        this.baseAddress = PRESENCE_ADDRESS.domain(domain);
        this.subscriptions = new Subscription[]{new Subscription(baseAddress, AT_LEAST_ONCE)};
    }

    void join(String consumerId, String address, Handler<AsyncResult<Void>> handleResult)
    {
        // First check whether subscribed - this will normally just wave through
        subscribed.future().setHandler(subscribeResult -> {
            if (subscribeResult.succeeded())
                setStatus(consumerId, address, AT_LEAST_ONCE, msgIdResult -> {
                    if (msgIdResult.succeeded())
                        joinHandlers.put(msgIdResult.result(), handleResult);
                    else
                        handleResult.handle(msgIdResult.mapEmpty());
                });
            else
                handleResult.handle(subscribeResult);
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

    @Override public Subscription[] subscriptions()
    {
        return subscriptions;
    }

    @Override public void onMessage(MqttPublishMessage msg, Subscription subscription)
    {
        final PresenceAddress presence = new PresenceAddress(msg.topicName());
        final String onTopicAddress = msg.payload().toString();
        if (DISCONNECTED.equals(onTopicAddress))
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
                            mqtt.clientId(), presence.clientId(), presence.consumerId().get(), onTopicAddress));
            present.computeIfAbsent(
                presence.clientId(), cid -> new HashMap<>()).put(presence.consumerId().get(), pattern(onTopicAddress));
        }
    }

    @Override public void onSubscribe(AsyncResult<Void> subscribeResult)
    {
        subscribed.handle(subscribeResult);
    }

    @Override public void onUnsubscribe(AsyncResult<Void> unsubscribeResult)
    {
        unsubscribed.handle(unsubscribeResult);
    }

    @Override public void onPublished(Integer id)
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
        mqtt.unsubscribe(this);
        unsubscribed.future().setHandler(closeHandlers);
    }

    private void setStatus(String consumerId, String address, ChannelOptions.Quality qos,
                           Handler<AsyncResult<Integer>> handleResult)
    {
        LOG.info(format("%s: Local consumer %s setting presence %s", mqtt.clientId(), consumerId, address));

        // Retain presence (but not absence) for new clients
        final boolean isRetain = !DISCONNECTED.equals(address);
        final String topic = baseAddress.withIds(mqtt.clientId(), consumerId).toString();
        mqtt.publish(topic, buffer(address), qos, false, isRetain).setHandler(handleResult);
    }
}
