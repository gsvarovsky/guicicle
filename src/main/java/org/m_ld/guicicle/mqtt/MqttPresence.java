/*
 * Copyright (c) George Svarovsky 2020. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import com.fasterxml.jackson.core.type.TypeReference;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.JacksonCodec;
import io.vertx.mqtt.MqttClientOptions;
import io.vertx.mqtt.messages.MqttPublishMessage;
import org.jetbrains.annotations.NotNull;
import org.m_ld.guicicle.Handlers;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import static io.vertx.core.Future.succeededFuture;
import static io.vertx.core.Promise.promise;
import static io.vertx.core.buffer.Buffer.buffer;
import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;
import static org.m_ld.guicicle.Handlers.Flag.SINGLE_USE;
import static org.m_ld.guicicle.channel.ChannelOptions.Quality.AT_LEAST_ONCE;
import static org.m_ld.guicicle.mqtt.MqttTopicAddress.pattern;

/**
 * Per https://github.com/mqtt/mqtt.github.io/wiki/presence
 */
public class MqttPresence implements MqttConsumer, MqttProducer
{
    private static final Logger LOG = Logger.getLogger(MqttPresence.class.getName());

    private static class PresenceAddress extends MqttTopicAddress<PresenceAddress>
    {
        PresenceAddress()
        {
            this("__presence/+/+");
        }

        PresenceAddress(String pattern)
        {
            this(pattern.split("/"));
        }

        PresenceAddress(String[] parts)
        {
            super(parts);
            if (parts.length != 3)
                throw new IllegalArgumentException("Presence address requires three parts");
        }

        PresenceAddress domain(String domain)
        {
            return substitute(1, domain);
        }

        String clientId()
        {
            return get(2);
        }

        PresenceAddress clientId(String clientId)
        {
            return substitute(2, clientId);
        }

        @Override protected PresenceAddress create(String[] parts)
        {
            return new PresenceAddress(parts);
        }
    }

    /**
     * Leave payload is empty, so that it's not retained
     * See http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc385349265
     */
    private static final Buffer DISCONNECTED = buffer();
    private static final PresenceAddress PRESENCE_ADDRESS = new PresenceAddress();
    private final Subscription[] subscriptions;
    private final PresenceAddress domainAddress, clientAddress;
    private final MqttEventClient mqtt;
    private final Map<String, Map<String, String>> present = new HashMap<>();
    private final Map<Integer, Handler<AsyncResult<Void>>> joinHandlers = new HashMap<>();
    private final Handlers<AsyncResult<Void>> closeHandlers = new Handlers<>(SINGLE_USE);
    private final Promise<Void> subscribed = promise(), unsubscribed = promise();

    MqttPresence(MqttEventClient mqtt, String domain)
    {
        this.mqtt = mqtt;
        this.domainAddress = PRESENCE_ADDRESS.domain(domain);
        this.clientAddress = this.domainAddress.clientId(mqtt.clientId());
        this.subscriptions = new Subscription[]{new Subscription(domainAddress, AT_LEAST_ONCE)};
    }

    static void setWill(MqttClientOptions options, String domain, String clientId)
    {
        options.setWillTopic(PRESENCE_ADDRESS.domain(domain).clientId(clientId).toString());
        options.setWillMessage(DISCONNECTED.toString());
        options.setWillFlag(true);
        options.setWillQoS(AT_LEAST_ONCE.ordinal());
        options.setWillRetain(true);
    }

    void join(String consumerId, String address, Handler<AsyncResult<Void>> handleResult)
    {
        LOG.fine(() -> format("%s: Local consumer %s joining presence on %s", mqtt.clientId(), consumerId, address));

        // First check whether subscribed - this will normally just wave through
        subscribed.future().onComplete(subscribeResult -> {
            if (subscribeResult.succeeded())
            {
                present.computeIfAbsent(mqtt.clientId(), cid -> new HashMap<>()).put(consumerId, address);
                setStatus(msgIdResult -> {
                    if (msgIdResult.succeeded())
                        joinHandlers.put(msgIdResult.result(), handleResult);
                    else
                        handleResult.handle(msgIdResult.mapEmpty());
                });
            }
            else
            {
                handleResult.handle(subscribeResult);
            }
        });
    }

    void leave(String consumerId, Handler<AsyncResult<Void>> handleResult)
    {
        left(mqtt.clientId(), consumerId);
        setStatus(i -> handleResult.handle(i.mapEmpty()));
    }

    private void setStatus(Handler<AsyncResult<Integer>> handleResult)
    {
        final Map<String, String> myPresent = present.get(mqtt.clientId());
        mqtt.publish(clientAddress.toString(),
                     myPresent == null ? DISCONNECTED : Json.encodeToBuffer(myPresent),
                     AT_LEAST_ONCE, false, true).onComplete(handleResult);
    }

    private void left(@NotNull String clientId, @NotNull String consumerId)
    {
        LOG.fine(format("%s: Client %s consumer %s disconnected",
                        mqtt.clientId(), clientId, consumerId));
        final Map<String, String> clientPresent = present.get(clientId);
        if (clientPresent != null)
        {
            clientPresent.remove(consumerId);
            if (clientPresent.isEmpty())
                left(clientId);
        }
        else
        {
            LOG.fine(format("%s: Client %s consumer %s was unknown",
                            mqtt.clientId(), clientId, consumerId));
        }
    }

    private void left(@NotNull String clientId)
    {
        // A whole client has disconnected
        LOG.fine(format("%s: Client %s disconnected", mqtt.clientId(), clientId));
        present.remove(clientId);
    }

    @NotNull Set<String> present(String address)
    {
        return present.entrySet().stream()
            .flatMap(e -> e.getValue().entrySet().stream())
            .filter(e -> pattern(e.getValue()).match(address).isPresent())
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
        if (msg.payload().length() == 0)
        {
            left(presence.clientId());
        }
        else
        {
            present.put(presence.clientId(),
                        JacksonCodec.decodeValue(msg.payload(), new TypeReference<Map<String, String>>() {}));
        }
    }

    @Override public void onSubscribe(AsyncResult<Void> subscribeResult)
    {
        LOG.fine(() -> format("Presence subscribed to %s with %s", domainAddress, subscribeResult));
        subscribed.handle(subscribeResult);
    }

    @Override public void onUnsubscribe(AsyncResult<Void> unsubscribeResult)
    {
        LOG.fine(() -> format("Presence unsubscribed from %s with %s", domainAddress, unsubscribeResult));
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
        unsubscribed.future().onComplete(closeHandlers);
    }
}
