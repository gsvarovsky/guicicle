/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.MqttClientOptions;
import io.vertx.mqtt.messages.MqttPublishMessage;
import org.m_ld.guicicle.Handlers;
import org.m_ld.guicicle.TimerProvider;
import org.m_ld.guicicle.TimerProvider.Periodic;
import org.m_ld.guicicle.Vertice;
import org.m_ld.guicicle.VertxCloseable;
import org.m_ld.guicicle.channel.Channel;
import org.m_ld.guicicle.channel.ChannelOptions;
import org.m_ld.guicicle.channel.ChannelOptions.Delivery;
import org.m_ld.guicicle.channel.ChannelProvider;

import java.util.*;

import static io.vertx.core.Future.succeededFuture;
import static java.lang.System.currentTimeMillis;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

/**
 * Channel provider using MQTT.
 * <p>
 * Supports {@link ChannelOptions.Quality}, publish and send (using {@link MqttPresence}) and delivery timeouts.
 */
public class MqttEventVertice implements ChannelProvider, Vertice, MqttEventClient
{
    static final int MQTTASYNC_BAD_QOS = -9, NO_PACKET = -1;
    static final Map<ChannelOptions.Quality, MqttQoS> MQTT_QOS = new HashMap<>();

    static
    {
        MQTT_QOS.put(ChannelOptions.Quality.AT_MOST_ONCE, MqttQoS.AT_MOST_ONCE);
        MQTT_QOS.put(ChannelOptions.Quality.AT_LEAST_ONCE, MqttQoS.AT_LEAST_ONCE);
        MQTT_QOS.put(ChannelOptions.Quality.EXACTLY_ONCE, MqttQoS.EXACTLY_ONCE);
    }

    private int port = MqttClientOptions.DEFAULT_PORT;
    private String host = MqttClientOptions.DEFAULT_HOST;
    private boolean connected = false;
    private final MqttClient mqtt;
    private final MqttEventCodec eventCodec;
    private final List<MqttConsumer> consumers = new ArrayList<>();
    private final List<MqttProducer> producers = new ArrayList<>();
    // Mqtt exception handling is global. Best we can do is provide for multiple handlers.
    private final Handlers<Throwable> exceptionHandlers = new Handlers<>();
    private final TickHandlers tickHandlers;
    private MqttPresence presence = null;

    private static class TickHandlers extends Handlers<Long>
    {
        final TimerProvider timerProvider;
        final long timerId;

        TickHandlers(TimerProvider timerProvider, long duration)
        {
            this.timerProvider = timerProvider;
            this.timerId = timerProvider.setTimer(duration, id -> handle(currentTimeMillis()));
        }

        void cancel()
        {
            timerProvider.cancelTimer(timerId);
        }
    }

    @Inject public MqttEventVertice(MqttClient mqtt, MqttEventCodec eventCodec, @Periodic TimerProvider timerProvider)
    {
        this.mqtt = mqtt;
        this.eventCodec = eventCodec;
        this.tickHandlers = new TickHandlers(timerProvider, SECONDS.toMillis(1));
    }

    @Inject(optional = true) public void setPort(@Named("config.mqtt.port") int port)
    {
        this.port = port;
    }

    @Inject(optional = true) public void setHost(@Named("config.mqtt.host") String host)
    {
        this.host = host;
    }

    @Inject(optional = true) public void setPresenceDomain(@Named("config.mqtt.presence.domain") String domain)
    {
        if (domain != null && presence == null)
        {
            presence = new MqttPresence(mqtt, domain);
            consumers.add(presence);
            producers.add(presence);
        }
        else if (domain == null && presence != null)
            throw new IllegalStateException("Cannot remove presence once requested");
    }

    @Override public void start(Future<Void> startFuture)
    {
        mqtt.publishHandler(msg -> consumers.forEach(c -> c.onMessage(msg)))
            .subscribeCompletionHandler(msg -> consumers.forEach(c -> c.onSubscribeAck(msg)))
            .unsubscribeCompletionHandler(msg -> consumers.forEach(c -> c.onUnsubscribeAck(msg)))
            .publishCompletionHandler(msg -> producers.forEach(p -> p.onProduced(msg)))
            .exceptionHandler(exceptionHandlers)
            .connect(port, host, connectResult -> {
                if (connectResult.succeeded())
                {
                    connected = true;
                    // Subscribe any pre-existing consumers
                    subscribeNow(consumers);
                    // Notify any pre-existing producers
                    producers.forEach(MqttProducer::onConnected);
                }
                startFuture.handle(connectResult.mapEmpty());
            });
    }

    @Override public void stop(Future<Void> stopFuture)
    {
        tickHandlers.cancel();
        final ArrayList<VertxCloseable> closeables = new ArrayList<>(producers);
        closeables.addAll(consumers);
        CompositeFuture.all(closeables.stream().map(
            closeable -> Vertice.<Void>when(closeable::close)).collect(toList()))
            .compose(allClosed -> Vertice.<Void>when(mqtt::disconnect))
            .setHandler(stopFuture);
    }

    @Override public <T> Channel<T> channel(String address, ChannelOptions options)
    {
        if (options.getDelivery() == Delivery.SEND)
            checkCanSend();

        return new MqttChannel<>(this, address, options);
    }

    @Override public void subscribe(MqttConsumer consumer)
    {
        if (!consumers.contains(consumer))
            addConsumer(consumer);

        if (connected)
            subscribeNow(singleton(consumer));
    }

    @Override public void addConsumer(MqttConsumer consumer)
    {
        consumers.add(consumer);
        consumer.addCloseHandler(v -> consumers.remove(consumer));
    }

    @Override public void addProducer(MqttProducer producer)
    {
        producers.add(producer);
        producer.addCloseHandler(v -> producers.remove(producer));
    }

    @Override public Handlers<Throwable> exceptionHandlers()
    {
        return exceptionHandlers;
    }

    @Override public Future<Integer> publish(String topic, Object payload, MqttQoS qos,
                                             DeliveryOptions options)
    {
        final Buffer buffer = eventCodec.encodeToWire(payload, options);
        if (qos == MqttQoS.AT_MOST_ONCE)
        {
            mqtt.publish(topic, buffer, qos, false, false);
            return succeededFuture(null);
        }
        else
        {
            final Promise<Integer> publishSent = Promise.promise();
            mqtt.publish(topic, buffer, qos, false, false, publishSent);
            return publishSent.future();
        }
    }

    @Override public CompositeFuture unsubscribe(MqttConsumer consumer)
    {
        return CompositeFuture.all(consumer.subscriptions().stream().map(
            sub -> Vertice.<String, Integer>when(mqtt::unsubscribe, sub.topic())).collect(toList()));
    }

    @Override public Object decodeFromWire(MqttPublishMessage message, MultiMap headers)
    {
        headers.add("__mqtt.isDup", Boolean.toString(message.isDup()));
        headers.add("__mqtt.isRetain", Boolean.toString(message.isRetain()));
        headers.add("__mqtt.qosLevel", message.qosLevel().name());
        headers.add("__mqtt.messageId", Integer.toString(message.messageId()));
        return eventCodec.decodeFromWire(message.payload(), headers);
    }

    @Override public Optional<MqttPresence> presence()
    {
        return Optional.ofNullable(presence);
    }

    @Override public Handlers<Long> tickHandlers()
    {
        return tickHandlers;
    }

    @Override public boolean isConnected()
    {
        return connected;
    }

    private void checkCanSend()
    {
        if (presence == null)
            throw new UnsupportedOperationException("Send with MQTT requires presence");
    }

    private void subscribeNow(Iterable<MqttConsumer> consumers)
    {
        final Map<String, Integer> topics = new LinkedHashMap<>();
        final Map<MqttConsumer, Integer> consumerQosIndex = new HashMap<>();
        consumers.forEach(mqttConsumer -> {
            consumerQosIndex.put(mqttConsumer, topics.size());
            mqttConsumer.subscriptions().forEach(sub -> topics.put(sub.topic(), sub.qos().ordinal()));
        });
        if (!topics.isEmpty())
            mqtt.subscribe(topics, sent -> consumerQosIndex.forEach(
                (mqttConsumer, qosIndex) -> mqttConsumer.onSubscribeSent(sent, qosIndex)));
    }
}
