/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.MqttClientOptions;
import io.vertx.mqtt.messages.MqttPublishMessage;
import io.vertx.mqtt.messages.MqttSubAckMessage;
import org.m_ld.guicicle.Handlers;
import org.m_ld.guicicle.TimerProvider;
import org.m_ld.guicicle.TimerProvider.Periodic;
import org.m_ld.guicicle.Vertice;
import org.m_ld.guicicle.VertxCloseable;
import org.m_ld.guicicle.channel.Channel;
import org.m_ld.guicicle.channel.ChannelOptions;
import org.m_ld.guicicle.channel.ChannelOptions.Delivery;
import org.m_ld.guicicle.channel.ChannelProvider;
import org.m_ld.guicicle.channel.MessageCodecs;
import org.m_ld.guicicle.mqtt.MqttConsumer.Subscription;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.stream;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.m_ld.guicicle.channel.ChannelOptions.Quality.*;
import static org.m_ld.guicicle.mqtt.MqttConsumer.Subscription.isComplete;

/**
 * Channel provider using MQTT.
 * <p>
 * Supports {@link ChannelOptions.Quality}, publish and send (using {@link MqttPresence}) and delivery timeouts.
 */
public class MqttEventVertice implements ChannelProvider, Vertice, MqttEventClient
{
    static final Map<ChannelOptions.Quality, MqttQoS> MQTT_QOS = new HashMap<>();

    static
    {
        MQTT_QOS.put(AT_MOST_ONCE, MqttQoS.AT_MOST_ONCE);
        MQTT_QOS.put(AT_LEAST_ONCE, MqttQoS.AT_LEAST_ONCE);
        MQTT_QOS.put(EXACTLY_ONCE, MqttQoS.EXACTLY_ONCE);
    }

    private int port = MqttClientOptions.DEFAULT_PORT;
    private String host = MqttClientOptions.DEFAULT_HOST;
    private boolean connected = false;
    private final MqttClient mqtt;
    private final MessageCodecs messageCodecs;
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

    @Inject public MqttEventVertice(MqttClient mqtt,
                                    MessageCodecs messageCodecs,
                                    @Periodic TimerProvider timerProvider)
    {
        this.mqtt = mqtt;
        this.messageCodecs = messageCodecs;
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
            presence = new MqttPresence(this, domain);
            consumers.add(presence);
            producers.add(presence);
        }
        else if (domain == null && presence != null)
            throw new IllegalStateException("Cannot remove presence once requested");
    }

    @Override public void start(Future<Void> startFuture)
    {
        mqtt.publishHandler(this::onMessage)
            .subscribeCompletionHandler(this::onSubscribeAck)
            .unsubscribeCompletionHandler(this::onUnsubscribeAck)
            .publishCompletionHandler(this::onPublishComplete)
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
        final Set<VertxCloseable> closeables = new HashSet<>(producers);
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

    @Override public Future<Integer> publish(String topic, Object event, ChannelOptions options)
    {
        return publish(topic, messageCodecs.encode(event, options), options.getQuality(), false, false);
    }

    @Override public Future<Integer> publish(
        String topic, Buffer body, ChannelOptions.Quality qos, boolean isDup, boolean isRetain)
    {
        if (qos == AT_MOST_ONCE)
        {
            mqtt.publish(topic, body, MqttQoS.AT_MOST_ONCE, isDup, isRetain);
            return succeededFuture(null);
        }
        else
        {
            final Promise<Integer> publishSent = Promise.promise();
            mqtt.publish(topic, body, MQTT_QOS.get(qos), isDup, isRetain, publishSent);
            return publishSent.future();
        }
    }

    @Override public void unsubscribe(MqttConsumer consumer)
    {
        CompositeFuture.all(stream(consumer.subscriptions()).map(
            subscription -> {
                if (!isComplete(subscription.subscribed))
                    // We never subscribed; indicate trivial success
                    return succeededFuture(subscription.unsubscribed = succeededFuture());
                else if (subscription.unsubscribed != null)
                    // We have already unsubscribed, or we're already waiting for an ack
                    return succeededFuture(subscription.unsubscribed);
                else
                    return Vertice.<String, Integer>when(mqtt::unsubscribe, subscription.address.toString())
                        // If the subscribe succeeds, wait for the subscribe ack
                        .map(unsubId -> subscription.unsubscribed = succeededFuture(unsubId))
                        .otherwise(failure -> subscription.unsubscribed = failedFuture(failure));
            }).collect(toList()))
            // In the case that all unsubscribes have failed, signal the failure to the consumer
            .setHandler(allResult -> signalIfDone(consumer, consumer::onUnsubscribe, s -> s.unsubscribed));
    }

    @Override public Object decodeFromWire(MqttPublishMessage message, MultiMap headers, String codecHint)
    {
        headers.set("__mqtt.isDup", Boolean.toString(message.isDup()));
        headers.set("__mqtt.isRetain", Boolean.toString(message.isRetain()));
        headers.set("__mqtt.qosLevel", message.qosLevel().name());
        headers.set("__mqtt.messageId", Integer.toString(message.messageId()));
        return messageCodecs.decode(message.payload(), headers, codecHint);
    }

    @Override public Optional<MqttPresence> presence()
    {
        return Optional.ofNullable(presence);
    }

    @Override public Handlers<Long> tickHandlers()
    {
        return tickHandlers;
    }

    @Override public String clientId()
    {
        return mqtt.clientId();
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
        for (MqttConsumer mqttConsumer : consumers)
        {
            stream(mqttConsumer.subscriptions()).forEach(subscription -> {
                topics.put(subscription.address.toString(), subscription.qos.ordinal());
                subscription.qosIndex = topics.size() - 1;
            });
        }
        if (!topics.isEmpty())
        {
            mqtt.subscribe(topics, sent -> {
                for (MqttConsumer consumer : consumers)
                {
                    stream(consumer.subscriptions()).forEach(sub -> sub.subscribed = sent);
                    // In the case that all subscribes have failed, signal the failure to the consumer
                    signalIfDone(consumer, consumer::onSubscribe, s -> s.subscribed);
                }
            });
        }
    }

    private void onSubscribeAck(MqttSubAckMessage subAckMessage)
    {
        for (MqttConsumer consumer : consumers)
        {
            boolean changed = false;
            for (Subscription subscription : consumer.subscriptions())
            {
                if (subscription.subscribed != null &&
                    Objects.equals(subscription.subscribed.result(), subAckMessage.messageId()))
                {
                    final MqttQoS qosReceived =
                        MqttQoS.valueOf(subAckMessage.grantedQoSLevels().get(subscription.qosIndex));
                    if (qosReceived == MQTT_QOS.get(subscription.qos))
                        subscription.subscribed = succeededFuture();
                    else
                        subscription.subscribed = failedFuture(
                            format("Mismatched QoS: expecting %s, received %s", subscription.qos, qosReceived));
                    changed = true;
                }
            }
            if (changed)
                signalIfDone(consumer, consumer::onSubscribe, s -> s.subscribed);
        }
    }

    private void onUnsubscribeAck(Integer id)
    {
        for (MqttConsumer consumer : consumers)
        {
            boolean changed = false;
            for (Subscription subscription : consumer.subscriptions())
            {
                if (subscription.unsubscribed != null &&
                    Objects.equals(subscription.unsubscribed.result(), id))
                {
                    subscription.unsubscribed = succeededFuture();
                    changed = true;
                }
            }
            if (changed)
                signalIfDone(consumer, consumer::onUnsubscribe, s -> s.unsubscribed);
        }
    }

    private void signalIfDone(MqttConsumer consumer,
                              Consumer<AsyncResult<Void>> signal,
                              Function<Subscription, AsyncResult<Integer>> getResult)
    {
        if (stream(consumer.subscriptions()).map(getResult).allMatch(Subscription::isComplete))
            signal.accept(stream(consumer.subscriptions()).map(getResult)
                              .filter(AsyncResult::failed).findAny()
                              .<Future<Void>>map(s -> failedFuture(s.cause()))
                              .orElse(succeededFuture()));
    }

    private void onMessage(MqttPublishMessage msg)
    {
        for (MqttConsumer consumer : consumers)
            for (Subscription subscription : consumer.subscriptions())
                if (subscription.address.match(msg.topicName()).isPresent())
                    consumer.onMessage(msg, subscription);
    }

    private void onPublishComplete(Integer msgId)
    {
        producers.forEach(p -> p.onPublished(msgId));
    }
}
