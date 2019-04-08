/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.*;
import io.vertx.core.eventbus.impl.BodyReadStream;
import io.vertx.core.streams.ReadStream;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.MqttClientOptions;
import io.vertx.mqtt.MqttException;
import io.vertx.mqtt.messages.MqttPublishMessage;
import io.vertx.mqtt.messages.MqttSubAckMessage;
import org.m_ld.guicicle.Handlers;
import org.m_ld.guicicle.Vertice;
import org.m_ld.guicicle.channel.Channel;
import org.m_ld.guicicle.channel.ChannelCodec;
import org.m_ld.guicicle.channel.ChannelOptions;
import org.m_ld.guicicle.channel.ChannelProvider;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.regex.Pattern;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.m_ld.guicicle.Handlers.Flag.SINGLE_INSTANCE;
import static org.m_ld.guicicle.Handlers.Flag.SINGLE_USE;

public class MqttEventVertice implements ChannelProvider, Vertice
{
    private static final int MQTTASYNC_BAD_QOS = -9;
    private static final int NO_PACKET = -1;
    private static final Map<ChannelOptions.Quality, MqttQoS> MQTT_QOS = new HashMap<>();
    static
    {
        MQTT_QOS.put(ChannelOptions.Quality.AT_MOST_ONCE, MqttQoS.AT_MOST_ONCE);
        MQTT_QOS.put(ChannelOptions.Quality.AT_LEAST_ONCE, MqttQoS.AT_LEAST_ONCE);
        MQTT_QOS.put(ChannelOptions.Quality.EXACTLY_ONCE, MqttQoS.EXACTLY_ONCE);
    }
    @Inject(optional = true) @Named("config.mqtt.buffer.size") private int bufferSize = 128;
    @Inject(optional = true) @Named("config.mqtt.port") private int port = MqttClientOptions.DEFAULT_PORT;
    @Inject(optional = true) @Named("config.mqtt.host") private String host = MqttClientOptions.DEFAULT_HOST;
    private boolean connected = false;
    private final MqttClient mqtt;
    private final Injector injector;

    private final List<MqttConsumer<?>> consumers = new ArrayList<>();

    @Inject public MqttEventVertice(MqttClient mqtt, Injector injector)
    {
        this.mqtt = mqtt;
        this.injector = injector;
    }

    @Override public void start(Future<Void> startFuture)
    {
        mqtt.publishHandler(this::onMessage)
            .subscribeCompletionHandler(this::onSubscriptionAck)
            .unsubscribeCompletionHandler(this::onUnsubscribeAck)
            .connect(port, host, connectResult -> {
                if (connectResult.succeeded())
                {
                    connected = true;
                    // Register any pre-existing consumers which have a handler
                    consumers.forEach(MqttConsumer::register);
                }
                startFuture.handle(connectResult.mapEmpty());
            });
    }

    @Override public void stop(Future<Void> stopFuture)
    {
        mqtt.disconnect(result -> {
            consumers.forEach(MqttConsumer::onEnd);
            stopFuture.handle(result.mapEmpty());
        });
    }

    @Override public <T> Channel<T> channel(String address, ChannelOptions options)
    {
        final ChannelCodec<T> codec = injector.getInstance(
            Key.get(new TypeLiteral<ChannelCodec<T>>() {}, Names.named(options.getCodecName())));
        final MqttQoS qos = MQTT_QOS.get(options.getQuality());
        final MqttConsumer<T> consumer = new MqttConsumer<>(address, qos, codec);
        final MqttProducer<T> producer = new MqttProducer<>();

        return new Channel<T>()
        {
            @Override public MessageConsumer<T> consumer()
            {
                return consumer;
            }

            @Override public MessageProducer<T> producer()
            {
                return producer;
            }
        };
    }

    private void onMessage(MqttPublishMessage message)
    {
        consumers.forEach(consumer -> {
            if (consumer.topicPattern.matcher(message.topicName()).matches())
                consumer.onMessage(message);
        });
    }

    private void onSubscriptionAck(MqttSubAckMessage subAckMessage)
    {
        consumers.forEach(consumer -> {
            if (consumer.subscribeId == subAckMessage.messageId())
            {
                try
                {
                    final int qosValue = subAckMessage.grantedQoSLevels().get(consumer.ordinal);
                    final MqttQoS qosReceived = MqttQoS.valueOf(qosValue);
                    if (qosReceived != consumer.qos)
                        throw new IllegalStateException(
                            format("Mismatched QoS: expecting %s, received %s", consumer.qos, qosReceived));
                    consumer.onSubscribe(succeededFuture());
                }
                catch (Exception e)
                {
                    final MqttException exception = new MqttException(MQTTASYNC_BAD_QOS, e.getMessage());
                    exception.initCause(e);
                    consumer.onSubscribe(failedFuture(exception));
                }
            }
        });
    }

    private void onUnsubscribeAck(int messageId)
    {
        consumers.forEach(consumer -> {
            if (consumer.unsubscribeId == messageId)
                consumer.onUnsubscribe();
        });
    }

    private class MqttProducer<T> implements MessageProducer<T>
    {
        @Override public MessageProducer<T> send(T message)
        {
            return null;
        }

        @Override public <R> MessageProducer<T> send(T message, Handler<AsyncResult<Message<R>>> replyHandler)
        {
            return null;
        }

        @Override public MessageProducer<T> exceptionHandler(Handler<Throwable> handler)
        {
            return null;
        }

        @Override public MessageProducer<T> write(T data)
        {
            return null;
        }

        @Override public MessageProducer<T> setWriteQueueMaxSize(int maxSize)
        {
            return null;
        }

        @Override public boolean writeQueueFull()
        {
            return false;
        }

        @Override public MessageProducer<T> drainHandler(Handler<Void> handler)
        {
            return null;
        }

        @Override public MessageProducer<T> deliveryOptions(DeliveryOptions options)
        {
            return null;
        }

        @Override public String address()
        {
            return null;
        }

        @Override public void end()
        {

        }

        @Override public void close()
        {

        }
    }

    private class MqttConsumer<T> implements MessageConsumer<T>
    {
        final String topicName;
        final Pattern topicPattern;
        final MqttQoS qos;
        final MessageCodec<?, T> codec;
        final Handlers<Message<T>> messageHandlers = new Handlers<>();
        final Handlers<AsyncResult<Void>> subscribeHandlers = new Handlers<>(SINGLE_USE);
        final Handlers<AsyncResult<Void>> unsubscribeHandler = new Handlers<>(SINGLE_INSTANCE, SINGLE_USE);
        final Handlers<Void> endHandlers = new Handlers<>(SINGLE_USE);
        int subscribeId = NO_PACKET, unsubscribeId = NO_PACKET;
        int ordinal = 0;
        int bufferSize = MqttEventVertice.this.bufferSize;
        boolean registered = false;
        Queue<MqttPublishMessage> buffer;

        MqttConsumer(String topicName, MqttQoS qos, MessageCodec<?, T> codec)
        {
            this.topicName = requireNonNull(topicName);
            this.topicPattern = Pattern.compile(
                Pattern.quote(topicName).replaceAll("\\+", "[^/]+").replaceAll("#", ".+"));
            this.qos = requireNonNull(qos);
            this.codec = codec;
            consumers.add(this);
        }

        @Override public MessageConsumer<T> handler(Handler<Message<T>> handler)
        {
            // Subscribe to the given address if we're connected
            if (connected && !registered)
                register();

            this.messageHandlers.add(handler);
            return this;
        }

        @Override public MessageConsumer<T> exceptionHandler(Handler<Throwable> handler)
        {
            mqtt.exceptionHandler(handler);
            return this;
        }

        @Override public MessageConsumer<T> pause()
        {
            buffer = new ArrayBlockingQueue<>(bufferSize);
            return this;
        }

        @Override public MessageConsumer<T> resume()
        {
            while (!buffer.isEmpty())
                handleMessage(buffer.remove());
            buffer = null;
            return this;
        }

        @Override public MessageConsumer<T> endHandler(Handler<Void> endHandler)
        {
            this.endHandlers.add(endHandler);
            return this;
        }

        @Override public ReadStream<T> bodyStream()
        {
            return new BodyReadStream<>(this);
        }

        @Override public boolean isRegistered()
        {
            return registered;
        }

        @Override public String address()
        {
            return topicName;
        }

        @Override public MessageConsumer<T> setMaxBufferedMessages(int maxBufferedMessages)
        {
            this.bufferSize = maxBufferedMessages;
            return this;
        }

        @Override public int getMaxBufferedMessages()
        {
            return bufferSize;
        }

        @Override public void completionHandler(Handler<AsyncResult<Void>> completionHandler)
        {
            subscribeHandlers.add(completionHandler);
        }

        @Override public void unregister()
        {
            mqtt.unsubscribe(topicName);
        }

        @Override public void unregister(Handler<AsyncResult<Void>> completionHandler)
        {
            unsubscribeHandler.set(completionHandler);
            mqtt.unsubscribe(topicName, unsubscribeSendResult -> {
                if (unsubscribeSendResult.succeeded())
                    unsubscribeId = unsubscribeSendResult.result();
                else
                    unsubscribeHandler.handle(failedFuture(unsubscribeSendResult.cause()));
            });
        }

        void register()
        {
            mqtt.subscribe(topicName, qos.ordinal(), subscribeSendResult -> {
                if (subscribeSendResult.succeeded())
                    subscribeId = subscribeSendResult.result();
                else
                    onSubscribe(failedFuture(subscribeSendResult.cause()));
            });
        }

        void onMessage(MqttPublishMessage message)
        {
            if (buffer == null)
                handleMessage(message);
            else
                buffer.add(message);
        }

        void handleMessage(MqttPublishMessage message)
        {
            messageHandlers.handle(new MqttMessageEvent<>(message, codec));
        }

        void onSubscribe(AsyncResult<Void> result)
        {
            subscribeHandlers.handle(result);
        }

        void onUnsubscribe()
        {
            unsubscribeHandler.handle(succeededFuture());
            onEnd();
        }

        void onEnd()
        {
            endHandlers.handle(null);
        }
    }
}
