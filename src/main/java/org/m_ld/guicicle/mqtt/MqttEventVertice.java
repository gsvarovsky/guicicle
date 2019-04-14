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
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.eventbus.impl.BodyReadStream;
import io.vertx.core.streams.ReadStream;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.MqttClientOptions;
import io.vertx.mqtt.MqttException;
import io.vertx.mqtt.messages.MqttPublishMessage;
import io.vertx.mqtt.messages.MqttSubAckMessage;
import org.jetbrains.annotations.NotNull;
import org.m_ld.guicicle.Handlers;
import org.m_ld.guicicle.Vertice;
import org.m_ld.guicicle.channel.*;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static io.vertx.core.buffer.Buffer.buffer;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.m_ld.guicicle.Handlers.Flag.SINGLE_INSTANCE;
import static org.m_ld.guicicle.Handlers.Flag.SINGLE_USE;
import static org.m_ld.guicicle.mqtt.MqttUtil.isMatch;

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
    private int bufferSize = 128;
    private int port = MqttClientOptions.DEFAULT_PORT;
    private String host = MqttClientOptions.DEFAULT_HOST;
    private boolean connected = false;
    private final MqttClient mqtt;
    private final Injector injector;

    private final List<MqttChannel<?>.MqttConsumer> consumers = new ArrayList<>();
    private final List<MqttChannel<?>.MqttProducer> producers = new ArrayList<>();

    @Inject public MqttEventVertice(MqttClient mqtt, Injector injector)
    {
        this.mqtt = mqtt;
        this.injector = injector;
    }

    @Inject(optional = true) @Named("config.mqtt.buffer.size") public void setBufferSize(int bufferSize)
    {
        this.bufferSize = bufferSize;
    }

    @Inject(optional = true) @Named("config.mqtt.port") public void setPort(int port)
    {
        this.port = port;
    }

    @Inject(optional = true) @Named("config.mqtt.host") public void setHost(String host)
    {
        this.host = host;
    }

    @Override public void start(Future<Void> startFuture)
    {
        mqtt.publishHandler(msg -> consumers.forEach(c -> c.onMessage(msg)))
            .subscribeCompletionHandler(msg -> consumers.forEach(c -> c.onSubscribeAck(msg)))
            .unsubscribeCompletionHandler(msg -> consumers.forEach(c -> c.onUnsubscribeAck(msg)))
            .publishCompletionHandler(msg -> producers.forEach(p -> p.onPublished(msg)))
            .connect(port, host, connectResult -> {
                if (connectResult.succeeded())
                {
                    connected = true;
                    // Register any pre-existing consumers which have a handler
                    // TODO: The protocol allows doing these in a batch
                    consumers.forEach(MqttChannel.MqttConsumer::register);
                    producers.forEach(MqttChannel.MqttProducer::register);
                }
                startFuture.handle(connectResult.mapEmpty());
            });
    }

    @Override public void stop(Future<Void> stopFuture)
    {
        mqtt.disconnect(result -> {
            new ArrayList<>(consumers).forEach(MqttChannel.MqttConsumer::onEnd);
            stopFuture.handle(result.mapEmpty());
        });
    }

    @Override public <T> Channel<T> channel(String address, ChannelOptions options)
    {
        if (options.getDelivery() == ChannelOptions.Delivery.SEND)
            throw new UnsupportedOperationException("Cannot send with MQTT, yet");

        return new MqttChannel<>(address, options);
    }

    private class MqttChannel<T> extends AbstractChannel<T>
    {
        final String topicName;
        final Handlers<AsyncResult<Object>> producedHandlers = new Handlers<>();
        DeliveryOptions options;
        MqttQoS qos;
        ChannelCodec<T> codec;

        MqttChannel(String address, ChannelOptions options)
        {
            this.topicName = requireNonNull(address);
            setDeliveryOptions(options);
        }

        void setDeliveryOptions(DeliveryOptions options)
        {
            this.options = options;
            if (options instanceof ChannelOptions)
                qos = MQTT_QOS.get(((ChannelOptions)options).getQuality());
            //noinspection unchecked
            codec = injector.getInstance(
                Key.get(new TypeLiteral<ChannelCodec>() {}, Names.named(options.getCodecName())));
        }

        @Override protected @NotNull MessageConsumer<T> createConsumer()
        {
            return new MqttConsumer();
        }

        @Override protected @NotNull MessageProducer<T> createProducer()
        {
            return new MqttProducer();
        }

        @Override public Channel<T> producedHandler(Handler<AsyncResult<Object>> producedHandler)
        {
            producedHandlers.add(producedHandler);
            return this;
        }

        class MqttProducer implements MessageProducer<T>
        {
            int writeQueueMaxSize = bufferSize;
            final Map<Object, T> writesInFlight = new LinkedHashMap<>(bufferSize);
            final Handlers<Void> drainHandlers = new Handlers<>(SINGLE_USE);
            final Handlers<Throwable> exceptionHandler = new Handlers<>(SINGLE_INSTANCE);

            MqttProducer()
            {
                producers.add(this);
            }

            @Override public MessageProducer<T> send(T message)
            {
                throw new UnsupportedOperationException("Cannot send with MQTT, yet");
            }

            @Override public <R> MessageProducer<T> send(T message,
                                                         Handler<AsyncResult<Message<R>>> replyHandler)
            {
                throw new UnsupportedOperationException("Cannot send with MQTT, yet");
            }

            @Override public MessageProducer<T> exceptionHandler(Handler<Throwable> handler)
            {
                mqtt.exceptionHandler(handler);
                exceptionHandler.set(handler);
                return this;
            }

            @Override public MessageProducer<T> write(T message)
            {
                if (connected)
                {
                    final Buffer buffer = buffer();
                    codec.encodeToWire(buffer, message);
                    if (qos == MqttQoS.AT_MOST_ONCE)
                        mqtt.publish(topicName, buffer, qos, false, false);
                    else
                        mqtt.publish(topicName, buffer, qos, false, false, published -> {
                            if (published.succeeded())
                                writesInFlight.put(published.result(), message);
                            else
                                producedHandlers.handle(published.mapEmpty());
                        });
                }
                else
                {
                    // Relying on the quaranteed ordering of the LinkedHashMap
                    writesInFlight.put(new Object(), message);
                }
                return this;
            }

            @Override public MessageProducer<T> setWriteQueueMaxSize(int maxSize)
            {
                writeQueueMaxSize = maxSize;
                return this;
            }

            @Override public boolean writeQueueFull()
            {
                return writesInFlight.size() >= writeQueueMaxSize;
            }

            @Override public MessageProducer<T> drainHandler(Handler<Void> handler)
            {
                drainHandlers.add(handler);
                return this;
            }

            @Override public MessageProducer<T> deliveryOptions(DeliveryOptions options)
            {
                setDeliveryOptions(options);
                return this;
            }

            @Override public String address()
            {
                return topicName;
            }

            @Override public void end()
            {
                close();
            }

            @Override public void close()
            {
                producers.remove(this);
            }

            void register()
            {
                // Drain down the queue
                final ArrayList<T> messages = new ArrayList<>(writesInFlight.values());
                writesInFlight.clear();
                messages.forEach(this::write);
            }

            void onPublished(Integer id)
            {
                final T message = writesInFlight.remove(id);
                if (message != null)
                {
                    producedHandlers.handle(succeededFuture(message));
                    if (writesInFlight.size() < writeQueueMaxSize/2)
                        drainHandlers.handle(null);
                }
            }
        }

        class MqttConsumer implements MessageConsumer<T>
        {
            final Handlers<Message<T>> messageHandlers = new Handlers<>();
            final Handlers<AsyncResult<Void>> subscribeHandlers = new Handlers<>(SINGLE_USE);
            final Handlers<AsyncResult<Void>> unsubscribeHandler = new Handlers<>(SINGLE_INSTANCE, SINGLE_USE);
            final Handlers<Void> endHandlers = new Handlers<>(SINGLE_USE);
            int subscribeId = NO_PACKET, unsubscribeId = NO_PACKET;
            int ordinal = 0;
            int maxBufferedMessages = bufferSize;
            boolean registered = false;
            Queue<MqttPublishMessage> pauseBuffer;

            MqttConsumer()
            {
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
                pauseBuffer = new ArrayBlockingQueue<>(maxBufferedMessages);
                return this;
            }

            @Override public MessageConsumer<T> resume()
            {
                while (!pauseBuffer.isEmpty())
                    handleMessage(pauseBuffer.remove());
                pauseBuffer = null;
                return this;
            }

            @Override public MessageConsumer<T> fetch(long amount)
            {
                // TODO: Handle back-pressure with our buffer, not just on pause
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
                this.maxBufferedMessages = maxBufferedMessages;
                return this;
            }

            @Override public int getMaxBufferedMessages()
            {
                return maxBufferedMessages;
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
                        subscribeHandlers.handle(failedFuture(subscribeSendResult.cause()));
                });
            }

            void onMessage(MqttPublishMessage message)
            {
                if (isMatch(message.topicName(), topicName))
                {
                    if (pauseBuffer == null)
                        handleMessage(message);
                    else
                        pauseBuffer.add(message);
                }
            }

            void handleMessage(MqttPublishMessage message)
            {
                messageHandlers.handle(new MqttEventMessage<>(message, options.getHeaders(), codec));
            }

            void onSubscribeAck(MqttSubAckMessage subAckMessage)
            {
                if (subscribeId == subAckMessage.messageId())
                {
                    try
                    {
                        final int qosValue = subAckMessage.grantedQoSLevels().get(ordinal);
                        final MqttQoS qosReceived = MqttQoS.valueOf(qosValue);
                        if (qosReceived != qos)
                            throw new IllegalStateException(
                                format("Mismatched QoS: expecting %s, received %s", qos, qosReceived));
                        subscribeHandlers.handle(succeededFuture());
                    }
                    catch (Exception e)
                    {
                        final MqttException exception = new MqttException(MQTTASYNC_BAD_QOS, e.getMessage());
                        exception.initCause(e);
                        subscribeHandlers.handle(failedFuture(exception));
                    }
                }
            }

            void onUnsubscribeAck(int messageId)
            {
                if (unsubscribeId == messageId)
                {
                    unsubscribeHandler.handle(succeededFuture());
                    onEnd();
                }
            }

            void onEnd()
            {
                endHandlers.handle(null);
                consumers.remove(this);
            }
        }
    }
}
