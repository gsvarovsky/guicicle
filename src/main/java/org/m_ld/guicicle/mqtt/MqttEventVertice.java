/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.*;
import io.vertx.core.eventbus.impl.BodyReadStream;
import io.vertx.core.eventbus.impl.CodecManager;
import io.vertx.core.streams.ReadStream;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.MqttClientOptions;
import io.vertx.mqtt.MqttException;
import io.vertx.mqtt.messages.MqttPublishMessage;
import io.vertx.mqtt.messages.MqttSubAckMessage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.m_ld.guicicle.Handlers;
import org.m_ld.guicicle.Vertice;
import org.m_ld.guicicle.channel.AbstractChannel;
import org.m_ld.guicicle.channel.Channel;
import org.m_ld.guicicle.channel.ChannelOptions;
import org.m_ld.guicicle.channel.ChannelOptions.Delivery;
import org.m_ld.guicicle.channel.ChannelProvider;
import org.m_ld.guicicle.mqtt.MqttDirectAddress.MqttReplyAddress;
import org.m_ld.guicicle.mqtt.MqttDirectAddress.MqttSendAddress;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static io.vertx.core.buffer.Buffer.buffer;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.m_ld.guicicle.Handlers.Flag.SINGLE_INSTANCE;
import static org.m_ld.guicicle.Handlers.Flag.SINGLE_USE;
import static org.m_ld.guicicle.mqtt.MqttConsumer.subscription;
import static org.m_ld.guicicle.mqtt.MqttDirectAddress.MqttReplyAddress.REPLY_ADDRESS;
import static org.m_ld.guicicle.mqtt.MqttDirectAddress.MqttSendAddress.SEND_ADDRESS;
import static org.m_ld.guicicle.mqtt.MqttTopicAddress.pattern;
import static org.m_ld.guicicle.mqtt.VertxMqttModule.generateRandomId;

/**
 * TODO: Timeout
 */
public class MqttEventVertice implements ChannelProvider, Vertice
{
    private static final int MQTTASYNC_BAD_QOS = -9, NO_PACKET = -1;
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
    private final CodecManager codecManager;
    private final List<MqttConsumer> consumers = new ArrayList<>();
    private final List<MqttProducer> producers = new ArrayList<>();
    // Mqtt exception handling is global. Best we can do is provide for multiple handlers.
    private final Handlers<Throwable> exceptionHandlers = new Handlers<>();
    private MqttPresence presence = null;

    @Inject public MqttEventVertice(MqttClient mqtt, CodecManager codecManager)
    {
        this.mqtt = mqtt;
        this.codecManager = codecManager;
    }

    @Inject(optional = true) @Named("config.mqtt.bufferSize") public void setBufferSize(int bufferSize)
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

    @Inject(optional = true) @Named("config.mqtt.hasPresence") public void setHasPresence(boolean hasPresence)
    {
        if (hasPresence && presence == null)
            consumers.add(presence = new MqttPresence(mqtt));
        else if (!hasPresence && presence != null)
            throw new IllegalStateException("Cannot remove presence once requested");
    }

    @Override public void start(Future<Void> startFuture)
    {
        mqtt.publishHandler(msg -> consumers.forEach(c -> c.onMessage(msg)))
            .subscribeCompletionHandler(msg -> consumers.forEach(c -> c.onSubscribeAck(msg)))
            .unsubscribeCompletionHandler(msg -> consumers.forEach(c -> c.onUnsubscribeAck(msg)))
            .publishCompletionHandler(msg -> producers.forEach(p -> p.onPublished(msg)))
            .exceptionHandler(exceptionHandlers)
            .connect(port, host, connectResult -> {
                if (connectResult.succeeded())
                {
                    connected = true;
                    // Subscribe any pre-existing consumers
                    subscribe(consumers);
                    // Notify any pre-existing producers
                    producers.forEach(MqttProducer::onConnected);
                }
                startFuture.handle(connectResult.mapEmpty());
            });
    }

    @Override public void stop(Future<Void> stopFuture)
    {
        mqtt.disconnect(result -> {
            new ArrayList<>(consumers).forEach(MqttConsumer::close);
            new ArrayList<>(producers).forEach(MqttProducer::close);
            stopFuture.handle(result.mapEmpty());
        });
    }

    @Override public <T> Channel<T> channel(String address, ChannelOptions options)
    {
        if (options.getDelivery() == Delivery.SEND)
            checkCanSend();

        return new MqttChannel<>(address, options);
    }

    private void subscribe(List<MqttConsumer> consumers)
    {
        final Map<String, Integer> topics = new LinkedHashMap<>();
        final Map<MqttConsumer, Integer> consumerQosIndex = new HashMap<>();
        consumers.forEach(mqttConsumer -> {
            consumerQosIndex.put(mqttConsumer, topics.size());
            mqttConsumer.subscriptions().forEach(sub -> topics.put(sub.topic(), sub.qos().ordinal()));
        });
        mqtt.subscribe(topics, sent -> consumerQosIndex.forEach(
            (mqttConsumer, qosIndex) -> mqttConsumer.onSubscribeSent(sent, qosIndex)));
    }

    private void checkCanSend()
    {
        if (presence == null)
            throw new UnsupportedOperationException("Send with MQTT requires presence");
    }

    private MultiMap messageHeaders(MqttPublishMessage message, MultiMap fixedHeaders)
    {
        final MultiMap headers = MultiMap.caseInsensitiveMultiMap();
        headers.addAll(fixedHeaders);
        headers.add("mqtt.isDup", Boolean.toString(message.isDup()));
        headers.add("mqtt.isRetain", Boolean.toString(message.isRetain()));
        headers.add("mqtt.qosLevel", message.qosLevel().name());
        headers.add("mqtt.messageId", Integer.toString(message.messageId()));
        return headers;
    }

    private class WriteInFlight<T>
    {
        final T message;
        // Only send messages have a message Id
        final String messageId;
        // Only sent messages have a reply handler
        final Handler<AsyncResult<Message>> replyHandler;

        WriteInFlight(T message, String messageId,
                      @Nullable Handler<? extends AsyncResult<? extends Message>> replyHandler)
        {
            this.message = message;
            this.messageId = messageId;
            //noinspection unchecked
            this.replyHandler = (Handler<AsyncResult<Message>>)replyHandler;
        }

        @Override public String toString()
        {
            return format("%s %s", messageId != null ? "Send" : "Publish", message);
        }
    }

    private class MqttChannel<T> extends AbstractChannel<T>
    {
        final String channelId = generateRandomId();
        final MqttTopicAddress topicAddress;
        final MqttChannelReplier replier;
        final Handlers<AsyncResult<Object>> producedHandlers = new Handlers<>();
        ChannelOptions options;
        MqttQoS qos;

        MqttChannel(String address, ChannelOptions options)
        {
            this.topicAddress = pattern(requireNonNull(address));
            setOptions(options);
            replier = new MqttChannelReplier();
            consumers.add(replier);
            producers.add(replier);
            replier.addEndHandler(v -> {
                consumers.remove(replier);
                producers.remove(replier);
            });
        }

        void setOptions(DeliveryOptions options)
        {
            if (options instanceof ChannelOptions)
                this.options = new ChannelOptions((ChannelOptions)options);
            else
                this.options.setDeliveryOptions(options);
            qos = MQTT_QOS.get(this.options.getQuality());
        }

        MessageCodec getCodec(Object message, @Nullable DeliveryOptions options)
        {
            return codecManager.lookupCodec(message, (options == null ? this.options : options).getCodecName());
        }

        @Override public <E> Channel<E> channel(String address, ChannelOptions options)
        {
            return new MqttChannel<>(this.topicAddress + "/" + address, options);
        }

        @Override public Channel<T> producedHandler(Handler<AsyncResult<Object>> producedHandler)
        {
            producedHandlers.add(producedHandler);
            return this;
        }

        @Override public ChannelOptions options()
        {
            return options;
        }

        @Override protected @NotNull MessageConsumer<T> createConsumer()
        {
            final MqttChannelConsumer consumer = new MqttChannelConsumer();
            consumers.add(consumer);
            consumer.addEndHandler(v -> consumers.remove(consumer));
            return consumer;
        }

        @Override protected @NotNull MessageProducer<T> createProducer()
        {
            final MqttChannelProducer producer = new MqttChannelProducer();
            producers.add(producer);
            producer.addEndHandler(v -> producers.remove(producer));
            return producer;
        }

        class MqttChannelStream
        {
            Handler<Throwable> exceptionHandler;
            final Handlers<Void> endHandlers = new Handlers<>(SINGLE_USE);

            public String address()
            {
                return topicAddress.toString();
            }

            void setExceptionHandler(Handler<Throwable> handler)
            {
                if (exceptionHandler != null)
                    exceptionHandlers.remove(exceptionHandler);
                exceptionHandlers.add(exceptionHandler = handler);
            }

            void addEndHandler(Handler<Void> endHandler)
            {
                this.endHandlers.add(endHandler);
            }

            public void close()
            {
                exceptionHandlers.remove(exceptionHandler);
                endHandlers.handle(null);
            }
        }

        abstract class MqttChannelWriter<M> extends MqttChannelStream implements MqttProducer
        {
            final Map<Object, WriteInFlight<M>> writesInFlight = new LinkedHashMap<>(bufferSize);
            final MqttSendAddress sendAddress = SEND_ADDRESS.fromId(channelId).topic(address());
            final Set<String> recentlySentTo = new HashSet<>();

            void publish(WriteInFlight<M> wif, @Nullable DeliveryOptions options)
            {
                final String address = wif.messageId == null ? address() : nextSendAddress(wif);
                if (address != null)
                {
                    final Buffer buffer = buffer();
                    //noinspection unchecked
                    getCodec(wif.message, options).encodeToWire(buffer, wif.message);
                    if (qos == MqttQoS.AT_MOST_ONCE)
                    {
                        mqtt.publish(address, buffer, qos, false, false);
                        onPublished(wif);
                    }
                    else
                    {
                        mqtt.publish(address, buffer, qos, false, false, published -> {
                            if (published.succeeded())
                                writesInFlight.put(published.result(), wif);
                            else
                                producedHandlers.handle(published.mapEmpty());
                        });
                    }
                }
                else if (qos != MqttQoS.AT_MOST_ONCE)
                {
                    producedHandlers.handle(failedFuture(format("No suitable address found for %s", wif)));
                }
            }

            String nextSendAddress(WriteInFlight<M> wif)
            {
                checkCanSend();

                final Set<String> present = presence.present(address());
                if (recentlySentTo.containsAll(present))
                    recentlySentTo.clear();

                final Optional<MqttSendAddress> sendAddress =
                    Sets.difference(present, recentlySentTo).stream().findAny().map(
                        toId -> this.sendAddress.toId(toId).messageId(wif.messageId));

                if (!sendAddress.isPresent() && wif.replyHandler != null)
                    wif.replyHandler.handle(failedFuture(format("No-one present on %s to send message to", address())));

                return sendAddress.map(MqttTopicAddress::toString).orElse(null);
            }

            @Override public void onPublished(Integer id)
            {
                final WriteInFlight<M> wif = writesInFlight.remove(id);
                if (wif != null)
                    onPublished(wif);
            }

            void onPublished(WriteInFlight<M> wif)
            {
                producedHandlers.handle(succeededFuture(wif.message));
                if (wif.replyHandler != null)
                    replier.repliesInFlight.put(wif.messageId, wif.replyHandler);
            }
        }

        class MqttChannelReplier extends MqttChannelWriter<Object> implements MqttConsumer
        {
            final MqttReplyAddress consumeReplyAddress = REPLY_ADDRESS.toId(channelId);
            final Map<String, Handler<AsyncResult<Message>>> repliesInFlight = new HashMap<>();

            @Override public List<Subscription> subscriptions()
            {
                return singletonList(subscription(consumeReplyAddress.toString(), qos));
            }

            @Override public void onMessage(MqttPublishMessage message)
            {
                consumeReplyAddress.match(message.topicName()).ifPresent(address -> {
                    // Received a reply to this channel, find a reply handler.
                    final Handler<AsyncResult<Message>> replyHandler = repliesInFlight.remove(address.messageId());
                    if (replyHandler != null)
                    {
                        final MultiMap headers = messageHeaders(message, options.getHeaders());
                        // TODO: This will only work for replies using the named channel codec
                        final Object payload = getCodec(null, null).decodeFromWire(0, message.payload());
                        replyHandler.handle(succeededFuture(new MqttEventMessage<>(
                            address(), headers, payload, replier.create(address))));
                    }
                });
            }

            @Override public void onSubscribeSent(AsyncResult<Integer> subscribeSendResult, int qosIndex)
            {
                if (subscribeSendResult.failed())
                    exceptionHandlers.handle(subscribeSendResult.cause());
            }

            MqttEventMessage.Replier create(MqttDirectAddress address)
            {
                return new MqttEventMessage.Replier()
                {
                    final MqttReplyAddress replyAddress = REPLY_ADDRESS
                        .fromId(channelId)
                        .toId(address.fromId())
                        .messageId(address.messageId());

                    @Override public String address()
                    {
                        return replyAddress.toString();
                    }

                    @Override public <R> void reply(Object message, @Nullable DeliveryOptions options,
                                                    @Nullable Handler<AsyncResult<Message<R>>> replyHandler)
                    {
                        publish(new WriteInFlight<>(message, replyAddress.messageId(), replyHandler), options);
                    }
                };
            }
        }

        class MqttChannelProducer extends MqttChannelWriter<T> implements MessageProducer<T>
        {
            int writeQueueMaxSize = bufferSize;
            final Handlers<Void> drainHandlers = new Handlers<>(SINGLE_USE);

            @Override public MqttChannelProducer send(T message)
            {
                return write(message, true, null);
            }

            @Override public <R> MqttChannelProducer send(T message, Handler<AsyncResult<Message<R>>> replyHandler)
            {
                return write(message, true, replyHandler);
            }

            @Override public MqttChannelProducer exceptionHandler(Handler<Throwable> handler)
            {
                setExceptionHandler(handler);
                return this;
            }

            @Override public MqttChannelProducer write(T message)
            {
                return write(message, options.getDelivery() == Delivery.SEND, null);
            }

            @Override public MqttChannelProducer setWriteQueueMaxSize(int maxSize)
            {
                writeQueueMaxSize = maxSize;
                return this;
            }

            @Override public boolean writeQueueFull()
            {
                return writesInFlight.size() >= writeQueueMaxSize;
            }

            @Override public MqttChannelProducer drainHandler(Handler<Void> handler)
            {
                drainHandlers.add(handler);
                return this;
            }

            @Override public MqttChannelProducer deliveryOptions(DeliveryOptions options)
            {
                setOptions(options);
                return this;
            }

            @Override public void end()
            {
                close();
            }

            <R> MqttChannelProducer write(T message, boolean send, Handler<AsyncResult<Message<R>>> replyHandler)
            {
                write(new WriteInFlight<>(message, send ? generateRandomId() : null, replyHandler));
                return this;
            }

            void write(WriteInFlight<T> wif)
            {
                if (connected)
                    publish(wif, options);
                else
                    // Relying on the guaranteed ordering of the LinkedHashMap
                    writesInFlight.put(new Object(), wif);
            }

            @Override void onPublished(WriteInFlight<T> wif)
            {
                super.onPublished(wif);
                if (writesInFlight.size() < writeQueueMaxSize / 2)
                    drainHandlers.handle(null);
            }

            @Override public void onConnected()
            {
                // Drain down the queue
                final ArrayList<WriteInFlight> messages = new ArrayList<>(writesInFlight.values());
                writesInFlight.clear();
                messages.forEach(this::write);
            }
        }

        class MqttChannelConsumer extends MqttChannelStream implements MqttConsumer, MessageConsumer<T>
        {
            final MqttSendAddress sendAddress = SEND_ADDRESS.toId(channelId).topic(address());
            final Handlers<Message<T>> messageHandlers = new Handlers<>();
            final Handlers<AsyncResult<Void>> subscribeHandlers = new Handlers<>(SINGLE_USE);
            final Handlers<AsyncResult<Void>> unsubscribeHandler = new Handlers<>(SINGLE_INSTANCE, SINGLE_USE);
            int subscribeId = NO_PACKET, unsubscribeId = NO_PACKET, qosIndex = NO_PACKET;
            int maxBufferedMessages = bufferSize;
            boolean subscribed = false;
            Queue<MqttEventMessage<T>> pauseBuffer;

            MqttChannelConsumer()
            {
                subscribeHandlers.add(result -> {
                    if (result.succeeded())
                        subscribed = true;
                });
            }

            @Override public MqttChannelConsumer handler(Handler<Message<T>> handler)
            {
                this.messageHandlers.add(handler);
                // Subscribe to the given address if we're connected
                if (connected)
                    subscribe(singletonList(this));
                return this;
            }

            @Override public MqttChannelConsumer exceptionHandler(Handler<Throwable> handler)
            {
                setExceptionHandler(handler);
                return this;
            }

            @Override public MqttChannelConsumer pause()
            {
                pauseBuffer = new ArrayBlockingQueue<>(maxBufferedMessages);
                return this;
            }

            @Override public MqttChannelConsumer resume()
            {
                while (!pauseBuffer.isEmpty())
                    messageHandlers.handle(pauseBuffer.remove());
                pauseBuffer = null;
                return this;
            }

            @Override public MqttChannelConsumer fetch(long amount)
            {
                // TODO: Handle back-pressure with our buffer, not just on pause
                return this;
            }

            @Override public MqttChannelConsumer endHandler(Handler<Void> endHandler)
            {
                addEndHandler(endHandler);
                return this;
            }

            @Override public ReadStream<T> bodyStream()
            {
                return new BodyReadStream<>(this);
            }

            @Override public boolean isRegistered()
            {
                return subscribed;
            }

            @Override public MqttChannelConsumer setMaxBufferedMessages(int maxBufferedMessages)
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
                mqtt.unsubscribe(address());
            }

            @Override public void unregister(Handler<AsyncResult<Void>> completionHandler)
            {
                unsubscribeHandler.set(completionHandler);
                mqtt.unsubscribe(address(), unsubscribeSendResult -> {
                    if (unsubscribeSendResult.succeeded())
                        unsubscribeId = unsubscribeSendResult.result();
                    else
                        unsubscribeHandler.handle(failedFuture(unsubscribeSendResult.cause()));
                });
            }

            @Override public List<Subscription> subscriptions()
            {
                if (!messageHandlers.isEmpty())
                {
                    if (presence == null)
                        return singletonList(subscription(address(), qos));
                    else
                        return asList(subscription(address(), qos), subscription(sendAddress.toString(), qos));
                }
                else
                {
                    return emptyList();
                }
            }

            @Override public void onSubscribeSent(AsyncResult<Integer> subscribeSendResult, int qosIndex)
            {
                if (subscribeSendResult.succeeded())
                {
                    this.qosIndex = qosIndex;
                    this.subscribeId = subscribeSendResult.result();
                }
                else
                {
                    subscribeHandlers.handle(failedFuture(subscribeSendResult.cause()));
                }
            }

            @Override public void onMessage(MqttPublishMessage message)
            {
                Optional<MqttSendAddress> sent = Optional.empty();
                if (topicAddress.match(message.topicName()).isPresent() ||
                    (sent = sendAddress.match(message.topicName())).isPresent())
                {
                    final MultiMap headers = messageHeaders(message, options.getHeaders());
                    //noinspection unchecked TODO: will not work for system codecs
                    final T payload = (T)getCodec(null, options).decodeFromWire(0, message.payload());
                    final MqttEventMessage<T> eventMessage = sent
                        .map(sentAddress -> new MqttEventMessage<>(
                            address(), headers, payload, replier.create(sentAddress)))
                        .orElseGet(() -> new MqttEventMessage<>(address(), headers, payload));

                    if (pauseBuffer == null)
                        messageHandlers.handle(eventMessage);
                    else
                        pauseBuffer.add(eventMessage);
                }
            }

            @Override public void onSubscribeAck(MqttSubAckMessage subAckMessage)
            {
                if (subscribeId == subAckMessage.messageId())
                {
                    try
                    {
                        final MqttQoS qosReceived =
                            MqttQoS.valueOf(subAckMessage.grantedQoSLevels().get(qosIndex));
                        if (qosReceived != qos)
                            throw new IllegalStateException(
                                format("Mismatched QoS: expecting %s, received %s", qos, qosReceived));
                        if (presence != null) // Announce our presence
                            presence.join(channelId, address());

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

            @Override public void onUnsubscribeAck(int messageId)
            {
                if (unsubscribeId == messageId)
                {
                    unsubscribeHandler.handle(succeededFuture());
                    close();
                }
            }

            @Override public void close()
            {
                super.close();
                if (presence != null)
                    presence.leave(channelId);
            }
        }
    }
}
