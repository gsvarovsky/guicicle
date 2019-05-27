/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import com.google.common.collect.Sets;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.eventbus.impl.BodyReadStream;
import io.vertx.core.streams.ReadStream;
import io.vertx.mqtt.MqttException;
import io.vertx.mqtt.messages.MqttPublishMessage;
import io.vertx.mqtt.messages.MqttSubAckMessage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.m_ld.guicicle.Handlers;
import org.m_ld.guicicle.VertxCloseable;
import org.m_ld.guicicle.channel.AbstractChannel;
import org.m_ld.guicicle.channel.Channel;
import org.m_ld.guicicle.channel.ChannelOptions;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.m_ld.guicicle.Handlers.Flag.SINGLE_INSTANCE;
import static org.m_ld.guicicle.Handlers.Flag.SINGLE_USE;
import static org.m_ld.guicicle.mqtt.MqttDirectAddress.MqttReplyAddress.REPLY_ADDRESS;
import static org.m_ld.guicicle.mqtt.MqttDirectAddress.MqttSendAddress.SEND_ADDRESS;
import static org.m_ld.guicicle.mqtt.MqttTopicAddress.pattern;

class MqttChannel<T> extends AbstractChannel<T>
{
    private final MqttEventClient mqttEventClient;
    private final MqttTopicAddress topicAddress;
    private final MqttChannelReplier replier;
    private final Handlers<AsyncResult<Object>> producedHandlers = new Handlers<>();

    MqttChannel(MqttEventClient mqttEventClient, String address, ChannelOptions options)
    {
        super(options);
        this.mqttEventClient = mqttEventClient;

        this.topicAddress = pattern(requireNonNull(address));
        this.replier = new MqttChannelReplier();

        mqttEventClient.subscribe(replier);
        mqttEventClient.addProducer(replier);
    }

    private MqttQoS qos(DeliveryOptions options)
    {
        return options instanceof ChannelOptions ?
            MqttEventVertice.MQTT_QOS.get(((ChannelOptions)options).getQuality()) : qos();
    }

    private MqttQoS qos()
    {
        return MqttEventVertice.MQTT_QOS.get(options().getQuality());
    }

    @Override public <E> Channel<E> channel(String address, ChannelOptions options)
    {
        return new MqttChannel<>(mqttEventClient, this.topicAddress + "/" + address, options);
    }

    @Override public Channel<T> producedHandler(Handler<AsyncResult<Object>> producedHandler)
    {
        producedHandlers.add(producedHandler);
        return this;
    }

    @Override protected @NotNull MessageConsumer<T> createConsumer()
    {
        final MqttChannelConsumer consumer = new MqttChannelConsumer();
        mqttEventClient.addConsumer(consumer);
        return consumer;
    }

    @Override protected @NotNull MessageProducer<T> createProducer()
    {
        final MqttChannelProducer producer = new MqttChannelProducer();
        mqttEventClient.addProducer(producer);
        return producer;
    }

    class MqttChannelStream implements VertxCloseable
    {
        Handler<Throwable> exceptionHandler;
        final Handlers<Void> endHandlers = new Handlers<>(SINGLE_USE);

        public String address()
        {
            return topicAddress.toString();
        }

        void setExceptionHandler(Handler<Throwable> handler)
        {
            mqttEventClient.exceptionHandlers().remove(exceptionHandler);
            mqttEventClient.exceptionHandlers().add(exceptionHandler = handler);
        }

        public void addCloseHandler(Handler<Void> endHandler)
        {
            this.endHandlers.add(endHandler);
        }

        public void close()
        {
            mqttEventClient.exceptionHandlers().remove(exceptionHandler);
            endHandlers.handle(null);
        }
    }

    abstract class MqttChannelWriter<M> extends MqttChannelStream implements MqttProducer
    {
        final Map<Object, MqttEventInFlight<M>> writesInFlight = new LinkedHashMap<>(options().getBufferSize());
        final MqttDirectAddress.MqttSendAddress sendAddress = SEND_ADDRESS.fromId(channelId()).topic(address());
        final Set<String> recentlySentTo = new HashSet<>();

        void write(MqttEventInFlight<M> wif, @Nullable DeliveryOptions options)
        {
            final String address = wif.isSend() ? nextSendAddress(wif) : address();
            if (address != null)
                write(address, wif, options);
            else if (qos(options) != MqttQoS.AT_MOST_ONCE)
                producedHandlers.handle(failedFuture(format("No suitable address found for %s", wif)));
        }

        void write(String address, MqttEventInFlight<M> wif, @Nullable DeliveryOptions options)
        {
            if (options == null)
                options = options();

            mqttEventClient
                .publish(address, wif.message, qos(options), options)
                .setHandler(published -> {
                    if (published.succeeded())
                        if (published.result() == null)
                            onProduced(wif);
                        else
                            writesInFlight.put(published.result(), wif);
                    else
                        producedHandlers.handle(published.mapEmpty());
                });
        }

        String nextSendAddress(MqttEventInFlight<M> wif)
        {
            final Set<String> present =
                mqttEventClient.presence().orElseThrow(UnsupportedOperationException::new).present(address());
            if (recentlySentTo.containsAll(present))
                recentlySentTo.clear();

            if (!options().isEcho())
                recentlySentTo.add(channelId());

            final Optional<MqttDirectAddress.MqttSendAddress> sendAddress =
                Sets.difference(present, recentlySentTo).stream().findAny().map(
                    toId -> this.sendAddress.toId(toId).messageId(wif.messageId));

            if (!sendAddress.isPresent())
            {
                if (wif.replyHandler != null)
                    wif.replyHandler.handle(
                        failedFuture(format("No-one present on %s to send message to", address())));
            }
            else
            {
                recentlySentTo.add(sendAddress.get().toId());
            }

            return sendAddress.map(MqttTopicAddress::toString).orElse(null);
        }

        @Override public void onProduced(Integer id)
        {
            final MqttEventInFlight<M> wif = writesInFlight.remove(id);
            if (wif != null)
                onProduced(wif);
        }

        void onProduced(MqttEventInFlight<M> wif)
        {
            producedHandlers.handle(succeededFuture(wif.message));
            if (wif.replyHandler != null)
                replier.repliesInFlight.put(wif.messageId, wif.replyHandler);
        }
    }

    class MqttChannelReplier extends MqttChannelWriter<Object> implements MqttConsumer
    {
        final MqttDirectAddress.MqttReplyAddress repliesAddress = REPLY_ADDRESS.toId(channelId());
        final Map<String, Handler<AsyncResult<Message>>> repliesInFlight = new HashMap<>();

        @Override public List<Subscription> subscriptions()
        {
            return singletonList(MqttConsumer.subscription(repliesAddress.toString(), qos()));
        }

        @Override public void onMessage(MqttPublishMessage message)
        {
            repliesAddress.match(message.topicName()).ifPresent(address -> {
                // Received a reply to this channel, find a reply handler.
                final Handler<AsyncResult<Message>> replyHandler = repliesInFlight.remove(address.sentMessageId());
                if (replyHandler != null)
                {
                    final MultiMap headers = MultiMap.caseInsensitiveMultiMap();
                    final Object body = mqttEventClient.decodeFromWire(message, headers);
                    replyHandler.handle(succeededFuture(new MqttEventMessage<>(
                        address(), headers, body, replier.create(address))));
                }
            });
        }

        @Override public void onSubscribeSent(AsyncResult<Integer> subscribeSendResult, int qosIndex)
        {
            if (subscribeSendResult.failed())
                mqttEventClient.exceptionHandlers().handle(subscribeSendResult.cause());
        }

        MqttEventMessage.Replier create(MqttDirectAddress sentAddress)
        {
            return new MqttEventMessage.Replier()
            {
                // Note this address is not complete: the messageId is added when replying
                final MqttDirectAddress.MqttReplyAddress replyAddress = REPLY_ADDRESS
                    .fromId(channelId())
                    .toId(sentAddress.fromId())
                    .sentMessageId(sentAddress.messageId());

                @Override public String address()
                {
                    return replyAddress.toString();
                }

                @Override public <R> void reply(Object message, @Nullable DeliveryOptions options,
                                                @Nullable Handler<AsyncResult<Message<R>>> replyHandler)
                {
                    final MqttEventInFlight<Object> wif = new MqttEventInFlight<>(message, replyHandler);
                    write(replyAddress.messageId(wif.messageId).toString(), wif, options);
                }
            };
        }
    }

    class MqttChannelProducer extends MqttChannelWriter<T> implements MessageProducer<T>
    {
        int writeQueueMaxSize = options().getBufferSize();
        final Handlers<Void> drainHandlers = new Handlers<>(SINGLE_USE);

        @Override public MqttChannelProducer send(T message)
        {
            write(new MqttEventInFlight<>(message, ChannelOptions.Delivery.SEND));
            return this;
        }

        @Override public <R> MqttChannelProducer send(T message, Handler<AsyncResult<Message<R>>> replyHandler)
        {
            write(new MqttEventInFlight<>(message, replyHandler));
            return this;
        }

        @Override public MqttChannelProducer exceptionHandler(Handler<Throwable> handler)
        {
            setExceptionHandler(handler);
            return this;
        }

        @Override public MqttChannelProducer write(T message)
        {
            write(new MqttEventInFlight<>(message, options().getDelivery()));
            return this;
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

        void write(MqttEventInFlight<T> wif)
        {
            if (mqttEventClient.isConnected())
                write(wif, options());
            else
                // Relying on the guaranteed ordering of the LinkedHashMap
                writesInFlight.put(new Object(), wif);
        }

        @Override void onProduced(MqttEventInFlight<T> wif)
        {
            super.onProduced(wif);
            if (writesInFlight.size() < writeQueueMaxSize / 2)
                drainHandlers.handle(null);
        }

        @Override public void onConnected()
        {
            // Drain down the queue
            final ArrayList<MqttEventInFlight> messages = new ArrayList<>(writesInFlight.values());
            writesInFlight.clear();
            messages.forEach(this::write);
        }
    }

    class MqttChannelConsumer extends MqttChannelStream implements MqttConsumer, MessageConsumer<T>
    {
        final MqttDirectAddress.MqttSendAddress sendAddress = SEND_ADDRESS.toId(channelId()).topic(address());
        final Handlers<Message<T>> messageHandlers = new Handlers<>();
        final Handlers<AsyncResult<Void>> subscribeHandlers = new Handlers<>(SINGLE_USE);
        final Handlers<AsyncResult<Void>> unsubscribeHandler = new Handlers<>(SINGLE_INSTANCE, SINGLE_USE);
        int subscribeId = MqttEventVertice.NO_PACKET,
            unsubscribeId = MqttEventVertice.NO_PACKET,
            qosIndex = MqttEventVertice.NO_PACKET,
            maxBufferedMessages = options().getBufferSize();
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
            // Subscribe to the given address
            mqttEventClient.subscribe(this);
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
            if (pauseBuffer != null)
            {
                while (!pauseBuffer.isEmpty())
                    handleMessage(pauseBuffer.remove());
                pauseBuffer = null;
            }
            return this;
        }

        @Override public MqttChannelConsumer fetch(long amount)
        {
            // TODO: Handle back-pressure with our buffer, not just on pause
            return this;
        }

        @Override public MqttChannelConsumer endHandler(Handler<Void> endHandler)
        {
            addCloseHandler(endHandler);
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
            mqttEventClient.unsubscribe(this);
        }

        @Override public void unregister(Handler<AsyncResult<Void>> completionHandler)
        {
            unsubscribeHandler.set(completionHandler);
            mqttEventClient.unsubscribe(this).setHandler(unsubResult -> {
                if (unsubResult.succeeded())
                    unsubscribeId = unsubResult.result().resultAt(0);
                else
                    unsubscribeHandler.handle(failedFuture(unsubResult.cause()));
            });
        }

        @Override public List<Subscription> subscriptions()
        {
            if (!messageHandlers.isEmpty())
            {
                if (mqttEventClient.presence().isPresent())
                    return asList(MqttConsumer.subscription(address(), qos()),
                                  MqttConsumer.subscription(sendAddress.toString(), qos()));
                else
                    return singletonList(MqttConsumer.subscription(address(), qos()));
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
            Optional<MqttDirectAddress.MqttSendAddress> sent = Optional.empty();
            if (topicAddress.match(message.topicName()).isPresent() ||
                (sent = sendAddress.match(message.topicName())).isPresent())
            {
                final MultiMap headers = MultiMap.caseInsensitiveMultiMap();
                //noinspection unchecked
                final T payload = (T)mqttEventClient.decodeFromWire(message, headers);
                final MqttEventMessage<T> eventMessage = sent
                    .map(sentAddress -> new MqttEventMessage<>(
                        address(), headers, payload, replier.create(sentAddress)))
                    .orElseGet(() -> new MqttEventMessage<>(address(), headers, payload));

                if (pauseBuffer == null)
                    handleMessage(eventMessage);
                else
                    pauseBuffer.add(eventMessage);
            }
        }

        void handleMessage(MqttEventMessage<T> eventMessage)
        {
            filterEcho(eventMessage, messageHandlers);
        }

        @Override public void onSubscribeAck(MqttSubAckMessage subAckMessage)
        {
            if (subscribeId == subAckMessage.messageId())
            {
                try
                {
                    final MqttQoS qosReceived =
                        MqttQoS.valueOf(subAckMessage.grantedQoSLevels().get(qosIndex));
                    if (qosReceived != qos())
                        throw new IllegalStateException(
                            format("Mismatched QoS: expecting %s, received %s", qos(), qosReceived));
                    final Optional<MqttPresence> presence = mqttEventClient.presence();
                    if (presence.isPresent()) // Announce our presence
                        presence.get().join(channelId(), address(), subscribeHandlers);
                    else
                        subscribeHandlers.handle(succeededFuture());
                }
                catch (Exception e)
                {
                    final MqttException exception = new MqttException(MqttEventVertice.MQTTASYNC_BAD_QOS,
                                                                      e.getMessage());
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
            mqttEventClient.presence().ifPresent(presence -> presence.leave(channelId()));
            unregister();
        }
    }
}
