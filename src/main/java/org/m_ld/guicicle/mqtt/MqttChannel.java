/*
 * Copyright (c) George Svarovsky 2020. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import com.google.common.collect.Sets;
import io.vertx.core.*;
import io.vertx.core.eventbus.*;
import io.vertx.core.eventbus.impl.BodyReadStream;
import io.vertx.core.streams.ReadStream;
import io.vertx.mqtt.messages.MqttPublishMessage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.m_ld.guicicle.Handlers;
import org.m_ld.guicicle.VertxCloseable;
import org.m_ld.guicicle.channel.AbstractChannel;
import org.m_ld.guicicle.channel.Channel;
import org.m_ld.guicicle.channel.ChannelOptions;
import org.m_ld.guicicle.mqtt.MqttDirectAddress.MqttReplyAddress;
import org.m_ld.guicicle.mqtt.MqttDirectAddress.MqttSendAddress;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static io.vertx.core.Promise.promise;
import static io.vertx.core.eventbus.ReplyFailure.NO_HANDLERS;
import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static org.m_ld.guicicle.Handlers.Flag.SINGLE_USE;
import static org.m_ld.guicicle.Vertice.when;
import static org.m_ld.guicicle.channel.ChannelOptions.Quality.AT_MOST_ONCE;
import static org.m_ld.guicicle.mqtt.MqttDirectAddress.MqttReplyAddress.REPLY_ADDRESS;
import static org.m_ld.guicicle.mqtt.MqttDirectAddress.MqttSendAddress.SEND_ADDRESS;
import static org.m_ld.guicicle.mqtt.MqttTopicAddress.pattern;

class MqttChannel<T> extends AbstractChannel<T>
{
    private static final Logger LOG = Logger.getLogger(MqttChannel.class.getName());
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

    private void produced(AsyncResult<Object> event, MqttEventInFlight<?> wif)
    {
        producedHandlers.handle(event);
        if (wif.producedHandler != null)
            wif.producedHandler.handle(event.mapEmpty());
    }

    private void handleIfFailed(AsyncResult<?> result)
    {
        if (result.failed())
            mqttEventClient.exceptionHandlers().handle(result.cause());
    }

    class MqttChannelStream implements VertxCloseable
    {
        Handler<Throwable> exceptionHandler;
        final Handlers<AsyncResult<Void>> endHandlers = new Handlers<>(SINGLE_USE);
        final Promise<Void> canClose = promise();

        public String address()
        {
            return topicAddress.toString();
        }

        void setExceptionHandler(Handler<Throwable> handler)
        {
            mqttEventClient.exceptionHandlers().remove(exceptionHandler);
            mqttEventClient.exceptionHandlers().add(exceptionHandler = handler);
        }

        public void addCloseHandler(Handler<AsyncResult<Void>> endHandler)
        {
            this.endHandlers.add(endHandler);
        }

        public void close()
        {
            // TODO: Timeout on canClose
            canClose.future().setHandler(result -> {
                endHandlers.handle(result);
                mqttEventClient.exceptionHandlers().remove(exceptionHandler);
            });
        }
    }

    abstract class MqttChannelWriter<M> extends MqttChannelStream implements MqttProducer
    {
        /**
         * This member is used for both enqueuing messages prior to connecting to the broker (with Object keys) and
         * messages awaiting a PUBACK (with Integer keys). It is not used for messages awaiting a reply, see {@link
         * MqttChannelReplier#repliesInFlight}.
         */
        final Map<Object, MqttEventInFlight<M>> writesInFlight = new LinkedHashMap<>(options().getBufferSize());
        final MqttSendAddress sendAddress = SEND_ADDRESS.fromId(channelId()).topic(address());
        final Set<String> recentlySentTo = new HashSet<>();

        void write(MqttEventInFlight<M> wif, @Nullable DeliveryOptions options)
        {
            final ChannelOptions cOptions = options(options);
            final Future<String> address = wif.isSend() ?
                nextSendAddress(wif, cOptions) : succeededFuture(address());
            address.setHandler(addressResult -> {
                if (addressResult.succeeded())
                    write(addressResult.result(), wif, options);
                else if (cOptions.getQuality() != AT_MOST_ONCE)
                    produced(addressResult.mapEmpty(), wif);
            });
        }

        void write(String address, MqttEventInFlight<M> wif, @Nullable DeliveryOptions options)
        {
            LOG.fine(format("<%s> Producing message on %s: %s", mqttEventClient.clientId(), address, wif.message));
            mqttEventClient
                .publish(address, wif.message, options(options))
                .setHandler(published -> {
                    if (published.succeeded())
                        if (published.result() == null) // QoS = 0
                            onProduced(wif);
                        else
                            writesInFlight.put(published.result(), wif);
                    else
                        produced(published.mapEmpty(), wif);
                });
        }

        Future<String> nextSendAddress(MqttEventInFlight<M> wif, DeliveryOptions options)
        {
            return nextSendAddress(wif).map(Future::succeededFuture).orElseGet(() -> {
                // No-one present. Keep re-checking on every client tick, up to the timeout, then give up.
                final Promise<String> promiseAddress = promise();
                mqttEventClient.tickHandlers().add(new Handler<Long>()
                {
                    @Override public void handle(Long time)
                    {
                        if (time - wif.time() > options.getSendTimeout())
                        {
                            promiseAddress.fail(format("No suitable address found for %s", wif));
                            if (wif.replyHandler != null)
                                wif.replyHandler.handle(failedFuture(new ReplyException(NO_HANDLERS, format(
                                    "No-one present on %s to send message to", MqttChannelWriter.this.address()))));
                            stopRetrying();
                        }
                        else
                        {
                            nextSendAddress(wif).ifPresent(address -> {
                                promiseAddress.complete(address);
                                stopRetrying();
                            });
                        }
                    }

                    void stopRetrying()
                    {
                        mqttEventClient.tickHandlers().remove(this);
                    }
                });
                return promiseAddress.future();
            });
        }

        private Optional<String> nextSendAddress(MqttEventInFlight<M> wif)
        {
            final Set<String> present = mqttEventClient.presence()
                .orElseThrow(UnsupportedOperationException::new).present(address());
            if (recentlySentTo.containsAll(present))
                recentlySentTo.clear();

            if (!options().isEcho())
                recentlySentTo.add(channelId());

            return Sets.difference(present, recentlySentTo).stream().findAny().map(toId -> {
                recentlySentTo.add(toId); // Side-effect of success
                return sendAddress.toId(toId).messageId(wif.messageId).toString();
            });
        }

        @Override public void onPublished(Integer id)
        {
            final MqttEventInFlight<M> wif = writesInFlight.remove(id);
            if (wif != null)
                onProduced(wif);
        }

        void onProduced(MqttEventInFlight<M> wif)
        {
            produced(succeededFuture(wif.message), wif);
            if (wif.replyHandler != null)
                replier.repliesInFlight.put(wif.messageId, wif.replyHandler);
        }
    }

    class MqttChannelReplier extends MqttChannelWriter<Object> implements MqttConsumer
    {
        final Subscription[] subscriptions = new Subscription[]{
            new Subscription(REPLY_ADDRESS.toId(channelId()), options().getQuality())
        };
        final Map<String, Handler<AsyncResult<Message>>> repliesInFlight = new HashMap<>();

        @Override public Subscription[] subscriptions()
        {
            return subscriptions;
        }

        @Override public void onMessage(MqttPublishMessage message, Subscription subscription)
        {
            // Received a reply to this channel, find a reply handler.
            final MqttReplyAddress repliedAddress = new MqttReplyAddress(message.topicName());
            final Handler<AsyncResult<Message>> replyHandler = repliesInFlight.remove(repliedAddress.sentMessageId());
            if (replyHandler != null)
            {
                final MultiMap headers = MultiMap.caseInsensitiveMultiMap();
                final Object body = mqttEventClient.decodeFromWire(message, headers, options().getCodecName());
                replyHandler.handle(succeededFuture(new MqttEventMessage<>(
                    address(), headers, body, replier.create(repliedAddress))));
            }
        }

        @Override public void onSubscribe(AsyncResult<Void> subscribeResult)
        {
            handleIfFailed(subscribeResult);
        }

        @Override public void onUnsubscribe(AsyncResult<Void> unsubscribeResult)
        {
            canClose.handle(unsubscribeResult);
        }

        @Override public void close()
        {
            mqttEventClient.unsubscribe(this);
            super.close();
        }

        MqttEventMessage.Replier create(MqttDirectAddress sentAddress)
        {
            return new MqttEventMessage.Replier()
            {
                // Note this address is not complete: the messageId is added when replying
                final MqttReplyAddress replyAddress = REPLY_ADDRESS
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

        public MqttChannelProducer()
        {
            // We have no close requirements
            canClose.complete();
        }

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

        @Override public MessageProducer<T> write(T message, Handler<AsyncResult<Void>> handler)
        {
            write(new MqttEventInFlight<>(message, options().getDelivery(), handler));
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

        @Override public void end(Handler<AsyncResult<Void>> handler)
        {
            close(handler);
        }

        @Override public void close(Handler<AsyncResult<Void>> handler)
        {
            endHandlers.add(handler);
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
        final Subscription
            publishSub = new Subscription(topicAddress, options().getQuality()),
            sendSub = new Subscription(SEND_ADDRESS.toId(channelId()).topic(address()), options().getQuality());
        final Subscription[] subscriptions = mqttEventClient.presence().isPresent() ?
            new Subscription[]{publishSub, sendSub} : new Subscription[]{publishSub};
        final Handlers<Message<T>> messageHandlers = new Handlers<>();
        final Handlers<AsyncResult<Void>>
            subscribeHandlers = new Handlers<>(SINGLE_USE),
            unsubscribeHandlers = new Handlers<>(SINGLE_USE);
        int maxBufferedMessages = options().getBufferSize();
        Queue<MqttEventMessage<T>> pauseBuffer;

        MqttChannelConsumer()
        {
            unsubscribeHandlers.add(canClose);
        }

        @Override public MqttChannelConsumer handler(Handler<Message<T>> handler)
        {
            messageHandlers.add(handler);
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
            addCloseHandler(result -> {
                if (result.succeeded())
                    endHandler.handle(null);
            });
            return this;
        }

        @Override public ReadStream<T> bodyStream()
        {
            return new BodyReadStream<>(this);
        }

        @Override public boolean isRegistered()
        {
            return !messageHandlers.isEmpty() && stream(subscriptions).allMatch(
                s -> s.subscribed != null && s.subscribed.succeeded());
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
            unsubscribeHandlers.add(completionHandler);
            mqttEventClient.unsubscribe(this);
        }

        @Override public Subscription[] subscriptions()
        {
            // Subscription is lazy per MessageConsumer contract, see handler()
            return !messageHandlers.isEmpty() ? subscriptions : new Subscription[0];
        }

        @Override public void onMessage(MqttPublishMessage message, Subscription subscription)
        {
            assert subscription == sendSub || subscription == publishSub;

            final MultiMap headers = MultiMap.caseInsensitiveMultiMap();
            //noinspection unchecked
            final T payload = (T)mqttEventClient.decodeFromWire(message, headers, options().getCodecName());
            LOG.fine(
                format("<%s> Consuming message on %s: %s", mqttEventClient.clientId(), subscription.address, payload));
            final MqttEventMessage<T> eventMessage;
            if (subscription == sendSub)
                eventMessage = new MqttEventMessage<>(
                    address(), headers, payload, replier.create(new MqttSendAddress(message.topicName())));
            else
                eventMessage = new MqttEventMessage<>(address(), headers, payload);

            if (pauseBuffer == null)
                handleMessage(eventMessage);
            else
                pauseBuffer.add(eventMessage);
        }

        void handleMessage(MqttEventMessage<T> eventMessage)
        {
            // Strictly, only published messages need to be filtered.
            // Sends are filtered in the producer (see MqttChannelWriter#nextSendAddress)
            // Note that isEcho will always return false if the original message options allowed echoes
            if (!isEcho(eventMessage))
                messageHandlers.handle(eventMessage);
        }

        @Override public void onSubscribe(AsyncResult<Void> subscribeResult)
        {
            final Optional<MqttPresence> presence = mqttEventClient.presence();
            if (subscribeResult.succeeded() && presence.isPresent()) // Announce our presence
                presence.get().join(channelId(), address(), subscribeHandlers);
            else
                subscribeHandlers.handle(subscribeResult);
        }

        @Override public void onUnsubscribe(AsyncResult<Void> unsubscribeResult)
        {
            unsubscribeHandlers.handle(unsubscribeResult);
        }

        @Override public void close()
        {
            mqttEventClient.presence()
                .map(presence -> when(presence::leave, channelId()))
                .orElse(succeededFuture()).setHandler(v -> {
                unregister();
                super.close();
            });
        }
    }
}
