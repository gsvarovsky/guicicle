/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.eventbus.impl.CodecManager;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.MqttClientOptions;
import io.vertx.mqtt.messages.MqttConnAckMessage;
import io.vertx.mqtt.messages.MqttPublishMessage;
import io.vertx.mqtt.messages.MqttSubAckMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.m_ld.guicicle.TimerProvider;
import org.m_ld.guicicle.channel.Channel;
import org.m_ld.guicicle.channel.ChannelOptions;
import org.m_ld.guicicle.channel.UuidCodec;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static io.vertx.core.Future.succeededFuture;
import static io.vertx.core.MultiMap.caseInsensitiveMultiMap;
import static java.util.UUID.randomUUID;
import static org.junit.Assert.*;
import static org.m_ld.guicicle.channel.ChannelOptions.Quality.AT_LEAST_ONCE;
import static org.m_ld.guicicle.channel.ChannelOptions.Quality.AT_MOST_ONCE;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MqttEventVerticeTest
{
    @Mock TimerProvider timerProvider;
    @Mock MqttClient mqtt;
    @Mock CodecManager codecManager;
    @Captor ArgumentCaptor<Handler<MqttPublishMessage>> publishCaptor;
    @Captor ArgumentCaptor<Handler<MqttSubAckMessage>> subAckCaptor;
    @Captor ArgumentCaptor<Handler<Integer>> publishCompleteCaptor;
    @Captor ArgumentCaptor<Handler<Integer>> unsubAckCaptor;
    @Captor ArgumentCaptor<Buffer> bufferCaptor;
    @Captor ArgumentCaptor<Handler<AsyncResult<Integer>>> publishSentCaptor;
    @Captor ArgumentCaptor<Handler<Throwable>> exceptionCaptor;
    @Captor ArgumentCaptor<Handler<AsyncResult<Integer>>> subSentCaptor;
    @Captor ArgumentCaptor<Map<String, Integer>> subQosCaptor;
    private MqttEventCodec eventCodec;
    private final UuidCodec uuidCodec = new UuidCodec();
    private final Future<Void> startFuture = future();

    @Before public void setUp()
    {
        eventCodec = new MqttEventCodec(codecManager);
        when(mqtt.publishHandler(publishCaptor.capture())).thenReturn(mqtt);
        when(mqtt.subscribeCompletionHandler(subAckCaptor.capture())).thenReturn(mqtt);
        when(mqtt.unsubscribeCompletionHandler(unsubAckCaptor.capture())).thenReturn(mqtt);
        when(mqtt.publishCompletionHandler(publishCompleteCaptor.capture())).thenReturn(mqtt);
        when(mqtt.exceptionHandler(exceptionCaptor.capture())).thenReturn(mqtt);
        when(mqtt.connect(anyInt(), any(), any())).then(inv -> {
            //noinspection unchecked
            ((Handler<AsyncResult<MqttConnAckMessage>>)inv.getArgument(2)).handle(succeededFuture());
            return mqtt;
        });
        when(mqtt.subscribe(subQosCaptor.capture(), subSentCaptor.capture())).thenReturn(mqtt);
        when(codecManager.lookupCodec(any(), anyString())).thenReturn(uuidCodec);
        when(codecManager.getCodec(anyString())).thenReturn(uuidCodec);
    }

    @Test public void testSignalsStart()
    {
        new MqttEventVertice(mqtt, eventCodec, timerProvider).start(startFuture);
        assertTrue(startFuture.succeeded());
    }

    @Test public void testSignalsStop()
    {
        when(mqtt.disconnect(any())).then(inv -> {
            //noinspection unchecked
            ((Handler<AsyncResult<Void>>)inv.getArgument(0)).handle(succeededFuture());
            return mqtt;
        });
        final Future<Void> stopFuture = future();
        final MqttEventVertice mqttEvents = new MqttEventVertice(mqtt, eventCodec, timerProvider);
        mqttEvents.start(future());
        mqttEvents.stop(stopFuture);
        assertTrue(stopFuture.succeeded());
    }

    @Test public void testConnects()
    {
        new MqttEventVertice(mqtt, eventCodec, timerProvider).start(future());
        verify(mqtt).connect(eq(MqttClientOptions.DEFAULT_PORT), eq(MqttClientOptions.DEFAULT_HOST), any());
    }

    @Test public void testConnectsWithHostPort()
    {
        final MqttEventVertice mqttEvents = new MqttEventVertice(mqtt, eventCodec, timerProvider);
        mqttEvents.setHost("m-ld.org");
        mqttEvents.setPort(9000);
        mqttEvents.start(future());
        verify(mqtt).connect(eq(9000), eq("m-ld.org"), any());
    }

    @Test public void testConsumerReceivesMessage()
    {
        final MqttEventVertice mqttEvents = new MqttEventVertice(mqtt, eventCodec, timerProvider);
        mqttEvents.start(future());
        final ChannelOptions options = new ChannelOptions().setCodecName("UUID").setQuality(AT_MOST_ONCE);
        final Channel<UUID> channel = mqttEvents.channel("channel", options);
        final AtomicReference<Message<UUID>> gotMessage = new AtomicReference<>();
        channel.consumer().handler(gotMessage::set);
        final MqttPublishMessage message = mock(MqttPublishMessage.class);
        when(message.topicName()).thenReturn("channel");
        final UUID value = randomUUID();
        final Buffer body = eventCodec.encodeToWire(value, options);
        when(message.payload()).thenReturn(body);
        when(message.qosLevel()).thenReturn(MqttQoS.AT_MOST_ONCE);
        publishCaptor.getValue().handle(message);
        assertEquals(value, gotMessage.get().body());
    }

    @Test public void testConsumerUnsubscribesOnClose()
    {
        final MqttEventVertice mqttEvents = new MqttEventVertice(mqtt, eventCodec, timerProvider);
        mqttEvents.start(future());
        final ChannelOptions options = new ChannelOptions().setCodecName("UUID").setQuality(AT_MOST_ONCE);
        final Channel<UUID> channel = mqttEvents.channel("channel", options);
        final AtomicReference<Message<UUID>> gotMessage = new AtomicReference<>();
        channel.consumer().handler(gotMessage::set);
        respondToSubscriptions();
        mqttEvents.stop(future());
        verify(mqtt).unsubscribe(eq("channel"), any());
    }

    private void respondToSubscriptions()
    {
        for (int i = 0; i < subQosCaptor.getAllValues().size(); i++)
        {
            final Map<String, Integer> subQos = subQosCaptor.getAllValues().get(i);
            final Handler<AsyncResult<Integer>> subAckHandler = subSentCaptor.getAllValues().get(i);
            subAckHandler.handle(succeededFuture(i));

            final MqttSubAckMessage subAckMessage = mock(MqttSubAckMessage.class);
            when(subAckMessage.messageId()).thenReturn(i);
            when(subAckMessage.grantedQoSLevels()).thenReturn(new ArrayList<>(subQos.values()));
            subAckCaptor.getValue().handle(subAckMessage);
        }
    }

    @Test public void testWriteEventAtMostOnce()
    {
        final MqttEventVertice mqttEvents = new MqttEventVertice(mqtt, eventCodec, timerProvider);
        mqttEvents.start(future());
        final Channel<UUID> channel = mqttEvents.channel(
            "channel", new ChannelOptions().setCodecName("UUID").setQuality(AT_MOST_ONCE));
        final UUID value = randomUUID();
        channel.producer().write(value);
        verify(mqtt).publish(eq("channel"), bufferCaptor.capture(), eq(MqttQoS.AT_MOST_ONCE), eq(false), eq(false));
        assertEquals(eventCodec.decodeFromWire(bufferCaptor.getValue(), caseInsensitiveMultiMap()), value);
    }

    @Test public void testWriteEventPreStart()
    {
        final MqttEventVertice mqttEvents = new MqttEventVertice(mqtt, eventCodec, timerProvider);
        final Channel<UUID> channel = mqttEvents.channel(
            "channel", new ChannelOptions().setCodecName("UUID").setQuality(AT_MOST_ONCE));
        final UUID value = randomUUID();
        channel.producer().write(value);
        mqttEvents.start(future());
        verify(mqtt).publish(eq("channel"), bufferCaptor.capture(), eq(MqttQoS.AT_MOST_ONCE), eq(false), eq(false));
        assertEquals(eventCodec.decodeFromWire(bufferCaptor.getValue(), caseInsensitiveMultiMap()), value);
    }

    @Test public void testWriteEventAtLeastOnce()
    {
        final MqttEventVertice mqttEvents = new MqttEventVertice(mqtt, eventCodec, timerProvider);
        final Set<Object> sentMessages = new HashSet<>();
        mqttEvents.start(future());
        final Channel<UUID> channel = mqttEvents.<UUID>channel(
            "channel", new ChannelOptions().setCodecName("UUID").setQuality(AT_LEAST_ONCE))
            .producedHandler(sendResult -> sentMessages.add(sendResult.result()));
        final UUID value = randomUUID();
        final MessageProducer<UUID> producer = channel.producer();
        producer.setWriteQueueMaxSize(1).write(value);
        verify(mqtt).publish(eq("channel"), bufferCaptor.capture(), eq(MqttQoS.AT_LEAST_ONCE),
                             eq(false), eq(false), publishSentCaptor.capture());
        assertEquals(eventCodec.decodeFromWire(bufferCaptor.getValue(), caseInsensitiveMultiMap()), value);
        publishSentCaptor.getValue().handle(succeededFuture(1));
        assertFalse(sentMessages.contains(value));
        assertTrue(producer.writeQueueFull());
        publishCompleteCaptor.getValue().handle(1);
        assertFalse(producer.writeQueueFull());
        assertTrue(sentMessages.contains(value));
    }

    private Future<Void> future()
    {
        return Promise.<Void>promise().future();
    }
}
