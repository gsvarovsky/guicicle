/*
 * Copyright (c) George Svarovsky 2019. All rights reserved.
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package org.m_ld.guicicle.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.messages.MqttPublishMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.atomic.AtomicReference;

import static io.vertx.core.Future.succeededFuture;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class) public class MqttPresenceTest
{
    @Mock MqttClient mqtt;
    @Captor ArgumentCaptor<Handler<AsyncResult<Integer>>> sendHandlerCapture;

    @Before
    public void setUp()
    {
        when(mqtt.clientId()).thenReturn("client1");
    }

    @Test public void testJoin()
    {
        final AtomicReference<AsyncResult<Void>> joined = new AtomicReference<>();
        final MqttPresence presence = new MqttPresence(mqtt, "domain");
        presence.join("channel1", "address", joined::set);
        verify(mqtt).publish(eq("__presence/domain/client1/channel1"),
                             eq(Buffer.buffer("address")),
                             eq(MqttQoS.AT_LEAST_ONCE),
                             eq(false),
                             eq(true),
                             sendHandlerCapture.capture());
        sendHandlerCapture.getValue().handle(succeededFuture(1));
        presence.onProduced(1);
        assertTrue(joined.get().succeeded());
    }

    @Test public void testLeave()
    {
        new MqttPresence(mqtt, "domain").leave("channel1");
        verify(mqtt).publish(eq("__presence/domain/client1/channel1"),
                             eq(Buffer.buffer("-")),
                             eq(MqttQoS.AT_MOST_ONCE),
                             eq(false),
                             eq(true),
                             any());
    }

    @Test public void testNoConsumeRandomMessage()
    {
        final MqttPublishMessage msg = mock(MqttPublishMessage.class);
        when(msg.topicName()).thenReturn("random/stuff");
        final MqttPresence presence = new MqttPresence(mqtt, "domain");
        presence.onMessage(msg);
    }

    @Test public void testConsumeConsumerConnectedMessage()
    {
        final MqttPublishMessage msg = mock(MqttPublishMessage.class);
        when(msg.topicName()).thenReturn("__presence/domain/client1/channel1");
        when(msg.payload()).thenReturn(Buffer.buffer("address"));
        final MqttPresence presence = new MqttPresence(mqtt, "domain");
        presence.onMessage(msg);
        assertEquals(singleton("channel1"), presence.present("address"));
    }

    @Test public void testPresenceMatchesAddress()
    {
        final MqttPublishMessage msg = mock(MqttPublishMessage.class);
        when(msg.topicName()).thenReturn("__presence/domain/client1/channel1");
        when(msg.payload()).thenReturn(Buffer.buffer("#"));
        final MqttPresence presence = new MqttPresence(mqtt, "domain");
        presence.onMessage(msg);
        assertEquals(singleton("channel1"), presence.present("address"));
    }

    @Test public void testConsumeUnknownConsumerDisconnectedMessage()
    {
        final MqttPublishMessage msg = mock(MqttPublishMessage.class);
        when(msg.topicName()).thenReturn("__presence/domain/client1/channel1");
        when(msg.payload()).thenReturn(Buffer.buffer("-"));
        final MqttPresence presence = new MqttPresence(mqtt, "domain");
        presence.onMessage(msg);
        assertEquals(emptySet(), presence.present("address"));
    }

    @Test public void testConsumeKnownConsumerDisconnectedMessage()
    {
        final MqttPublishMessage msg1 = mock(MqttPublishMessage.class), msg2 = mock(MqttPublishMessage.class);
        when(msg1.topicName()).thenReturn("__presence/domain/client1/channel1");
        when(msg1.payload()).thenReturn(Buffer.buffer("address"));
        when(msg2.topicName()).thenReturn("__presence/domain/client1/channel1");
        when(msg2.payload()).thenReturn(Buffer.buffer("-"));
        final MqttPresence presence = new MqttPresence(mqtt, "domain");
        presence.onMessage(msg1);
        presence.onMessage(msg2);
        assertEquals(emptySet(), presence.present("address"));
    }

    @Test public void testConsumeClientLwtMessage()
    {
        final MqttPublishMessage msg1 = mock(MqttPublishMessage.class), msg2 = mock(MqttPublishMessage.class);
        when(msg1.topicName()).thenReturn("__presence/domain/client1/channel1");
        when(msg1.payload()).thenReturn(Buffer.buffer("address"));
        when(msg2.topicName()).thenReturn("__presence/domain/client1");
        when(msg2.payload()).thenReturn(Buffer.buffer("-"));
        final MqttPresence presence = new MqttPresence(mqtt, "domain");
        presence.onMessage(msg1);
        presence.onMessage(msg2);
        assertEquals(emptySet(), presence.present("address"));
    }
}